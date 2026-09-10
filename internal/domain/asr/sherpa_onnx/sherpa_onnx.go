package sherpa_onnx

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"xiaozhi-esp32-server-golang/constants"
	"xiaozhi-esp32-server-golang/internal/data/audio"
	"xiaozhi-esp32-server-golang/internal/domain/asr/types"
	log "xiaozhi-esp32-server-golang/logger"

	"github.com/gorilla/websocket"
)

// Config SherpaOnnx ASR 引擎配置
type Config struct {
	Host              string // asr_server 服务地址（默认 127.0.0.1）
	Port              string // asr_server 端口（默认 9000）
	SampleRate        int    // 采样率（默认 16000）
	Timeout           int    // 连接超时秒数（默认 30）
	WaitResultTimeout int    // 音频结束后等待最终结果的超时秒数（默认 3）
}

// DefaultConfig 默认配置
var DefaultConfig = Config{
	Host:              "127.0.0.1",
	Port:              "9000",
	SampleRate:        audio.SampleRate,
	Timeout:           30,
	WaitResultTimeout: 3,
}

// SherpaOnnx 实现 ASR 接口，通过 WebSocket 连接嵌入的 asr_server（sherpa-onnx SenseVoice）
type SherpaOnnx struct {
	config Config

	// 连接管理（每个 StreamingRecognize 调用建立独立连接，避免结果混淆）
	conn      *websocket.Conn
	connMutex sync.RWMutex
	// 发送锁，确保同一时间只有一个请求在使用连接
	sendMutex sync.Mutex
}

// responseMessage asr_server WebSocket 返回的 JSON 消息
type responseMessage struct {
	Type    string `json:"type"`    // connection / final / error
	Text    string `json:"text"`    // 识别文本（仅 type=final 时）
	Message string `json:"message"` // 提示/错误信息（仅 type=connection/error 时）
}

// NewSherpaOnnx 创建新的 SherpaOnnx 实例
func NewSherpaOnnx(config Config) *SherpaOnnx {
	if config.Host == "" {
		config.Host = DefaultConfig.Host
	}
	if config.Port == "" {
		config.Port = DefaultConfig.Port
	}
	if config.SampleRate == 0 {
		config.SampleRate = DefaultConfig.SampleRate
	}
	if config.Timeout == 0 {
		config.Timeout = DefaultConfig.Timeout
	}
	if config.WaitResultTimeout == 0 {
		config.WaitResultTimeout = DefaultConfig.WaitResultTimeout
	}
	return &SherpaOnnx{
		config: config,
	}
}

// getConnection 建立到 asr_server 的 WebSocket 连接
func (s *SherpaOnnx) getConnection(ctx context.Context) (*websocket.Conn, error) {
	// 先尝试读取现有连接
	s.connMutex.RLock()
	conn := s.conn
	s.connMutex.RUnlock()

	if conn != nil {
		log.Debugf("SherpaOnnx WebSocket 复用连接: conn=%p", conn)
		return conn, nil
	}

	// 需要创建新连接
	s.connMutex.Lock()
	defer s.connMutex.Unlock()

	// 双重检查
	if s.conn != nil {
		return s.conn, nil
	}

	// 创建新连接
	url := fmt.Sprintf("ws://%s:%s/ws", s.config.Host, s.config.Port)
	dialer := websocket.Dialer{
		HandshakeTimeout: time.Duration(s.config.Timeout) * time.Second,
	}

	conn, _, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		return nil, fmt.Errorf("连接到 asr_server 失败: %v", err)
	}

	// 读取连接确认消息
	var msg responseMessage
	if err := conn.ReadJSON(&msg); err != nil {
		conn.Close()
		return nil, fmt.Errorf("读取 asr_server 连接确认失败: %v", err)
	}
	if msg.Type != "connection" {
		conn.Close()
		return nil, fmt.Errorf("意外的 asr_server 连接消息: type=%s", msg.Type)
	}

	s.conn = conn
	log.Infof("SherpaOnnx WebSocket 连接已建立: conn=%p", conn)
	return conn, nil
}

// clearConnection 清空连接
func (s *SherpaOnnx) clearConnection() {
	s.connMutex.Lock()
	defer s.connMutex.Unlock()

	if s.conn != nil {
		log.Infof("SherpaOnnx WebSocket 连接已清空: conn=%p", s.conn)
		s.conn.Close()
		s.conn = nil
	}
}

// writeMessage 安全地向 WebSocket 连接写入消息
func (s *SherpaOnnx) writeMessage(conn *websocket.Conn, messageType int, data []byte) error {
	s.connMutex.RLock()
	defer s.connMutex.RUnlock()

	if conn == nil {
		return fmt.Errorf("连接已关闭")
	}
	return conn.WriteMessage(messageType, data)
}

// StreamingRecognize 实现流式识别接口
// 从 audioStream 接收音频数据，通过 resultChan 返回结果
// 可以通过 ctx 控制识别过程的取消和超时
func (s *SherpaOnnx) StreamingRecognize(ctx context.Context, audioStream <-chan []float32) (chan types.StreamingResult, error) {
	// 使用发送锁保护，确保同一时间只有一个请求在使用连接
	s.sendMutex.Lock()

	// 获取连接（每次调用建立独立连接，避免结果混淆）
	conn, err := s.getConnection(ctx)
	if err != nil {
		s.sendMutex.Unlock()
		return nil, err
	}

	subCtx, cancel := context.WithCancel(ctx)
	resultChan := make(chan types.StreamingResult, 20)

	// 在后台 goroutine 中运行流式处理，完成后释放锁
	go func() {
		defer s.sendMutex.Unlock()
		s.runStreaming(subCtx, cancel, conn, audioStream, resultChan)
	}()

	return resultChan, nil
}

// runStreaming 运行流式识别的主循环
func (s *SherpaOnnx) runStreaming(
	ctx context.Context,
	cancel context.CancelFunc,
	conn *websocket.Conn,
	audioStream <-chan []float32,
	resultChan chan types.StreamingResult,
) {
	defer func() {
		// 确保连接被清理
		s.clearConnection()
		// 关闭结果通道
		close(resultChan)
	}()

	// 启动接收 goroutine：读取识别结果
	recvDone := make(chan struct{})
	var recvFinal atomic.Bool
	go func() {
		defer close(recvDone)
		s.recvResult(ctx, conn, resultChan, &recvFinal)
	}()

	// 转发音频数据
	forwardDone := make(chan struct{})
	go func() {
		defer close(forwardDone)
		s.forwardStreamAudio(ctx, conn, audioStream)
	}()

	// 等待音频转发完成
	<-forwardDone

	// 音频已发送完毕，等待识别结果
	if ctx.Err() == nil {
		waitTimeout := time.Duration(s.config.WaitResultTimeout) * time.Second
		timer := time.NewTimer(waitTimeout)
		defer timer.Stop()

		select {
		case <-recvDone:
			// 已经收到最终结果或 goroutine 已退出
		case <-timer.C:
			// 超时仍未收到结果（可能是静音），发送空结果
			if !recvFinal.Load() {
				select {
				case resultChan <- types.StreamingResult{
					Text:        "",
					IsFinal:     true,
					AsrType:     constants.AsrTypeSherpaOnnx,
					EmptyReason: types.EmptyReasonProviderEmptyFinal,
				}:
				default:
				}
			}
		case <-ctx.Done():
		}
	}

	// 取消上下文，并关闭连接解除 recvResult 对 conn.ReadMessage 的阻塞。
	// 否则超时未收到最终结果时 recvResult 会永久阻塞在读上，导致后续
	// clearConnection() 和 sendMutex 释放都不会执行，整个 ASR 识别链路挂起。
	cancel()
	s.clearConnection()
	// 等待 recv 完成
	<-recvDone
}

// recvResult 读取并处理识别结果
func (s *SherpaOnnx) recvResult(ctx context.Context, conn *websocket.Conn, resultChan chan types.StreamingResult, recvFinal *atomic.Bool) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			// 连接关闭或读取错误，退出
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) &&
				!strings.Contains(err.Error(), "use of closed network connection") {
				log.Debugf("SherpaOnnx recvResult 读取消息失败: %v", err)
			}
			return
		}

		var msg responseMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			log.Debugf("SherpaOnnx recvResult 解析消息失败: %v, raw=%s", err, string(message))
			continue
		}

		switch msg.Type {
		case "final":
			result := types.StreamingResult{
				Text:    msg.Text,
				IsFinal: true,
				AsrType: constants.AsrTypeSherpaOnnx,
			}
			if strings.TrimSpace(msg.Text) == "" {
				result.EmptyReason = types.EmptyReasonProviderEmptyFinal
			}

			select {
			case <-ctx.Done():
				return
			case resultChan <- result:
			}

			recvFinal.Store(true)
			// 主服务 VAD 已按单句话音切分，asr_server 每段语音返回一个 final 结果；
			// 收到首个 final 即完成本轮，退出等待下一次流式调用
			return

		case "error":
			// 服务端错误
			log.Errorf("SherpaOnnx 服务端错误: %s", msg.Message)
			select {
			case <-ctx.Done():
			case resultChan <- types.StreamingResult{
				Text:    "",
				Error:   fmt.Errorf("asr_server 错误: %s", msg.Message),
				IsFinal: true,
				AsrType: constants.AsrTypeSherpaOnnx,
			}:
			}
			return

		case "connection":
			// 连接确认消息，忽略
			continue

		default:
			// 其他消息（如 stats），忽略
			continue
		}
	}
}

// forwardStreamAudio 转发音频数据到 asr_server
func (s *SherpaOnnx) forwardStreamAudio(ctx context.Context, conn *websocket.Conn, audioStream <-chan []float32) {
	for {
		select {
		case <-ctx.Done():
			return
		case pcmChunk, ok := <-audioStream:
			if !ok {
				// 音频通道已关闭，发送完毕
				return
			}

			// 将 float32 PCM 转换为 int16 字节
			audioBytes := float32SliceToInt16Bytes(pcmChunk)

			// 发送二进制消息
			if err := s.writeMessage(conn, websocket.BinaryMessage, audioBytes); err != nil {
				log.Debugf("SherpaOnnx forwardStreamAudio 发送音频失败: %v", err)
				return
			}
		}
	}
}

// Process 处理整段音频（一次性识别）
func (s *SherpaOnnx) Process(pcmData []float32) (string, error) {
	ctx := context.Background()

	// 获取连接
	conn, err := s.getConnection(ctx)
	if err != nil {
		return "", err
	}

	// 发送音频数据
	audioBytes := float32SliceToInt16Bytes(pcmData)
	if err := s.writeMessage(conn, websocket.BinaryMessage, audioBytes); err != nil {
		s.clearConnection()
		return "", fmt.Errorf("发送音频数据失败: %v", err)
	}

	// 设置读取超时
	conn.SetReadDeadline(time.Now().Add(time.Duration(s.config.Timeout) * time.Second))

	// 读取结果
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			s.clearConnection()
			return "", fmt.Errorf("读取结果失败: %v", err)
		}

		var msg responseMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			continue
		}

		if msg.Type == "final" {
			return msg.Text, nil
		}
		if msg.Type == "error" {
			s.clearConnection()
			return "", fmt.Errorf("asr_server 错误: %s", msg.Message)
		}
		// 忽略 connection 等其他消息
	}
}

// Close 关闭资源
func (s *SherpaOnnx) Close() error {
	s.clearConnection()
	return nil
}

// IsValid 检查资源是否有效
func (s *SherpaOnnx) IsValid() bool {
	s.connMutex.RLock()
	valid := s.conn != nil
	s.connMutex.RUnlock()
	return valid
}

// float32SliceToInt16Bytes 将 float32 PCM 转换为 int16 小端字节
// 输入范围 [-1.0, 1.0]，输出 16-bit 有符号整数 PCM 字节
func float32SliceToInt16Bytes(samples []float32) []byte {
	data := make([]byte, len(samples)*2)
	for i, sample := range samples {
		if sample > 1.0 {
			sample = 1.0
		} else if sample < -1.0 {
			sample = -1.0
		}
		i16 := int16(sample * 32767)
		data[i*2] = byte(i16)
		data[i*2+1] = byte(i16 >> 8)
	}
	return data
}
