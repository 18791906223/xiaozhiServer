package funasr

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"xiaozhi-esp32-server-golang/constants"
	log "xiaozhi-esp32-server-golang/logger"

	"github.com/gorilla/websocket"

	"xiaozhi-esp32-server-golang/internal/data/audio"
	"xiaozhi-esp32-server-golang/internal/domain/asr/types"
)

// FunasrConfig 配置结构体
type FunasrConfig struct {
	Host          string // FunASR 服务主机地址
	Port          string // FunASR 服务端口
	Mode          string // 识别模式，如 "online"
	SampleRate    int    // 采样率
	ChunkSize     []int  // 分块大小
	ChunkInterval int    // 分块间隔
	Timeout       int    // 连接超时时间（秒）
	AutoEnd       bool   // 是否超时 xx ms自动结束，不依赖 isSpeaking为false
}

// DefaultConfig 默认配置
var DefaultConfig = FunasrConfig{
	Host:          "localhost",
	Port:          "10095",
	Mode:          "online",
	SampleRate:    audio.SampleRate,
	ChunkInterval: 10,
	ChunkSize:     []int{5, 10, 5},
	Timeout:       30,
}

// Funasr 实现ASR接口
type Funasr struct {
	config FunasrConfig

	// 连接管理
	conn      *websocket.Conn
	connMutex sync.RWMutex
	// 发送锁，确保同一时间只有一个请求在使用连接
	sendMutex sync.Mutex
}

var funasrStreamSeq atomic.Uint64
var funasrStreamPrefix = uuid.NewString()

type streamDebugState struct {
	audioChunkCount  atomic.Uint64
	audioSampleCount atomic.Uint64
}

// FunasrRequest FunASR WebSocket请求结构体
type FunasrRequest struct {
	Mode          string `json:"mode,omitempty"`           // 识别模式，如 "online"
	ChunkSize     []int  `json:"chunk_size,omitempty"`     // 分块大小
	ChunkInterval int    `json:"chunk_interval,omitempty"` // 分块间隔
	AudioFs       int    `json:"audio_fs,omitempty"`       // 采样率
	WavName       string `json:"wav_name,omitempty"`       // 音频名称
	WavFormat     string `json:"wav_format,omitempty"`     // 音频格式
	IsSpeaking    bool   `json:"is_speaking"`              // 是否在说话
	Hotwords      string `json:"hotwords,omitempty"`       // 热词
	Itn           bool   `json:"itn,omitempty"`            // 是否进行文本规整
}

// FunasrResponse FunASR WebSocket响应结构体
type FunasrResponse struct {
	Text       string  `json:"text"`       // 识别的文本
	IsFinal    bool    `json:"is_final"`   // 是否为最终结果
	WavName    string  `json:"wav_name"`   // 音频名称
	TimeStamp  string  `json:"timestamp"`  // 时间戳
	Mode       string  `json:"mode"`       // 模式
	Confidence float64 `json:"confidence"` // 置信度
}

// NewFunasr 创建一个新的Funasr实例
func NewFunasr(config FunasrConfig) (*Funasr, error) {
	if config.Host == "" {
		config = DefaultConfig
	}

	return &Funasr{
		config: config,
	}, nil
}

// getConnection 获取连接，如果不存在则创建
func (f *Funasr) getConnection(ctx context.Context) (*websocket.Conn, error) {
	// 先尝试读取现有连接
	f.connMutex.RLock()
	conn := f.conn
	f.connMutex.RUnlock()

	if conn != nil {
		log.Debugf("FunASR WebSocket 复用连接: conn=%p", conn)
		return conn, nil
	}

	// 需要创建新连接
	f.connMutex.Lock()
	defer f.connMutex.Unlock()

	// 双重检查，可能其他 goroutine 已经创建了连接
	if f.conn != nil {
		return f.conn, nil
	}

	// 创建新连接
	url := fmt.Sprintf("ws://%s:%s/", f.config.Host, f.config.Port)
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		return nil, fmt.Errorf("连接到FunASR服务失败: %v", err)
	}

	f.conn = conn
	log.Infof("FunASR WebSocket 连接已建立: conn=%p", conn)
	return conn, nil
}

// clearConnection 清空连接（用于断线重连）
func (f *Funasr) clearConnection() {
	f.connMutex.Lock()
	defer f.connMutex.Unlock()

	if f.conn != nil {
		log.Infof("FunASR WebSocket 连接已清空: conn=%p", f.conn)
		f.conn.Close()
		f.conn = nil
	}
}

// StreamingResult 流式识别结果
type StreamingResult struct {
	Text    string // 识别的文本
	IsFinal bool   // 是否为最终结果
}

// isTimeoutError 判断是否为超时错误
func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}

	// 检查是否为网络超时错误
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}

	// 检查错误消息中是否包含超时关键词
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "timeout") || strings.Contains(errMsg, "i/o timeout")
}

// isConnectionClosedError 判断是否为连接关闭错误
func isConnectionClosedError(err error) bool {
	if err == nil {
		return false
	}

	// 检查是否为 WebSocket 关闭错误
	if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway,
		websocket.CloseAbnormalClosure, websocket.CloseNoStatusReceived) {
		return true
	}

	// 检查错误消息中是否包含连接关闭关键词
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "connection closed") ||
		strings.Contains(errMsg, "broken pipe") ||
		strings.Contains(errMsg, "connection reset") ||
		strings.Contains(errMsg, "use of closed network connection")
}

// writeMessage 安全地向 WebSocket 连接写入消息
func (f *Funasr) writeMessage(conn *websocket.Conn, messageType int, data []byte) error {
	// 使用读锁保护连接写入操作，防止并发写入导致数据混乱
	f.connMutex.RLock()
	defer f.connMutex.RUnlock()

	// 检查连接是否有效
	if conn == nil {
		return fmt.Errorf("连接已关闭")
	}

	return conn.WriteMessage(messageType, data)
}

// StreamingRecognize 实现流式识别
// 从audioStream接收音频数据，通过resultChan返回结果
// 可以通过ctx控制识别过程的取消和超时
func (f *Funasr) StreamingRecognize(ctx context.Context, audioStream <-chan []float32) (chan types.StreamingResult, error) {
	// 使用发送锁保护，确保同一时间只有一个请求在使用连接
	f.sendMutex.Lock()
	// 注意：不在函数返回时释放锁，而是在 goroutine 完成时释放

	// 获取连接（复用或创建）
	conn, err := f.getConnection(ctx)
	if err != nil {
		f.sendMutex.Unlock() // 获取连接失败时立即释放锁
		return nil, err
	}

	subCtx, cancelFunc := context.WithCancel(ctx)
	streamID := fmt.Sprintf("funasr-stream-%s-%d", funasrStreamPrefix, funasrStreamSeq.Add(1))
	wavName := streamID
	debugState := &streamDebugState{}

	// 发送初始消息
	firstMessage := FunasrRequest{
		Mode:          f.config.Mode,
		ChunkSize:     []int{5, 10, 5},
		ChunkInterval: f.config.ChunkInterval,
		AudioFs:       f.config.SampleRate,
		WavName:       wavName,
		WavFormat:     "pcm",
		IsSpeaking:    true,
		Hotwords:      "{\"阿里巴巴\":20,\"hello world\":40}",
		Itn:           true,
	}

	log.Debugf(
		"funasr StreamingRecognize 开始: stream_id=%s, conn=%p, mode=%s, chunk_interval=%d, chunk_size=%v, wav_name=%s",
		streamID,
		conn,
		f.config.Mode,
		f.config.ChunkInterval,
		firstMessage.ChunkSize,
		firstMessage.WavName,
	)

	messageBytes, err := json.Marshal(firstMessage)
	if err != nil {
		cancelFunc()
		f.sendMutex.Unlock() // 序列化失败时立即释放锁
		return nil, fmt.Errorf("序列化初始消息失败: %v", err)
	}

	err = f.writeMessage(conn, websocket.TextMessage, messageBytes)
	if err != nil {
		// 发送失败，清空连接，下次使用时自动重连
		log.Errorf("发送初始消息失败: %v，清空连接", err)
		f.clearConnection()
		cancelFunc()
		f.sendMutex.Unlock() // 发送失败时立即释放锁
		return nil, fmt.Errorf("发送初始消息失败: %v", err)
	}

	// 创建结果通道，带缓冲避免阻塞
	resultChan := make(chan types.StreamingResult, 20)

	// 使用 WaitGroup 等待两个 goroutine 完成
	var wg sync.WaitGroup
	wg.Add(2)

	// 启动goroutine接收和发送数据
	// 在 goroutine 完成时释放锁
	go func() {
		defer wg.Done()
		f.recvResult(subCtx, conn, streamID, wavName, debugState, resultChan)
	}()

	go func() {
		defer wg.Done()
		f.forwardStreamAudio(subCtx, cancelFunc, conn, streamID, wavName, debugState, audioStream)
	}()

	// 在后台等待 goroutine 完成并释放锁
	go func() {
		wg.Wait()
		f.clearConnection()
		f.sendMutex.Unlock()
		log.Debugf(
			"funasr StreamingRecognize goroutine 完成，已释放 sendMutex: stream_id=%s, wav_name=%s, chunks=%d, samples=%d",
			streamID,
			wavName,
			debugState.audioChunkCount.Load(),
			debugState.audioSampleCount.Load(),
		)
	}()

	return resultChan, nil
}

func (f *Funasr) recvResult(ctx context.Context, conn *websocket.Conn, streamID string, wavName string, debugState *streamDebugState, resultChan chan types.StreamingResult) {
	defer func() {
		close(resultChan)
	}()

	for {
		select {
		case <-ctx.Done():
			// 上下文取消，退出goroutine
			log.Debugf("funasr recvResult 已取消: %v", ctx.Err())
			return
		default:
			// 继续正常处理
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Debugf("funasr recvResult 读取识别结果失败: stream_id=%s, conn=%p, err=%v，清空连接", streamID, conn, err)
			// 读取失败，清空连接，下次使用时自动重连
			f.clearConnection()
			return
		}
		log.Debugf(
			"funasr recvResult 读取识别结果: stream_id=%s, conn=%p, chunks=%d, samples=%d, payload=%v",
			streamID,
			conn,
			debugState.audioChunkCount.Load(),
			debugState.audioSampleCount.Load(),
			string(message),
		)

		var response FunasrResponse
		err = json.Unmarshal(message, &response)
		if err != nil {
			log.Debugf("funasr recvResult 解析识别结果失败: %v", err)
			continue
		}

		if response.WavName != "" && response.WavName != wavName {
			log.Warnf(
				"funasr recvResult 忽略非当前流结果: stream_id=%s, expected_wav=%s, actual_wav=%s, conn=%p, chunks=%d, samples=%d",
				streamID,
				wavName,
				response.WavName,
				conn,
				debugState.audioChunkCount.Load(),
				debugState.audioSampleCount.Load(),
			)
			continue
		}

		// 只有有文本时才发送结果
		/*if response.Text == "" {
			continue
		}*/

		streamingResult := f.toStreamingResult(response)

		// 发送识别结果
		select {
		case <-ctx.Done():
			// 上下文取消，退出goroutine
			log.Debugf("funasr recvResult 已取消: %v", ctx.Err())
			return
		case resultChan <- streamingResult:
		}
		/*if f.config.AutoEnd {
			log.Debugf("funasr recvResult autoend")
			return
		}*/
		// 结果发送成功
		// 如果是最终结果且输入已结束，则退出循环
		if streamingResult.IsFinal {
			log.Debugf(
				"funasr recvResult isfinal: stream_id=%s, conn=%p, response_mode=%s, raw_is_final=%v, text_len=%d, wav_name=%s, chunks=%d, samples=%d",
				streamID,
				conn,
				response.Mode,
				response.IsFinal,
				len([]rune(response.Text)),
				response.WavName,
				debugState.audioChunkCount.Load(),
				debugState.audioSampleCount.Load(),
			)
			return
		}
	}
}

func (f *Funasr) toStreamingResult(response FunasrResponse) types.StreamingResult {
	result := types.StreamingResult{
		Text:    response.Text,
		IsFinal: response.IsFinal,
		AsrType: constants.AsrTypeFunAsr,
		Mode:    response.Mode,
	}

	if strings.EqualFold(strings.TrimSpace(f.config.Mode), "2pass") {
		switch strings.ToLower(strings.TrimSpace(response.Mode)) {
		case "2pass-online":
			result.IsFinal = false
		case "2pass-offline":
			result.IsFinal = true
		}
	}

	if result.IsFinal && strings.TrimSpace(result.Text) == "" {
		result.EmptyReason = types.EmptyReasonProviderEmptyFinal
	}

	return result
}

func (f *Funasr) forwardStreamAudio(ctx context.Context, cancelFunc context.CancelFunc, conn *websocket.Conn, streamID string, wavName string, debugState *streamDebugState, audioStream <-chan []float32) {
	sendEndMsg := func() {
		// 发送终止消息
		endMessage := FunasrRequest{
			Mode:          f.config.Mode,
			ChunkInterval: f.config.ChunkInterval,
			ChunkSize:     []int{5, 10, 5},
			WavName:       wavName,
			IsSpeaking:    false,
		}
		endMessageBytes, _ := json.Marshal(endMessage)
		log.Debugf(
			"funasr forwardStreamAudio 发送结束消息: stream_id=%s, conn=%p, chunks=%d, samples=%d, payload=%v",
			streamID,
			conn,
			debugState.audioChunkCount.Load(),
			debugState.audioSampleCount.Load(),
			string(endMessageBytes),
		)
		err := f.writeMessage(conn, websocket.TextMessage, endMessageBytes)
		if err != nil {
			log.Debugf("funasr forwardStreamAudio 发送结束消息失败: stream_id=%s, conn=%p, err=%v，清空连接", streamID, conn, err)
			f.clearConnection()
		}
	}
	// 处理输入音频流
	for {
		select {
		case <-ctx.Done():
			// 上下文取消，发送结束消息并退出
			log.Debugf(
				"funasr forwardStreamAudio 上下文已取消: stream_id=%s, conn=%p, chunks=%d, samples=%d, err=%v",
				streamID,
				conn,
				debugState.audioChunkCount.Load(),
				debugState.audioSampleCount.Load(),
				ctx.Err(),
			)
			// 注意：这里不需要调用 cancelFunc()，因为 ctx.Done() 已经被触发说明上下文已取消
			sendEndMsg()
			return
		case pcmChunk, ok := <-audioStream:
			if !ok {
				// 通道已关闭，结束输入，需要通知接收goroutine停止
				log.Debugf(
					"funasr forwardStreamAudio 音频通道关闭: stream_id=%s, conn=%p, chunks=%d, samples=%d",
					streamID,
					conn,
					debugState.audioChunkCount.Load(),
					debugState.audioSampleCount.Load(),
				)
				sendEndMsg()
				return
			}

			// 转换PCM数据为字节
			audioBytes := Float32SliceToBytes(pcmChunk)

			//log.Debugf("funasr forwardStreamAudio 发送音频数据, pcmChunk len: %v, audioBytes len: %v", len(pcmChunk), len(audioBytes))

			// 发送音频数据
			err := f.writeMessage(conn, websocket.BinaryMessage, audioBytes)
			if err != nil {
				log.Debugf("funasr forwardStreamAudio 发送音频数据失败: stream_id=%s, conn=%p, err=%v，清空连接", streamID, conn, err)
				f.clearConnection()
				cancelFunc() // 发送失败时取消上下文，通知 recvResult goroutine 停止
				return
			}
			chunkCount := debugState.audioChunkCount.Add(1)
			sampleCount := debugState.audioSampleCount.Add(uint64(len(pcmChunk)))
			if chunkCount <= 3 || chunkCount%10 == 0 {
				log.Debugf(
					"funasr forwardStreamAudio 已发送音频块: stream_id=%s, conn=%p, chunk=%d, chunk_samples=%d, total_samples=%d, bytes=%d",
					streamID,
					conn,
					chunkCount,
					len(pcmChunk),
					sampleCount,
					len(audioBytes),
				)
			}
		}
	}
}

// Process 处理音频数据并返回识别结果
func (f *Funasr) Process(pcmData []float32) (string, error) {
	ctx := context.Background()

	// 使用发送锁保护，确保同一时间只有一个请求在使用连接
	f.sendMutex.Lock()
	defer f.sendMutex.Unlock()

	// 获取连接（复用或创建）
	conn, err := f.getConnection(ctx)
	if err != nil {
		return "", err
	}

	audioBytes := Float32SliceToBytes(pcmData)

	// 发送初始消息
	firstMessage := FunasrRequest{
		Mode:          f.config.Mode,
		ChunkSize:     []int{5, 10, 5},
		ChunkInterval: f.config.ChunkInterval,
		AudioFs:       f.config.SampleRate,
		WavName:       "stream",
		WavFormat:     "pcm",
		IsSpeaking:    true,
		Hotwords:      "",
		Itn:           true,
	}

	messageBytes, err := json.Marshal(firstMessage)
	if err != nil {
		return "", fmt.Errorf("序列化初始消息失败: %v", err)
	}

	err = f.writeMessage(conn, websocket.TextMessage, messageBytes)
	if err != nil {
		// 发送失败，清空连接，下次使用时自动重连
		log.Errorf("发送初始消息失败: %v，清空连接", err)
		f.clearConnection()
		return "", fmt.Errorf("发送初始消息失败: %v", err)
	}

	// 将音频数据按块发送
	chunkSize := int(audio.SampleRate * 0.1) // 每块大小约100ms的音频 (16000 * 0.1)
	for i := 0; i < len(audioBytes); i += chunkSize {
		end := i + chunkSize
		if end > len(audioBytes) {
			end = len(audioBytes)
		}
		chunk := audioBytes[i:end]

		err = f.writeMessage(conn, websocket.BinaryMessage, chunk)
		if err != nil {
			// 发送失败，清空连接，下次使用时自动重连
			log.Errorf("发送音频数据失败: %v，清空连接", err)
			f.clearConnection()
			return "", fmt.Errorf("发送音频数据失败: %v", err)
		}
	}

	// 发送终止消息
	endMessage := FunasrRequest{
		IsSpeaking: false,
	}
	endMessageBytes, _ := json.Marshal(endMessage)
	err = f.writeMessage(conn, websocket.TextMessage, endMessageBytes)
	if err != nil {
		// 发送失败，清空连接，下次使用时自动重连
		log.Errorf("发送终止消息失败: %v，清空连接", err)
		f.clearConnection()
		return "", fmt.Errorf("发送终止消息失败: %v", err)
	}

	// 设置读取超时
	conn.SetReadDeadline(time.Now().Add(time.Duration(f.config.Timeout) * time.Second))

	// 读取结果
	var result string
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			if isTimeoutError(err) {
				log.Debugf("funasr Process 读取结果超时: %v", err)
				f.clearConnection() // 读取超时，清空连接
				return "", fmt.Errorf("读取结果超时: %v", err)
			}
			if isConnectionClosedError(err) {
				log.Debugf("funasr Process 读取结果连接已关闭: %v", err)
				f.clearConnection() // 连接已关闭，清空连接
				return "", fmt.Errorf("连接已关闭: %v", err)
			}
			// 读取失败，清空连接，下次使用时自动重连
			log.Errorf("funasr Process 读取结果失败: %v，清空连接", err)
			f.clearConnection()
			return "", fmt.Errorf("读取结果失败: %v", err)
		}

		var response FunasrResponse
		err = json.Unmarshal(message, &response)
		if err != nil {
			continue
		}

		// 检查是否为最终结果
		if response.IsFinal {
			result = response.Text
			break
		}
	}

	return result, nil
}

func Float32ToInt16(sample float32) int16 {
	// 限制在 [-1, 1]，避免溢出
	if sample > 1.0 {
		sample = 1.0
	} else if sample < -1.0 {
		sample = -1.0
	}
	return int16(sample * 32767)
}

func Float32SliceToBytes(samples []float32) []byte {
	data := make([]byte, len(samples)*2)
	for i, s := range samples {
		i16 := Float32ToInt16(s)
		data[2*i] = byte(i16)
		data[2*i+1] = byte(i16 >> 8)
	}
	return data
}

// Close 关闭资源，释放连接
func (f *Funasr) Close() error {
	f.clearConnection()
	return nil
}

// IsValid 检查资源是否有效
func (f *Funasr) IsValid() bool {
	f.connMutex.RLock()
	conn := f.conn
	f.connMutex.RUnlock()
	return conn != nil
}

/*
错误类型判断使用示例：

1. 超时错误判断：
   if isTimeoutError(err) {
       // 处理超时情况，可能需要重试或调整超时时间
       log.Warnf("操作超时: %v", err)
   }

2. 连接关闭错误判断：
   if isConnectionClosedError(err) {
       // 处理连接关闭情况，可能需要重新建立连接
       log.Warnf("连接已关闭: %v", err)
   }

3. 综合错误处理：
   _, message, err := conn.ReadMessage()
   if err != nil {
       if isTimeoutError(err) {
           // 超时：可能是网络延迟或服务器响应慢
           // 建议：调整超时时间或重试
       } else if isConnectionClosedError(err) {
           // 连接关闭：可能是服务器主动断开或网络中断
           // 建议：重新建立连接
       } else {
           // 其他错误：可能是协议错误或数据格式错误
           // 建议：检查数据格式或协议实现
       }
   }

常见错误类型：
- 超时错误：i/o timeout, context deadline exceeded
- 连接关闭：connection closed, broken pipe, connection reset
- WebSocket关闭：close 1000 (normal), close 1001 (going away)
*/
