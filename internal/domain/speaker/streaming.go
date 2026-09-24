package speaker

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"sync"
	"time"

	log "xiaozhi-esp32-server-golang/logger"

	"github.com/gorilla/websocket"
)

// StreamingClient WebSocket 流式识别客户端
type StreamingClient struct {
	wsURL      string
	conn       *websocket.Conn
	sampleRate int
	mutex      sync.Mutex
	writeMu    sync.Mutex
	peekMu     sync.Mutex
	finishWait chan finishResponse
	peekWaits  map[string]chan peekResponse
	lastPeekAt time.Time
}

type finishResponse struct {
	result *IdentifyResult
	err    error
}

type peekResponse struct {
	result    *IdentifyResult
	throttled bool
	err       error
}

// NewStreamingClient 创建流式识别客户端
func NewStreamingClient(baseURL string) *StreamingClient {
	wsURL := deriveWebSocketURL(baseURL)
	return &StreamingClient{
		wsURL: wsURL,
	}
}

// deriveWebSocketURL 从 HTTP base_url 推导 WebSocket URL
func deriveWebSocketURL(baseURL string) string {
	u, err := url.Parse(baseURL)
	if err != nil {
		log.Errorf("解析 base_url 失败: %v, 使用默认值", err)
		return "ws://localhost:8080/api/v1/speaker/identify_ws"
	}

	scheme := "ws"
	if u.Scheme == "https" {
		scheme = "wss"
	}

	return fmt.Sprintf("%s://%s/api/v1/speaker/identify_ws", scheme, u.Host)
}

// Connect 连接到声纹识别服务的 WebSocket
func (sc *StreamingClient) Connect(sampleRate int, agentId string, threshold float32) error {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	sc.sampleRate = sampleRate

	// 如果已存在连接，使用 Ping 检测连接是否仍然有效
	if sc.conn != nil {
		if sc.pingConnectionLocked() {
			// 连接有效，复用现有连接
			return nil
		}
		// 连接已断开，关闭旧连接准备重连
		log.Debugf("检测到旧连接已断开，将重新建立连接")
		sc.closeConnectionLocked()
	}

	// 构建 WebSocket URL，包含采样率、agent_id 和 threshold 参数
	wsURL := fmt.Sprintf("%s?sample_rate=%d", sc.wsURL, sampleRate)
	if agentId != "" {
		wsURL += fmt.Sprintf("&agent_id=%s", url.QueryEscape(agentId))
	}
	// 如果阈值大于 0，则传递阈值参数
	if threshold > 0 {
		wsURL += fmt.Sprintf("&threshold=%.6f", threshold)
	}

	// 连接 WebSocket
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		return fmt.Errorf("WebSocket 连接失败: %v", err)
	}

	sc.conn = conn
	sc.finishWait = nil
	sc.peekWaits = make(map[string]chan peekResponse)

	// 设置读取超时
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))

	// 接收连接确认消息
	var connectionMsg map[string]interface{}
	if err := conn.ReadJSON(&connectionMsg); err != nil {
		conn.Close()
		sc.conn = nil
		return fmt.Errorf("读取连接确认消息失败: %v", err)
	}

	if msgType, ok := connectionMsg["type"].(string); !ok || msgType != "connection" {
		conn.Close()
		sc.conn = nil
		return fmt.Errorf("意外的连接消息: %v", connectionMsg)
	}
	conn.SetReadDeadline(time.Time{})

	log.Debugf("声纹识别 WebSocket 连接成功，采样率: %d Hz, agent_id: %s, 阈值: %.4f", sampleRate, agentId, threshold)
	go sc.readLoop(conn)
	return nil
}

// SendAudioChunk 发送音频数据块
func (sc *StreamingClient) SendAudioChunk(audioData []float32) error {
	conn := sc.getConn()
	if conn == nil {
		return fmt.Errorf("not connected")
	}

	// 将 float32 数组转换为二进制字节
	chunkBytes := float32ToBytes(audioData)

	// 发送二进制消息
	sc.writeMu.Lock()
	err := conn.WriteMessage(websocket.BinaryMessage, chunkBytes)
	sc.writeMu.Unlock()
	if err != nil {
		// 发送失败时关闭连接
		sc.failConnection(conn, fmt.Errorf("发送音频数据失败: %v", err))
		return fmt.Errorf("发送音频数据失败: %v", err)
	}

	return nil
}

// FinishAndIdentify 完成输入并获取识别结果
func (sc *StreamingClient) FinishAndIdentify(ctx context.Context) (*IdentifyResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	resultCh := make(chan finishResponse, 1)

	sc.mutex.Lock()
	if sc.conn == nil {
		sc.mutex.Unlock()
		return nil, fmt.Errorf("not connected")
	}
	if sc.finishWait != nil {
		sc.mutex.Unlock()
		return nil, fmt.Errorf("finish already in progress")
	}
	sc.finishWait = resultCh
	conn := sc.conn
	sc.mutex.Unlock()

	// 发送完成命令
	finishCmd := map[string]interface{}{
		"action": "finish",
	}
	sc.writeMu.Lock()
	err := conn.WriteJSON(finishCmd)
	sc.writeMu.Unlock()
	if err != nil {
		sc.clearFinishWait(resultCh)
		sc.failConnection(conn, fmt.Errorf("发送完成命令失败: %v", err))
		return nil, fmt.Errorf("发送完成命令失败: %v", err)
	}

	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()

	select {
	case resp := <-resultCh:
		return resp.result, resp.err
	case <-ctx.Done():
		sc.clearFinishWait(resultCh)
		return nil, ctx.Err()
	case <-timer.C:
		sc.clearFinishWait(resultCh)
		return nil, fmt.Errorf("等待最终识别结果超时")
	}
}

// PeekAndIdentify 获取中间识别结果（不结束当前轮次）
// 返回: 识别结果, 是否被服务端防抖, 错误
func (sc *StreamingClient) PeekAndIdentify(ctx context.Context, requestID string) (*IdentifyResult, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if requestID == "" {
		requestID = fmt.Sprintf("peek_%d", time.Now().UnixNano())
	}

	sc.peekMu.Lock()
	peekStarted := false
	defer func() {
		if peekStarted {
			sc.lastPeekAt = time.Now()
		}
		sc.peekMu.Unlock()
	}()
	if !sc.lastPeekAt.IsZero() && time.Since(sc.lastPeekAt) < 200*time.Millisecond {
		return nil, true, nil
	}
	peekStarted = true

	respCh := make(chan peekResponse, 1)

	sc.mutex.Lock()
	if sc.conn == nil {
		sc.mutex.Unlock()
		return nil, false, fmt.Errorf("not connected")
	}
	if sc.peekWaits == nil {
		sc.peekWaits = make(map[string]chan peekResponse)
	}
	sc.peekWaits[requestID] = respCh
	conn := sc.conn
	sc.mutex.Unlock()

	peekCmd := map[string]interface{}{
		"action": "peek",
	}
	peekCmd["request_id"] = requestID
	sc.writeMu.Lock()
	err := conn.WriteJSON(peekCmd)
	sc.writeMu.Unlock()
	if err != nil {
		sc.removePeekWait(requestID, respCh)
		sc.failConnection(conn, fmt.Errorf("发送peek命令失败: %v", err))
		return nil, false, fmt.Errorf("发送peek命令失败: %v", err)
	}

	timer := time.NewTimer(1500 * time.Millisecond)
	defer timer.Stop()

	select {
	case resp := <-respCh:
		return resp.result, resp.throttled, resp.err
	case <-ctx.Done():
		sc.removePeekWait(requestID, respCh)
		return nil, false, ctx.Err()
	case <-timer.C:
		sc.removePeekWait(requestID, respCh)
		return nil, false, fmt.Errorf("等待peek结果超时")
	}
}

// Close 关闭连接
func (sc *StreamingClient) Close() error {
	sc.mutex.Lock()
	conn := sc.conn
	sc.conn = nil
	finishWait, peekWaits := sc.takePendingLocked()
	sc.mutex.Unlock()

	if conn != nil {
		if err := conn.Close(); err != nil {
			sc.signalPending(finishWait, peekWaits, fmt.Errorf("连接已关闭: %v", err))
			return err
		}
	}
	sc.signalPending(finishWait, peekWaits, fmt.Errorf("连接已关闭"))
	return nil
}

// closeConnectionLocked 关闭连接（必须在已持有 mutex 的情况下调用）
func (sc *StreamingClient) closeConnectionLocked() error {
	if sc.conn != nil {
		err := sc.conn.Close()
		sc.conn = nil
		return err
	}
	return nil
}

// IsConnected 检查是否已连接
func (sc *StreamingClient) IsConnected() bool {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	return sc.conn != nil
}

// pingConnectionLocked 使用 Ping 检测连接是否有效（必须在已持有 mutex 的情况下调用）
func (sc *StreamingClient) pingConnectionLocked() bool {
	if sc.conn == nil {
		return false
	}

	// 使用 Ping 消息检测连接活性
	sc.writeMu.Lock()
	sc.conn.SetWriteDeadline(time.Now().Add(1000 * time.Millisecond))
	err := sc.conn.WriteMessage(websocket.PingMessage, nil)
	sc.conn.SetWriteDeadline(time.Time{})
	sc.writeMu.Unlock()

	return err == nil
}

func (sc *StreamingClient) getConn() *websocket.Conn {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	return sc.conn
}

func (sc *StreamingClient) clearFinishWait(waitCh chan finishResponse) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	if sc.finishWait == waitCh {
		sc.finishWait = nil
	}
}

func (sc *StreamingClient) removePeekWait(requestID string, waitCh chan peekResponse) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	if existing, ok := sc.peekWaits[requestID]; ok && existing == waitCh {
		delete(sc.peekWaits, requestID)
	}
}

func (sc *StreamingClient) takePendingLocked() (chan finishResponse, []chan peekResponse) {
	finishWait := sc.finishWait
	sc.finishWait = nil

	peekWaits := make([]chan peekResponse, 0, len(sc.peekWaits))
	for requestID, waitCh := range sc.peekWaits {
		peekWaits = append(peekWaits, waitCh)
		delete(sc.peekWaits, requestID)
	}
	return finishWait, peekWaits
}

func (sc *StreamingClient) signalPending(finishWait chan finishResponse, peekWaits []chan peekResponse, err error) {
	if finishWait != nil {
		select {
		case finishWait <- finishResponse{err: err}:
		default:
		}
	}
	for _, waitCh := range peekWaits {
		if waitCh == nil {
			continue
		}
		select {
		case waitCh <- peekResponse{err: err}:
		default:
		}
	}
}

func (sc *StreamingClient) failConnection(conn *websocket.Conn, err error) {
	sc.mutex.Lock()
	if sc.conn != conn {
		sc.mutex.Unlock()
		return
	}
	_ = sc.closeConnectionLocked()
	finishWait, peekWaits := sc.takePendingLocked()
	sc.mutex.Unlock()
	sc.signalPending(finishWait, peekWaits, err)
}

func (sc *StreamingClient) readLoop(conn *websocket.Conn) {
	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			sc.failConnection(conn, fmt.Errorf("读取消息失败: %v", err))
			return
		}
		if messageType != websocket.TextMessage {
			continue
		}

		var msg map[string]interface{}
		if err := json.Unmarshal(message, &msg); err != nil {
			log.Warnf("解析声纹消息失败: %v", err)
			continue
		}

		if !sc.dispatchMessage(msg) {
			sc.failConnection(conn, parseServerError(msg))
			return
		}
	}
}

func (sc *StreamingClient) dispatchMessage(msg map[string]interface{}) bool {
	msgType, _ := msg["type"].(string)
	switch msgType {
	case "partial_result":
		requestID := getString(msg, "request_id")
		throttled := getBool(msg, "throttled")

		sc.mutex.Lock()
		waitCh := sc.peekWaits[requestID]
		if waitCh != nil {
			delete(sc.peekWaits, requestID)
		}
		sc.mutex.Unlock()
		if waitCh == nil {
			return true
		}

		var result *IdentifyResult
		if resultData, ok := msg["result"].(map[string]interface{}); ok && resultData != nil {
			result = identifyResultFromMap(resultData)
		}
		select {
		case waitCh <- peekResponse{result: result, throttled: throttled}:
		default:
		}
		return true
	case "result":
		sc.mutex.Lock()
		waitCh := sc.finishWait
		sc.finishWait = nil
		sc.mutex.Unlock()
		if waitCh == nil {
			return true
		}

		var result *IdentifyResult
		if resultData, ok := msg["result"].(map[string]interface{}); ok && resultData != nil {
			result = identifyResultFromMap(resultData)
		}
		select {
		case waitCh <- finishResponse{result: result}:
		default:
		}
		return true
	case "error":
		return false
	default:
		// audio_received/connection/ready/cancelled/closing 等消息仅用于状态提示，这里直接忽略
		return true
	}
}

func parseServerError(msg map[string]interface{}) error {
	if errMsg, ok := msg["message"].(string); ok && errMsg != "" {
		return fmt.Errorf("服务器错误: %s", errMsg)
	}
	return fmt.Errorf("服务器错误: %v", msg)
}

// float32ToBytes 将 float32 数组转换为二进制字节（小端序）
func float32ToBytes(samples []float32) []byte {
	buf := make([]byte, len(samples)*4)
	for i, sample := range samples {
		bits := math.Float32bits(sample)
		binary.LittleEndian.PutUint32(buf[i*4:], bits)
	}
	return buf
}

// 辅助函数：从 map 中安全获取值
func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func getBool(m map[string]interface{}, key string) bool {
	if v, ok := m[key].(bool); ok {
		return v
	}
	return false
}

func getFloat32(m map[string]interface{}, key string) float32 {
	if v, ok := m[key].(float64); ok {
		return float32(v)
	}
	return 0.0
}

func identifyResultFromMap(resultData map[string]interface{}) *IdentifyResult {
	return &IdentifyResult{
		Identified:  getBool(resultData, "identified"),
		SpeakerID:   getString(resultData, "speaker_id"),
		SpeakerName: getString(resultData, "speaker_name"),
		Confidence:  getFloat32(resultData, "confidence"),
		Threshold:   getFloat32(resultData, "threshold"),
	}
}
