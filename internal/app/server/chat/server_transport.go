// 文件：server_transport.go
// 职责：把底层连接（websocket / mqtt+udp）再包一层，作为「协议消息发送的唯一出口」。
// 设备通信协议里每一种下行消息（hello / tts / stt / iot / mcp / goodbye / speak_request）
// 都在这里被组装成 ServerMessage JSON 后交给底层连接发出。
//
// 为什么要在 transport 之上再包一层：
//  1. 统一构造协议 JSON：调用点只需要传业务参数（文本、音频帧），不必关心 JSON 字段名、
//     session_id 从哪来、state 该用哪个常量，协议细节集中在一处，改协议不会牵动全局。
//  2. 顺带维护 ClientState 状态：发 tts 相关消息时同步更新 IsTtsStart / Status / 欢迎语标记，
//     让「当前是播报中还是可监听」这类判断只有一个写入来源，避免各处各写一遍而不同步。
//  3. 为 MCP 收包提供缓冲 channel：MQTT/UDP 场景下 MCP 响应是异步到达的，
//     用 McpRecvMsgChan 把「回调式收包」转成「阻塞式接收」，供 MCP 客户端消费。
//
// 核心类型：ServerTransport。
// 在流程中的位置：位于 ChatManager / ChatSession / TTSManager 之下，
// 上层所有「要发消息给设备」的动作最终都会落到这里的某个 Send* 方法。
// 与哪些文件配合：
//   - chat.go：ChatManager 持有它，握手时发 hello，断连清理时调用 Close/CloseWithoutTransport，
//     并用 HasActiveUDPBinding / GetUDPLastActiveTs 判断「热链路」。
//   - tts.go：runSenderLoop 通过它发 tts start/stop、sentence_start/end 以及音频帧。
//   - mcp.go：McpTransport 把 MCP 收发转发到这里。
//   - internal/data/client（ClientState）：本文件写入其状态字段。
package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
	"xiaozhi-esp32-server-golang/internal/app/server/mqtt_udp"
	types_conn "xiaozhi-esp32-server-golang/internal/app/server/types"
	types_audio "xiaozhi-esp32-server-golang/internal/data/audio"
	. "xiaozhi-esp32-server-golang/internal/data/client"
	. "xiaozhi-esp32-server-golang/internal/data/msg"
	log "xiaozhi-esp32-server-golang/logger"
)

// ServerTransport handles sending messages to the client via the transport layer
// (原ServerMsgService)
//
// ServerTransport 是面向设备的协议消息发送与状态维护封装（前身叫 ServerMsgService）。
// 所有下行 JSON 消息都由这里产出，并在此顺带同步 ClientState 里的 TTS 与状态字段。
type ServerTransport struct {
	// transport 底层连接实现（websocket 或 mqtt+udp），真正的字节发送由它完成。
	// 本结构只负责「协议编排」，不关心字节怎么走。
	transport types_conn.IConn

	// clientState 与连接共享的设备状态：用于取 session_id、写 TTS/状态字段、打日志。
	// 它是「同一设备连接」的共享数据，因此这里持有指针而不是副本。
	clientState *ClientState

	// McpRecvMsgChan MCP 下行消息的缓冲队列。
	// 为什么存在：底层收包是回调式（HandleMcpMessage 被调用），
	// 而 MCP 客户端是「请求-响应」式（RecvMcpMsg 阻塞等待）。
	// 用带缓冲 channel 把两者解耦，容量 100 用于吸收一次工具调用期间突发的多条响应。
	McpRecvMsgChan chan []byte

	// closed 是否已关闭，由 mu 保护；关闭后不再接受新的 MCP 收包。
	closed bool

	// mu 保护 closed 以及「关闭时发 goodbye / 关 channel」这些动作的原子性，
	// 避免两个 goroutine 同时关闭导致重复发 goodbye 或重复 close channel（会 panic）。
	mu sync.Mutex
}

// udpSessionProvider 用于从底层 transport 里「可选地」取出 UDP 会话。
// 只有 mqtt_udp 传输实现它；websocket 传输不实现，
// 因此调用处用类型断言判断，取不到就按「没有 UDP 通道」处理，而不是强依赖接口实现。
type udpSessionProvider interface {
	GetUdpSession() *mqtt_udp.UdpSession
}

// DrainPendingAudio clears transport-level buffered audio before an interrupt stop.
//
// DrainPendingAudio 清空底层 UDP 侧已排队但尚未发出的音频，返回清掉的帧数。
// 为什么需要它：用户打断（barge-in）时，如果只停止「还没生成的音频」，
// 设备仍会继续收到已经排在 transport 里的旧音频，听起来就是「让它停却还在说」。
// 所以在发 tts_stop 之前先把 transport 里积压的音频丢掉，从源头上让旧音频不再播。
// 仅 mqtt_udp 有 UDP 会话，其它传输（如 websocket）直接返回 0。
func (s *ServerTransport) DrainPendingAudio() int {
	if s == nil || s.transport == nil {
		return 0
	}
	provider, ok := s.transport.(udpSessionProvider)
	if !ok {
		return 0
	}
	udpSession := provider.GetUdpSession()
	if udpSession == nil {
		return 0
	}
	drained := udpSession.DrainPendingAudio()
	if drained > 0 {
		log.Infof("drained pending transport audio: device=%s drained=%d", s.clientState.DeviceID, drained)
	}
	return drained
}

// NewServerTransport 创建协议发送封装。
// McpRecvMsgChan 在构造函数里就分配好（容量 100），而不是懒加载：
// MCP 初始化会在 hello 之后异步进行，收包回调随时可能到达，不能等到第一次用时才建通道。
func NewServerTransport(transport types_conn.IConn, clientState *ClientState) *ServerTransport {
	return &ServerTransport{
		transport:      transport,
		clientState:    clientState,
		McpRecvMsgChan: make(chan []byte, 100),
	}
}

// [5.3] 发 tts start：发送 tts state=start，并标记 ClientState.IsTtsStart=true。
// 状态同步放在发送成功之后：只有消息确实发出去了，本端记录「已在播报」才有意义，
// 否则会给上层一个假的「正在播报」结论。
func (s *ServerTransport) SendTtsStart() error {
	msg := ServerMessage{
		Type:      ServerMessageTypeTts,
		State:     MessageStateStart,
		SessionID: s.clientState.SessionID,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	err = s.transport.SendCmd(bytes)
	if err != nil {
		return err
	}
	s.clientState.SetTtsStart(true)
	return nil
}

// [5.5] 发 tts stop：发送 tts state=stop，并收尾本端的播报状态。
// 为什么除了发消息还要改状态：
//   - IsWelcomePlaying=false：欢迎语播放标记必须清掉，否则下一轮会被误判成「还在放欢迎语」，
//     从而拒绝新的播报；
//   - Status=listenStop：本轮播报结束，回到「可以触发下一轮对话」的状态；
//   - IsTtsStart=false：与 SendTtsStart 对称，TTS 通道已关闭。
//
// 方法开头的日志刻意打印关闭前的完整状态（状态、监听阶段、欢迎语标记），
// 因为「状态卡住」这类问题几乎只能靠对比关闭前后的字段来定位。
func (s *ServerTransport) SendTtsStop() error {
	log.Infof(
		"SendTtsStop: device=%s session=%s status=%s phase=%s welcomeSpeaking=%v welcomePlaying=%v",
		s.clientState.DeviceID,
		s.clientState.SessionID,
		s.clientState.GetStatus(),
		s.clientState.GetListenPhase(),
		s.clientState.IsWelcomeSpeaking,
		s.clientState.IsWelcomePlaying,
	)
	msg := ServerMessage{
		Type:      ServerMessageTypeTts,
		State:     MessageStateStop,
		SessionID: s.clientState.SessionID,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	err = s.transport.SendCmd(bytes)
	if err != nil {
		return err
	}
	s.clientState.IsWelcomePlaying = false
	// 一轮对话播报结束后，回到可触发下一轮对话的状态。
	s.clientState.SetStatus(ClientStatusListenStop)
	s.clientState.SetTtsStart(false)
	return nil
}

// SendSpeakRequest 发送主动播报请求（服务端主动让设备说话，而不是由用户提问触发）。
// 强制要求 session_id 非空：设备要依靠它把这条播报挂到对应会话上，
// 没有会话就无从关联，所以直接返回错误让上层重试或放弃，而不是发一条设备无法处理的空会话消息。
func (s *ServerTransport) SendSpeakRequest(text string, autoListen bool) error {
	sessionID := strings.TrimSpace(s.clientState.SessionID)
	if sessionID == "" {
		return fmt.Errorf("speak_request requires session_id")
	}
	msg := ServerMessage{
		Type:       ServerMessageTypeSpeakRequest,
		Text:       text,
		SessionID:  sessionID,
		AutoListen: &autoListen,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return s.transport.SendCmd(bytes)
}

// SendMqttGoodbye 给设备发 goodbye，通知对端本端要断开。
// MQTT 场景下这是协议要求：设备收到 goodbye 才会结束本地会话状态，
// 否则设备侧可能长期以为连接还在（尤其 UDP 音频通道容易残留成半开状态）。
func (s *ServerTransport) SendMqttGoodbye() error {
	msg := ServerMessage{
		Type:      ServerMessageTypeGoodBye,
		State:     MessageStateStop,
		SessionID: s.clientState.SessionID,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	err = s.transport.SendCmd(bytes)
	if err != nil {
		return err
	}
	return nil
}

// [0.3] 回握手：回复 hello 消息，携带输出音频格式与 UDP 参数。
// 设备依赖 AudioFormat 决定怎么解码下行音频（采样率/声道/帧长），
// 依赖 Udp 决定往哪个地址发上行音频；两者都必须由上层按协商结果传进来，本方法不做默认值猜测。
func (s *ServerTransport) SendHello(transportType string, audioFormat *types_audio.AudioFormat, udpConfig *UdpConfig) error {
	msg := ServerMessage{
		Type:        MessageTypeHello,
		Text:        "欢迎使用小智服务器",
		SessionID:   s.clientState.SessionID,
		Transport:   transportType,
		AudioFormat: audioFormat,
		Udp:         udpConfig,
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	err = s.transport.SendCmd(bytes)
	if err != nil {
		return err
	}
	return nil
}

// SendIot 回 iot 消息（设备端控件状态的应答），state 固定为 success。
// msg.Text 原样透传：IoT 的载荷结构由设备端定义，服务端只做转发，不解析内容。
func (s *ServerTransport) SendIot(msg *ClientMessage) error {
	resp := ServerMessage{
		Type:      ServerMessageTypeIot,
		Text:      msg.Text,
		SessionID: s.clientState.SessionID,
		State:     MessageStateSuccess,
	}
	bytes, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	err = s.transport.SendCmd(bytes)
	if err != nil {
		return err
	}
	return nil
}

// [2.2] 发 stt：把识别文本以 stt 消息发给设备（端上屏幕显示识别结果）。
//
// SendAsrResult 把识别文本以 stt 消息发给设备。
// 它的用途是「端上屏幕显示用户说了什么」，不参与音频播放，
// 因此这里只发文本，不涉及任何 TTS 状态变更。
func (s *ServerTransport) SendAsrResult(text string) error {
	resp := ServerMessage{
		Type:      ServerMessageTypeStt,
		Text:      text,
		SessionID: s.clientState.SessionID,
	}
	bytes, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	err = s.transport.SendCmd(bytes)
	if err != nil {
		return err
	}
	return nil
}

// [5.3] 发字幕：发送 sentence_start（句级文本消息，端上用于字幕/UI 展示），
// 并把状态置为 ttsStart。
// 为什么在这里改状态而不是等 tts_start：句级消息一到就说明本端已经进入播报链路，
// 提前置状态能让「是否在播报」的判断更早生效，减少状态滞后的窗口。
func (s *ServerTransport) SendSentenceStart(text string) error {
	response := ServerMessage{
		Type:      ServerMessageTypeTts,
		State:     MessageStateSentenceStart,
		Text:      text,
		SessionID: s.clientState.SessionID,
	}
	bytes, err := json.Marshal(response)
	if err != nil {
		return err
	}
	err = s.transport.SendCmd(bytes)
	if err != nil {
		return err
	}
	s.clientState.SetStatus(ClientStatusTTSStart)
	return nil
}

// [5.3] 发字幕：发送 sentence_end（与 SendSentenceStart 配对，表示这一句播报结束），
// 同样把状态置为 ttsStart：一句结束不代表整轮播报结束，后续句子还会继续。
func (s *ServerTransport) SendSentenceEnd(text string) error {
	response := ServerMessage{
		Type:      ServerMessageTypeTts,
		State:     MessageStateSentenceEnd,
		Text:      text,
		SessionID: s.clientState.SessionID,
	}
	bytes, err := json.Marshal(response)
	if err != nil {
		return err
	}
	err = s.transport.SendCmd(bytes)
	if err != nil {
		return err
	}
	s.clientState.SetStatus(ClientStatusTTSStart)
	return nil
}

// SendCmd 直接透传一条已编码好的控制消息。
// 作为「逃生口」存在：极少数上层已经拿到原始字节的场景可以直接发，
// 不需要为了它再在本结构上加一个专用方法。
func (s *ServerTransport) SendCmd(cmdBytes []byte) error {
	return s.transport.SendCmd(cmdBytes)
}

// [5.4] 发音频帧：直接透传一帧下行音频（Opus）给设备。
// 音频数据量大且热路径敏感，因此不做任何包装或状态维护，直接交给底层连接。
func (s *ServerTransport) SendAudio(audio []byte) error {
	return s.transport.SendAudio(audio)
}

// GetTransportType 返回底层传输类型（websocket / mqtt_udp 等）。
// 上层据此选择不同的关闭/重连策略（例如 MQTT 需要先发 goodbye）。
func (s *ServerTransport) GetTransportType() string {
	return s.transport.GetTransportType()
}

// GetData 从底层连接读取附加数据（由具体 transport 决定语义）。
func (s *ServerTransport) GetData(key string) (interface{}, error) {
	return s.transport.GetData(key)
}

// HasActiveUDPBinding 判断当前是否已建立可用的 UDP 上行绑定（能拿到对端地址）。
// 供 chat.go 判断「热链路」用：只有存在活跃绑定，才认为这条通道真的在收音频；
// websocket 传输或 UDP 尚未绑定时返回 false。
func (s *ServerTransport) HasActiveUDPBinding() bool {
	provider, ok := s.transport.(udpSessionProvider)
	if !ok {
		return false
	}
	session := provider.GetUdpSession()
	if session == nil {
		return false
	}
	return session.GetRemoteAddr() != nil
}

// GetUDPLastActiveTs 返回 UDP 会话最近一次活跃的时间戳（毫秒），不可用时为 0。
// 这是「热链路」判定的核心证据：上层会把它和本地记录取较大者，
// 因为「上一次真实音频活动」的时刻比「刚刚 hello 成功」更能说明通道确实可用。
func (s *ServerTransport) GetUDPLastActiveTs() int64 {
	provider, ok := s.transport.(udpSessionProvider)
	if !ok {
		return 0
	}
	session := provider.GetUdpSession()
	if session == nil || session.GetRemoteAddr() == nil {
		return 0
	}
	return session.LastActive.UnixMilli()
}

// SendMcpMsg 把一段 MCP JSON-RPC 载荷作为 mcp 消息发给设备。
// 由 mcp.go 的 McpTransport 调用，用于「服务端 ↔ 设备端 MCP 工具」的调用，
// 例如让设备端执行某个 IoT 动作。
func (s *ServerTransport) SendMcpMsg(payload []byte) error {
	response := ServerMessage{
		Type:      MessageTypeMcp,
		SessionID: s.clientState.SessionID,
		PayLoad:   payload,
	}
	bytes, err := json.Marshal(response)
	if err != nil {
		return err
	}
	err = s.transport.SendCmd(bytes)
	if err != nil {
		return err
	}
	return nil
}

// RecvMcpMsg 阻塞等待一条 MCP 下行消息，支持 context 取消与超时。
// 超时单独返回错误而不是无限阻塞：让 MCP 客户端能感知「设备没回」并及时失败重试，
// 否则一次丢包就会把调用方永久挂在这里。
func (s *ServerTransport) RecvMcpMsg(ctx context.Context, timeOut int) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg, ok := <-s.McpRecvMsgChan:
		if !ok {
			return nil, fmt.Errorf("transport is closed")
		}
		return msg, nil
	case <-time.After(time.Duration(timeOut) * time.Millisecond):
		return nil, fmt.Errorf("mcp 接收消息超时")
	}
}

// HandleMcpMessage 是底层收包回调：把设备发来的 MCP 消息塞进缓冲 channel。
// 这里用 select+default 而不是阻塞发送，是因为它跑在收包协程上：
// 队列满时丢弃一条 MCP 响应最多导致某次工具调用超时，
// 但若阻塞在这里把收包协程卡死，整条连接（音频、控制消息）都会停摆。
func (s *ServerTransport) HandleMcpMessage(payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("transport is closed")
	}
	select {
	case s.McpRecvMsgChan <- payload:
	default:
		log.Warnf("mcp 接收消息通道已满, 丢弃消息")
	}
	return nil
}

// IsClosed 返回本封装是否已关闭（注意：不代表底层连接一定已关闭）。
func (s *ServerTransport) IsClosed() bool {
	return s.closed
}

// Close 关闭本封装，并连带关闭底层 transport（即 close(true)）。
// 用于「本端主动断开整条连接」的正常关闭路径。
func (s *ServerTransport) Close() error {
	return s.close(true)
}

// CloseWithoutTransport 只关闭本封装，不碰底层连接（即 close(false)）。
// 用在「连接已经断了」的清理路径上：此时再对底层做 Close 没有意义，
// 甚至可能对已释放的资源操作；chat.go 的断线清理就走这条路径。
func (s *ServerTransport) CloseWithoutTransport() error {
	return s.close(false)
}

// close 是关闭的统一实现，closeUnderlyingTransport 决定是否连带关闭底层连接。
// 两个关键细节：
//  1. mqtt+udp 传输在关闭前要先发一次 goodbye（best-effort，失败只告警）：
//     让设备侧知道是服务端主动断开而不是网络抖动，否则设备可能长时间挂在半开连接上。
//     仅在「连带关闭」（close(true)）时才发——如果本就是断线后清理，对端已不可达。
//  2. 无论是否关闭底层，都要关闭 McpRecvMsgChan：
//     这样等待 RecvMcpMsg 的调用方会立刻收到 "transport is closed"，而不是干等到超时。
//
// 重复调用是安全的（已关闭直接返回 nil），因此多个清理路径都调用它也不会出问题。
func (s *ServerTransport) close(closeUnderlyingTransport bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil // Already closed
	}

	s.closed = true

	if closeUnderlyingTransport && s.transport.GetTransportType() == types_conn.TransportTypeMqttUdp {
		if err := s.SendMqttGoodbye(); err != nil {
			log.Warnf("发送 mqtt goodbye 失败: %v", err)
		}
	}

	close(s.McpRecvMsgChan)
	if closeUnderlyingTransport {
		return s.transport.Close()
	}
	return nil
}

// RecvAudio 从底层连接阻塞读取一帧上行音频，支持 context 取消与超时。
// 主要用于 UDP 侧的主动拉取场景（websocket 通常走回调而非主动拉）。
func (s *ServerTransport) RecvAudio(ctx context.Context, timeOut int) ([]byte, error) {
	return s.transport.RecvAudio(ctx, timeOut)
}

// RecvCmd 从底层连接阻塞读取一条控制消息，支持 context 取消与超时。
func (s *ServerTransport) RecvCmd(ctx context.Context, timeOut int) ([]byte, error) {
	return s.transport.RecvCmd(ctx, timeOut)
}
