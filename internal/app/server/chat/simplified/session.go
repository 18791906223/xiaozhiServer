package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// session 是一条设备连接上的会话。
//
// 真实实现把它拆成了两层：
//   - ChatManager（chat.go）：连接级，管两个收发循环
//   - ChatSession（session.go）：对话轮级，按需创建/销毁
//
// 简化版合并成一个对象：一条连接 = 一个 session。
// 这样读代码时不用在两层之间来回跳。
type session struct {
	conn     *transport
	deviceID string

	sessionID string
	inFormat  AudioFormat // 上行（设备 → 服务端）
	outFormat AudioFormat // 下行（服务端 → 设备）

	// listening 为 true 表示处于 listen start 和 listen stop 之间。
	// 真实实现用一个更复杂的状态机（ClientStatus）表达，简化版一个 bool 够了。
	listening bool

	ctx    context.Context
	cancel context.CancelFunc
}

func newSession(conn *transport, deviceID string) *session {
	ctx, cancel := context.WithCancel(context.Background())
	return &session{
		conn:      conn,
		deviceID:  deviceID,
		sessionID: fmt.Sprintf("%s-%d", deviceID, time.Now().UnixNano()),
		inFormat:  inAudioFormat,
		outFormat: outAudioFormat,
		ctx:       ctx,
		cancel:    cancel,
	}
}

// run 启动两个消费循环，然后阻塞到连接关闭。
//
//	handleCmdLoop   消费文本帧（控制指令）
//	handleAudioLoop 消费二进制帧（Opus 音频）
//
// 对应真实实现 ChatManager.Start 里的 cmdMessageLoop / audioMessageLoop。
// 之所以拆成两个循环，是因为两者的节奏完全不同：
// 文本指令是偶发的，音频帧是持续 60ms 一帧的流，混在一起会互相阻塞。
func (s *session) run() {
	defer s.cancel()

	done := make(chan struct{}, 2)
	go func() { s.handleCmdLoop(); done <- struct{}{} }()
	go func() { s.handleAudioLoop(); done <- struct{}{} }()

	select {
	case <-s.conn.Done():
	case <-s.ctx.Done():
	case <-done:
	}
	s.conn.Close()
}

// handleCmdLoop 消费设备发来的文本指令。
func (s *session) handleCmdLoop() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case data := <-s.conn.Cmd():
			var msg ClientMessage
			if err := json.Unmarshal(data, &msg); err != nil {
				log.Printf("[%s] 解析文本消息失败: %v，原文: %s", s.deviceID, err, data)
				continue
			}
			log.Printf("[%s] 收到 %s", s.deviceID, data)
			s.handleClientMessage(&msg)
		}
	}
}

// handleClientMessage 是所有上行指令的唯一分发点。
// 对应真实实现 chat.go 的 handleTextMessage。
func (s *session) handleClientMessage(msg *ClientMessage) {
	switch msg.Type {
	case MsgHello:
		s.handleHello(msg)
	case MsgListen:
		s.handleListen(msg)
	case MsgAbort:
		log.Printf("[%s] 收到 abort（打断），阶段 6 实现", s.deviceID)
	case MsgGoodbye:
		log.Printf("[%s] 收到 goodbye，主动断开", s.deviceID)
		s.conn.Close()
	case MsgIot:
		// 设备侧 IoT 状态上报，简化版忽略
	default:
		log.Printf("[%s] 忽略未知消息类型: %s", s.deviceID, msg.Type)
	}
}

// handleHello 握手。
//
// 对应真实实现 chat.go 的 HandleHelloMessage。真实实现要处理：
// 会话 ID 复用/新建、设备是否激活、配置刷新、MCP 能力协商、UDP 通道下发……
// 这里只做三件事：记录上行音频格式、生成 session_id、回一个 hello。
func (s *session) handleHello(msg *ClientMessage) {
	if msg.AudioParams != nil {
		s.inFormat = *msg.AudioParams
	}
	if msg.DeviceID != "" && msg.DeviceID != s.deviceID {
		s.deviceID = msg.DeviceID
	}
	log.Printf("[%s] hello，上行音频格式: %+v", s.deviceID, s.inFormat)

	s.reply(ServerMessage{
		Type:        SrvHello,
		Text:        "欢迎使用小智服务器（简化版）",
		SessionID:   s.sessionID,
		Transport:   "websocket",
		AudioParams: &s.outFormat,
	})
}

// handleListen 处理「开始说话 / 说完了」。
//
//	state = start → 开始收音
//	state = stop  → 用户松手，本轮语音结束
//	state = detect → 设备侧唤醒词检测结果，简化版不用
func (s *session) handleListen(msg *ClientMessage) {
	switch msg.State {
	case StateStart:
		s.listening = true
		log.Printf("[%s] listen start（mode=%s）", s.deviceID, msg.Mode)
		// 阶段 2 起：这里重置 VAD、准备接收音频
	case StateStop:
		s.listening = false
		log.Printf("[%s] listen stop", s.deviceID)
		// 阶段 3 起：这里结束 ASR，进入 ASR → LLM → TTS
	case StateDetect:
		// 唤醒词检测，简化版忽略
	default:
		log.Printf("[%s] listen 未知 state: %s", s.deviceID, msg.State)
	}
}

// handleAudioLoop 消费设备上传的 Opus 音频帧。
//
// 对应真实实现 ChatManager.audioMessageLoop：它把帧塞进
// clientState.OpusAudioBuffer，再由 VAD 循环取出来处理。
// 简化版把「缓冲区」这一步留到阶段 2 再加上。
func (s *session) handleAudioLoop() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case frame := <-s.conn.Audio():
			if !s.listening {
				continue // 不在收音窗口内，直接丢弃
			}
			// 阶段 2 起：这里把 frame 交给 VAD 循环
			_ = frame
		}
	}
}

// reply 发送一条服务端消息，自动补上 session_id。
func (s *session) reply(msg ServerMessage) {
	if msg.SessionID == "" {
		msg.SessionID = s.sessionID
	}
	if err := s.conn.SendCmd(msg); err != nil {
		log.Printf("[%s] 发送消息失败: %v", s.deviceID, err)
	}
}
