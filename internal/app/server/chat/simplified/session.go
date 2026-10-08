package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
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

	codec *codec // Opus 编解码，hello 拿到音频格式后创建

	// opusQueue 对应真实实现 clientState.OpusAudioBuffer：
	// 收帧循环只管往这里塞，音频处理循环慢慢消费，两边解耦。
	opusQueue chan []byte

	vad *vad

	// mu 保护下面这几个跨 goroutine 的状态
	// （handleCmdLoop 会改 listening，processAudioLoop 会改 collecting/utterance）
	mu         sync.Mutex
	listening  bool      // 处于 listen start 和 listen stop 之间
	collecting bool      // 正在攒一段语音
	utterance  []float32 // 攒下来的 PCM

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
		opusQueue: make(chan []byte, 100),
		vad:       newVAD(),
		ctx:       ctx,
		cancel:    cancel,
	}
}

// run 启动三个循环，然后阻塞到连接关闭。
//
//	handleCmdLoop     消费文本帧（控制指令）
//	handleAudioLoop   消费二进制帧（Opus），转手塞进 opusQueue
//	processAudioLoop  从 opusQueue 取帧：解码 → VAD → 断句 → 交给 ASR
//
// 对应真实实现 ChatManager.Start 的 cmdMessageLoop / audioMessageLoop，
// 以及 ChatSession 里的 ProcessVadAudio 循环。
//
// 为什么要拆这么细：文本指令是偶发的，音频帧是 60ms 一帧的持续流，
// 解码 + VAD 又是 CPU 开销较大的一步。混在一个循环里会互相阻塞 ——
// 解码慢了一下，控制指令就延迟；反之亦然。
func (s *session) run() {
	defer s.cancel()

	done := make(chan struct{}, 3)
	go func() { s.handleCmdLoop(); done <- struct{}{} }()
	go func() { s.handleAudioLoop(); done <- struct{}{} }()
	go func() { s.processAudioLoop(); done <- struct{}{} }()

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
// 这里只做四件事：记录上行音频格式、建编解码器、生成 session_id、回一个 hello。
func (s *session) handleHello(msg *ClientMessage) {
	if msg.AudioParams != nil {
		s.inFormat = *msg.AudioParams
	}
	if msg.DeviceID != "" && msg.DeviceID != s.deviceID {
		s.deviceID = msg.DeviceID
	}

	c, err := newCodec(s.inFormat)
	if err != nil {
		log.Printf("[%s] 创建 Opus 编解码器失败: %v", s.deviceID, err)
		return
	}
	s.codec = c

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
//	state = stop  → 用户松手，本轮语音立刻结束（不等 VAD 的静音超时）
//	state = detect → 设备侧唤醒词检测结果，简化版不用
func (s *session) handleListen(msg *ClientMessage) {
	switch msg.State {
	case StateStart:
		s.beginTurn()
		log.Printf("[%s] listen start（mode=%s）", s.deviceID, msg.Mode)
	case StateStop:
		log.Printf("[%s] listen stop", s.deviceID)
		s.endTurn("client listen stop")
	case StateDetect:
		// 唤醒词检测，简化版忽略
	default:
		log.Printf("[%s] listen 未知 state: %s", s.deviceID, msg.State)
	}
}

// beginTurn 开始一轮收音：清空缓冲、重置 VAD。
func (s *session) beginTurn() {
	s.mu.Lock()
	s.listening = true
	s.collecting = false
	s.utterance = nil
	s.mu.Unlock()
	s.vad.Reset()
}

// endTurn 结束当前这一轮，把攒下来的 PCM 送去识别。
// 如果当前并不在收音（没人说话），什么都不做。
func (s *session) endTurn(reason string) {
	s.mu.Lock()
	s.listening = false
	pcm := s.utterance
	s.utterance = nil
	wasCollecting := s.collecting
	s.collecting = false
	s.mu.Unlock()

	s.vad.Reset()

	if !wasCollecting || len(pcm) == 0 {
		return
	}
	log.Printf("[%s] 本轮收音结束（%s）", s.deviceID, reason)
	s.recognize(pcm)
}

// handleAudioLoop 消费设备上传的 Opus 音频帧，转手塞进 opusQueue。
//
// 对应真实实现 ChatManager.audioMessageLoop：它只做搬运，
// 把帧丢进 clientState.OpusAudioBuffer 就返回。
// 队列满了就丢弃 —— 宁可丢帧也不能阻塞收包（真实实现同样是 select+default 丢弃）。
func (s *session) handleAudioLoop() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case frame := <-s.conn.Audio():
			s.mu.Lock()
			listening := s.listening
			s.mu.Unlock()
			if !listening {
				continue // 不在收音窗口内，直接丢弃
			}
			select {
			case s.opusQueue <- frame:
			default:
				log.Printf("[%s] 音频队列已满，丢弃一帧", s.deviceID)
			}
		}
	}
}

// processAudioLoop 是音频处理的主循环。
//
// 对应真实实现 asr.go 的 ProcessVadAudio。它做三件事：
//  1. Opus 解码成 PCM
//  2. 每帧送进 VAD，判断有没有人声、一句话有没有说完
//  3. 把 PCM 攒起来；一旦 VAD 判定「说完了」，就把整段送去识别
//
// 真实实现这一步还顺带做了：把音频喂给声纹识别、判断是否需要打断播报、
// 统计首字延迟埋点……这里全部去掉，只留主干。
func (s *session) processAudioLoop() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case frame := <-s.opusQueue:
			if s.codec == nil {
				continue // 还没收到 hello，不知道音频格式
			}
			pcm, err := s.codec.decode(frame)
			if err != nil {
				log.Printf("[%s] Opus 解码失败: %v", s.deviceID, err)
				continue
			}
			s.handlePCM(pcm)
		}
	}
}

// handlePCM 处理一帧 PCM：跑 VAD，决定是攒着还是收尾。
func (s *session) handlePCM(pcm []float32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 本轮已经结束（或还没开始），队列里剩下的帧直接丢掉。
	// 队列里很可能还压着上一轮遗留的音频。
	if !s.listening {
		return
	}

	frameMs := len(pcm) / s.codec.samplesPerMs() // 60ms 一帧 → 960/16

	result, spokenMs := s.vad.Push(pcm, frameMs)

	switch result {
	case vadSpeech:
		if !s.collecting {
			s.collecting = true
			log.Printf("[%s] VAD：检测到人声，开始收音", s.deviceID)
		}
		s.utterance = append(s.utterance, pcm...)

	case vadSpeechEnd:
		// 这一帧本身还是语音的一部分，先收进去
		s.utterance = append(s.utterance, pcm...)
		pcmAll := s.utterance
		s.utterance = nil
		s.collecting = false
		s.listening = false // 本轮结束，等设备下一次 listen start

		// 尾部那段静音对识别没用，按 VAD 给出的净语音时长裁掉，
		// 这样送进 ASR 的音频刚好是「人说的那一段」。
		if maxSamples := spokenMs * s.codec.samplesPerMs(); len(pcmAll) > maxSamples {
			pcmAll = pcmAll[:maxSamples]
		}

		log.Printf("[%s] VAD：静音超过 %v，判断一句话说完了（净语音约 %dms）",
			s.deviceID, s.vad.silenceTimeout, spokenMs)
		go s.recognize(pcmAll) // 识别可能很慢，不要挡住音频循环

	default: // vadSilence
		// 已经在攒语音了，中间的停顿也要带上，否则送进 ASR 的音频是断的
		if s.collecting {
			s.utterance = append(s.utterance, pcm...)
		}
	}
}

// recognize 把一整段语音变成文本。
//
// 阶段 2 先用一个占位实现，让你能立刻看到「VAD 切出了一句话」这件事；
// 阶段 3 会把它换成真正的 ASR。
func (s *session) recognize(pcm []float32) {
	ms := len(pcm) / s.codec.samplesPerMs()
	text := fmt.Sprintf("[阶段2占位] 我听到了约 %d 毫秒的语音", ms)
	log.Printf("[%s] ASR（占位）→ %s", s.deviceID, text)

	s.reply(ServerMessage{
		Type: SrvStt,
		Text: text,
	})
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
