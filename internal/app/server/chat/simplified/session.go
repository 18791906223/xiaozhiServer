package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
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

	// 上下行各一个编解码器：上行按设备 hello 上报的格式解码，
	// 下行按服务端下发的格式编码。两者格式不一定相同。
	inCodec  *codec
	outCodec *codec

	// asr / llm / tts 是本轮对话流水线的三个环节，对应真实实现里的
	// AsrProvider / LLMProvider / TTSProvider。
	asr asrProvider
	llm llmProvider
	tts ttsProvider

	// history 是最近几轮对话，作为上下文一并发给 LLM。
	// 真实实现还要做工具消息对齐、超长裁剪、长期记忆注入。
	history []message

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
	s := &session{
		conn:      conn,
		deviceID:  deviceID,
		sessionID: fmt.Sprintf("%s-%d", deviceID, time.Now().UnixNano()),
		inFormat:  inAudioFormat,
		outFormat: outAudioFormat,
		opusQueue: make(chan []byte, 100),
		vad:       newVAD(),
		asr:       appConfig.newASR(),
		llm:       appConfig.newLLM(),
		ctx:       ctx,
		cancel:    cancel,
	}
	// 下行编解码器立刻就能建（格式由服务端决定）；
	// 上行要等 hello 拿到设备上报的格式再建。
	s.outCodec, _ = newCodec(s.outFormat)
	s.tts = appConfig.newTTS(s.outCodec)
	return s
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
	s.inCodec = c

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
			if s.inCodec == nil {
				continue // 还没收到 hello，不知道音频格式
			}
			pcm, err := s.inCodec.decode(frame)
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

	frameMs := len(pcm) / s.inCodec.samplesPerMs() // 60ms 一帧 → 960/16

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
		if maxSamples := spokenMs * s.inCodec.samplesPerMs(); len(pcmAll) > maxSamples {
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

// recognize 把一整段语音变成文本，然后交给大模型。
//
// 对应真实实现 asr.go 的识别循环 + chat.go 的 addAsrResultToQueue：
// 识别结果先回一个 stt 消息（设备端可以显示字幕），再进入 LLM 环节。
// 真实实现把结果塞进 chatTextQueue 就返回，由另一个循环去请求 LLM；
// 简化版直接在原地往下走。
func (s *session) recognize(pcm []float32) {
	text, err := s.asr.Recognize(s.ctx, pcm)
	if err != nil {
		log.Printf("[%s] 识别失败（%s）: %v", s.deviceID, s.asr.Name(), err)
		return
	}
	if text == "" {
		log.Printf("[%s] 识别结果为空，本轮结束", s.deviceID)
		return
	}

	log.Printf("[%s] 识别结果（%s）→ %s", s.deviceID, s.asr.Name(), text)
	s.reply(ServerMessage{
		Type: SrvStt,
		Text: text,
	})

	s.replyWithLLM(s.ctx, text)
}

// replyWithLLM 把用户说的话交给大模型，并把回复按句下发给设备。
//
// 对应真实实现 llm.go 的 DoLLmRequest → handleLLMResponse →
// ttsManager.handleTextResponseWithHooks 这一串。
//
// 消息顺序和设备看到的效果：
//
//	tts/start
//	tts/sentence_start「第一句」 + 音频 + tts/sentence_end
//	tts/sentence_start「第二句」 + 音频 + tts/sentence_end
//	tts/stop
func (s *session) replyWithLLM(ctx context.Context, userText string) {
	messages := s.buildMessages(userText)

	stream, err := s.llm.Chat(ctx, messages)
	if err != nil {
		log.Printf("[%s] 请求大模型失败（%s）: %v", s.deviceID, s.llm.Name(), err)
		return
	}

	log.Printf("[%s] 开始播报（%s）", s.deviceID, s.llm.Name())
	s.reply(ServerMessage{Type: SrvTts, State: StateStart})

	splitter := &sentenceSplitter{}
	var reply strings.Builder
	for delta := range stream {
		reply.WriteString(delta)
		// 每收到一小段，就看有没有攒出完整的句子 —— 有就立刻合成、立刻下发。
		// 这一步是「边说边播」的关键：不用等整段回复写完。
		for _, sentence := range splitter.Push(delta) {
			s.speakSentence(ctx, sentence)
		}
		if ctx.Err() != nil {
			break // 被打断（阶段 6）
		}
	}
	// 最后一句通常不带句末标点，别忘了。
	if tail := splitter.Flush(); tail != "" && ctx.Err() == nil {
		s.speakSentence(ctx, tail)
	}

	s.reply(ServerMessage{Type: SrvTts, State: StateStop})
	s.appendMessage(message{Role: "assistant", Content: reply.String()})
}

// buildMessages 拼出发给 LLM 的消息列表：系统提示词 + 最近若干轮历史。
//
// 对应真实实现 llm.go 的 GetMessages：它会拼上角色设定、全局提示词、
// 当前时间、长期记忆、声纹说话人、知识库路由策略……
func (s *session) buildMessages(userText string) []message {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.history = append(s.history, message{Role: "user", Content: userText})
	if len(s.history) > maxHistoryMessages {
		s.history = s.history[len(s.history)-maxHistoryMessages:]
	}

	messages := make([]message, 0, len(s.history)+1)
	messages = append(messages, message{Role: "system", Content: systemPrompt})
	return append(messages, s.history...)
}

// appendMessage 把助手回复追加进历史，并做条数裁剪。
func (s *session) appendMessage(m message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.history = append(s.history, m)
	if len(s.history) > maxHistoryMessages {
		s.history = s.history[len(s.history)-maxHistoryMessages:]
	}
}

// speakSentence 播报一句话：sentence_start → 音频帧 → sentence_end。
//
// 对应真实实现 tts.go 的 handleTextResponseWithHooks → handleTts →
// runSenderLoop 那条链路。
//
// 真实实现的音频要经过好几级队列（ttsQueue → sessionAudioQueue → socket），
// 由专门的发送循环按播放节奏控制发送速度；简化版直接同步写下去，
// 顺序一样，只是没有「提前缓冲」的优化。
//
// 阶段 5 补上了中间那段音频 —— 这正是设备判断「一句话说完了没有」的依据：
// 它收到 sentence_end 就知道这句的音频到齐了。
func (s *session) speakSentence(ctx context.Context, sentence string) {
	s.reply(ServerMessage{Type: SrvTts, State: StateSentenceStart, Text: sentence})

	frames, err := s.tts.Synthesize(ctx, sentence)
	if err != nil {
		log.Printf("[%s] 语音合成失败（%s）: %v", s.deviceID, s.tts.Name(), err)
		frames = nil
	}
	sent := 0
	if frames != nil {
		for frame := range frames {
			if ctx.Err() != nil {
				break
			}
			s.sendAudio(frame)
			sent++
		}
	}

	if sent == 0 {
		log.Printf("[%s] 这一句没有产出音频: %q", s.deviceID, sentence)
	}
	s.reply(ServerMessage{Type: SrvTts, State: StateSentenceEnd, Text: sentence})
}

// sendAudio 把一个 Opus 帧写成 WebSocket 二进制帧。
func (s *session) sendAudio(frame []byte) {
	if err := s.conn.SendAudio(frame); err != nil {
		log.Printf("[%s] 发送音频失败: %v", s.deviceID, err)
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
