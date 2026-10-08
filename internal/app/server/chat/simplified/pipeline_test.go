package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
	"gopkg.in/hraban/opus.v2"
)

// 本文件用一个「进程内的假设备」来验证整条链路。
// 每个实现阶段都会在这里加一个测试，所以 `go test ./internal/app/server/chat/simplified/`
// 在任何一次提交上都能跑，方便你 checkout 到某个阶段时验证它确实能工作。

// ---------------------------------------------------------------------------
// 测试辅助
// ---------------------------------------------------------------------------

// startTestServer 在进程内起一个服务端，返回设备的连接地址。
func startTestServer(t *testing.T) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/xiaozhi/v1/", handleChat)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/xiaozhi/v1/"
}

// device 是一个模拟设备，封装了 WebSocket 收发。
type device struct {
	t    *testing.T
	conn *websocket.Conn
}

// dialDevice 模拟一台设备连上来（通过 Device-Id 头表明身份，和真机一致）。
func dialDevice(t *testing.T, url, deviceID string) *device {
	t.Helper()
	h := http.Header{}
	h.Set("Device-Id", deviceID)
	conn, _, err := websocket.DefaultDialer.Dial(url, h)
	if err != nil {
		t.Fatalf("设备连接失败: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &device{t: t, conn: conn}
}

// hello 发送握手消息。
func (d *device) hello() {
	d.t.Helper()
	d.sendJSON(ClientMessage{
		Type:        MsgHello,
		AudioParams: &inAudioFormat,
		Features:    map[string]bool{"mcp": true},
	})
}

func (d *device) listen(state string) {
	d.t.Helper()
	d.sendJSON(ClientMessage{Type: MsgListen, State: state, Mode: "auto"})
}

// startListening 模拟真实设备：先发 listen start，稍等片刻再开始推音频流。
//
// 这一步不是为了「凑时间」，而是必须的：服务端的控制帧和音频帧由两个
// 独立的消费循环处理（cmdMessageLoop / audioMessageLoop），如果设备把
// listen start 和 24 帧音频在同一毫秒内一起发出来，音频循环有可能先跑完，
// 此时服务端还没把 listening 置为 true，整段音频就被丢掉了。
// 真实设备本来就是「先建立收音状态，麦克风数据随后才到」。
func (d *device) startListening() {
	d.t.Helper()
	d.listen(StateStart)
	time.Sleep(80 * time.Millisecond)
}

func (d *device) sendJSON(msg ClientMessage) {
	d.t.Helper()
	data, err := json.Marshal(msg)
	if err != nil {
		d.t.Fatalf("序列化失败: %v", err)
	}
	if err := d.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		d.t.Fatalf("发送失败: %v", err)
	}
}

// sendAudio 发送若干 Opus 音频帧（模拟设备端上行）。
func (d *device) sendAudio(frames [][]byte) {
	d.t.Helper()
	for _, f := range frames {
		if err := d.conn.WriteMessage(websocket.BinaryMessage, f); err != nil {
			d.t.Fatalf("发送音频失败: %v", err)
		}
	}
}

// encodeOpus 把 PCM 按 60ms 一帧编成 Opus。
// 这是测试在扮演「设备端」的编码工作 —— 服务端只负责解码。
func encodeOpus(t *testing.T, pcm []float32) [][]byte {
	t.Helper()
	enc, err := opus.NewEncoder(inAudioFormat.SampleRate, inAudioFormat.Channels, opus.AppAudio)
	if err != nil {
		t.Fatalf("创建编码器失败: %v", err)
	}
	frameSamples := inAudioFormat.SampleRate * inAudioFormat.FrameDuration / 1000

	var frames [][]byte
	buf := make([]byte, 4000)
	for i := 0; i+frameSamples <= len(pcm); i += frameSamples {
		n, err := enc.EncodeFloat32(pcm[i:i+frameSamples], buf)
		if err != nil {
			t.Fatalf("编码失败: %v", err)
		}
		frames = append(frames, append([]byte(nil), buf[:n]...))
	}
	return frames
}

// speakTone 生成一段「人声」（正弦波），振幅 0.5 —— 远高于 VAD 阈值。
func speakTone(ms int, freq float64) []float32 {
	return tone(ms, inAudioFormat.SampleRate, freq)
}

// silence 生成一段静音。
func silence(ms int) []float32 {
	return make([]float32, inAudioFormat.SampleRate*ms/1000)
}

// readJSON 在超时内读一条 JSON 消息。
func (d *device) readJSON(wait time.Duration) ServerMessage {
	d.t.Helper()
	_ = d.conn.SetReadDeadline(time.Now().Add(wait))
	for {
		msgType, data, err := d.conn.ReadMessage()
		if err != nil {
			d.t.Fatalf("读取失败: %v", err)
		}
		if msgType != websocket.TextMessage {
			continue // 跳过音频帧
		}
		var msg ServerMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			d.t.Fatalf("解析失败: %v，原文: %s", err, data)
		}
		return msg
	}
}

// expectJSON 断言下一条 JSON 消息的类型和状态。
func (d *device) expectJSON(wait time.Duration, wantType, wantState string) ServerMessage {
	d.t.Helper()
	msg := d.readJSON(wait)
	if msg.Type != wantType || (wantState != "" && msg.State != wantState) {
		d.t.Fatalf("期望 type=%s state=%s，实际 type=%s state=%s", wantType, wantState, msg.Type, msg.State)
	}
	return msg
}

// collectUntil 持续读取消息，直到遇到指定的 type/state 为止，
// 返回读到的全部消息（包含目标那条）。用来观察一整段流程的消息序列。
func (d *device) collectUntil(wait time.Duration, wantType, wantState string) []ServerMessage {
	d.t.Helper()
	deadline := time.Now().Add(wait)
	var got []ServerMessage
	for time.Now().Before(deadline) {
		msg := d.readJSON(time.Until(deadline))
		got = append(got, msg)
		if msg.Type == wantType && (wantState == "" || msg.State == wantState) {
			return got
		}
	}
	d.t.Fatalf("等待 %s/%s 超时，已收到 %d 条消息", wantType, wantState, len(got))
	return nil
}

// received 是设备收到的一条消息：要么是 JSON 文本，要么是一帧音频。
type received struct {
	text  ServerMessage
	audio []byte
}

// collectUntilFull 和 collectUntil 一样，但会连音频帧一起收集。
func (d *device) collectUntilFull(wait time.Duration, wantType, wantState string) []received {
	d.t.Helper()
	deadline := time.Now().Add(wait)
	var got []received
	for time.Now().Before(deadline) {
		_ = d.conn.SetReadDeadline(time.Now().Add(time.Until(deadline)))
		msgType, data, err := d.conn.ReadMessage()
		if err != nil {
			d.t.Fatalf("读取失败: %v（已收到 %d 条）", err, len(got))
		}
		if msgType == websocket.BinaryMessage {
			got = append(got, received{audio: data})
			continue
		}
		var msg ServerMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			d.t.Fatalf("解析失败: %v，原文: %s", err, data)
		}
		got = append(got, received{text: msg})
		if msg.Type == wantType && (wantState == "" || msg.State == wantState) {
			return got
		}
	}
	d.t.Fatalf("等待 %s/%s 超时，已收到 %d 条消息", wantType, wantState, len(got))
	return nil
}

// ---------------------------------------------------------------------------
// 阶段 1：握手与文本指令
// ---------------------------------------------------------------------------

func TestStage1_Handshake(t *testing.T) {
	url := startTestServer(t)
	dev := dialDevice(t, url, "test-device-1")

	dev.hello()
	resp := dev.expectJSON(3*time.Second, SrvHello, "")
	if resp.SessionID == "" {
		t.Fatal("hello 应答缺少 session_id")
	}
	if resp.AudioParams == nil || resp.AudioParams.SampleRate != 16000 {
		t.Fatalf("hello 应答的 audio_params 不对: %+v", resp.AudioParams)
	}
	t.Logf("session_id=%s audio_params=%+v", resp.SessionID, resp.AudioParams)

	// listen start / stop 不应导致服务端报错或断开
	dev.listen(StateStart)
	dev.listen(StateStop)
	time.Sleep(100 * time.Millisecond)
}

// ---------------------------------------------------------------------------
// 阶段 2：Opus 解码 + VAD 断句
// ---------------------------------------------------------------------------

func TestStage2_SpeechEndTriggersRecognition(t *testing.T) {
	url := startTestServer(t)
	dev := dialDevice(t, url, "test-device-vad")
	dev.hello()
	dev.expectJSON(3*time.Second, SrvHello, "")

	dev.startListening()
	// 说 1 秒（模拟人声的 440Hz 正弦波）
	dev.sendAudio(encodeOpus(t, speakTone(1000, 440)))
	// 再静音 600ms —— 超过 400ms 的静音阈值，VAD 应判定「说完了」
	dev.sendAudio(encodeOpus(t, silence(600)))

	msg := dev.expectJSON(3*time.Second, SrvStt, "")
	if !strings.Contains(msg.Text, "模拟识别") {
		t.Fatalf("期望拿到模拟识别的结果，实际: %q", msg.Text)
	}
	t.Logf("识别结果: %s", msg.Text)
}

func TestStage2_ShortNoiseDoesNotTrigger(t *testing.T) {
	url := startTestServer(t)
	dev := dialDevice(t, url, "test-device-noise")
	dev.hello()
	dev.expectJSON(3*time.Second, SrvHello, "")

	dev.startListening()
	// 只响 100ms —— 短于 minSpeech(200ms)，应被当作噪声丢掉
	dev.sendAudio(encodeOpus(t, speakTone(100, 440)))
	dev.sendAudio(encodeOpus(t, silence(600)))

	// 不该收到 stt；用一个很短的超时确认「什么都没有」
	_ = dev.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if _, data, err := dev.conn.ReadMessage(); err == nil {
		t.Fatalf("短促噪声不应触发识别，却收到了: %s", data)
	}
}

// ---------------------------------------------------------------------------
// 阶段 3：ASR
// ---------------------------------------------------------------------------

func TestStage3_WAVHeader(t *testing.T) {
	pcm := speakTone(100, 440) // 100ms @16kHz 单声道 = 1600 个采样点
	wav := pcmToWAV(pcm, 16000, 1)

	if got := string(wav[0:4]); got != "RIFF" {
		t.Fatalf("RIFF 标记不对: %q", got)
	}
	if got := string(wav[8:12]); got != "WAVE" {
		t.Fatalf("WAVE 标记不对: %q", got)
	}
	if got := string(wav[36:40]); got != "data" {
		t.Fatalf("data 标记不对: %q", got)
	}
	wantDataLen := uint32(len(pcm) * 2)
	if got := binary.LittleEndian.Uint32(wav[40:44]); got != wantDataLen {
		t.Fatalf("data 长度不对: got %d want %d", got, wantDataLen)
	}
	if len(wav) != 44+int(wantDataLen) {
		t.Fatalf("WAV 总长不对: got %d want %d", len(wav), 44+wantDataLen)
	}
}

func TestStage3_OpenAIASRRequestShape(t *testing.T) {
	var (
		gotPath  string
		gotAuth  string
		gotModel string
		gotFile  []byte
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotAuth = r.URL.Path, r.Header.Get("Authorization")
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Errorf("解析 multipart 失败: %v", err)
			return
		}
		gotModel = r.FormValue("model")
		f, _, err := r.FormFile("file")
		if err != nil {
			t.Errorf("取 file 字段失败: %v", err)
			return
		}
		defer f.Close()
		gotFile, _ = io.ReadAll(f)

		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"text":"你好世界"}`)
	}))
	defer ts.Close()

	a := newOpenAIASR(ts.URL, "test-key", "whisper-1")
	text, err := a.Recognize(context.Background(), speakTone(500, 440))
	if err != nil {
		t.Fatalf("识别失败: %v", err)
	}
	if text != "你好世界" {
		t.Fatalf("识别结果不对: %q", text)
	}
	if gotPath != "/audio/transcriptions" {
		t.Fatalf("请求路径不对: %s", gotPath)
	}
	if gotAuth != "Bearer test-key" {
		t.Fatalf("Authorization 不对: %s", gotAuth)
	}
	if gotModel != "whisper-1" {
		t.Fatalf("model 字段不对: %s", gotModel)
	}
	if len(gotFile) < 44 || string(gotFile[0:4]) != "RIFF" {
		t.Fatalf("上传的不是合法 WAV，长度 %d", len(gotFile))
	}
}

func TestStage3_MockASRErrorPropagation(t *testing.T) {
	// 服务端返回 500 时，错误要能带出来（而不是静默返回空文本）
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"error":"boom"}`)
	}))
	defer ts.Close()

	a := newOpenAIASR(ts.URL, "k", "m")
	if _, err := a.Recognize(context.Background(), speakTone(100, 440)); err == nil {
		t.Fatal("期望返回错误，实际为 nil")
	}
}

// ---------------------------------------------------------------------------
// 阶段 4：LLM 与分句
// ---------------------------------------------------------------------------

func TestStage4_SentenceSplitter(t *testing.T) {
	s := &sentenceSplitter{}
	var got []string
	// 模拟 LLM 一个字一个字地吐
	for _, delta := range []string{"你好", "，", "世界", "。", "今天", "天气", "不错", "！", "对了"} {
		got = append(got, s.Push(delta)...)
	}
	if tail := s.Flush(); tail != "" {
		got = append(got, tail)
	}

	want := []string{"你好，世界。", "今天天气不错！", "对了"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("分句结果不对:\n got %q\nwant %q", got, want)
	}
}

func TestStage4_SentenceSplitterMergesContinuousEnders(t *testing.T) {
	s := &sentenceSplitter{}
	got := s.Push("真的吗？！好的。")
	want := []string{"真的吗？！", "好的。"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("连续标点应合为一句:\n got %q\nwant %q", got, want)
	}
}

func TestStage4_SentenceSplitterMaxLength(t *testing.T) {
	s := &sentenceSplitter{}
	// 一直不出标点，应该有最长长度兜底，否则用户会等很久才有声音
	got := s.Push(strings.Repeat("啊", maxSentenceRunes+5))
	if len(got) != 1 || utf8.RuneCountInString(got[0]) != maxSentenceRunes {
		t.Fatalf("最长长度兜底失效: %q", got)
	}
	if s.Len() != 5 {
		t.Fatalf("剩余缓冲不对: %d", s.Len())
	}
}

func TestStage4_OpenAILLMStream(t *testing.T) {
	var gotPath, gotAuth string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotAuth = r.URL.Path, r.Header.Get("Authorization")

		var req struct {
			Model    string    `json:"model"`
			Stream   bool      `json:"stream"`
			Messages []message `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("解析请求失败: %v", err)
			return
		}
		if !req.Stream || req.Model != "test-model" || len(req.Messages) != 2 {
			t.Errorf("请求体不符合预期: %+v", req)
		}

		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		for _, token := range []string{"你", "好", "。"} {
			_, _ = fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", token)
			flusher.Flush()
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	defer ts.Close()

	l := newOpenAILLM(ts.URL, "test-key", "test-model")
	ch, err := l.Chat(context.Background(), []message{
		{Role: "system", Content: "s"},
		{Role: "user", Content: "u"},
	})
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	var got strings.Builder
	for token := range ch {
		got.WriteString(token)
	}
	if got.String() != "你好。" {
		t.Fatalf("流式拼接结果不对: %q", got.String())
	}
	if gotPath != "/chat/completions" {
		t.Fatalf("请求路径不对: %s", gotPath)
	}
	if gotAuth != "Bearer test-key" {
		t.Fatalf("Authorization 不对: %s", gotAuth)
	}
}

func TestStage4_EndToEndText(t *testing.T) {
	url := startTestServer(t)
	dev := dialDevice(t, url, "test-device-llm")
	dev.hello()
	dev.expectJSON(3*time.Second, SrvHello, "")

	dev.startListening()
	dev.sendAudio(encodeOpus(t, speakTone(800, 440)))
	dev.sendAudio(encodeOpus(t, silence(600)))

	stt := dev.expectJSON(3*time.Second, SrvStt, "")
	if !strings.Contains(stt.Text, "模拟识别") {
		t.Fatalf("stt 内容不对: %q", stt.Text)
	}

	// 收集从 tts/start 到 tts/stop 之间的所有消息
	msgs := dev.collectUntil(10*time.Second, SrvTts, StateStop)
	if msgs[0].State != StateStart {
		t.Fatalf("第一条应是 tts/start，实际 %s/%s", msgs[0].Type, msgs[0].State)
	}

	var starts, ends int
	for _, m := range msgs {
		switch m.State {
		case StateSentenceStart:
			starts++
			if m.Text == "" {
				t.Fatal("sentence_start 缺少文本")
			}
		case StateSentenceEnd:
			ends++
		}
	}
	if starts == 0 || starts != ends {
		t.Fatalf("句子配对不对: sentence_start=%d sentence_end=%d", starts, ends)
	}

	var sentences []string
	for _, m := range msgs {
		if m.State == StateSentenceStart {
			sentences = append(sentences, m.Text)
		}
	}
	t.Logf("分句结果（%d 句）: %q", len(sentences), sentences)
}

// ---------------------------------------------------------------------------
// 阶段 5：TTS 与音频下发
// ---------------------------------------------------------------------------

func TestStage5_Resampler(t *testing.T) {
	// 24kHz → 16kHz：每 3 个输入采样点产出 2 个输出采样点
	in := tone(100, 24000, 440) // 2400 个采样点

	whole := newResampler(24000, 16000)
	wholeOut := whole.Push(in)
	if n := len(wholeOut); n < 1500 || n > 1620 {
		t.Fatalf("整块重采样输出点数不对: %d（期望约 1600）", n)
	}

	// 分块喂入必须和整块喂入结果一致 —— 否则跨块状态处理有 bug
	chunked := newResampler(24000, 16000)
	var chunkedOut []float32
	for i := 0; i < len(in); i += 100 {
		end := i + 100
		if end > len(in) {
			end = len(in)
		}
		chunkedOut = append(chunkedOut, chunked.Push(in[i:end])...)
	}
	if len(chunkedOut) != len(wholeOut) {
		t.Fatalf("分块与整块输出长度不同: %d vs %d", len(chunkedOut), len(wholeOut))
	}
	for i := range wholeOut {
		if d := wholeOut[i] - chunkedOut[i]; d > 1e-6 || d < -1e-6 {
			t.Fatalf("第 %d 个采样点不一致: %v vs %v", i, wholeOut[i], chunkedOut[i])
		}
	}
}

func TestStage5_OpenAITTSStreamAndResample(t *testing.T) {
	const srcRate = 24000

	// 假的 TTS 服务：返回 24kHz 的 440Hz 正弦波（300ms）
	var body bytes.Buffer
	for _, s := range tone(300, srcRate, 440) {
		_ = binary.Write(&body, binary.LittleEndian, floatToInt16(s))
	}
	srcAudio := body.Bytes()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/audio/speech" {
			t.Errorf("请求路径不对: %s", r.URL.Path)
		}
		var req struct {
			Model          string `json:"model"`
			Input          string `json:"input"`
			Voice          string `json:"voice"`
			ResponseFormat string `json:"response_format"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("解析请求失败: %v", err)
			return
		}
		if req.Input != "你好" || req.ResponseFormat != "pcm" || req.Voice != "test-voice" {
			t.Errorf("请求体不符合预期: %+v", req)
		}
		// 故意分两次写出，模拟流式返回
		_, _ = w.Write(srcAudio[:1000])
		w.(http.Flusher).Flush()
		_, _ = w.Write(srcAudio[1000:])
	}))
	defer ts.Close()

	outC, err := newCodec(outAudioFormat)
	if err != nil {
		t.Fatalf("建编解码器失败: %v", err)
	}
	tts := newOpenAITTS(ts.URL, "k", "test-model", "test-voice", srcRate, outC)

	frames, err := tts.Synthesize(context.Background(), "你好")
	if err != nil {
		t.Fatalf("合成失败: %v", err)
	}

	dec, _ := newCodec(outAudioFormat)
	var total int
	var peak float32
	for frame := range frames {
		pcm, err := dec.decode(frame)
		if err != nil {
			t.Fatalf("解码失败: %v", err)
		}
		total += len(pcm)
		if e := rms(pcm); e > peak {
			peak = e
		}
	}

	// 300ms @16kHz = 4800 个采样点（最后一帧会补零，允许一点误差）
	if total < 4300 || total > 5300 {
		t.Fatalf("重采样后的总时长不对: %d 个采样点（期望约 4800）", total)
	}
	if peak < 0.05 {
		t.Fatalf("重采样/解码后能量过低（听起来会像没声音）: %v", peak)
	}
	t.Logf("合成得到 %d 个采样点，峰值 %.3f", total, peak)
}

func TestStage5_EndToEndAudio(t *testing.T) {
	url := startTestServer(t)
	dev := dialDevice(t, url, "test-device-tts")
	dev.hello()
	dev.expectJSON(3*time.Second, SrvHello, "")

	dev.startListening()
	dev.sendAudio(encodeOpus(t, speakTone(800, 440)))
	dev.sendAudio(encodeOpus(t, silence(600)))

	dev.expectJSON(3*time.Second, SrvStt, "")
	msgs := dev.collectUntilFull(20*time.Second, SrvTts, StateStop)

	var (
		sentences   int
		audioFrames int
		perSentence int
		firstAudio  []byte
		totalAudio  int
	)
	for _, m := range msgs {
		if m.audio != nil {
			audioFrames++
			perSentence++
			totalAudio += len(m.audio)
			if firstAudio == nil {
				firstAudio = m.audio
			}
			continue
		}
		switch m.text.State {
		case StateSentenceStart:
			perSentence = 0
		case StateSentenceEnd:
			sentences++
			// 每一句都必须夹着音频，否则设备那端是「哑」的
			if perSentence == 0 {
				t.Fatalf("句子 %q 没有夹带任何音频", m.text.Text)
			}
		}
	}

	if sentences == 0 {
		t.Fatal("没有收到任何句子")
	}
	if firstAudio == nil {
		t.Fatal("没有收到任何音频帧")
	}
	t.Logf("共 %d 句，%d 帧音频，共 %d 字节", sentences, audioFrames, totalAudio)

	// 把第一帧解出来，确认里面真的是声音而不是静音
	dec, _ := newCodec(outAudioFormat)
	pcm, err := dec.decode(firstAudio)
	if err != nil {
		t.Fatalf("解码第一帧失败: %v", err)
	}
	if rms(pcm) <= 0.01 {
		t.Fatal("第一帧音频是静音")
	}
}

// ---------------------------------------------------------------------------
// 阶段 6：打断
// ---------------------------------------------------------------------------

// startPlaying 让设备问一句，并等到助手真的开始播报为止。
func startPlaying(t *testing.T, dev *device) {
	t.Helper()
	dev.startListening()
	dev.sendAudio(encodeOpus(t, speakTone(800, 440)))
	dev.sendAudio(encodeOpus(t, silence(600)))
	dev.expectJSON(3*time.Second, SrvStt, "")
	dev.expectJSON(5*time.Second, SrvTts, StateStart)
	dev.expectJSON(5*time.Second, SrvTts, StateSentenceStart)
}

// assertSilent 断言在 wait 时间内既没有音频帧，也没有新的句子开始/结束。
// 用来验证「打断之后旧轮次的音频确实停了」。
func assertSilent(t *testing.T, dev *device, wait time.Duration) {
	t.Helper()
	_ = dev.conn.SetReadDeadline(time.Now().Add(wait))
	for {
		msgType, data, err := dev.conn.ReadMessage()
		if err != nil {
			return // 读到超时 = 确实安静了，这正是期望的结果
		}
		if msgType == websocket.BinaryMessage {
			t.Fatalf("打断之后仍收到 %d 字节音频", len(data))
		}
		var msg ServerMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			t.Fatalf("解析失败: %v", err)
		}
		if msg.State == StateSentenceStart || msg.State == StateSentenceEnd {
			t.Fatalf("打断之后仍收到 %s/%s: %q", msg.Type, msg.State, msg.Text)
		}
	}
}

func TestStage6_BargeInByVoice(t *testing.T) {
	url := startTestServer(t)
	dev := dialDevice(t, url, "test-device-bargein")
	dev.hello()
	dev.expectJSON(3*time.Second, SrvHello, "")
	startPlaying(t, dev)

	// 用户在助手播报期间持续说话 600ms（远超 240ms 的插话阈值）
	dev.sendAudio(encodeOpus(t, speakTone(600, 440)))

	// 应当收到打断产生的 tts/stop，而不是等整段播报自然结束
	dev.expectJSON(5*time.Second, SrvTts, StateStop)
	assertSilent(t, dev, 400*time.Millisecond)
}

func TestStage6_BargeInSpeechBecomesNewTurn(t *testing.T) {
	url := startTestServer(t)
	dev := dialDevice(t, url, "test-device-bargein-2")
	dev.hello()
	dev.expectJSON(3*time.Second, SrvHello, "")
	startPlaying(t, dev)

	dev.sendAudio(encodeOpus(t, speakTone(600, 440)))
	dev.expectJSON(5*time.Second, SrvTts, StateStop)

	// 用户把插话说完，静音之后应当被当成新的一轮送去识别 ——
	// 而不是要求用户把刚才那句再说一遍。
	dev.sendAudio(encodeOpus(t, silence(700)))
	stt := dev.expectJSON(5*time.Second, SrvStt, "")
	if !strings.Contains(stt.Text, "模拟识别") {
		t.Fatalf("插话内容没有被识别: %q", stt.Text)
	}
	t.Logf("插话被识别为: %s", stt.Text)
}

func TestStage6_ShortBlipDoesNotInterrupt(t *testing.T) {
	url := startTestServer(t)
	dev := dialDevice(t, url, "test-device-blip")
	dev.hello()
	dev.expectJSON(3*time.Second, SrvHello, "")
	startPlaying(t, dev)

	// 只响一帧（60ms，低于 240ms 阈值），不该打断播报
	dev.sendAudio(encodeOpus(t, speakTone(60, 440)))

	// 播报应该正常走完：模拟回复一共 3 句，
	// 其中第 1 句的 sentence_start 已经在 startPlaying 里被读掉了。
	msgs := dev.collectUntilFull(20*time.Second, SrvTts, StateStop)
	sentences := 1
	for _, m := range msgs {
		if m.audio == nil && m.text.State == StateSentenceStart {
			sentences++
		}
	}
	if sentences < 3 {
		t.Fatalf("短暂噪声不该打断播报，实际只播了 %d 句", sentences)
	}
}

func TestStage6_AbortMessage(t *testing.T) {
	url := startTestServer(t)
	dev := dialDevice(t, url, "test-device-abort")
	dev.hello()
	dev.expectJSON(3*time.Second, SrvHello, "")
	startPlaying(t, dev)

	// 设备侧主动打断
	dev.sendJSON(ClientMessage{Type: MsgAbort})
	dev.expectJSON(5*time.Second, SrvTts, StateStop)
	assertSilent(t, dev, 400*time.Millisecond)
}

func TestStage6_VADMarkSpeakingThenSilence(t *testing.T) {
	v := newVAD()
	// 模拟「用户插话打断播报」：已经确认他说了 bargeInSpeechMs 那么久。
	// 接下来如果他直接住口（不再有语音帧），也必须能判出「说完了」。
	v.MarkSpeaking(bargeInSpeechMs)

	var (
		got    = vadSilence
		spoken int
	)
	for i := 0; i < 20 && got != vadSpeechEnd; i++ {
		got, spoken = v.Push(silence(60), 60)
	}
	if got != vadSpeechEnd {
		t.Fatal("MarkSpeaking 之后只发静音，应当能判出「说完了」")
	}
	if spoken != bargeInSpeechMs {
		t.Fatalf("净语音时长不对: %d", spoken)
	}
}
