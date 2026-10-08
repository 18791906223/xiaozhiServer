package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
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

// tone 生成一段正弦波，振幅 0.5 —— 远高于 VAD 阈值，模拟人声。
func tone(ms int, freq float64) []float32 {
	n := inAudioFormat.SampleRate * ms / 1000
	out := make([]float32, n)
	for i := range out {
		out[i] = 0.5 * float32(math.Sin(2*math.Pi*freq*float64(i)/float64(inAudioFormat.SampleRate)))
	}
	return out
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

	dev.listen(StateStart)
	// 说 1 秒（模拟人声的 440Hz 正弦波）
	dev.sendAudio(encodeOpus(t, tone(1000, 440)))
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

	dev.listen(StateStart)
	// 只响 100ms —— 短于 minSpeech(200ms)，应被当作噪声丢掉
	dev.sendAudio(encodeOpus(t, tone(100, 440)))
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
	pcm := tone(100, 440) // 100ms @16kHz 单声道 = 1600 个采样点
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
	text, err := a.Recognize(context.Background(), tone(500, 440))
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
	if _, err := a.Recognize(context.Background(), tone(100, 440)); err == nil {
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

	dev.listen(StateStart)
	dev.sendAudio(encodeOpus(t, tone(800, 440)))
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
