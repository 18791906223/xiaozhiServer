package main

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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
	if !strings.Contains(msg.Text, "阶段2占位") {
		t.Fatalf("期望拿到阶段 2 的占位识别结果，实际: %q", msg.Text)
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
