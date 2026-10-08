package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
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
