package aliyun_funasr

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestStreamingRecognizeSendsLanguageHintsInRunTask(t *testing.T) {
	firstMessage := make(chan []byte, 1)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade failed: %v", err)
			return
		}
		defer conn.Close()

		_, message, err := conn.ReadMessage()
		if err != nil {
			t.Errorf("read first message failed: %v", err)
			return
		}
		firstMessage <- message
		_ = conn.WriteJSON(Event{Header: Header{Event: "task-started"}})
		<-r.Context().Done()
	}))
	defer server.Close()

	asr, err := NewAliyunFunASR(Config{
		APIKey:        "test-key",
		WsURL:         "ws" + strings.TrimPrefix(server.URL, "http"),
		Model:         "fun-asr-realtime",
		Format:        "pcm",
		SampleRate:    16000,
		LanguageHints: []string{"zh"},
		Timeout:       time.Second,
	})
	if err != nil {
		t.Fatalf("create asr failed: %v", err)
	}
	defer asr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	audioStream := make(chan []float32)
	resultChan, err := asr.StreamingRecognize(ctx, audioStream)
	if err != nil {
		t.Fatalf("streaming recognize failed: %v", err)
	}
	defer func() {
		cancel()
		close(audioStream)
		for range resultChan {
		}
	}()

	select {
	case message := <-firstMessage:
		var event Event
		if err := json.Unmarshal(message, &event); err != nil {
			t.Fatalf("unmarshal first message failed: %v, raw=%s", err, string(message))
		}
		if got := event.Header.Action; got != "run-task" {
			t.Fatalf("first websocket action = %q, want run-task; raw=%s", got, string(message))
		}
		if got := event.Payload.Parameters.LanguageHints; len(got) != 1 || got[0] != "zh" {
			t.Fatalf("language_hints = %#v, want [zh]; raw=%s", got, string(message))
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for first websocket message: %v", ctx.Err())
	}
}
