package aliyun_qwen3

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

func TestServerEventUnmarshalConversationItemCreatedStringID(t *testing.T) {
	raw := []byte(`{"event_id":"event_1","type":"conversation.item.created","item":{"id":"item_123","object":"realtime.item","type":"message","status":"in_progress","role":"assistant","content":[{"type":"input_audio"}]}}`)

	var event ServerEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}

	if event.Item == nil {
		t.Fatal("expected item to be parsed")
	}
	if event.Item.ID != "item_123" {
		t.Fatalf("expected string item id, got %q", event.Item.ID)
	}
}

func TestGetTranscriptionTextPrefersTranscript(t *testing.T) {
	event := &ServerEvent{
		Transcript: "你们看花花呢？",
		Stash:      "你们看花花呢",
	}

	if got := GetTranscriptionText(event); got != "你们看花花呢？" {
		t.Fatalf("expected transcript text, got %q", got)
	}
}

func TestGetTranscriptionTextFallsBackToStash(t *testing.T) {
	event := &ServerEvent{
		Text:  "",
		Stash: "你们看花花呢",
	}

	if got := GetTranscriptionText(event); got != "你们看花花呢" {
		t.Fatalf("expected stash fallback, got %q", got)
	}
}

func TestStreamingRecognizeSendsSessionUpdateWithLanguageBeforeAudio(t *testing.T) {
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
		_ = conn.WriteJSON(ServerEvent{Type: "session.updated"})
	}))
	defer server.Close()

	asr, err := NewAliyunQwen3ASR(Config{
		APIKey:     "test-key",
		WsURL:      "ws" + strings.TrimPrefix(server.URL, "http"),
		Model:      "qwen3-asr-flash-realtime",
		Format:     "pcm",
		SampleRate: 16000,
		Language:   "zh",
		Timeout:    time.Second,
	})
	if err != nil {
		t.Fatalf("create asr failed: %v", err)
	}
	defer asr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	audioStream := make(chan []float32, 1)
	audioStream <- []float32{0.1, 0.2}

	resultChan, err := asr.StreamingRecognize(ctx, audioStream)
	if err != nil {
		t.Fatalf("streaming recognize failed: %v", err)
	}
	defer func() {
		cancel()
		for range resultChan {
		}
	}()

	select {
	case message := <-firstMessage:
		var event ClientEvent
		if err := json.Unmarshal(message, &event); err != nil {
			t.Fatalf("unmarshal first message failed: %v, raw=%s", err, string(message))
		}
		if event.Type != "session.update" {
			t.Fatalf("first websocket message = %q, want session.update; raw=%s", event.Type, string(message))
		}
		if event.Session == nil || event.Session.InputAudioTranscription == nil {
			t.Fatalf("session.update missing input_audio_transcription: raw=%s", string(message))
		}
		if got := event.Session.InputAudioTranscription.Language; got != "zh" {
			t.Fatalf("session.update language = %q, want zh; raw=%s", got, string(message))
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for first websocket message: %v", ctx.Err())
	}
}
