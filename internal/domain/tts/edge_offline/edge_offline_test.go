package edge_offline

import (
	"context"
	"testing"
	"time"
)

func TestNewEdgeOfflineTTSProviderDefaultsAndLifecycle(t *testing.T) {
	provider := NewEdgeOfflineTTSProvider(map[string]interface{}{})

	if provider.ServerURL != "ws://localhost:8080/tts" {
		t.Fatalf("ServerURL = %q", provider.ServerURL)
	}
	if provider.Timeout != 30*time.Second {
		t.Fatalf("Timeout = %v", provider.Timeout)
	}
	if provider.HandshakeTimeout != 10*time.Second {
		t.Fatalf("HandshakeTimeout = %v", provider.HandshakeTimeout)
	}
	if provider.IsValid() {
		t.Fatal("new provider should not be valid until a websocket connection exists")
	}
	if err := provider.SetVoice(map[string]interface{}{"voice": "ignored"}); err != nil {
		t.Fatalf("SetVoice error = %v", err)
	}
	if err := provider.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}
}

func TestEdgeOfflineTextToSpeechInvalidServerReturnsError(t *testing.T) {
	provider := NewEdgeOfflineTTSProvider(map[string]interface{}{
		"server_url":        "ws://127.0.0.1:1/tts",
		"handshake_timeout": float64(1),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, err := provider.TextToSpeech(ctx, "测试文本", 16000, 1, 20); err == nil {
		t.Fatal("expected invalid websocket server to fail")
	}
}

func TestEdgeOfflineTextToSpeechStreamConnectionErrorClosesChannel(t *testing.T) {
	provider := NewEdgeOfflineTTSProvider(map[string]interface{}{
		"server_url":        "ws://127.0.0.1:1/tts",
		"handshake_timeout": float64(1),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	outputChan, err := provider.TextToSpeechStream(ctx, "测试文本", 16000, 1, 20)
	if err != nil {
		t.Fatalf("TextToSpeechStream should return async channel, got error: %v", err)
	}

	select {
	case _, ok := <-outputChan:
		if ok {
			t.Fatal("expected channel to close without frames after connection error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("output channel did not close after connection error")
	}
}
