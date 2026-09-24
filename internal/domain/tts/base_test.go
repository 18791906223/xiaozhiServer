package tts

import (
	"context"
	"strings"
	"testing"
	"time"

	"xiaozhi-esp32-server-golang/constants"
	"xiaozhi-esp32-server-golang/internal/domain/tts/openai"
)

type fakeStreamProvider struct {
	stream    chan []byte
	streamCtx context.Context
}

func (p *fakeStreamProvider) TextToSpeech(ctx context.Context, text string, sampleRate int, channels int, frameDuration int) ([][]byte, error) {
	return nil, nil
}

func (p *fakeStreamProvider) TextToSpeechStream(ctx context.Context, text string, sampleRate int, channels int, frameDuration int) (chan []byte, error) {
	p.streamCtx = ctx
	return p.stream, nil
}

func TestGetTTSProviderUsesConfigProviderOverride(t *testing.T) {
	provider, err := GetTTSProvider(constants.TtsTypeEdge, map[string]interface{}{
		"provider": constants.TtsTypeOpenAI,
		"api_key":  "test-key",
	})
	if err != nil {
		t.Fatalf("GetTTSProvider error = %v", err)
	}

	adapter, ok := provider.(*ContextTTSAdapter)
	if !ok {
		t.Fatalf("provider type = %T", provider)
	}
	if _, ok := adapter.Provider.(*openai.OpenAITTSProvider); !ok {
		t.Fatalf("wrapped provider type = %T", adapter.Provider)
	}
}

func TestGetTTSProviderRejectsUnknown(t *testing.T) {
	if _, err := GetTTSProvider("missing_provider", nil); err == nil {
		t.Fatal("expected unknown provider to fail")
	}
}

func TestBuildIndexTTSOpenAIConfigNormalizesURLAndDefaults(t *testing.T) {
	config := buildIndexTTSOpenAIConfig(map[string]interface{}{
		"api_url": "http://127.0.0.1:7860",
	})

	if config["api_url"] != "http://127.0.0.1:7860/audio/speech" {
		t.Fatalf("api_url = %#v", config["api_url"])
	}
	if config["model"] != "indextts-vllm" {
		t.Fatalf("model = %#v", config["model"])
	}
	if config["response_format"] != "wav" {
		t.Fatalf("response_format = %#v", config["response_format"])
	}
	if config["stream"] != false {
		t.Fatalf("stream = %#v", config["stream"])
	}
	if config["speed"] != float64(1.0) {
		t.Fatalf("speed = %#v", config["speed"])
	}
}

func TestBuildIndexTTSOpenAIConfigKeepsExplicitAudioSpeechPath(t *testing.T) {
	config := buildIndexTTSOpenAIConfig(map[string]interface{}{
		"api_url": "http://127.0.0.1:7860/audio/speech/",
		"model":   "custom",
	})

	if got := config["api_url"].(string); !strings.HasSuffix(got, "/audio/speech") {
		t.Fatalf("api_url = %q", got)
	}
	if config["model"] != "custom" {
		t.Fatalf("model = %#v", config["model"])
	}
}

func TestContextTTSAdapterStreamWithContextFallbackCancel(t *testing.T) {
	provider := &fakeStreamProvider{stream: make(chan []byte)}
	adapter := &ContextTTSAdapter{Provider: provider}

	outputChan, cancel, err := adapter.TextToSpeechStreamWithContext(context.Background(), "hello", 16000, 1, 20)
	if err != nil {
		t.Fatalf("TextToSpeechStreamWithContext error = %v", err)
	}
	if cancel == nil {
		t.Fatal("cancel func should not be nil")
	}

	cancel()

	select {
	case _, ok := <-outputChan:
		if ok {
			t.Fatal("output channel should close after cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("output channel did not close after cancel")
	}

	select {
	case <-provider.streamCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("wrapped stream context was not canceled")
	}
}
