package indextts_vllm

import (
	"context"
	"testing"

	"xiaozhi-esp32-server-golang/internal/data/audio"
)

func TestNewIndexTTSVLLMProviderDefaultsAndSetVoice(t *testing.T) {
	provider := NewIndexTTSVLLMProvider(map[string]interface{}{
		"api_url": "http://127.0.0.1:7860/",
	})

	if provider.BaseURL != "http://127.0.0.1:7860" {
		t.Fatalf("BaseURL = %q", provider.BaseURL)
	}
	if provider.Model != defaultIndexTTSModel {
		t.Fatalf("Model = %q", provider.Model)
	}
	if provider.ResponseFormat != defaultIndexResponseFormat {
		t.Fatalf("ResponseFormat = %q", provider.ResponseFormat)
	}
	if provider.FrameDuration != audio.FrameDuration {
		t.Fatalf("FrameDuration = %d", provider.FrameDuration)
	}
	if !provider.IsValid() {
		t.Fatal("provider should be valid")
	}
	if err := provider.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}

	if err := provider.SetVoice(map[string]interface{}{"voice": "voice-1"}); err != nil {
		t.Fatalf("SetVoice voice error = %v", err)
	}
	if provider.Voice != "voice-1" {
		t.Fatalf("Voice = %q", provider.Voice)
	}
	if err := provider.SetVoice(map[string]interface{}{"character": "character-1"}); err != nil {
		t.Fatalf("SetVoice character error = %v", err)
	}
	if provider.Voice != "character-1" {
		t.Fatalf("Voice = %q", provider.Voice)
	}
	if err := provider.SetVoice(map[string]interface{}{}); err == nil {
		t.Fatal("expected missing voice/character to fail")
	}
}

func TestIndexTTSVLLMRequiresVoiceBeforeNetwork(t *testing.T) {
	provider := NewIndexTTSVLLMProvider(map[string]interface{}{})

	if _, err := provider.TextToSpeechStream(context.Background(), "hello", 16000, 1, 20); err == nil {
		t.Fatal("expected missing voice to fail")
	}
}
