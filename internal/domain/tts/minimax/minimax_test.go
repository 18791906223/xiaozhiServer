package minimax

import "testing"

func TestNewMinimaxTTSProviderDefaultsAndLifecycle(t *testing.T) {
	provider := NewMinimaxTTSProvider(map[string]interface{}{})

	if provider.Model != "speech-2.8-hd" {
		t.Fatalf("Model = %q", provider.Model)
	}
	if provider.Voice != "male-qn-qingse" {
		t.Fatalf("Voice = %q", provider.Voice)
	}
	if provider.Speed != 1.0 {
		t.Fatalf("Speed = %v", provider.Speed)
	}
	if provider.Volume != 1.0 {
		t.Fatalf("Volume = %v", provider.Volume)
	}
	if provider.SampleRate != 32000 {
		t.Fatalf("SampleRate = %d", provider.SampleRate)
	}
	if provider.Bitrate != 128000 {
		t.Fatalf("Bitrate = %d", provider.Bitrate)
	}
	if provider.Format != "mp3" {
		t.Fatalf("Format = %q", provider.Format)
	}
	if provider.Channel != 1 {
		t.Fatalf("Channel = %d", provider.Channel)
	}
	if provider.IsValid() {
		t.Fatal("new provider should not be valid until websocket connection exists")
	}
	if err := provider.SetVoice(map[string]interface{}{"voice": "ignored"}); err != nil {
		t.Fatalf("SetVoice error = %v", err)
	}
	if err := provider.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}
}
