package qwen

import (
	"bytes"
	"encoding/binary"
	"testing"

	"xiaozhi-esp32-server-golang/internal/data/audio"
)

func TestNewQwenTTSProviderDefaultsAndSetVoice(t *testing.T) {
	provider := NewQwenTTSProvider(map[string]interface{}{})

	if provider.APIURL != defaultAPIURLBeijing {
		t.Fatalf("APIURL = %q", provider.APIURL)
	}
	if provider.Model != defaultQwenModel {
		t.Fatalf("Model = %q", provider.Model)
	}
	if provider.Voice != defaultQwenVoice {
		t.Fatalf("Voice = %q", provider.Voice)
	}
	if provider.LanguageType != defaultQwenLanguageType {
		t.Fatalf("LanguageType = %q", provider.LanguageType)
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

	if err := provider.SetVoice(map[string]interface{}{"voice": "Chelsie"}); err != nil {
		t.Fatalf("SetVoice error = %v", err)
	}
	if provider.Voice != "Chelsie" {
		t.Fatalf("Voice = %q", provider.Voice)
	}
	if err := provider.SetVoice(map[string]interface{}{}); err == nil {
		t.Fatal("expected missing voice to fail")
	}
}

func TestNewQwenTTSProviderSingaporeRegion(t *testing.T) {
	provider := NewQwenTTSProvider(map[string]interface{}{"region": "singapore"})
	if provider.APIURL != defaultAPIURLSingapore {
		t.Fatalf("APIURL = %q", provider.APIURL)
	}
}

func TestCleanBase64RemovesWhitespace(t *testing.T) {
	got := cleanBase64(" YWJj\nZGU=\t")
	if got != "YWJjZGU=" {
		t.Fatalf("cleanBase64 = %q", got)
	}
}

func TestNormalizeLeadingQwenAudioStripsWAVHeader(t *testing.T) {
	payload := []byte{1, 2, 3, 4}
	wav := makeTestWAV(payload)

	normalized, needMore, detectedWAV, err := normalizeLeadingQwenAudio(wav)
	if err != nil {
		t.Fatalf("normalizeLeadingQwenAudio error = %v", err)
	}
	if needMore {
		t.Fatal("needMore = true")
	}
	if !detectedWAV {
		t.Fatal("detectedWAV = false")
	}
	if !bytes.Equal(normalized, payload) {
		t.Fatalf("normalized = %v", normalized)
	}
}

func TestNormalizeLeadingQwenAudioKeepsPCM(t *testing.T) {
	pcm := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}

	normalized, needMore, detectedWAV, err := normalizeLeadingQwenAudio(pcm)
	if err != nil {
		t.Fatalf("normalizeLeadingQwenAudio error = %v", err)
	}
	if needMore || detectedWAV {
		t.Fatalf("needMore=%v detectedWAV=%v", needMore, detectedWAV)
	}
	if !bytes.Equal(normalized, pcm) {
		t.Fatalf("normalized = %v", normalized)
	}
}

func makeTestWAV(payload []byte) []byte {
	var buf bytes.Buffer
	buf.WriteString("RIFF")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(36+len(payload)))
	buf.WriteString("WAVE")
	buf.WriteString("fmt ")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(16))
	_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
	_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(24000))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(48000))
	_ = binary.Write(&buf, binary.LittleEndian, uint16(2))
	_ = binary.Write(&buf, binary.LittleEndian, uint16(16))
	buf.WriteString("data")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(len(payload)))
	buf.Write(payload)
	return buf.Bytes()
}
