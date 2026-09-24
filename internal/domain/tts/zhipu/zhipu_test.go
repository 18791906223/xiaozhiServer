package zhipu

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"testing"

	"xiaozhi-esp32-server-golang/internal/data/audio"
)

func TestNewZhipuTTSProviderDefaultsAndSetVoice(t *testing.T) {
	provider := NewZhipuTTSProvider(map[string]interface{}{})

	if provider.APIURL != "https://open.bigmodel.cn/api/paas/v4/audio/speech" {
		t.Fatalf("APIURL = %q", provider.APIURL)
	}
	if provider.Model != "glm-tts" {
		t.Fatalf("Model = %q", provider.Model)
	}
	if provider.Voice != "tongtong" {
		t.Fatalf("Voice = %q", provider.Voice)
	}
	if provider.ResponseFormat != "pcm" {
		t.Fatalf("ResponseFormat = %q", provider.ResponseFormat)
	}
	if provider.Speed != 1.0 {
		t.Fatalf("Speed = %v", provider.Speed)
	}
	if provider.Volume != 1.0 {
		t.Fatalf("Volume = %v", provider.Volume)
	}
	if provider.EncodeFormat != "base64" {
		t.Fatalf("EncodeFormat = %q", provider.EncodeFormat)
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

	if err := provider.SetVoice(map[string]interface{}{"voice": "xiaoxiao"}); err != nil {
		t.Fatalf("SetVoice error = %v", err)
	}
	if provider.Voice != "xiaoxiao" {
		t.Fatalf("Voice = %q", provider.Voice)
	}
	if err := provider.SetVoice(map[string]interface{}{}); err == nil {
		t.Fatal("expected missing voice to fail")
	}
}

func TestDecodeAudioContentBase64AndHex(t *testing.T) {
	provider := &ZhipuTTSProvider{EncodeFormat: "base64"}
	payload := []byte{1, 2, 3}

	got, err := provider.decodeAudioContent(base64.StdEncoding.EncodeToString(payload))
	if err != nil {
		t.Fatalf("decode base64 error = %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("decoded base64 = %v", got)
	}

	provider.EncodeFormat = "hex"
	got, err = provider.decodeAudioContent(hex.EncodeToString(payload))
	if err != nil {
		t.Fatalf("decode hex error = %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("decoded hex = %v", got)
	}
}

func TestApplyPCM16MonoLeadingFadeInClonesAndScalesLeadingSamples(t *testing.T) {
	data := pcm16Samples(1000, 1000, 1000)

	faded := applyPCM16MonoLeadingFadeIn(data, 3)
	if string(data) == string(faded) {
		t.Fatal("expected faded data to differ")
	}
	if binary.LittleEndian.Uint16(data[0:2]) != 1000 {
		t.Fatal("input data should not be modified")
	}

	got := []uint16{
		binary.LittleEndian.Uint16(faded[0:2]),
		binary.LittleEndian.Uint16(faded[2:4]),
		binary.LittleEndian.Uint16(faded[4:6]),
	}
	want := []uint16{0, 333, 666}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sample %d = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestLeadingFadeInSampleCountHasMinimum(t *testing.T) {
	if got := leadingFadeInSampleCount(0, 5); got != 120 {
		t.Fatalf("default sample count = %d", got)
	}
	if got := leadingFadeInSampleCount(16000, 0); got != 0 {
		t.Fatalf("zero fade sample count = %d", got)
	}
}

func pcm16Samples(samples ...int16) []byte {
	out := make([]byte, len(samples)*2)
	for i, sample := range samples {
		binary.LittleEndian.PutUint16(out[i*2:i*2+2], uint16(sample))
	}
	return out
}
