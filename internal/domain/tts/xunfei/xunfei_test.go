package xunfei

import (
	"encoding/binary"
	"net/url"
	"strings"
	"testing"
)

func TestNewXunfeiTTSProviderDefaultsAndSetVoice(t *testing.T) {
	provider := NewXunfeiTTSProvider(map[string]interface{}{})

	if provider.WSURL != defaultXunfeiWSURL {
		t.Fatalf("WSURL = %q", provider.WSURL)
	}
	if provider.Voice != defaultXunfeiVoice {
		t.Fatalf("Voice = %q", provider.Voice)
	}
	if provider.AudioEncoding != "raw" || provider.AUE != "raw" {
		t.Fatalf("audio encoding = %q/%q", provider.AudioEncoding, provider.AUE)
	}
	if provider.SampleRate != defaultXunfeiSampleRate {
		t.Fatalf("SampleRate = %d", provider.SampleRate)
	}
	if provider.Speed != defaultXunfeiSpeed {
		t.Fatalf("Speed = %d", provider.Speed)
	}
	if provider.Volume != defaultXunfeiVolume {
		t.Fatalf("Volume = %d", provider.Volume)
	}
	if provider.Pitch != defaultXunfeiPitch {
		t.Fatalf("Pitch = %d", provider.Pitch)
	}
	if !provider.IsValid() {
		t.Fatal("provider should be valid")
	}
	if err := provider.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}

	if err := provider.SetVoice(map[string]interface{}{"voice": "aisjiuxu"}); err != nil {
		t.Fatalf("SetVoice error = %v", err)
	}
	if provider.Voice != "aisjiuxu" {
		t.Fatalf("Voice = %q", provider.Voice)
	}
	if err := provider.SetVoice(map[string]interface{}{}); err == nil {
		t.Fatal("expected missing voice to fail")
	}
}

func TestMapXunfeiAudioEncoding(t *testing.T) {
	tests := []struct {
		name          string
		audioEncoding string
		sampleRate    int
		wantAUE       string
		wantPayload   int
		wantErr       bool
	}{
		{name: "raw16k", audioEncoding: "raw", sampleRate: 16000, wantAUE: "raw"},
		{name: "opus8k", audioEncoding: "opus", sampleRate: 8000, wantAUE: "opus", wantPayload: 20},
		{name: "opus16k", audioEncoding: "opus", sampleRate: 16000, wantAUE: "opus-wb", wantPayload: 40},
		{name: "rawInvalidRate", audioEncoding: "raw", sampleRate: 24000, wantErr: true},
		{name: "unknown", audioEncoding: "mp3", sampleRate: 16000, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotAUE, gotPayload, err := mapXunfeiAudioEncoding(tc.audioEncoding, tc.sampleRate)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("mapXunfeiAudioEncoding error = %v", err)
			}
			if gotAUE != tc.wantAUE || gotPayload != tc.wantPayload {
				t.Fatalf("got %q/%d", gotAUE, gotPayload)
			}
		})
	}
}

func TestXunfeiBuildSignedURLIncludesAuthQuery(t *testing.T) {
	provider := NewXunfeiTTSProvider(map[string]interface{}{
		"api_key":    "key",
		"api_secret": "secret",
		"ws_url":     "wss://example.com/v2/tts?existing=1",
	})

	signedURL, err := provider.buildSignedURL()
	if err != nil {
		t.Fatalf("buildSignedURL error = %v", err)
	}

	parsed, err := url.Parse(signedURL)
	if err != nil {
		t.Fatalf("parse signed url error = %v", err)
	}
	query := parsed.Query()
	if parsed.Scheme != "wss" || parsed.Host != "example.com" {
		t.Fatalf("signed url = %s", signedURL)
	}
	for _, key := range []string{"authorization", "date", "host", "existing"} {
		if strings.TrimSpace(query.Get(key)) == "" {
			t.Fatalf("query %s missing in %s", key, signedURL)
		}
	}
}

func TestDecodeXunfeiOpusFrames(t *testing.T) {
	provider := &XunfeiTTSProvider{ExpectedOpusPayloadLen: 0}
	payload := []byte{1, 2, 3, 4}
	chunk := make([]byte, 2+len(payload))
	binary.LittleEndian.PutUint16(chunk[:2], uint16(len(payload)))
	copy(chunk[2:], payload)

	frames, err := provider.decodeXunfeiOpusFrames(chunk)
	if err != nil {
		t.Fatalf("decodeXunfeiOpusFrames error = %v", err)
	}
	if len(frames) != 1 || string(frames[0]) != string(payload) {
		t.Fatalf("frames = %v", frames)
	}

	provider.ExpectedOpusPayloadLen = len(payload)
	frames, err = provider.decodeXunfeiOpusFrames(payload)
	if err != nil {
		t.Fatalf("decode single payload error = %v", err)
	}
	if len(frames) != 1 || string(frames[0]) != string(payload) {
		t.Fatalf("single payload frames = %v", frames)
	}
}
