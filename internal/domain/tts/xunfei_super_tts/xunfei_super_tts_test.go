package xunfei_super_tts

import (
	"encoding/base64"
	"encoding/binary"
	"net/url"
	"strings"
	"testing"
)

func TestNewXunfeiSuperTTSProviderDefaultsAndSetVoice(t *testing.T) {
	provider := NewXunfeiSuperTTSProvider(map[string]interface{}{})

	if provider.WSURL != defaultXunfeiSuperWSURL {
		t.Fatalf("WSURL = %q", provider.WSURL)
	}
	if provider.Voice != defaultXunfeiSuperVoice {
		t.Fatalf("Voice = %q", provider.Voice)
	}
	if provider.AudioEncoding != "raw" || provider.Encoding != "raw" {
		t.Fatalf("audio encoding = %q/%q", provider.AudioEncoding, provider.Encoding)
	}
	if provider.SampleRate != defaultXunfeiSuperSampleRate {
		t.Fatalf("SampleRate = %d", provider.SampleRate)
	}
	if provider.OralLevel != defaultXunfeiSuperOralLevel {
		t.Fatalf("OralLevel = %q", provider.OralLevel)
	}
	if !provider.IsValid() {
		t.Fatal("provider should be valid")
	}
	if err := provider.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}

	if err := provider.SetVoice(map[string]interface{}{"voice": "x4_demo"}); err != nil {
		t.Fatalf("SetVoice error = %v", err)
	}
	if provider.Voice != "x4_demo" {
		t.Fatalf("Voice = %q", provider.Voice)
	}
	if err := provider.SetVoice(map[string]interface{}{}); err == nil {
		t.Fatal("expected missing voice to fail")
	}
}

func TestMapXunfeiSuperAudioEncoding(t *testing.T) {
	tests := []struct {
		name          string
		audioEncoding string
		sampleRate    int
		wantEncoding  string
		wantPayload   int
		wantErr       bool
	}{
		{name: "raw24k", audioEncoding: "raw", sampleRate: 24000, wantEncoding: "raw"},
		{name: "opus8k", audioEncoding: "opus", sampleRate: 8000, wantEncoding: "opus", wantPayload: 20},
		{name: "opus16k", audioEncoding: "opus", sampleRate: 16000, wantEncoding: "opus-wb", wantPayload: 40},
		{name: "opus24k", audioEncoding: "opus", sampleRate: 24000, wantEncoding: "opus-swb"},
		{name: "invalidRate", audioEncoding: "raw", sampleRate: 44100, wantErr: true},
		{name: "unknown", audioEncoding: "aac", sampleRate: 24000, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotEncoding, gotPayload, err := mapXunfeiSuperAudioEncoding(tc.audioEncoding, tc.sampleRate)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("mapXunfeiSuperAudioEncoding error = %v", err)
			}
			if gotEncoding != tc.wantEncoding || gotPayload != tc.wantPayload {
				t.Fatalf("got %q/%d", gotEncoding, gotPayload)
			}
		})
	}
}

func TestMapXunfeiSuperResponseAudioFormat(t *testing.T) {
	tests := map[string]string{
		"raw":     "pcm",
		"lame":    "mp3",
		"mp3":     "mp3",
		"opus-wb": "opus",
	}
	for input, want := range tests {
		got, err := mapXunfeiSuperResponseAudioFormat(input)
		if err != nil {
			t.Fatalf("map response format %q error = %v", input, err)
		}
		if got != want {
			t.Fatalf("format %q = %q", input, got)
		}
	}
	if _, err := mapXunfeiSuperResponseAudioFormat("aac"); err == nil {
		t.Fatal("expected unsupported response format to fail")
	}
}

func TestBuildXunfeiSuperSynthesisRequests(t *testing.T) {
	provider := NewXunfeiSuperTTSProvider(map[string]interface{}{
		"app_id": "app",
		"voice":  "x4_demo",
	})

	requests, err := provider.buildSynthesisRequests(" 你好 ")
	if err != nil {
		t.Fatalf("buildSynthesisRequests error = %v", err)
	}
	if len(requests) != 1 {
		t.Fatalf("request count = %d", len(requests))
	}
	req := requests[0]
	if req.Header.AppID != "app" || req.Header.Status != 2 {
		t.Fatalf("header = %#v", req.Header)
	}
	if req.Parameter.TTS.VCN != "x4_demo" {
		t.Fatalf("VCN = %q", req.Parameter.TTS.VCN)
	}
	if req.Parameter.Oral == nil {
		t.Fatal("x4 voice should include oral params")
	}
	decoded, err := base64.StdEncoding.DecodeString(req.Payload.Text.Text)
	if err != nil {
		t.Fatalf("decode text payload error = %v", err)
	}
	if string(decoded) != "你好" {
		t.Fatalf("decoded text = %q", decoded)
	}

	if _, err := provider.buildSynthesisRequests(" "); err == nil {
		t.Fatal("expected empty text to fail")
	}
	if _, err := provider.buildSynthesisRequests(strings.Repeat("a", maxXunfeiSuperTextBytes+1)); err == nil {
		t.Fatal("expected oversized text to fail")
	}
}

func TestXunfeiSuperBuildSignedURLIncludesAuthQuery(t *testing.T) {
	provider := NewXunfeiSuperTTSProvider(map[string]interface{}{
		"api_key":    "key",
		"api_secret": "secret",
		"ws_url":     "wss://example.com/v1/private/demo?existing=1",
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

func TestDecodeXunfeiSuperOpusFrames(t *testing.T) {
	payload := []byte{1, 2, 3, 4}
	chunk := make([]byte, 2+len(payload))
	binary.LittleEndian.PutUint16(chunk[:2], uint16(len(payload)))
	copy(chunk[2:], payload)

	frames, err := decodeXunfeiSuperOpusFrames(chunk, 0)
	if err != nil {
		t.Fatalf("decode length-prefixed opus error = %v", err)
	}
	if len(frames) != 1 || string(frames[0]) != string(payload) {
		t.Fatalf("frames = %v", frames)
	}

	frames, err = decodeXunfeiSuperOpusFrames(payload, len(payload))
	if err != nil {
		t.Fatalf("decode raw payload error = %v", err)
	}
	if len(frames) != 1 || string(frames[0]) != string(payload) {
		t.Fatalf("raw frames = %v", frames)
	}
}

func TestParseXunfeiSuperCed(t *testing.T) {
	if got, ok := parseXunfeiSuperCed("ced: 12 ms"); !ok || got != 12 {
		t.Fatalf("parse ced = %d/%v", got, ok)
	}
	if _, ok := parseXunfeiSuperCed("no progress"); ok {
		t.Fatal("expected ced without digits to fail")
	}
}
