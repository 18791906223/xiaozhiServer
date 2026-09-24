package edge

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestNewEdgeTTSProviderDefaultsAndSetVoice(t *testing.T) {
	provider := NewEdgeTTSProvider(map[string]interface{}{})

	if provider.Rate != "+0%" {
		t.Fatalf("Rate = %q", provider.Rate)
	}
	if provider.Volume != "+0%" {
		t.Fatalf("Volume = %q", provider.Volume)
	}
	if provider.Pitch != "+0Hz" {
		t.Fatalf("Pitch = %q", provider.Pitch)
	}
	if provider.ConnectTimeout != 10 {
		t.Fatalf("ConnectTimeout = %d", provider.ConnectTimeout)
	}
	if provider.ReceiveTimeout != 60 {
		t.Fatalf("ReceiveTimeout = %d", provider.ReceiveTimeout)
	}
	if !provider.IsValid() {
		t.Fatal("provider should be valid")
	}
	if err := provider.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}

	if err := provider.SetVoice(map[string]interface{}{"voice": "zh-CN-YunxiNeural"}); err != nil {
		t.Fatalf("SetVoice error = %v", err)
	}
	if provider.Voice != "zh-CN-YunxiNeural" {
		t.Fatalf("Voice = %q", provider.Voice)
	}
	if err := provider.SetVoice(map[string]interface{}{}); err == nil {
		t.Fatal("expected missing voice to fail")
	}
}

func TestEdgeTTSProvider(t *testing.T) {
	if os.Getenv("RUN_EDGE_TEST") != "1" {
		t.Skip("跳过 Edge 在线 TTS 测试，设置 RUN_EDGE_TEST=1 以启用")
	}

	config := map[string]interface{}{
		"voice":           "zh-CN-XiaoxiaoNeural",
		"rate":            "+0%",
		"volume":          "+0%",
		"pitch":           "+0Hz",
		"connect_timeout": 10,
		"receive_timeout": 60,
	}

	provider := NewEdgeTTSProvider(config)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	t.Run("TestTextToSpeech", func(t *testing.T) {
		frames, err := provider.TextToSpeech(ctx, "你好，EdgeTTS测试", 16000, 1, 60)
		if err != nil {
			t.Fatalf("TextToSpeech失败: %v", err)
		}
		if len(frames) == 0 {
			t.Error("未返回任何音频帧")
		}
	})

	t.Run("TestTextToSpeechStream", func(t *testing.T) {
		outputChan, err := provider.TextToSpeechStream(ctx, "你好，EdgeTTS流式测试", 16000, 1, 60)
		if err != nil {
			t.Fatalf("TextToSpeechStream失败: %v", err)
		}
		var receivedFrames [][]byte
		timeout := time.After(20 * time.Second)
	ReceiveLoop:
		for {
			select {
			case frame, ok := <-outputChan:
				if !ok {
					break ReceiveLoop
				}
				receivedFrames = append(receivedFrames, frame)
			case <-timeout:
				t.Error("接收音频帧超时")
				break ReceiveLoop
			}
		}
		if len(receivedFrames) == 0 {
			t.Error("未接收到任何音频帧")
		}
	})
}
