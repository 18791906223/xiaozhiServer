package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"
)

// ============================================================================
// TTS 的最小接口
// ============================================================================

// ttsProvider 是语音合成（TTS）的最小接口。
//
// 对应真实实现 internal/domain/tts/base.go：
//
//	type BaseTTSProvider interface {
//	    TextToSpeech(ctx, text, sampleRate, channels, frameDuration) ([][]byte, error)
//	    TextToSpeechStream(ctx, text, sampleRate, channels, frameDuration) (chan []byte, error)
//	}
//	type TTSProvider interface {
//	    BaseTTSProvider
//	    SetVoice(voiceConfig map[string]interface{}) error
//	    Close() error
//	    IsValid() bool
//	}
//
// 真实实现有十几个厂商（豆包、CosyVoice、Edge、讯飞、MiniMax、OpenAI…），
// 还有「双流」接口（DualStreamProvider）用于边出字边合成。
// 简化版只保留流式一种：给一句话，返回一串 Opus 帧。
type ttsProvider interface {
	// Synthesize 把一句话合成成 Opus 帧并逐帧吐出。
	//
	// 返回的帧必须已经按 outAudioFormat 编码好了 —— 服务端拿到的就是
	// 可以直接转发给设备的二进制帧。真实实现里各家 TTS 大多也直接
	// 返回 Opus（豆包就是），服务端基本只做搬运。
	Synthesize(ctx context.Context, text string) (<-chan []byte, error)
	// Name 用于日志。
	Name() string
}

// ============================================================================
// 实现一：内置模拟合成
// ============================================================================

// mockTTS 不真的合成语音，而是按文字长度生成一段提示音。
//
// 它的价值：让整条链路在没有任何 TTS 服务的情况下也能跑通，
// 并且你在设备上真的能听到声音 —— 这是验证「音频确实回传了」最直接的方式。
type mockTTS struct {
	codec *codec
}

func (m *mockTTS) Name() string { return "mock" }

func (m *mockTTS) Synthesize(ctx context.Context, text string) (<-chan []byte, error) {
	// 用字数估算时长：每个字 120ms，最短 300ms
	ms := utf8.RuneCountInString(text) * 120
	if ms < 300 {
		ms = 300
	}

	// 660Hz 的提示音，模拟「音箱在说话」
	pcm := tone(ms, m.codec.sampleRate, 660)

	out := make(chan []byte, 64)
	go func() {
		defer close(out)

		// 按帧切开。最后一帧不够长就补零 ——
		// Opus 只接受固定长度的帧，这是所有 TTS 实现都要处理的一步。
		frameSamples := m.codec.samplesPerFrame()
		for offset := 0; offset < len(pcm); offset += frameSamples {
			chunk := make([]float32, frameSamples)
			copy(chunk, pcm[offset:])

			frame, err := m.codec.encode(chunk)
			if err != nil {
				log.Printf("模拟 TTS 编码失败: %v", err)
				return
			}
			select {
			case out <- frame:
			case <-ctx.Done():
				return
			}
			// 加一点延迟，模拟真实合成的速度（约 10 倍速于实时）
			select {
			case <-time.After(time.Duration(m.codec.frameDuration) * time.Millisecond / 4):
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// ============================================================================
// 实现二：OpenAI 兼容的 HTTP 合成接口
// ============================================================================

// openAITTS 调用的接口长这样：
//
//	POST {baseURL}/audio/speech
//	{"model":..., "input":"要合成的文字", "voice":"alloy", "response_format":"pcm"}
//	→ 裸 PCM 字节流（16bit 小端）
//
// 这里有个真实的坑：OpenAI 的 pcm 格式固定是 24kHz，而设备要的是 16kHz，
// 所以中间必须做一次重采样。很多国产厂商（硅基流动、阿里云等）支持直接
// 指定 sample_rate，那就把 -tts-sample-rate 设成 16000，跳过重采样。
type openAITTS struct {
	baseURL    string
	apiKey     string
	model      string
	voice      string
	sourceRate int // 接口返回的采样率
	codec      *codec
	client     *http.Client
}

func newOpenAITTS(baseURL, apiKey, model, voice string, sourceRate int, outC *codec) *openAITTS {
	return &openAITTS{
		baseURL:    strings.TrimRight(baseURL, "/"),
		apiKey:     apiKey,
		model:      model,
		voice:      voice,
		sourceRate: sourceRate,
		codec:      outC,
		client:     &http.Client{Timeout: 60 * time.Second},
	}
}

func (t *openAITTS) Name() string {
	return fmt.Sprintf("openai-compatible:%s/%s(%dHz)", t.model, t.voice, t.sourceRate)
}

func (t *openAITTS) Synthesize(ctx context.Context, text string) (<-chan []byte, error) {
	payload, err := json.Marshal(map[string]any{
		"model":           t.model,
		"input":           text,
		"voice":           t.voice,
		"response_format": "pcm",
		"speed":           1.0,
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		t.baseURL+"/audio/speech", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+t.apiKey)

	resp, err := t.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求合成服务失败: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, fmt.Errorf("合成服务返回 %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	out := make(chan []byte, 64)
	go func() {
		defer close(out)
		defer resp.Body.Close()

		var rs *resampler
		if t.sourceRate != t.codec.sampleRate {
			rs = newResampler(t.sourceRate, t.codec.sampleRate)
		}
		frameSamples := t.codec.samplesPerFrame()

		var (
			pcm      []float32 // 重采样后、还没凑满一帧的 PCM
			rawRead  = make([]byte, 8192)
			carry    byte // 上次读到的、孤立的半个 16bit 采样点
			carrySet bool
			emit     = func(frame []float32) bool {
				enc, err := t.codec.encode(frame)
				if err != nil {
					log.Printf("TTS 编码失败: %v", err)
					return false
				}
				select {
				case out <- enc:
					return true
				case <-ctx.Done():
					return false
				}
			}
		)

		for {
			n, readErr := resp.Body.Read(rawRead)
			if n > 0 {
				data := rawRead[:n]
				// 16bit 采样点不能跨块断裂，先把上次剩下的半个字节接回来
				if carrySet {
					data = append([]byte{carry}, data...)
					carrySet = false
				}
				if len(data)%2 == 1 {
					carry = data[len(data)-1]
					carrySet = true
					data = data[:len(data)-1]
				}

				samples := make([]float32, len(data)/2)
				for i := range samples {
					samples[i] = int16ToFloat32(int16(binary.LittleEndian.Uint16(data[i*2:])))
				}
				pcm = append(pcm, rs.Push(samples)...)

				// 凑够一帧就立刻发出去 —— 这样第一帧音频不必等整句合成完
				for len(pcm) >= frameSamples {
					if !emit(pcm[:frameSamples]) {
						return
					}
					pcm = pcm[frameSamples:]
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				if ctx.Err() == nil {
					log.Printf("读取合成音频失败: %v", readErr)
				}
				return
			}
		}

		// 收尾：补上重采样器的尾巴，并把最后一帧补零凑满
		if rs != nil {
			pcm = append(pcm, rs.Flush()...)
		}
		if len(pcm) > 0 {
			last := make([]float32, frameSamples)
			copy(last, pcm)
			emit(last)
		}
	}()
	return out, nil
}

// ============================================================================
// 重采样
// ============================================================================

// resampler 做线性插值的采样率转换。
//
// 为什么需要：TTS 输出的采样率由厂商决定（OpenAI 的 pcm 固定 24kHz，
// 讯飞 16kHz，豆包 24kHz…），而设备只认握手时约定的那一个采样率。
// 采样率对不上，设备放出来就是快放/慢放的效果。
type resampler struct {
	step float64   // 每产出一个输出采样点，输入位置前进多少个采样点
	buf  []float32 // 还没消费完的输入
	pos  float64   // 当前读取位置（相对 buf 开头）
}

func newResampler(srcRate, dstRate int) *resampler {
	return &resampler{step: float64(srcRate) / float64(dstRate)}
}

// Push 送入一批输入采样，返回现此刻能产出的输出采样。
// 跨块调用时会保留必要的状态，所以可以按任意粒度喂数据。
func (r *resampler) Push(in []float32) []float32 {
	r.buf = append(r.buf, in...)

	var out []float32
	for {
		i := int(r.pos)
		if i+1 >= len(r.buf) {
			break // 还需要下一个输入点才能插值
		}
		frac := float32(r.pos - float64(i))
		out = append(out, r.buf[i]*(1-frac)+r.buf[i+1]*frac)
		r.pos += r.step
	}

	// 丢掉已经消费完的输入，避免 buf 无限增长
	if drop := int(r.pos); drop > 0 {
		r.buf = append([]float32(nil), r.buf[drop:]...)
		r.pos -= float64(drop)
	}
	return out
}

// Flush 输入结束时调用，把最后剩的那个采样点补上。
func (r *resampler) Flush() []float32 {
	if len(r.buf) == 0 {
		return nil
	}
	return []float32{r.buf[len(r.buf)-1]}
}

// ============================================================================
// 组装
// ============================================================================

// newTTS 按配置选择 TTS 实现。
// 对应真实实现 internal/domain/tts/base.go 的 GetTTSProvider。
func (c *config) newTTS(outC *codec) ttsProvider {
	if c.ttsURL == "" || c.ttsKey == "" {
		log.Print("TTS：未配置 -tts-url / -tts-key，使用内置模拟合成（会听到 660Hz 提示音）")
		return &mockTTS{codec: outC}
	}
	model := c.ttsModel
	if model == "" {
		model = "tts-1"
	}
	voice := c.ttsVoice
	if voice == "" {
		voice = "alloy"
	}
	log.Printf("TTS：使用 OpenAI 兼容接口 %s（模型 %s，音色 %s，输入 %dHz）",
		c.ttsURL, model, voice, c.ttsSourceRate)
	return newOpenAITTS(c.ttsURL, c.ttsKey, model, voice, c.ttsSourceRate, outC)
}
