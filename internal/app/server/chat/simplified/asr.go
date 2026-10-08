package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"strings"
	"time"
)

// ============================================================================
// ASR 的最小接口
// ============================================================================

// asrProvider 是语音识别（ASR）的最小接口。
//
// 对应真实实现 internal/domain/asr/base.go 的 AsrProvider：
//
//	type AsrProvider interface {
//	    Process(pcmData []float32) (string, error)
//	    StreamingRecognize(ctx context.Context, audioStream <-chan []float32) (chan types.StreamingResult, error)
//	    Close() error
//	    IsValid() bool
//	}
//
// 真实实现要同时支持「流式识别」（边说边出字，用来做实时字幕和提前触发 LLM）
// 和「整段识别」，还要管连接池、重试、空结果原因上报。
// 简化版只保留最直观的一种：收完整段 PCM，返回一句话。
type asrProvider interface {
	// Recognize 识别一整段语音。
	// pcm 为 16kHz 单声道 float32，取值约 [-1,1]。
	Recognize(ctx context.Context, pcm []float32) (string, error)
	// Name 用于日志，让你一眼看出当前跑的是哪个实现。
	Name() string
}

// ============================================================================
// 实现一：内置模拟识别
// ============================================================================

// mockASR 不联网、不需要 Key，只回一句带时长的占位文本。
// 它的意义是：在没有任何 API Key 的情况下，整条链路也能跑通、能听到声音。
type mockASR struct{}

func (m *mockASR) Name() string { return "mock" }

func (m *mockASR) Recognize(_ context.Context, pcm []float32) (string, error) {
	ms := len(pcm) * 1000 / inAudioFormat.SampleRate
	return fmt.Sprintf("[模拟识别] 我听到了约 %d 毫秒的语音", ms), nil
}

// ============================================================================
// 实现二：OpenAI 兼容的 HTTP 识别接口
// ============================================================================

// openAIASR 调用的接口长这样：
//
//	POST {baseURL}/audio/transcriptions
//	Content-Type: multipart/form-data
//	  file=<wav 文件>
//	  model=<模型名>
//	→ {"text": "识别出来的文字"}
//
// 硅基流动、阿里云百炼、官方 OpenAI、自建的 faster-whisper 网关……
// 一大批服务都实现这个协议，所以一个实现能对接很多家。
// 真实实现里对接各家厂商 SDK 做的是同一件事，只是字段更多、更细。
type openAIASR struct {
	baseURL string
	apiKey  string
	model   string
	client  *http.Client
}

func newOpenAIASR(baseURL, apiKey, model string) *openAIASR {
	return &openAIASR{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		model:   model,
		client:  &http.Client{Timeout: 30 * time.Second},
	}
}

func (a *openAIASR) Name() string { return "openai-compatible:" + a.model }

func (a *openAIASR) Recognize(ctx context.Context, pcm []float32) (string, error) {
	// 1. 打包成 WAV。这类 HTTP 接口几乎都只认容器格式，不收裸 PCM，
	//    所以要临时套一个 44 字节的 WAV 头。
	wav := pcmToWAV(pcm, inAudioFormat.SampleRate, inAudioFormat.Channels)

	// 2. 拼 multipart 请求体
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)

	part, err := mw.CreateFormFile("file", "audio.wav")
	if err != nil {
		return "", err
	}
	if _, err := part.Write(wav); err != nil {
		return "", err
	}
	_ = mw.WriteField("model", a.model)
	_ = mw.WriteField("response_format", "json")
	if err := mw.Close(); err != nil {
		return "", err
	}

	// 3. 发请求
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		a.baseURL+"/audio/transcriptions", &body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+a.apiKey)

	resp, err := a.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("请求识别服务失败: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("识别服务返回 %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var out struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("解析识别结果失败: %w，原文: %s", err, raw)
	}
	return strings.TrimSpace(out.Text), nil
}

// ============================================================================
// 组装
// ============================================================================

// newASR 按配置选择 ASR 实现。
// 对应真实实现 internal/domain/asr/base.go 的 NewAsrProvider
// 加 internal/pool 的资源池选型逻辑。
func (c *config) newASR() asrProvider {
	if c.asrURL == "" || c.asrKey == "" {
		log.Print("ASR：未配置 -asr-url / -asr-key，使用内置模拟识别")
		return &mockASR{}
	}
	model := c.asrModel
	if model == "" {
		model = "whisper-1"
	}
	log.Printf("ASR：使用 OpenAI 兼容接口 %s（模型 %s）", c.asrURL, model)
	return newOpenAIASR(c.asrURL, c.asrKey, model)
}

// pcmToWAV 把 float32 PCM 打包成 16bit WAV。
//
// 格式固定的 44 字节头 + 小端 16bit 采样：
//
//	"RIFF" <总长> "WAVE" "fmt " <16> <1=PCM> <声道> <采样率> <字节率> <块对齐> <位深> "data" <数据长>
func pcmToWAV(pcm []float32, sampleRate, channels int) []byte {
	const bitsPerSample = 16
	dataSize := len(pcm) * bitsPerSample / 8

	buf := new(bytes.Buffer)
	buf.WriteString("RIFF")
	_ = binary.Write(buf, binary.LittleEndian, uint32(36+dataSize))
	buf.WriteString("WAVE")

	buf.WriteString("fmt ")
	_ = binary.Write(buf, binary.LittleEndian, uint32(16))                                  // chunk 长度
	_ = binary.Write(buf, binary.LittleEndian, uint16(1))                                   // 1 = PCM
	_ = binary.Write(buf, binary.LittleEndian, uint16(channels))                            // 声道数
	_ = binary.Write(buf, binary.LittleEndian, uint32(sampleRate))                          // 采样率
	_ = binary.Write(buf, binary.LittleEndian, uint32(sampleRate*channels*bitsPerSample/8)) // 字节率
	_ = binary.Write(buf, binary.LittleEndian, uint16(channels*bitsPerSample/8))            // 块对齐
	_ = binary.Write(buf, binary.LittleEndian, uint16(bitsPerSample))                       // 位深

	buf.WriteString("data")
	_ = binary.Write(buf, binary.LittleEndian, uint32(dataSize))
	for _, s := range pcm {
		_ = binary.Write(buf, binary.LittleEndian, floatToInt16(s))
	}
	return buf.Bytes()
}
