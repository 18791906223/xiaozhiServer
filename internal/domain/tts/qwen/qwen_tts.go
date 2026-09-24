package qwen

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"xiaozhi-esp32-server-golang/internal/data/audio"
	"xiaozhi-esp32-server-golang/internal/util"
	log "xiaozhi-esp32-server-golang/logger"

	"github.com/gopxl/beep"
	sse "github.com/tmaxmax/go-sse"
)

const (
	defaultAPIURLBeijing    = "https://dashscope.aliyuncs.com/api/v1/services/aigc/multimodal-generation/generation"
	defaultAPIURLSingapore  = "https://dashscope-intl.aliyuncs.com/api/v1/services/aigc/multimodal-generation/generation"
	defaultQwenModel        = "qwen3-tts-flash"
	defaultQwenVoice        = "Cherry"
	defaultQwenLanguageType = "Chinese"
)

// 全局HTTP客户端，实现连接池
var (
	httpClient     *http.Client
	httpClientOnce sync.Once
)

// 获取配置了连接池的HTTP客户端
func getHTTPClient() *http.Client {
	httpClientOnce.Do(func() {
		transport := &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   10,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}
		httpClient = &http.Client{
			Transport: transport,
			Timeout:   60 * time.Second,
		}
	})
	return httpClient
}

// QwenTTSProvider 阿里云千问 TTS 提供者
type QwenTTSProvider struct {
	APIKey        string
	APIURL        string
	Model         string
	Voice         string
	LanguageType  string
	Stream        bool
	FrameDuration int
}

// qwenRequest 请求结构体
type qwenRequest struct {
	Model string           `json:"model"`
	Input qwenRequestInput `json:"input"`
}

type qwenRequestInput struct {
	Text         string `json:"text"`
	Voice        string `json:"voice"`
	LanguageType string `json:"language_type,omitempty"`
}

// qwenResponse 非流式/流式统一响应结构
type qwenResponse struct {
	StatusCode int        `json:"status_code"`
	RequestID  string     `json:"request_id"`
	Code       string     `json:"code"`
	Message    string     `json:"message"`
	Output     qwenOutput `json:"output"`
	Usage      qwenUsage  `json:"usage"`
}

type qwenOutput struct {
	Text         interface{}   `json:"text"`
	FinishReason string        `json:"finish_reason"`
	Choices      interface{}   `json:"choices"`
	Audio        qwenAudioInfo `json:"audio"`
}

type qwenAudioInfo struct {
	Data      string `json:"data"`       // 流式输出时的 Base64 音频数据（16bit PCM）
	URL       string `json:"url"`        // 非流式输出的 WAV URL
	ID        string `json:"id"`         // 音频 ID
	ExpiresAt int64  `json:"expires_at"` // URL 过期时间戳
}

type qwenUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
	Characters   int `json:"characters"`
}

// NewQwenTTSProvider 创建新的阿里云千问 TTS 提供者
func NewQwenTTSProvider(config map[string]interface{}) *QwenTTSProvider {
	apiKey, _ := config["api_key"].(string)
	apiURL, _ := config["api_url"].(string)
	model, _ := config["model"].(string)
	voice, _ := config["voice"].(string)
	languageType, _ := config["language_type"].(string)
	stream, _ := config["stream"].(bool)
	frameDuration, _ := config["frame_duration"].(float64)
	region, _ := config["region"].(string)

	// 处理 API URL / 地域
	if apiURL == "" {
		if strings.EqualFold(region, "singapore") {
			apiURL = defaultAPIURLSingapore
		} else {
			apiURL = defaultAPIURLBeijing
		}
	}

	// 默认值
	if model == "" {
		model = defaultQwenModel
	}
	if voice == "" {
		voice = defaultQwenVoice
	}
	if languageType == "" {
		languageType = defaultQwenLanguageType
	}
	if frameDuration == 0 {
		frameDuration = audio.FrameDuration
	}

	return &QwenTTSProvider{
		APIKey:        apiKey,
		APIURL:        apiURL,
		Model:         model,
		Voice:         voice,
		LanguageType:  languageType,
		Stream:        stream,
		FrameDuration: int(frameDuration),
	}
}

// TextToSpeech 非流式文本转语音：调用 HTTP 接口，下载 WAV 并解码为帧
func (p *QwenTTSProvider) TextToSpeech(ctx context.Context, text string, sampleRate int, channels int, frameDuration int) ([][]byte, error) {
	startTs := time.Now().UnixMilli()

	// 构造请求体
	reqBody := qwenRequest{
		Model: p.Model,
		Input: qwenRequestInput{
			Text:         text,
			Voice:        p.Voice,
			LanguageType: p.LanguageType,
		},
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("序列化请求失败: %v", err)
	}

	// 创建HTTP请求
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.APIURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", p.APIKey))

	client := getHTTPClient()
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("发送请求失败: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API请求失败，状态码: %d, 响应: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %v", err)
	}

	var ttsResp qwenResponse
	if err := json.Unmarshal(body, &ttsResp); err != nil {
		return nil, fmt.Errorf("解析响应失败: %v, 响应体: %s", err, string(body))
	}

	if ttsResp.StatusCode != 200 {
		return nil, fmt.Errorf("千问 TTS API 错误 [%s]: %s", ttsResp.Code, ttsResp.Message)
	}

	if ttsResp.Output.Audio.URL == "" {
		return nil, fmt.Errorf("响应中未包含音频 URL")
	}

	log.Debugf("千问 TTS 非流式，下载音频 URL: %s", ttsResp.Output.Audio.URL)

	// 下载 WAV，并通过通用解码器转为帧
	wavReq, err := http.NewRequestWithContext(ctx, http.MethodGet, ttsResp.Output.Audio.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("创建音频下载请求失败: %v", err)
	}

	wavResp, err := client.Do(wavReq)
	if err != nil {
		return nil, fmt.Errorf("下载音频失败: %v", err)
	}
	defer wavResp.Body.Close()

	if wavResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(wavResp.Body)
		return nil, fmt.Errorf("下载音频失败，状态码: %d, 响应: %s", wavResp.StatusCode, string(body))
	}

	outputChan := make(chan []byte, 1000)

	decoder, err := util.CreateAudioDecoderWithSampleRate(ctx, wavResp.Body, outputChan, frameDuration, "wav", sampleRate)
	if err != nil {
		return nil, fmt.Errorf("创建千问音频解码器失败: %v", err)
	}

	// 启动解码
	go func() {
		if err := decoder.Run(startTs); err != nil {
			log.Errorf("千问 TTS 非流式音频解码失败: %v", err)
		}
	}()

	var frames [][]byte
	for frame := range outputChan {
		frames = append(frames, frame)
	}

	log.Debugf("千问 TTS 非流式完成，从输入到获取音频数据结束耗时: %d ms", time.Now().UnixMilli()-startTs)
	return frames, nil
}

// TextToSpeechStream 流式文本转语音实现
func (p *QwenTTSProvider) TextToSpeechStream(ctx context.Context, text string, sampleRate int, channels int, frameDuration int) (outputChan chan []byte, err error) {

	startTs := time.Now().UnixMilli()

	// 构造请求体
	reqBody := qwenRequest{
		Model: p.Model,
		Input: qwenRequestInput{
			Text:         text,
			Voice:        p.Voice,
			LanguageType: p.LanguageType,
		},
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("序列化请求失败: %v", err)
	}

	// 创建HTTP请求
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.APIURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", p.APIKey))
	req.Header.Set("X-DashScope-SSE", "enable") // 启用流式输出

	client := getHTTPClient()

	outputChan = make(chan []byte, 100)

	go func() {

		resp, err := client.Do(req)
		if err != nil {
			log.Errorf("发送千问流式请求失败: %v", err)
			close(outputChan)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			log.Errorf("千问流式 API请求失败，状态码: %d, 响应: %s", resp.StatusCode, string(body))
			close(outputChan)
			return
		}

		contentType := resp.Header.Get("Content-Type")
		if !strings.Contains(contentType, "text/event-stream") {
			log.Warnf("千问流式 API返回的Content-Type不是text/event-stream: %s", contentType)
			close(outputChan)
			return
		}

		// 管道：解析 SSE -> PCM -> 解码为帧
		pipeReader, pipeWriter := io.Pipe()

		// 解析 SSE，写入原始 PCM 数据。
		// Qwen 流式返回的 audio.data 在实测中可能携带一次 WAV 头，需先剥离再按 PCM 处理。
		go func() {
			defer func() {
				if err := pipeWriter.Close(); err != nil {
					log.Debugf("关闭千问管道写入端失败: %v", err)
				}
			}()

			if err := p.parseEventStream(ctx, resp.Body, pipeWriter, text); err != nil {
				log.Errorf("解析千问 Event Stream 失败: %v", err)
			}
		}()

		// 创建音频解码器，从管道读取 PCM，输出 opus 帧
		decoder, err := util.CreateAudioDecoderWithSampleRate(
			ctx,
			pipeReader,
			outputChan,
			frameDuration,
			"pcm", // parseEventStream 会在需要时剥离 WAV 头，输出纯 16bit PCM
			sampleRate,
		)
		if err != nil {
			log.Errorf("创建千问流式音频解码器失败: %v", err)
			close(outputChan)
			pipeReader.Close()
			return
		}

		// 告诉解码器 PCM 的采样率/声道信息
		decoder.WithFormat(beep.Format{
			SampleRate:  beep.SampleRate(24000),
			NumChannels: 1,
		})

		// decoder.Run() 内部会关闭 outputChan
		// 使用 sync.Once 确保即使 decoder.Run() 关闭了 channel，defer 也不会重复关闭
		if err := decoder.Run(startTs); err != nil {
			log.Errorf("千问流式音频解码失败: %v", err)
			return
		}

		// 如果 decoder.Run() 成功完成，它会关闭 channel
		// 所以这里需要取消 defer 的关闭操作（通过 sync.Once 已经处理了）

		select {
		case <-ctx.Done():
			log.Debugf("千问 TTS流式合成取消, 文本: %s", text)
			return
		default:
			log.Debugf("千问 TTS流式耗时: 从输入至获取音频数据结束耗时: %d ms", time.Now().UnixMilli()-startTs)
		}
	}()

	return outputChan, nil
}

// parseEventStream 使用 go-sse 解析阿里云千问的 SSE，解码 Base64 PCM 并写入管道
func (p *QwenTTSProvider) parseEventStream(ctx context.Context, reader io.Reader, writer *io.PipeWriter, text string) error {
	var leadingAudio bytes.Buffer
	wroteLeadingAudio := false

	for ev, evErr := range sse.Read(reader, nil) {
		if evErr != nil {
			return fmt.Errorf("读取千问 SSE 事件失败: %w", evErr)
		}

		select {
		case <-ctx.Done():
			log.Debugf("千问 TTS流式合成取消, 文本: %s", text)
			return ctx.Err()
		default:
		}

		dataValue := strings.TrimSpace(ev.Data)
		if dataValue == "" {
			continue
		}

		var eventResp qwenResponse
		if err := json.Unmarshal([]byte(dataValue), &eventResp); err != nil {
			log.Warnf("解析千问 Event Stream JSON 失败: %v, 数据: %s", err, previewString(dataValue, 200))
			continue
		}

		// 检查业务状态码（流式 data 里可能不包含 status_code，未包含时为 0，视为成功）
		if eventResp.StatusCode != 0 && eventResp.StatusCode != 200 {
			return fmt.Errorf("千问流式 API 错误 [%s]: %s", eventResp.Code, eventResp.Message)
		}

		// 解码 Base64 PCM 数据
		if eventResp.Output.Audio.Data != "" {
			encoded := cleanBase64(eventResp.Output.Audio.Data)
			audioBytes, err := base64.StdEncoding.DecodeString(encoded)
			if err != nil {
				log.Errorf("解码千问 Base64 PCM 失败: %v", err)
				continue
			}

			if len(audioBytes) > 0 {
				if !wroteLeadingAudio {
					leadingAudio.Write(audioBytes)
					normalized, needMore, detectedWAV, err := normalizeLeadingQwenAudio(leadingAudio.Bytes())
					if err != nil {
						return fmt.Errorf("解析千问流式音频头失败: %w", err)
					}
					if needMore {
						continue
					}
					wroteLeadingAudio = true
					if detectedWAV {
						log.Infof("千问流式音频检测到 WAV 头，已剥离后按 PCM 处理")
					}
					if len(normalized) == 0 {
						continue
					}
					if _, err := writer.Write(normalized); err != nil {
						return fmt.Errorf("写入 PCM 到管道失败: %v", err)
					}
					continue
				}

				if _, err := writer.Write(audioBytes); err != nil {
					return fmt.Errorf("写入 PCM 到管道失败: %v", err)
				}
			}
		}

		// 检查是否完成
		if eventResp.Output.FinishReason == "stop" {
			log.Debugf("千问流式收到 finish_reason=stop，请求 ID: %s", eventResp.RequestID)
			return nil
		}
	}

	return nil
}

func normalizeLeadingQwenAudio(data []byte) (normalized []byte, needMore bool, detectedWAV bool, err error) {
	if len(data) < 12 {
		return nil, true, false, nil
	}

	if !bytes.HasPrefix(data, []byte("RIFF")) || !bytes.Equal(data[8:12], []byte("WAVE")) {
		return data, false, false, nil
	}

	offset, needMore, err := qwenWAVDataOffset(data)
	if err != nil {
		return nil, false, true, err
	}
	if needMore {
		return nil, true, true, nil
	}
	if offset > len(data) {
		return nil, false, true, fmt.Errorf("WAV data offset 越界: %d > %d", offset, len(data))
	}
	return data[offset:], false, true, nil
}

func qwenWAVDataOffset(data []byte) (offset int, needMore bool, err error) {
	if len(data) < 12 {
		return 0, true, nil
	}
	if !bytes.HasPrefix(data, []byte("RIFF")) || !bytes.Equal(data[8:12], []byte("WAVE")) {
		return 0, false, fmt.Errorf("不是有效的 WAV 头")
	}

	offset = 12
	for {
		if len(data) < offset+8 {
			return 0, true, nil
		}

		chunkID := string(data[offset : offset+4])
		chunkSize := int(binary.LittleEndian.Uint32(data[offset+4 : offset+8]))
		if chunkSize < 0 {
			return 0, false, fmt.Errorf("非法 WAV chunk size: %d", chunkSize)
		}
		offset += 8

		if chunkID == "data" {
			return offset, false, nil
		}

		nextOffset := offset + chunkSize
		if chunkSize%2 == 1 {
			nextOffset++
		}
		if len(data) < nextOffset {
			return 0, true, nil
		}
		offset = nextOffset
	}
}

// SetVoice 设置音色
func (p *QwenTTSProvider) SetVoice(voiceConfig map[string]interface{}) error {
	if voice, ok := voiceConfig["voice"].(string); ok && voice != "" {
		p.Voice = voice
		return nil
	}
	return fmt.Errorf("无效的音色配置: 缺少 voice")
}

// Close 关闭资源（无状态 Provider，无需关闭）
func (p *QwenTTSProvider) Close() error {
	return nil
}

// IsValid 检查资源是否有效
func (p *QwenTTSProvider) IsValid() bool {
	return p != nil
}

// cleanBase64 移除 Base64 字符串中的所有空白字符
func cleanBase64(s string) string {
	if s == "" {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t' {
			continue
		}
		b.WriteByte(ch)
	}
	return b.String()
}

// previewString 返回字符串的前 n 个字符用于日志
func previewString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
