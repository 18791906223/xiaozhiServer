package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gopxl/beep"

	"xiaozhi-esp32-server-golang/internal/data/audio"
	"xiaozhi-esp32-server-golang/internal/util"
	log "xiaozhi-esp32-server-golang/logger"
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
			Timeout:   60 * time.Second, // OpenAI TTS 可能需要更长时间
		}
	})
	return httpClient
}

// OpenAITTSProvider OpenAI TTS提供者
type OpenAITTSProvider struct {
	APIKey         string
	APIURL         string
	Model          string
	Voice          string
	ResponseFormat string
	Speed          float64
	Stream         bool
	FrameDuration  int
}

// 请求结构体
type openAIRequest struct {
	Model          string  `json:"model"`
	Input          string  `json:"input"`
	Voice          string  `json:"voice"`
	ResponseFormat string  `json:"response_format,omitempty"`
	Speed          float64 `json:"speed,omitempty"`
	Stream         bool    `json:"stream,omitempty"`
}

// NewOpenAITTSProvider 创建新的OpenAI TTS提供者
func NewOpenAITTSProvider(config map[string]interface{}) *OpenAITTSProvider {
	apiKey, _ := config["api_key"].(string)
	apiURL, _ := config["api_url"].(string)
	model, _ := config["model"].(string)
	voice, _ := config["voice"].(string)
	responseFormat, _ := config["response_format"].(string)
	speed, _ := config["speed"].(float64)
	stream, _ := config["stream"].(bool)
	frameDuration, _ := config["frame_duration"].(float64)

	// 设置默认值
	if apiURL == "" {
		apiURL = "https://api.openai.com/v1/audio/speech"
	}
	if model == "" {
		model = "tts-1" // tts-1 或 tts-1-hd
	}
	if voice == "" {
		voice = "alloy" // alloy, echo, fable, onyx, nova, shimmer
	}
	if responseFormat == "" {
		responseFormat = "mp3" // mp3, opus, aac, flac, wav, pcm
	}
	if speed == 0 {
		speed = 1.0 // 0.25 到 4.0
	}
	if frameDuration == 0 {
		frameDuration = audio.FrameDuration
	}

	return &OpenAITTSProvider{
		APIKey:         apiKey,
		APIURL:         apiURL,
		Model:          model,
		Voice:          voice,
		ResponseFormat: responseFormat,
		Stream:         stream,
		Speed:          speed,
		FrameDuration:  int(frameDuration),
	}
}

// TextToSpeech 将文本转换为语音，返回音频帧数据和错误
func (p *OpenAITTSProvider) TextToSpeech(ctx context.Context, text string, sampleRate int, channels int, frameDuration int) ([][]byte, error) {
	streamChan, err := p.TextToSpeechStream(ctx, text, sampleRate, channels, frameDuration)
	if err != nil {
		return nil, err
	}

	audioFrames := make([][]byte, 0, 32)
	for frame := range streamChan {
		audioFrames = append(audioFrames, frame)
	}
	if len(audioFrames) == 0 {
		return nil, fmt.Errorf("OpenAI TTS 返回音频为空")
	}
	return audioFrames, nil
}

// TextToSpeechStream 流式语音合成实现
func (p *OpenAITTSProvider) TextToSpeechStream(ctx context.Context, text string, sampleRate int, channels int, frameDuration int) (outputChan chan []byte, err error) {
	startTs := time.Now().UnixMilli()

	// 创建请求体
	reqBody := openAIRequest{
		Model:          p.Model,
		Input:          text,
		Voice:          p.Voice,
		ResponseFormat: p.ResponseFormat,
		Speed:          p.Speed,
		Stream:         p.Stream,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("序列化请求失败: %v", err)
	}

	//log.Debugf("OpenAI TTS请求: %s", string(jsonData))

	// 创建HTTP请求
	req, err := http.NewRequestWithContext(ctx, "POST", p.APIURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %v", err)
	}

	// 设置请求头
	req.Header.Set("Content-Type", "application/json")
	if p.APIKey != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", p.APIKey))
	}

	// 使用连接池创建客户端
	client := getHTTPClient()

	// 创建输出通道
	outputChan = make(chan []byte, 100)

	// 启动goroutine处理流式响应
	go func() {
		// 发送请求
		resp, err := client.Do(req)
		if err != nil {
			log.Errorf("发送OpenAI请求失败: %v", err)
			close(outputChan)
			return
		}
		defer resp.Body.Close()

		// 检查响应状态码
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			log.Errorf("OpenAI API请求失败，状态码: %d, 响应: %s", resp.StatusCode, string(body))
			close(outputChan)
			return
		}

		// 检查响应内容长度
		contentLength := resp.ContentLength
		log.Debugf("收到OpenAI TTS响应，Content-Length: %d", contentLength)

		// 判断Content-Length是否合理
		if contentLength == 0 {
			log.Errorf("OpenAI API返回空响应，Content-Length为0")
			close(outputChan)
			return
		}

		responseFormat := strings.ToLower(strings.TrimSpace(p.ResponseFormat))
		decoderFormat := responseFormat
		if responseFormat == "opus" {
			decoderFormat = "ogg_opus"
			contentTypeFormat := util.GetAudioFormatByMimeType(strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Type"))))
			if contentTypeFormat == "ogg_opus" || contentTypeFormat == "opus" {
				decoderFormat = contentTypeFormat
			}
		}

		if decoderFormat != "mp3" && decoderFormat != "wav" && decoderFormat != "pcm" && decoderFormat != "opus" && decoderFormat != "ogg_opus" {
			log.Errorf("当前仅支持 mp3/wav/pcm/opus/ogg_opus 格式的流式合成")
			close(outputChan)
			return
		}

		decoder, err := util.CreateAudioDecoderWithSampleRate(ctx, resp.Body, outputChan, frameDuration, decoderFormat, sampleRate)
		if err != nil {
			log.Errorf("创建OpenAI音频解码器失败: %v", err)
			close(outputChan)
			return
		}
		if decoderFormat == "opus" {
			sourceChannels := channels
			if sourceChannels < 1 {
				sourceChannels = 1
			}
			decoder.WithFormat(beep.Format{
				SampleRate:  beep.SampleRate(util.NormalizeOpusSampleRate(sampleRate)),
				NumChannels: sourceChannels,
			})
		}

		if err := decoder.Run(startTs); err != nil {
			log.Errorf("OpenAI音频解码失败: %v", err)
			return
		}

		select {
		case <-ctx.Done():
			log.Debugf("OpenAI TTS流式合成取消, 文本: %s", text)
			return
		default:
			log.Infof("OpenAI TTS耗时: 从输入至获取音频数据结束耗时: %d ms", time.Now().UnixMilli()-startTs)
		}
	}()

	return outputChan, nil
}

// SetVoice 设置音色参数
func (p *OpenAITTSProvider) SetVoice(voiceConfig map[string]interface{}) error {
	if voice, ok := voiceConfig["voice"].(string); ok && voice != "" {
		p.Voice = voice
		return nil
	}
	return fmt.Errorf("无效的音色配置: 缺少 voice")
}

// Close 关闭资源（无状态 Provider，无需关闭）
func (p *OpenAITTSProvider) Close() error {
	return nil
}

// IsValid 检查资源是否有效
func (p *OpenAITTSProvider) IsValid() bool {
	return p != nil
}
