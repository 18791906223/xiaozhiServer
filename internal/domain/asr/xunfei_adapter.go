package asr

import (
	"context"
	"time"

	asrtypes "xiaozhi-esp32-server-golang/internal/domain/asr/types"
	"xiaozhi-esp32-server-golang/internal/domain/asr/xunfei"
)

type XunfeiAdapter struct {
	engine *xunfei.ASR
}

func NewXunfeiAdapter(config map[string]interface{}) (AsrProvider, error) {
	xunfeiConfig := xunfei.Config{}
	if appID, ok := config["appid"].(string); ok {
		xunfeiConfig.AppID = appID
	}
	if apiKey, ok := config["api_key"].(string); ok {
		xunfeiConfig.APIKey = apiKey
	}
	if apiSecret, ok := config["api_secret"].(string); ok {
		xunfeiConfig.APISecret = apiSecret
	}
	if host, ok := config["host"].(string); ok {
		xunfeiConfig.Host = host
	}
	if path, ok := config["path"].(string); ok {
		xunfeiConfig.Path = path
	}
	if language, ok := config["language"].(string); ok {
		xunfeiConfig.Language = language
	}
	if accent, ok := config["accent"].(string); ok {
		xunfeiConfig.Accent = accent
	}
	if domain, ok := config["domain"].(string); ok {
		xunfeiConfig.Domain = domain
	}
	if sampleRate, ok := config["sample_rate"].(int); ok && sampleRate > 0 {
		xunfeiConfig.SampleRate = sampleRate
	} else if sampleRateFloat, ok := config["sample_rate"].(float64); ok && sampleRateFloat > 0 {
		xunfeiConfig.SampleRate = int(sampleRateFloat)
	}
	if timeout, ok := config["timeout"].(int); ok && timeout > 0 {
		xunfeiConfig.Timeout = time.Duration(timeout) * time.Second
	} else if timeoutFloat, ok := config["timeout"].(float64); ok && timeoutFloat > 0 {
		xunfeiConfig.Timeout = time.Duration(timeoutFloat) * time.Second
	}

	engine, err := xunfei.New(xunfeiConfig)
	if err != nil {
		return nil, err
	}
	return &XunfeiAdapter{engine: engine}, nil
}

func (a *XunfeiAdapter) Process(pcmData []float32) (string, error) {
	return a.engine.Process(pcmData)
}

func (a *XunfeiAdapter) StreamingRecognize(ctx context.Context, audioStream <-chan []float32) (chan asrtypes.StreamingResult, error) {
	return a.engine.StreamingRecognize(ctx, audioStream)
}

func (a *XunfeiAdapter) Close() error {
	if a.engine != nil {
		return a.engine.Close()
	}
	return nil
}

func (a *XunfeiAdapter) IsValid() bool {
	return a != nil && a.engine != nil && a.engine.IsValid()
}
