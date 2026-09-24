package asr

import (
	"context"

	"xiaozhi-esp32-server-golang/internal/domain/asr/aliyun_funasr"
	"xiaozhi-esp32-server-golang/internal/domain/asr/types"
	log "xiaozhi-esp32-server-golang/logger"
)

// AliyunFunASRAdapter adapts Aliyun FunASR to AsrProvider.
type AliyunFunASRAdapter struct {
	engine *aliyun_funasr.AliyunFunASR
}

// NewAliyunFunASRAdapter creates the adapter.
func NewAliyunFunASRAdapter(config map[string]interface{}) (AsrProvider, error) {
	aliyunConfig := aliyun_funasr.ConfigFromMap(config)
	log.Log().Infof("aliyun funasr config: %+v", aliyunConfig)

	engine, err := aliyun_funasr.NewAliyunFunASR(aliyunConfig)
	if err != nil {
		return nil, err
	}
	return &AliyunFunASRAdapter{engine: engine}, nil
}

// Process implements AsrProvider.
func (a *AliyunFunASRAdapter) Process(pcmData []float32) (string, error) {
	return a.engine.Process(pcmData)
}

// StreamingRecognize implements AsrProvider.
func (a *AliyunFunASRAdapter) StreamingRecognize(ctx context.Context, audioStream <-chan []float32) (chan types.StreamingResult, error) {
	return a.engine.StreamingRecognize(ctx, audioStream)
}

// Close releases resources.
func (a *AliyunFunASRAdapter) Close() error {
	if a.engine != nil {
		return a.engine.Close()
	}
	return nil
}

// IsValid checks whether the adapter is usable.
func (a *AliyunFunASRAdapter) IsValid() bool {
	return a != nil && a.engine != nil && a.engine.IsValid()
}
