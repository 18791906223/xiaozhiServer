package asr

import (
	"context"

	"xiaozhi-esp32-server-golang/internal/domain/asr/aliyun_qwen3"
	"xiaozhi-esp32-server-golang/internal/domain/asr/types"
	log "xiaozhi-esp32-server-golang/logger"
)

// AliyunQwen3Adapter adapts Qwen3 ASR to AsrProvider.
type AliyunQwen3Adapter struct {
	engine *aliyun_qwen3.AliyunQwen3ASR
}

// NewAliyunQwen3Adapter creates the adapter.
func NewAliyunQwen3Adapter(config map[string]interface{}) (AsrProvider, error) {
	aliyunConfig := aliyun_qwen3.ConfigFromMap(config)
	log.Log().Infof("aliyun qwen3 asr config: %+v", aliyunConfig)

	engine, err := aliyun_qwen3.NewAliyunQwen3ASR(aliyunConfig)
	if err != nil {
		return nil, err
	}
	return &AliyunQwen3Adapter{engine: engine}, nil
}

// Process implements AsrProvider.
func (a *AliyunQwen3Adapter) Process(pcmData []float32) (string, error) {
	return a.engine.Process(pcmData)
}

// StreamingRecognize implements AsrProvider.
func (a *AliyunQwen3Adapter) StreamingRecognize(ctx context.Context, audioStream <-chan []float32) (chan types.StreamingResult, error) {
	return a.engine.StreamingRecognize(ctx, audioStream)
}

// Close releases resources.
func (a *AliyunQwen3Adapter) Close() error {
	if a.engine != nil {
		return a.engine.Close()
	}
	return nil
}

// IsValid validates the instance.
func (a *AliyunQwen3Adapter) IsValid() bool {
	return a != nil && a.engine != nil && a.engine.IsValid()
}
