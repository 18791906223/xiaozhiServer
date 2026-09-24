package asr

import (
	"context"
	"strconv"

	"xiaozhi-esp32-server-golang/internal/domain/asr/sherpa_onnx"
	"xiaozhi-esp32-server-golang/internal/domain/asr/types"
	log "xiaozhi-esp32-server-golang/logger"
)

// SherpaOnnxAdapter 适配 sherpa_onnx 包到 asr 接口
type SherpaOnnxAdapter struct {
	engine *sherpa_onnx.SherpaOnnx
}

// NewSherpaOnnxAdapter 创建一个新的 SherpaOnnx 适配器
// 该适配器连接主进程内嵌的 asr_server（sherpa-onnx SenseVoice），
// 取代外部 FunASR 服务作为主会话的实时语音识别引擎。
func NewSherpaOnnxAdapter(config map[string]interface{}) (AsrProvider, error) {
	sherpaConfig := sherpa_onnx.Config{}

	log.Log().Infof("sherpa_onnx config: %+v", config)

	// 从 map 中获取配置项
	if host, ok := config["host"].(string); ok && host != "" {
		sherpaConfig.Host = host
	}
	if port, ok := config["port"].(string); ok && port != "" {
		sherpaConfig.Port = port
	} else if portInt, ok := config["port"].(int); ok && portInt > 0 {
		sherpaConfig.Port = strconv.Itoa(portInt)
	} else if portFloat, ok := config["port"].(float64); ok && portFloat > 0 {
		sherpaConfig.Port = strconv.Itoa(int(portFloat))
	}

	if sampleRate, ok := config["sample_rate"].(int); ok && sampleRate > 0 {
		sherpaConfig.SampleRate = sampleRate
	} else if sampleRateFloat, ok := config["sample_rate"].(float64); ok && sampleRateFloat > 0 {
		sherpaConfig.SampleRate = int(sampleRateFloat)
	}

	if timeout, ok := config["timeout"].(int); ok && timeout > 0 {
		sherpaConfig.Timeout = timeout
	} else if timeoutFloat, ok := config["timeout"].(float64); ok && timeoutFloat > 0 {
		sherpaConfig.Timeout = int(timeoutFloat)
	}

	if waitResultTimeout, ok := config["wait_result_timeout"].(int); ok && waitResultTimeout > 0 {
		sherpaConfig.WaitResultTimeout = waitResultTimeout
	} else if waitResultTimeoutFloat, ok := config["wait_result_timeout"].(float64); ok && waitResultTimeoutFloat > 0 {
		sherpaConfig.WaitResultTimeout = int(waitResultTimeoutFloat)
	}

	// 创建引擎
	engine := sherpa_onnx.NewSherpaOnnx(sherpaConfig)
	return &SherpaOnnxAdapter{engine: engine}, nil
}

// Process 实现 Asr 接口
func (a *SherpaOnnxAdapter) Process(pcmData []float32) (string, error) {
	return a.engine.Process(pcmData)
}

// StreamingRecognize 实现流式识别接口
func (a *SherpaOnnxAdapter) StreamingRecognize(ctx context.Context, audioStream <-chan []float32) (chan types.StreamingResult, error) {
	return a.engine.StreamingRecognize(ctx, audioStream)
}

// Close 关闭资源
func (a *SherpaOnnxAdapter) Close() error {
	return a.engine.Close()
}

// IsValid 检查资源是否有效
func (a *SherpaOnnxAdapter) IsValid() bool {
	return a != nil && a.engine != nil && a.engine.IsValid()
}
