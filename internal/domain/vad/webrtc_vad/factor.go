package webrtc_vad

import (
	"fmt"
	"time"
	"xiaozhi-esp32-server-golang/internal/util"
)

// WebRTCVADConfig WebRTC VAD 配置
type WebRTCVADConfig struct {
	SampleRate int
	Mode       int
}

// WebRTCVADFactory WebRTC VAD 工厂，实现 ResourceFactory 接口
type WebRTCVADFactory struct {
	config WebRTCVADConfig
}

// NewWebRTCVADFactory 创建WebRTC VAD工厂
func NewWebRTCVADFactory(config WebRTCVADConfig) *WebRTCVADFactory {
	if config.SampleRate == 0 {
		config.SampleRate = DefaultSampleRate
	}
	if config.Mode < 0 || config.Mode > 3 {
		config.Mode = DefaultMode
	}

	return &WebRTCVADFactory{
		config: config,
	}
}

// Create 创建新的WebRTC VAD资源实例
func (f *WebRTCVADFactory) Create() (util.Resource, error) {
	vad := &WebRTCVAD{
		sampleRate: f.config.SampleRate,
		mode:       f.config.Mode,
		lastUsed:   time.Now(),
	}

	// 初始化实例
	if err := vad.init(); err != nil {
		return nil, fmt.Errorf("failed to initialize WebRTC VAD: %w", err)
	}

	return vad, nil
}

// Validate 验证资源是否有效
func (f *WebRTCVADFactory) Validate(resource util.Resource) bool {
	vad, ok := resource.(*WebRTCVAD)
	if !ok {
		return false
	}
	return vad.IsValid()
}

// Reset 重置资源状态
func (f *WebRTCVADFactory) Reset(resource util.Resource) error {
	vad, ok := resource.(*WebRTCVAD)
	if !ok {
		return fmt.Errorf("invalid resource type")
	}
	return vad.Reset()
}
