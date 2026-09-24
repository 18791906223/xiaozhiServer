package vad

import (
	"errors"
	"xiaozhi-esp32-server-golang/constants"
	"xiaozhi-esp32-server-golang/internal/domain/vad/inter"
	"xiaozhi-esp32-server-golang/internal/domain/vad/silero_vad"
	"xiaozhi-esp32-server-golang/internal/domain/vad/ten_vad"
	// "xiaozhi-esp32-server-golang/internal/domain/vad/webrtc_vad"
)

func AcquireVAD(provider string, config map[string]interface{}) (inter.VAD, error) {
	// 优先使用 config 中的 provider，否则使用参数中的 provider
	if configProvider, ok := config["provider"].(string); ok && configProvider != "" {
		provider = configProvider
	}

	// 如果 provider 为空，返回明确的错误信息
	if provider == "" {
		return nil, errors.New("vad provider is empty, please set provider in config (supported: silero_vad, ten_vad)")
	}

	switch provider {
	case constants.VadTypeSileroVad:
		return silero_vad.AcquireVAD(config)
	// case constants.VadTypeWebRTCVad:
	// 	return webrtc_vad.AcquireVAD(config)
	case constants.VadTypeTenVad:
		return ten_vad.AcquireVAD(config)
	default:
		return nil, errors.New("invalid vad provider: " + provider + " (supported: silero_vad, ten_vad)")
	}
}

func ReleaseVAD(vad inter.VAD) error {
	//根据vad的类型，调用对应的ReleaseVAD方法
	switch vad.(type) {
	// case *webrtc_vad.WebRTCVAD:
	// 	return webrtc_vad.ReleaseVAD(vad)
	case *silero_vad.SileroVAD:
		return silero_vad.ReleaseVAD(vad)
	case *ten_vad.TenVAD:
		return ten_vad.ReleaseVAD(vad)
	default:
		return errors.New("invalid vad type")
	}
}
