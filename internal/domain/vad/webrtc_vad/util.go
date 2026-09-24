package webrtc_vad

func getVadConfigFromMap(config map[string]interface{}) WebRTCVADConfig {
	sampleRate := DefaultSampleRate
	mode := DefaultMode

	if val, ok := config["vad_sample_rate"]; ok {
		if sr, ok := val.(int); ok {
			sampleRate = sr
		}
	}
	if val, ok := config["vad_mode"]; ok {
		if m, ok := val.(int); ok {
			mode = m
		}
	}
	return WebRTCVADConfig{
		SampleRate: sampleRate,
		Mode:       mode,
	}
}
