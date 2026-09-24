package xunfei

import "time"

// Config 讯飞听写(WebSocket IAT)配置
// 文档: https://www.xfyun.cn/doc/asr/voicedictation/API.html
type Config struct {
	AppID      string
	APIKey     string
	APISecret  string
	Host       string
	Path       string
	Language   string
	Accent     string
	Domain     string
	SampleRate int
	Timeout    time.Duration
}

func defaultConfig() Config {
	return Config{
		Host:       "iat-api.xfyun.cn",
		Path:       "/v2/iat",
		Language:   "zh_cn",
		Accent:     "mandarin",
		Domain:     "iat",
		SampleRate: 16000,
		Timeout:    30 * time.Second,
	}
}
