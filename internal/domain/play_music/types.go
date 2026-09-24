package play_music

import (
	"context"
)

// MusicPlayerInterface 音乐播放器接口
type MusicPlayerInterface interface {
	// PlayMusicStream 从URL播放音乐，返回音频流通道
	PlayMusicStream(ctx context.Context, url string) (chan []byte, error)

	// GetPlayerInfo 获取播放器信息
	GetPlayerInfo() map[string]interface{}

	// Stop 停止播放器
	Stop() error
}

// MusicPlayerConfig 音乐播放器配置
type MusicPlayerConfig struct {
	FrameDuration int    `json:"frame_duration"` // 帧时长(ms)，默认20ms
	AudioFormat   string `json:"audio_format"`   // 音频格式，默认"mp3"
}

// DefaultMusicPlayerConfig 默认音乐播放器配置
func DefaultMusicPlayerConfig() *MusicPlayerConfig {
	return &MusicPlayerConfig{
		FrameDuration: 20,    // 20ms
		AudioFormat:   "mp3", // MP3格式
	}
}

// ToMap 将配置转换为map
func (c *MusicPlayerConfig) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"frame_duration": c.FrameDuration,
		"audio_format":   c.AudioFormat,
	}
}

// AudioStreamInfo 音频流信息
type AudioStreamInfo struct {
	URL           string `json:"url"`
	Format        string `json:"format"`         // 音频格式，如 "mp3", "wav"
	SampleRate    int    `json:"sample_rate"`    // 采样率
	Channels      int    `json:"channels"`       // 声道数
	Duration      int64  `json:"duration"`       // 时长(毫秒)
	ContentLength int64  `json:"content_length"` // 内容长度(字节)
}

// PlaybackStatus 播放状态
type PlaybackStatus int

const (
	StatusIdle PlaybackStatus = iota
	StatusPlaying
	StatusPaused
	StatusStopped
	StatusError
)

// String 返回状态的字符串表示
func (s PlaybackStatus) String() string {
	switch s {
	case StatusIdle:
		return "idle"
	case StatusPlaying:
		return "playing"
	case StatusPaused:
		return "paused"
	case StatusStopped:
		return "stopped"
	case StatusError:
		return "error"
	default:
		return "unknown"
	}
}

// PlaybackEvent 播放事件
type PlaybackEvent struct {
	Type      string      `json:"type"`      // 事件类型: "started", "progress", "finished", "error"
	Timestamp int64       `json:"timestamp"` // 时间戳
	Message   string      `json:"message"`   // 事件消息
	Data      interface{} `json:"data"`      // 额外数据
}

// StreamingStats 流式播放统计信息
type StreamingStats struct {
	BytesDownloaded int64          `json:"bytes_downloaded"` // 已下载字节数
	BytesDecoded    int64          `json:"bytes_decoded"`    // 已解码字节数
	FramesGenerated int64          `json:"frames_generated"` // 已生成帧数
	StartTime       int64          `json:"start_time"`       // 开始时间
	FirstFrameTime  int64          `json:"first_frame_time"` // 首帧时间
	Status          PlaybackStatus `json:"status"`           // 当前状态
	ErrorCount      int            `json:"error_count"`      // 错误次数
}
