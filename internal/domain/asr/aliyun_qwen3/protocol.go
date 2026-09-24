package aliyun_qwen3

import (
	"encoding/base64"
	"encoding/json"

	log "xiaozhi-esp32-server-golang/logger"
)

// ClientEvent 客户端发送事件基础结构
type ClientEvent struct {
	EventID string   `json:"event_id,omitempty"`
	Type    string   `json:"type"`
	Session *Session `json:"session,omitempty"`
	Audio   string   `json:"audio,omitempty"` // Base64 编码的音频数据
}

// Session session.update 事件中的 session 配置
type Session struct {
	Modalities              []string                 `json:"modalities"`
	InputAudioFormat        string                   `json:"input_audio_format,omitempty"`
	SampleRate              int                      `json:"sample_rate,omitempty"`
	InputAudioTranscription *InputAudioTranscription `json:"input_audio_transcription,omitempty"`
	TurnDetection           *TurnDetection           `json:"turn_detection"`
}

// InputAudioTranscription 音频转录配置
type InputAudioTranscription struct {
	Language string `json:"language,omitempty"`
}

// TurnDetection VAD 配置
type TurnDetection struct {
	Type              string  `json:"type,omitempty"`                // "server_vad" 或不设置
	Threshold         float64 `json:"threshold,omitempty"`           // VAD 阈值
	SilenceDurationMs int     `json:"silence_duration_ms,omitempty"` // 静音持续时间（毫秒）
}

// ServerEvent 服务端响应事件基础结构
type ServerEvent struct {
	Type            string     `json:"type"`
	EventID         string     `json:"event_id,omitempty"`
	PreviousEventID string     `json:"previous_event_id,omitempty"`
	Session         *Session   `json:"session,omitempty"`
	Item            *Item      `json:"item,omitempty"`
	Text            string     `json:"text,omitempty"`
	Stash           string     `json:"stash,omitempty"`
	Transcript      string     `json:"transcript,omitempty"`
	Error           *ErrorInfo `json:"error,omitempty"`
}

// Item 会话项（如输入音频转录结果）
type Item struct {
	ID            string         `json:"id,omitempty"`
	Type          string         `json:"type,omitempty"`
	Status        string         `json:"status,omitempty"`
	Transcription *Transcription `json:"transcription,omitempty"`
}

// Transcription 转录结果
type Transcription struct {
	Text     string `json:"text,omitempty"`
	Language string `json:"language,omitempty"`
}

// ErrorInfo 错误信息
type ErrorInfo struct {
	Message string `json:"message,omitempty"`
	Code    string `json:"code,omitempty"`
}

// NewSessionUpdateEvent 创建 session.update 事件
func NewSessionUpdateEvent(config Config) *ClientEvent {
	session := &Session{
		Modalities:              []string{"text"},
		InputAudioFormat:        config.Format,
		SampleRate:              config.SampleRate,
		InputAudioTranscription: &InputAudioTranscription{Language: config.Language},
	}

	if config.AutoEnd {
		session.TurnDetection = &TurnDetection{
			Type:              "server_vad",
			Threshold:         config.VADThreshold,
			SilenceDurationMs: config.VADSilenceMs,
		}
	} else {
		session.TurnDetection = nil
	}

	event := &ClientEvent{
		EventID: "session_update",
		Type:    "session.update",
		Session: session,
	}

	// 调试：打印 session.update 事件
	if jsonBytes, err := json.Marshal(event); err == nil {
		log.Debugf("[aliyun_qwen3] session.update JSON: %s", string(jsonBytes))
	}

	return event
}

// NewAudioAppendEvent 创建 input_audio_buffer.append 事件
func NewAudioAppendEvent(audioData []byte) *ClientEvent {
	encoded := base64.StdEncoding.EncodeToString(audioData)
	return &ClientEvent{
		Type:  "input_audio_buffer.append",
		Audio: encoded,
	}
}

// NewAudioCommitEvent 创建 input_audio_buffer.commit 事件
func NewAudioCommitEvent() *ClientEvent {
	return &ClientEvent{
		EventID: "audio_commit",
		Type:    "input_audio_buffer.commit",
	}
}

// NewSessionFinishEvent 创建 session.finish 事件
func NewSessionFinishEvent() *ClientEvent {
	return &ClientEvent{
		EventID: "session_finish",
		Type:    "session.finish",
	}
}

// IsTranscriptionEvent 判断是否为转录事件
func IsTranscriptionEvent(event *ServerEvent) bool {
	return event.Type == "conversation.item.input_audio_transcription.text" ||
		event.Type == "conversation.item.input_audio_transcription.completed"
}

// IsFinalTranscription 判断是否为最终转录结果
func IsFinalTranscription(event *ServerEvent) bool {
	return event.Type == "conversation.item.input_audio_transcription.completed"
}

// GetTranscriptionText 获取转录文本
func GetTranscriptionText(event *ServerEvent) string {
	if event == nil {
		return ""
	}
	if event.Item != nil && event.Item.Transcription != nil && event.Item.Transcription.Text != "" {
		return event.Item.Transcription.Text
	}
	if event.Transcript != "" {
		return event.Transcript
	}
	if event.Text != "" {
		return event.Text
	}
	if event.Stash != "" {
		return event.Stash
	}
	return ""
}
