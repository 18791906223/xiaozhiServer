// Package main 是小智服务端「实时语音对话」主链路的最小可用实现。
//
// 它把 internal/app/server/chat 里约 15000 行、耦合了数据上报 / MCP / 媒体播放 /
// 记忆 / 知识库 / OpenClaw 的逻辑，压缩成一条能完整跑通的直线：
//
//	WebSocket 握手 → 收 Opus 音频 → VAD 断句 → ASR → LLM → 分句 → TTS → 回传 Opus
//
// 这里刻意保留了真实实现的消息格式和环节划分，但每个环节只留一种最简单的做法。
// 详细说明见同目录 README.md。
package main

import "encoding/json"

// ============================================================================
// 消息类型常量
// 对应真实实现：internal/data/msg/message_types.go
// ============================================================================

// 设备 → 服务端
const (
	MsgHello   = "hello"   // 握手
	MsgListen  = "listen"  // 开始/结束收音
	MsgAbort   = "abort"   // 打断当前播报
	MsgGoodbye = "goodbye" // 主动断开
	MsgIot     = "iot"     // 设备侧 IoT 状态，简化版忽略
)

// 服务端 → 设备
const (
	SrvHello = "hello" // 握手应答
	SrvStt   = "stt"   // 语音识别结果
	SrvTts   = "tts"   // TTS 控制与文本
	SrvIot   = "iot"   // IoT 应答
)

// listen / tts 消息的 state 字段
const (
	StateStart         = "start"          // listen: 开始收音
	StateStop          = "stop"           // listen: 结束收音 / tts: 本轮播报结束
	StateDetect        = "detect"         // listen: 唤醒词检测结果
	StateSentenceStart = "sentence_start" // tts: 一句话开始，紧跟其后是音频帧
	StateSentenceEnd   = "sentence_end"   // tts: 一句话结束
)

// ============================================================================
// 音频参数
// 对应真实实现：internal/data/audio/audio.go
// ============================================================================

// AudioFormat 描述一路音频的格式，握手时双方交换。
type AudioFormat struct {
	Format        string `json:"format,omitempty"`         // 固定 "opus"
	SampleRate    int    `json:"sample_rate,omitempty"`    // 采样率
	Channels      int    `json:"channels,omitempty"`       // 声道数
	FrameDuration int    `json:"frame_duration,omitempty"` // 每帧毫秒数
}

// 简化版固定用这一组格式。真实实现里上行格式由设备的 hello 上报，
// 下行格式还要看 TTS 供应商（例如小智官方 TTS 会强制 24000Hz/20ms）。
var (
	inAudioFormat  = AudioFormat{Format: "opus", SampleRate: 16000, Channels: 1, FrameDuration: 60}
	outAudioFormat = AudioFormat{Format: "opus", SampleRate: 16000, Channels: 1, FrameDuration: 60}
)

// ============================================================================
// 消息结构体
// 对应真实实现：internal/data/client/client.go 的 ClientMessage
//              internal/data/msg/message_types.go 的 ServerMessage
// 去掉了 mqtt/udp 相关的字段。
// ============================================================================

// ClientMessage 设备 → 服务端。
type ClientMessage struct {
	Type        string          `json:"type"`
	DeviceID    string          `json:"device_id,omitempty"`
	SessionID   string          `json:"session_id,omitempty"`
	Text        string          `json:"text,omitempty"`
	Mode        string          `json:"mode,omitempty"`  // auto / manual / realtime，简化版不区分
	State       string          `json:"state,omitempty"` // listen 的 start/stop/detect
	Token       string          `json:"token,omitempty"`
	Version     int             `json:"version,omitempty"`
	Transport   string          `json:"transport,omitempty"`
	Features    map[string]bool `json:"features,omitempty"`
	AudioParams *AudioFormat    `json:"audio_params,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
}

// ServerMessage 服务端 → 设备。
type ServerMessage struct {
	Type        string       `json:"type"`
	Text        string       `json:"text,omitempty"`
	SessionID   string       `json:"session_id,omitempty"`
	Version     int          `json:"version,omitempty"`
	State       string       `json:"state,omitempty"`
	Transport   string       `json:"transport,omitempty"`
	AudioParams *AudioFormat `json:"audio_params,omitempty"`
}
