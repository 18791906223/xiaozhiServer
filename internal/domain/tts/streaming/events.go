package streaming

// SentenceSignalType 表示一段音频前需要发送的句子级控制信号类型。
type SentenceSignalType string

const (
	SentenceSignalStart SentenceSignalType = "sentence_start"
	SentenceSignalEnd   SentenceSignalType = "sentence_end"
)

// SentenceSignal 表示与当前音频块绑定的有序句子边界信号。
type SentenceSignal struct {
	Type SentenceSignalType
	Text string
}

// SynthesisEvent 表示一段双流式 TTS 输出。
// Audio 为当前音频块；SentenceSignals 表示在发送该音频块前需先发送的句子边界信号。
type SynthesisEvent struct {
	Audio           []byte
	SentenceSignals []SentenceSignal
}
