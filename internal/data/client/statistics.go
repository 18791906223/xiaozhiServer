package client

import "time"

// Statistic 结构体已废弃，请使用 statistic_plugin 在 MetricTtsStop 时获取统计信息
type Statistic struct {
	TurnStartTs        int64
	VoiceSilenceTs     int64
	AsrFirstTextTs     int64
	AsrFinalTextTs     int64
	LlmStartTs         int64
	LlmFirstTokenTs    int64
	LlmFirstSentenceTs int64
	LlmEndTs           int64
	TtsStartTs         int64
	TtsFirstFrameTs    int64
	TtsStopTs          int64
}

// MarkTurnStart 记录轮次开始时间
func (state *ClientState) MarkTurnStart() {
	state.Statistic.TurnStartTs = time.Now().UnixMilli()
	state.Statistic.VoiceSilenceTs = 0
	state.Statistic.AsrFirstTextTs = 0
	state.Statistic.AsrFinalTextTs = 0
}

// MarkVoiceSilenceAt 记录语音静默开始时间，返回本轮是否首次记录
func (state *ClientState) MarkVoiceSilenceAt(ts int64) bool {
	if state.Statistic.VoiceSilenceTs != 0 {
		return false
	}
	state.Statistic.VoiceSilenceTs = ts
	return true
}

// MarkVoiceSilence 记录语音静默开始时间，返回本轮是否首次记录
func (state *ClientState) MarkVoiceSilence() bool {
	return state.MarkVoiceSilenceAt(time.Now().UnixMilli())
}

// MarkAsrFirstText 记录 ASR 首次返回文本时间
func (state *ClientState) MarkAsrFirstText() {
	if state.Statistic.AsrFirstTextTs == 0 {
		state.Statistic.AsrFirstTextTs = time.Now().UnixMilli()
	}
}

// MarkAsrFinalText 记录 ASR 最终文本时间
func (state *ClientState) MarkAsrFinalText() {
	state.MarkAsrFinalTextAt(time.Now().UnixMilli())
}

// MarkAsrFinalTextAt 记录 ASR 最终文本时间，返回本轮是否首次记录
func (state *ClientState) MarkAsrFinalTextAt(ts int64) bool {
	if state.Statistic.AsrFinalTextTs != 0 {
		return false
	}
	state.Statistic.AsrFinalTextTs = ts
	return true
}

// MarkLlmStart 记录 LLM 开始时间
func (state *ClientState) MarkLlmStart() {
	state.Statistic.LlmStartTs = time.Now().UnixMilli()
	state.Statistic.LlmFirstTokenTs = 0
	state.Statistic.LlmFirstSentenceTs = 0
	state.Statistic.LlmEndTs = 0
}

// MarkLlmFirstToken 记录 LLM 首次返回 token 时间
func (state *ClientState) MarkLlmFirstToken() {
	state.Statistic.LlmFirstTokenTs = time.Now().UnixMilli()
}

// MarkLlmFirstSentenceAt 记录 LLM 首句输出时间，返回本轮是否首次记录
func (state *ClientState) MarkLlmFirstSentenceAt(ts int64) bool {
	if state.Statistic.LlmFirstSentenceTs != 0 {
		return false
	}
	state.Statistic.LlmFirstSentenceTs = ts
	return true
}

// MarkLlmFirstSentence 记录 LLM 首句输出时间，返回本轮是否首次记录
func (state *ClientState) MarkLlmFirstSentence() bool {
	return state.MarkLlmFirstSentenceAt(time.Now().UnixMilli())
}

// MarkLlmEnd 记录 LLM 结束时间
func (state *ClientState) MarkLlmEnd() {
	state.Statistic.LlmEndTs = time.Now().UnixMilli()
}

// MarkTtsStart 记录 TTS 开始时间
func (state *ClientState) MarkTtsStart() {
	state.Statistic.TtsStartTs = time.Now().UnixMilli()
	state.Statistic.TtsFirstFrameTs = 0
	state.Statistic.TtsStopTs = 0
}

// MarkTtsFirstFrame 记录 TTS 首帧时间
func (state *ClientState) MarkTtsFirstFrame() {
	if state.Statistic.TtsFirstFrameTs == 0 {
		state.Statistic.TtsFirstFrameTs = time.Now().UnixMilli()
	}
}

// MarkTtsStop 记录 TTS 结束时间
func (state *ClientState) MarkTtsStop() {
	state.Statistic.TtsStopTs = time.Now().UnixMilli()
}

// SetStartAsrTs 设置 ASR 开始时间（别名，为了兼容）
func (state *ClientState) SetStartAsrTs() { state.MarkVoiceSilence() }

// SetStartLlmTs 设置 LLM 开始时间（别名，为了兼容）
func (state *ClientState) SetStartLlmTs() { state.MarkLlmStart() }

// SetStartTtsTs 设置 TTS 开始时间（别名，为了兼容）
func (state *ClientState) SetStartTtsTs() { state.MarkTtsStart() }

// GetAsrDuration 获取 ASR 处理耗时（已废弃，仅保留方法签名）
func (state *ClientState) GetAsrDuration() int64 {
	return calcStatisticDuration(state.Statistic.VoiceSilenceTs, state.Statistic.AsrFinalTextTs)
}

// GetAsrLlmTtsDuration 获取整体耗时（已废弃，仅保留方法签名）
func (state *ClientState) GetAsrLlmTtsDuration() int64 {
	return calcStatisticDuration(state.Statistic.VoiceSilenceTs, state.Statistic.TtsFirstFrameTs)
}

// GetLlmDuration 获取 LLM 耗时（已废弃，仅保留方法签名）
func (state *ClientState) GetLlmDuration() int64 {
	return calcStatisticDuration(state.Statistic.LlmStartTs, state.Statistic.LlmEndTs)
}

// GetTtsDuration 获取 TTS 耗时（已废弃，仅保留方法签名）
func (state *ClientState) GetTtsDuration() int64 {
	return calcStatisticDuration(state.Statistic.TtsStartTs, state.Statistic.TtsStopTs)
}

func calcStatisticDuration(start, end int64) int64 {
	if start <= 0 || end <= 0 || end < start {
		return 0
	}
	return end - start
}

func (s *Statistic) Reset() {
	if s == nil {
		return
	}
	*s = Statistic{}
}
