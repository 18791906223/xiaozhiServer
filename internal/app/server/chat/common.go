// 文件：common.go
// 职责：会话级的「公共动作」——打断/停止说话（stopSpeaking 系列）与「重置回静默态」。
// 这里是所有打断路径的收敛点：abort 消息、realtime 检测到新语音、OpenClaw 抢占、
// 退出模式、会话重置等，最终都会调到本文件的某个入口。
//
// 核心概念（三个入口的差异在于「打断强度」）：
//   - StopSpeaking / StopSpeakingWithReason：完整打断——取消会话上下文、清空文本/LLM/TTS 三条队列、
//     停止音频发送，并挂起媒体播放。
//   - StopAssistantOutputAfterAsrWithReason：轻量打断——只停 assistant 的输出，保留会话上下文与媒体播放，
//     这正是 realtime 模式需要的语义（用户开口时把 TTS 掐掉，但音乐继续播）。
//   - ResetToSilentState：不关会话，只把会话恢复成「没在播、也没在算」的干净状态。
//
// 在流程中的位置：被 session.go / chat.go 在各类打断与收尾路径上调用。
// 与哪些文件配合：
//   - session.go：HandleAbortMessage、OpenClaw 抢占、退出模式等调用入口。
//   - tts.go：InterruptAndStopSyncWithReason 真正停掉音频发送。
//   - llm.go / session.go：ClearLLMResponseQueue / ClearChatTextQueue 清队列。
//   - media_player.go：Suspend 挂起媒体播放。
package chat

import (
	"context"
	"errors"
	"fmt"
	"time"

	log "xiaozhi-esp32-server-golang/logger"
)

// stopSpeakingInterruptTimeout 是打断时「同步等待发送协程收口」的超时上限。
// 为什么要超时：打断要清 TTS 队列并等发送协程收口，正常情况下很快；
// 但一旦发送协程卡住或队列出现异常，没有超时就会把调用方（很可能是收包协程）永久挂住。
// 有超时后最坏结果只是「本次打断没清干净」，连接仍能继续服务。
const stopSpeakingInterruptTimeout = 2 * time.Second

// [5.6] 打断入口：取消会话上下文、清空文本/LLM/TTS 队列、挂起媒体播放。
//
// StopSpeaking 是完整打断的对外入口（不带原因版本）。
// 语义等同「把这一轮作废」：取消会话上下文并挂起媒体播放。
func (s *ChatSession) StopSpeaking(isSendTtsStop bool) {
	s.StopSpeakingWithReason(isSendTtsStop, "ChatSession.StopSpeaking")
}

// StopSpeakingWithReason 同 StopSpeaking，但带原因字符串便于排查。
// 所有打断都应尽量传原因：出问题时「是谁、因为什么打断的」往往是最关键的线索。
func (s *ChatSession) StopSpeakingWithReason(isSendTtsStop bool, reason string) {
	s.stopSpeakingWithLock(true, isSendTtsStop, true, reason)
}

// StopAssistantOutputAfterAsrWithReason 只停止当前 assistant 输出，不挂起媒体播放。
// 关键差异：cancelSession=false（不动会话上下文，监听侧继续工作）、suspendMedia=false。
// 这正是 realtime 模式需要的语义——用户开口时只把 TTS 掐掉，正在播的音乐继续播。
func (s *ChatSession) StopAssistantOutputAfterAsrWithReason(isSendTtsStop bool, reason string) {
	s.stopSpeakingWithLock(false, isSendTtsStop, false, reason)
}

// stopSpeakingWithLock 在 stopSpeaking 基础上增加了 mutex 保护
//
// 为什么需要这层锁：打断可能从多个 goroutine 同时发起（收包协程、ASR 循环、TTS 回调），
// 并发清理同一批队列与状态会互相干扰（例如一方刚清完队列、另一方又把旧元素推进去），
// 因此用 stopSpeakingMu 把整段打断动作串行化。
func (s *ChatSession) stopSpeakingWithLock(cancelSession bool, isSendTtsStop bool, suspendMedia bool, reason string) {
	s.stopSpeakingMu.Lock()
	defer s.stopSpeakingMu.Unlock()
	s.stopSpeaking(cancelSession, isSendTtsStop, suspendMedia, reason)
}

// stopSpeaking 是打断的实际实现，三个布尔参数决定打断强度：
//   - cancelSession：是否取消会话上下文（true = 连这一轮对话一起作废）。
//   - isSendTtsStop：是否给设备发 tts state=stop（false 用于「只清队列、不发协议」的内部收尾）。
//   - suspendMedia：是否挂起媒体播放（false 让音乐继续播）。
//   - reason：日志与状态标记用的原因。
func (s *ChatSession) stopSpeaking(cancelSession bool, isSendTtsStop bool, suspendMedia bool, reason string) {
	reason = normalizeTTSReason(reason)
	// 先把打断前的 TTS 内部状态打进日志：打断的核心目的就是「让状态回到干净」，
	// 事后想判断有没有清干净，只能靠对比打断前后的状态。
	state := "tts_manager=nil"
	if s != nil && s.ttsManager != nil {
		state = s.ttsManager.debugState()
	}
	log.Infof(
		"stop speaking requested: device=%s reason=%s cancelSession=%v sendTtsStop=%v suspendMedia=%v state={%s}",
		s.clientState.DeviceID,
		reason,
		cancelSession,
		isSendTtsStop,
		suspendMedia,
		state,
	)

	// 只有完整打断才作废会话上下文；轻量打断（只停 assistant 输出）必须保留它，
	// 否则监听侧的状态机也会被重置，realtime 下会导致后续听不到用户说话。
	if cancelSession {
		s.clientState.SessionCtx.CancelWithReason(fmt.Sprintf("ChatSession.stopSpeaking(%s): session_ctx", reason))
		// 作废本轮 listen 起始标记，避免异步的 OnListenStart 回调在打断之后又启动一次 ASR。
		s.invalidateListenStart()
	}
	// AfterAsrSessionCtx 代表「ASR 之后」的输出阶段，无论哪种打断都要取消：
	// 它承载 LLM/TTS 的输出流程，不取消的话旧输出会继续往前推进。
	s.clientState.AfterAsrSessionCtx.CancelWithReason(fmt.Sprintf("ChatSession.stopSpeaking(%s): after_asr_ctx", reason))
	// 通知「等待欢迎语播放结束」的逻辑提前结束等待，否则它会一直等到播放完成或超时。
	s.completeWelcomePlaybackWait(false)

	// 挂起媒体播放：媒体与 TTS 共用同一条发送通道，不挂起就无法保证「立刻安静」，
	// 而且恢复播放的时机应由播放器自己管理。轻量打断时跳过这一步（音乐要继续听）。
	if suspendMedia && s.mediaPlayer != nil {
		if err := s.mediaPlayer.Suspend(); err != nil && !errors.Is(err, context.Canceled) {
			log.Warnf("stopSpeaking 挂起媒体播放失败: %v", err)
		}
	}

	// 欢迎语播放标记必须重置：打断后它若还是 true，
	// 后续逻辑会认为「欢迎语仍在播」而拒绝新的输出，表现为「打断后就不说话了」。
	s.clientState.IsWelcomePlaying = false

	// 清空三条队列，对应「文本 → LLM → TTS」三段流水线。
	// 必须三条都清：只清 TTS 的话，已排队的用户文本还会再走一遍 LLM 并重新开口说话，
	// 也就是打断之后「死灰复燃」。
	s.ClearChatTextQueue()
	s.llmManager.ClearLLMResponseQueue()
	s.ttsManager.ClearTTSQueue()
	// 用带超时的独立 ctx 做同步打断：InterruptAndStopSyncWithReason 会等发送协程收口，
	// 超时只记录告警而不向上报错——打断是「尽力而为」的动作，
	// 失败不应该影响调用方继续处理连接（否则一次卡顿会导致整个连接被关掉）。
	interruptCtx, cancel := context.WithTimeout(context.Background(), stopSpeakingInterruptTimeout)
	defer cancel()
	if err := s.ttsManager.InterruptAndStopSyncWithReason(interruptCtx, isSendTtsStop, context.Canceled, fmt.Sprintf("ChatSession.stopSpeaking(%s)", reason)); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			log.Warnf("stopSpeaking sync interrupt timed out, cancel_session=%v", cancelSession)
		} else if !errors.Is(err, context.Canceled) {
			log.Warnf("stopSpeaking sync interrupt failed, cancel_session=%v, err=%v", cancelSession, err)
		}
	}
}

// MqttClose 给设备发 MQTT goodbye，用于上层决定断开连接时通知设备侧。
func (s *ChatSession) MqttClose() {
	s.serverTransport.SendMqttGoodbye()
}

// ResetToSilentState 把会话恢复成「静默态」：不关闭会话，
// 只把它变成「没有输出、没有待处理任务」的干净状态。
// 为什么在 goodbye 之后用它而不是直接关会话：
// 设备说完 goodbye 通常只是这一轮结束，之后很可能马上被唤醒再说一次。
// 直接 Close 会把 ASR/LLM/TTS 资源全部释放，下次唤醒要重新初始化（有明显延迟）；
// 重置成静默态则保留会话与资源，下次唤醒可以直接复用（见 chat.go 的会话保留机制）。
func (s *ChatSession) ResetToSilentState() {
	if s == nil || s.IsClosing() {
		return
	}

	// 取消「检测」去抖任务：静默态下不应该再有任何延迟产生的输出。
	s.cancelPendingDetectLLM()
	// 结束暖场（不打断 TTS，因为下面马上会整体打断一次，无需重复清队列）。
	s.finishOpenClawWarmup("", false)
	// 丢弃所有 OpenClaw 回复流：它们属于上一轮，不能带到下一轮，否则会串轮播报。
	s.clearOpenClawStreams()
	// 清掉暂存的声纹结果：避免上一轮的说话人身份影响下一轮的音色或权限判断。
	s.clearPendingSpeakerResult()

	if s.clientState != nil {
		s.clientState.Abort = false
		s.clientState.IsWelcomeSpeaking = false
	}

	// 到这里才做真正的打断：取消上下文、清三条队列、发 tts_stop、挂起媒体。
	s.stopSpeakingWithLock(true, true, true, "ChatSession.ResetToSilentState")

	if s.clientState != nil {
		// Destroy 把音频缓冲、VAD、统计、上下文等全部重置回初始态（相当于「刚连上」）。
		s.clientState.Destroy()
		// Destroy 内部已重置过一次，这里再显式置一遍做兜底：
		// Destroy 之后可能还有残留回调写入这些标记，保证最终确实是静默状态。
		s.clientState.Abort = false
		s.clientState.IsWelcomeSpeaking = false
		s.clientState.IsWelcomePlaying = false
	}
}
