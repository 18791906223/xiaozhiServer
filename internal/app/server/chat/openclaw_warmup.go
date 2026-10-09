// 文件：openclaw_warmup.go
// 职责：OpenClaw 模式下的「暖场话术」机制。
// OpenClaw 是外部智能体（Agent）通道，主回复经常要等几秒到几十秒才回来。
// 等待期间如果一直不出声，用户会以为助手掉线了。
// 本文件的做法是：先让 LLM 一次性生成 11 条很短的「接话」，再按固定时间点
// （第 1/10/20/…/100 秒）依次播出去，维持「有人在答应」的体感。
//
// 核心类型：openClawWarmupTask（一次等待期的暖场任务，承载话术计划与播报状态机）。
//
// 在流程中的位置：
//   - session.go 的 OpenClaw 路由把用户文本转发给 Agent 成功后调用 startOpenClawWarmup；
//   - Agent 首段正文经 InjectOpenClawResponse 到达时，调用 cancelOpenClawWarmup / finishOpenClawWarmup 抢占并打断暖场。
//
// 与哪些文件配合：
//   - session.go：任务的创建、抢占、清理（含 ResetToSilentState 里的统一清场）。
//   - tts.go：暖场音频通过 ttsManager.handleTextResponse 进入正常的 TTS 发送链路，
//     并用 ClearAudioHistory / GetAndClearAudioHistory 隔离暖场音频（不让它进入消息保存）。
//   - llm.go、pool：复用池化 LLM Provider 生成话术计划。
//   - util.go：removePunctuation 等文本清洗。
package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/eino/schema"

	"xiaozhi-esp32-server-golang/internal/domain/llm"
	llm_common "xiaozhi-esp32-server-golang/internal/domain/llm/common"
	"xiaozhi-esp32-server-golang/internal/pool"
	log "xiaozhi-esp32-server-golang/logger"
)

// openClawWarmupSchedule 是暖场话术的 11 个播报时间点（相对「话术计划就绪」的时刻）。
// 为什么这么排：
//   - 第 1 秒先接一句，用户一沉默就立刻有人应声；
//   - 之后每 10 秒一条，间隔一致以便覆盖「等几十秒」的常见情形；
//   - 措辞会随时序从「我看一下」逐步过渡到「还在确认」，因此时间点必须与
//     openClawWarmupSystemPrompt 里描述的时间点一一对应。
//
// 长度必须与 openClawWarmupPlanSize 一致，否则会出现「有话术但没有时间点播」或反之。
var openClawWarmupSchedule = []time.Duration{
	1 * time.Second,
	10 * time.Second,
	20 * time.Second,
	30 * time.Second,
	40 * time.Second,
	50 * time.Second,
	60 * time.Second,
	70 * time.Second,
	80 * time.Second,
	90 * time.Second,
	100 * time.Second,
}

const (
	// openClawWarmupPlanTimeout 生成话术计划的超时。
	// 暖场的意义是「快速接话」，若 LLM 迟迟给不出计划，宁可不暖场也不能一直占着资源等。
	openClawWarmupPlanTimeout = 8 * time.Second
	// openClawWarmupPlanSize 话术条数，与 openClawWarmupSchedule 的长度、
	// 以及提示词里要求的「11 条」严格一致。
	openClawWarmupPlanSize = 11
)

// openClawWarmupSystemPrompt 是生成暖场话术的系统提示词。
// 为什么约束要写得这么细：
//   - 暖场内容会被直接播出去，一旦 LLM「顺手」把答案说了，正式回复再播一遍就是重复甚至矛盾，
//     所以第 1 条硬性禁止给事实/结论/建议/步骤/解释。
//   - 用户输入往往是命令句（「帮我查一下北京天气」），原样复述会像系统在念指令，
//     因此第 4/5 条要求只能提炼成助手视角的名词短语。
//   - 前 1~2 条刻意要求更轻（「我看一下」）：一上来就说很重的安慰话反而显得在拖延；
//     后段才逐步承认等待变久，让语气自然。
//   - 限制 4~16 个汉字是语音播报约束：太短没信息，太长会盖住正式回复的开头。
//   - 禁止「正在为您处理」这类客服腔，是因为它听起来像系统通知，不像真人在通话里接话。
//   - 强制严格 JSON 数组，是为了让 parseOpenClawWarmupPlan 能稳定解析。
const openClawWarmupSystemPrompt = `你是实时语音对话里的暖场助手，不是主回答者。

你的任务是：在主回复返回前，生成 11 条很短的中文接话，让等待过程听起来一直有人在回应。

硬性要求：
1. 只负责暖场，不能直接回答问题，不能给出事实、结论、建议、步骤、分析、解释或推测。
2. 语气要像真人在通话里轻声接话：简短、自然、口语化、有耐心。
3. 不要像客服，不要像系统提示，不要像通知播报，不要像写文案。
4. 禁止复述用户原话，尤其不要把“帮我查一下”“帮我看看”“帮我查询一下”“告诉我”这类用户指令原样拼进回复。
5. 如果需要提到主题，只能提炼成助手视角的名词短语，例如“北京后天的天气”“这个安排”；不要用命令句。
6. 前 1 到 2 条尽量更轻，不一定带主题词，例如“我看一下”“等我一下”；不要一上来就说很重的安慰话。
7. 后面的句子再逐步表达“我还在看”“我还在确认”，但要自然，不要机械重复。
8. 避免使用“正在为您处理”“请稍候”“持续跟进”“调取数据”“连接服务中”这类生硬说法。
9. 每条都必须是单句短中文，适合语音播报，长度控制在 4 到 16 个汉字。
10. 你会拿到实际播报时间点。11 条话术必须严格按这些时间点依次设计：
   - 第 1 秒：像刚接到问题，轻轻接一句。
   - 第 10 秒：自然补一句，语气仍然轻。
   - 第 20、30 秒：开始表达“我还在看”，但不要机械。
   - 第 40、50、60 秒：继续安抚，允许更明确地说“还在确认”。
   - 第 70、80、90、100 秒：承认时间有点久，但仍然自然、平静，不抱怨。
11. 只输出严格 JSON 数组，长度必须为 11。
12. JSON 每项格式必须为：{"text":"暖场语"}。
13. 禁止输出编号、Markdown、解释、代码块或 JSON 之外的任何内容。`

// openClawWarmupTask 表示一次 OpenClaw 等待期的暖场任务。
// 生命周期：startOpenClawWarmup 创建 → 后台 goroutine 生成计划并按时间点播报 →
// 正式回复到达（cancel/finish）或会话重置/退出时结束。
type openClawWarmupTask struct {
	// correlationID 本轮请求的关联 ID（来自 OpenClaw 返回的消息 ID）。
	// 用它把「本次暖场」与「对应的正式回复流」绑定，避免抢占/停止时错认成别的轮次。
	correlationID string
	// sessionCtx 会话上下文：暖场音频的 TTS 与发送都挂在这个上下文上，
	// 会话被打断或被关闭时暖场随之失效（不需要额外处理）。
	sessionCtx context.Context
	// warmupCtx 暖场自身可取消的上下文：用于「只停掉这个暖场任务」。
	// 与会话上下文解耦的好处是：单独取消暖场不会误伤正在进行的正式回复流程。
	warmupCtx context.Context
	// cancelWarmup 取消 warmupCtx 的函数，是本任务唯一的「停止开关」。
	cancelWarmup context.CancelFunc

	// linesMu 保护 lines：写入发生在生成计划的 goroutine，读取发生在播报循环，
	// 两者并发，因此用读写锁（读多写少）。
	linesMu sync.RWMutex
	// lines 已清洗过的话术，按时间点下标存放。
	// 空串表示该时间点没有可用话术（生成失败或该条被过滤），播报时跳过。
	lines []string

	// stateMu 保护下面这一组「播报状态机」字段。
	// 它们会被多个 goroutine 触碰（播报循环、正式回复到达时的抢占路径），必须串行化，
	// 否则会出现重复发 tts_start、状态错乱等问题。
	stateMu sync.Mutex
	// speechStarted 是否已经为本次暖场发出过 tts_start（见 beginOpenClawSpeech）。
	speechStarted bool
	// speechEnded 是否已经收尾（见 endOpenClawSpeech）。
	// 与 speechStarted 一起保证 begin/end 成对且各只执行一次。
	speechEnded bool
	// nextWarmupSegmentIsStart 下一条暖场话术是否要带 IsStart 标记。
	// 语义：整轮暖场只有第一条是「段首」，后续都是续接。
	// 若每条都标段首，TTS 会把每一句都当成新对话开始，反复重置播放状态。
	nextWarmupSegmentIsStart bool
	// planReadyAt 话术计划就绪的时刻，也是 11 个时间点的计时基准。
	// 零值表示计划生成失败（此时不会播报任何话术）。
	planReadyAt time.Time
	// planReadySignaled 防止重复关闭 planReadyCh（对已关闭的 channel 再 close 会 panic）。
	planReadySignaled bool

	// spokeAny 是否已经真正播报过至少一条暖场话术。
	// 用原子类型是因为读取路径（取消判断）可能不持有 stateMu。
	// 它决定取消时要不要去打断 TTS 队列：没播过就没必要清。
	spokeAny atomic.Bool
	// planReadyCh 计划就绪通知通道，用「关闭通道」的方式广播（见 markPlanReady）。
	planReadyCh chan struct{}
}

// openClawWarmupLine 是 LLM 返回的 JSON 数组元素格式：{"text":"暖场语"}。
type openClawWarmupLine struct {
	Text string `json:"text"`
}

// startOpenClawWarmup 启动一次暖场任务（在把用户文本成功转给 OpenClaw 之后调用）。
// 上下文层级刻意取 AfterAsrSessionCtx：暖场属于「ASR 之后」的输出阶段，
// 这样 StopAssistantOutputAfterAsrWithReason 这类「只停 assistant 输出」的打断能连带停掉暖场，
// 同时又不会影响监听侧上下文（realtime 下监听必须继续）。
func (s *ChatSession) startOpenClawWarmup(correlationID string, userText string) {
	correlationID = strings.TrimSpace(correlationID)
	if correlationID == "" || s == nil || s.clientState == nil {
		return
	}

	sessionCtx := s.clientState.SessionCtx.Get(s.clientState.Ctx)
	parentCtx := s.clientState.AfterAsrSessionCtx.Get(sessionCtx)
	warmupCtx, cancelWarmup := context.WithCancel(parentCtx)
	task := &openClawWarmupTask{
		correlationID:            correlationID,
		sessionCtx:               parentCtx,
		warmupCtx:                warmupCtx,
		cancelWarmup:             cancelWarmup,
		lines:                    make([]string, openClawWarmupPlanSize),
		nextWarmupSegmentIsStart: true,
		planReadyCh:              make(chan struct{}),
	}

	s.replaceOpenClawWarmup(task)
	log.Infof("OpenClaw warmup started: device=%s correlation_id=%s", s.clientState.DeviceID, correlationID)

	go s.runOpenClawWarmupTask(task, userText)
}

// replaceOpenClawWarmup 用新任务替换当前任务，并取消旧任务。
// 为什么要替换而不是直接并存：同一条会话同一时刻只应该有一个暖场在跑，
// 旧任务若不取消，两批暖场话术会同时在 TTS 队列里抢着说话。
func (s *ChatSession) replaceOpenClawWarmup(task *openClawWarmupTask) {
	s.openClawWarmupMu.Lock()
	oldTask := s.openClawWarmup
	s.openClawWarmup = task
	s.openClawWarmupMu.Unlock()

	if oldTask != nil {
		oldTask.cancelWarmupOnly()
	}
}

// cancelWarmupOnly 只取消暖场上下文（不碰 TTS 队列、不改任务状态）。
// 用于「不再需要继续生成/播报暖场，但已有音频可能还在播」的轻量场景。
func (task *openClawWarmupTask) cancelWarmupOnly() {
	if task == nil || task.cancelWarmup == nil {
		return
	}
	task.cancelWarmup()
}

// markSpeechStarted 标记「本次暖场开始发声」，返回是否是首次标记成功。
// 用返回值做门禁：只有第一次调用会拿到 true，从而避免重复发 tts_start；
// 若已经结束（speechEnded）则拒绝重新开始，防止暖场被暂停后又被拉起来。
func (task *openClawWarmupTask) markSpeechStarted() bool {
	if task == nil {
		return false
	}
	task.stateMu.Lock()
	defer task.stateMu.Unlock()
	if task.speechStarted || task.speechEnded {
		return false
	}
	task.speechStarted = true
	return true
}

// markSpeechEnded 标记暖场发声结束，返回是否首次成功。
// 前提是必须已经 started：保证 begin/end 严格配对，不会出现「没开始就结束」的脏状态。
func (task *openClawWarmupTask) markSpeechEnded() bool {
	if task == nil {
		return false
	}
	task.stateMu.Lock()
	defer task.stateMu.Unlock()
	if !task.speechStarted || task.speechEnded {
		return false
	}
	task.speechEnded = true
	return true
}

// takeWarmupSegmentStartFlag 取出并消费「下一条是否为段首」标记，返回值同时被翻转。
// 必须「取出即翻转」：否则每一条话术都会被当成新段的开始，
// TTS 会不断重置状态，端上也会把同一轮暖场显示成多轮。
func (task *openClawWarmupTask) takeWarmupSegmentStartFlag() bool {
	if task == nil {
		return true
	}
	task.stateMu.Lock()
	defer task.stateMu.Unlock()
	isStart := task.nextWarmupSegmentIsStart
	task.nextWarmupSegmentIsStart = false
	return isStart
}

// markPlanReady 通知「话术计划已就绪/已失败」，并记录计时基准时刻。
// readyAt 为零值表示生成失败：此时同样关闭通道（让等待方立刻继续，不用干等超时），
// 但 waitPlanReady 会因 planReadyAt 为零而返回 false，播报循环随之退出。
// 为什么用「关闭通道」而不是发送值：这是一次性广播，可能有多个等待者，
// close 天然支持广播且不需要关心接收者数量；planReadySignaled 保证只 close 一次。
func (task *openClawWarmupTask) markPlanReady(readyAt time.Time) {
	if task == nil {
		return
	}
	task.stateMu.Lock()
	if task.planReadySignaled {
		task.stateMu.Unlock()
		return
	}
	task.planReadyAt = readyAt
	task.planReadySignaled = true
	close(task.planReadyCh)
	task.stateMu.Unlock()
}

// waitPlanReady 等待话术计划就绪，返回计时基准时刻与是否成功。
// 失败情形有两种：上下文被取消（暖场已经不需要了），或计划生成本身失败（planReadyAt 为零）。
// 拿不到基准时刻就不该播报，否则时间点会从错误的起点开始算。
func (task *openClawWarmupTask) waitPlanReady(ctx context.Context) (time.Time, bool) {
	if task == nil {
		return time.Time{}, false
	}

	select {
	case <-ctx.Done():
		return time.Time{}, false
	case <-task.planReadyCh:
	}

	task.stateMu.Lock()
	defer task.stateMu.Unlock()
	if task.planReadyAt.IsZero() {
		return time.Time{}, false
	}
	return task.planReadyAt, true
}

// hasSpokenAny 是否已播报过至少一条暖场话术（决定取消时是否需要打断 TTS）。
func (task *openClawWarmupTask) hasSpokenAny() bool {
	if task == nil {
		return false
	}
	return task.spokeAny.Load()
}

// getOpenClawWarmupTask 取当前暖场任务（不摘除）；correlationID 非空时要求与之匹配。
// 用于只读检查，例如判断某条正文流当初是否挂了暖场。
// correlationID 不匹配就返回 nil：防止把别的轮次的任务当成自己的（错停/错抢）。
func (s *ChatSession) getOpenClawWarmupTask(correlationID string) *openClawWarmupTask {
	if s == nil {
		return nil
	}
	correlationID = strings.TrimSpace(correlationID)
	s.openClawWarmupMu.Lock()
	defer s.openClawWarmupMu.Unlock()
	task := s.openClawWarmup
	if task == nil {
		return nil
	}
	if correlationID != "" && task.correlationID != correlationID {
		return nil
	}
	return task
}

// takeOpenClawWarmupTask 取出并摘除当前暖场任务（把 s.openClawWarmup 置空）。
// 「摘除」保证收尾流程只执行一次：谁 take 到谁负责 finish，
// 之后并发的清理调用只会拿到 nil，不会重复打断 TTS。
func (s *ChatSession) takeOpenClawWarmupTask(correlationID string) *openClawWarmupTask {
	if s == nil {
		return nil
	}
	correlationID = strings.TrimSpace(correlationID)
	s.openClawWarmupMu.Lock()
	defer s.openClawWarmupMu.Unlock()
	task := s.openClawWarmup
	if task == nil {
		return nil
	}
	if correlationID != "" && task.correlationID != correlationID {
		return nil
	}
	s.openClawWarmup = nil
	return task
}

// cancelOpenClawWarmup 取消暖场（保留任务引用，不清空 s.openClawWarmup），返回是否真的取消了。
// 与 finishOpenClawWarmup 的区别：
//   - cancel：只停「后续话术」与被取消的音频，任务对象仍留在会话上
//     （外部状态机可能还要拿它做判断，例如 InjectOpenClawResponse 之后还要 beginOpenClawSpeech）；
//   - finish：会把任务 take（摘除）并执行发声收尾（endOpenClawSpeech），用于本次等待彻底结束。
//
// interrupt=true 且确实播过话术时才去打断 TTS 队列：
// 什么都没播过却清队列，会把正式回复刚推进队列的内容一起清掉。
// 已经取消过（warmupCtx.Err() != nil）时返回 false，避免重复打断。
func (s *ChatSession) cancelOpenClawWarmup(correlationID string, interrupt bool) bool {
	if s == nil {
		return false
	}

	task := s.getOpenClawWarmupTask(correlationID)
	if task == nil {
		return false
	}
	if task.warmupCtx.Err() != nil {
		return false
	}

	task.cancelWarmupOnly()
	if interrupt && task.hasSpokenAny() {
		s.InterruptAndClearTTSQueueWithReason(fmt.Sprintf("OpenClaw warmup canceled correlation_id=%s", correlationID))
	}

	log.Infof(
		"OpenClaw warmup canceled: device=%s correlation_id=%s interrupt=%v spoke_any=%v",
		s.clientState.DeviceID,
		task.correlationID,
		interrupt,
		task.hasSpokenAny(),
	)
	return true
}

// finishOpenClawWarmup 结束并摘除暖场任务：取消话术、按需打断 TTS、并收尾发声状态。
// interrupt=true 用于「正式回复/退出模式」等需要立刻让暖场闭嘴的场景；
// 这里的打断是无条件执行的（不像 cancel 会先看 spoke_any），
// 因为调用方已经确定本轮输出权要交给别人，哪怕只播了半句也必须掐掉。
func (s *ChatSession) finishOpenClawWarmup(correlationID string, interrupt bool) bool {
	task := s.takeOpenClawWarmupTask(correlationID)
	if task == nil {
		return false
	}

	task.cancelWarmupOnly()
	if interrupt {
		s.InterruptAndClearTTSQueueWithReason(fmt.Sprintf("OpenClaw warmup finished correlation_id=%s interrupt", correlationID))
	}
	s.endOpenClawSpeech(task)

	log.Infof(
		"OpenClaw warmup finished: device=%s correlation_id=%s interrupt=%v spoke_any=%v",
		s.clientState.DeviceID,
		task.correlationID,
		interrupt,
		task.hasSpokenAny(),
	)
	return true
}

// beginOpenClawSpeech 进入暖场发声阶段：清理上一段音频历史并排队一条 tts_start。
// 只在首次标记成功时执行，避免重复发 tts_start。
// 清空音频历史的原因：音频历史用于「两阶段消息保存」，
// 暖场音频不属于任何一条对话消息，若与正式回复的音频混在一起，
// 会被错误地挂到正式回复的消息记录上。
func (s *ChatSession) beginOpenClawSpeech(task *openClawWarmupTask) {
	if task == nil {
		return
	}
	if !task.markSpeechStarted() {
		return
	}
	s.ttsManager.ClearAudioHistory()
	s.ttsManager.EnqueueTtsStartWithReason(task.sessionCtx, fmt.Sprintf("OpenClaw warmup start correlation_id=%s", task.correlationID))
}

// endOpenClawSpeech 收尾暖场发声：取出并丢弃暖场期间累积的音频历史。
// 注意是「丢弃」——暖场音频只是占位话术，不能进入历史保存，也不能影响正式回复的音频统计。
func (s *ChatSession) endOpenClawSpeech(task *openClawWarmupTask) {
	if task == nil {
		return
	}
	if !task.markSpeechEnded() {
		return
	}
	s.ttsManager.GetAndClearAudioHistory()
}

// runOpenClawWarmupTask 是暖场任务的主循环（在独立 goroutine 中运行）。
// 分两个阶段：
//  1. 并发地生成话术计划（不阻塞后续准备），再用 waitPlanReady 等它完成；
//     必须以「计划就绪时刻」作为计时基准，否则生成耗时会把所有时间点整体推迟。
//  2. 按 openClawWarmupSchedule 逐个到点播报，任何时刻上下文被取消就立刻退出。
//
// 结尾刻意不清理 active task：最后一条暖场音频可能仍在发送/播放中，
// 此时仍需允许正式回复首句到达时执行抢占打断；若提前摘除任务，抢占就找不到目标了。
func (s *ChatSession) runOpenClawWarmupTask(task *openClawWarmupTask, userText string) {
	planCtx, cancel := context.WithTimeout(task.warmupCtx, openClawWarmupPlanTimeout)
	defer cancel()
	defer log.Infof(
		"OpenClaw warmup task stopped: device=%s correlation_id=%s warmup_err=%v session_err=%v spoke_any=%v",
		s.clientState.DeviceID,
		task.correlationID,
		task.warmupCtx.Err(),
		task.sessionCtx.Err(),
		task.hasSpokenAny(),
	)

	go func() {
		lines, err := s.generateOpenClawWarmupPlan(planCtx, task.correlationID, userText)
		if err != nil {
			// 超时导致的失败不重复告警：超时本身已由外层日志体现，避免刷屏。
			if planCtx.Err() == nil {
				log.Warnf("OpenClaw warmup plan generation failed: device=%s correlation_id=%s err=%v", s.clientState.DeviceID, task.correlationID, err)
			}
			task.markPlanReady(time.Time{})
			return
		}
		task.setLines(lines)
		task.markPlanReady(time.Now())
		log.Infof("OpenClaw warmup plan ready: device=%s correlation_id=%s line_count=%d", s.clientState.DeviceID, task.correlationID, len(lines))
	}()

	baseAt, ok := task.waitPlanReady(task.warmupCtx)
	if !ok {
		return
	}

	for idx, delay := range openClawWarmupSchedule {
		if !waitOpenClawWarmupUntil(task.warmupCtx, baseAt.Add(delay)) {
			return
		}
		if task.warmupCtx.Err() != nil {
			return
		}

		text := task.lineAt(idx)
		if text == "" {
			// 该时间点没有可用话术（生成失败或被过滤），静默跳过而不是播空内容。
			continue
		}

		log.Infof(
			"OpenClaw warmup speaking: device=%s correlation_id=%s slot=%d text=%q",
			s.clientState.DeviceID,
			task.correlationID,
			idx,
			text,
		)
		if err := s.speakOpenClawWarmupLine(task, text); err != nil && task.sessionCtx.Err() == nil {
			log.Warnf("OpenClaw warmup speak failed: device=%s correlation_id=%s slot=%d err=%v", s.clientState.DeviceID, task.correlationID, idx, err)
			return
		}
		task.spokeAny.Store(true)
	}

	// 不在这里清理 active task：最后一条暖场音频可能仍在发送/播放中，
	// 需要继续允许 OpenClaw 首句到达时执行抢占打断。
}

// waitOpenClawWarmupUntil 等到 deadline，返回是否正常到达（上下文取消则 false）。
// 注意已经过点（wait<=0）时也要检查一次上下文：
// 否则在「已取消但恰好该播下一条」的时序下，可能又播出一句不该播的话。
func waitOpenClawWarmupUntil(ctx context.Context, deadline time.Time) bool {
	wait := time.Until(deadline)
	if wait <= 0 {
		return ctx.Err() == nil
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// setLines 写入话术计划：逐条清洗，非法或超长的条目置空（对应时间点跳过）。
// 加锁是因为写入发生在生成 goroutine，而读取发生在播报循环，两者并发。
func (task *openClawWarmupTask) setLines(lines []string) {
	if task == nil || len(lines) == 0 {
		return
	}

	task.linesMu.Lock()
	defer task.linesMu.Unlock()

	if task.lines == nil {
		task.lines = make([]string, openClawWarmupPlanSize)
	}
	for idx := 0; idx < openClawWarmupPlanSize && idx < len(lines); idx++ {
		if text := sanitizeOpenClawWarmupText(lines[idx]); text != "" {
			task.lines[idx] = text
		}
	}
}

// lineAt 取第 index 条话术（已去首尾空白）。
// 越界或该条为空时返回空串，由调用方决定跳过——这样调用方无需做边界判断。
func (task *openClawWarmupTask) lineAt(index int) string {
	if task == nil || index < 0 {
		return ""
	}

	task.linesMu.RLock()
	defer task.linesMu.RUnlock()

	if index >= len(task.lines) {
		return ""
	}
	return strings.TrimSpace(task.lines[index])
}

// speakOpenClawWarmupLine 播报一条暖场话术：必要时先进入发声阶段，再交给 TTS 链路。
// IsEnd 固定为 true：每条暖场都是独立短句，说完即结束这一段，
// 这样 TTS 侧不必等待后续文本就能立刻合成播报（首帧延迟最低）。
// 最后一个参数 true 表示「确保已入队」：让调用方通过返回值及时发现话术没能进入发送链路。
func (s *ChatSession) speakOpenClawWarmupLine(task *openClawWarmupTask, text string) error {
	text = sanitizeOpenClawWarmupText(text)
	if text == "" {
		return nil
	}
	if task == nil {
		return nil
	}
	if task.sessionCtx.Err() != nil {
		return task.sessionCtx.Err()
	}

	s.beginOpenClawSpeech(task)
	if task.sessionCtx.Err() != nil {
		return task.sessionCtx.Err()
	}

	resp := llm_common.LLMResponseStruct{
		Text:    text,
		IsStart: task.takeWarmupSegmentStartFlag(),
		IsEnd:   true,
	}
	// 暖场句需要确保已经进入发送链路，避免被后续正式回复“看起来像没生效”。
	return s.ttsManager.handleTextResponse(task.sessionCtx, resp, true)
}

// generateOpenClawWarmupPlan 调 LLM 生成 11 条暖场话术。
// 复用池化的 LLM Provider 而不是新建连接：暖场是偶发的短请求，
// 池化能省掉每次等待都付出的建连/鉴权开销。
// sessionID 里带上 correlationID，是为了让上游 LLM 侧按轮次隔离上下文，避免不同轮次串味。
func (s *ChatSession) generateOpenClawWarmupPlan(ctx context.Context, correlationID string, userText string) ([]string, error) {
	llmWrapper, err := pool.Acquire[llm.LLMProvider](
		"llm",
		s.clientState.DeviceConfig.Llm.Provider,
		s.clientState.DeviceConfig.Llm.Config,
	)
	if err != nil {
		return nil, fmt.Errorf("acquire llm provider: %w", err)
	}
	defer pool.Release(llmWrapper)

	dialogue := []*schema.Message{
		schema.SystemMessage(openClawWarmupSystemPrompt),
		schema.UserMessage(buildOpenClawWarmupUserPrompt(userText)),
	}

	msgChan := llmWrapper.GetProvider().ResponseWithContext(
		ctx,
		buildOpenClawWarmupSessionID(s.clientState.SessionID, correlationID),
		dialogue,
		nil,
	)

	raw, err := collectOpenClawWarmupResponse(ctx, msgChan)
	if err != nil {
		return nil, err
	}
	lines := parseOpenClawWarmupPlan(raw)
	if countOpenClawWarmupLines(lines) == 0 {
		// 一条可用话术都没有时按失败处理，让上层把这次暖场当作「没有话术」，而不是播一堆空串。
		return nil, fmt.Errorf("empty warmup plan")
	}
	return lines, nil
}

// buildOpenClawWarmupUserPrompt 组装用户侧提示词：既描述本轮任务，也再次强调播报时间点。
// 特意把「主题」单独提炼成一行（而不是原样塞入用户指令），
// 是为了防止模型把「帮我查一下」这类祈使句当内容播出来。
func buildOpenClawWarmupUserPrompt(userText string) string {
	trimmed := strings.TrimSpace(userText)
	topic := formatOpenClawWarmupTopic(buildOpenClawWarmupHint(userText))
	topicLine := "不要复述“帮我查一下”这类用户指令。"
	if topic != "" {
		topicLine = fmt.Sprintf("如果需要提到主题，只能提炼成名词短语“%s”，不要复述“帮我查一下”这类用户指令。", topic)
	}
	return fmt.Sprintf(
		"用户本轮任务：\n%s\n\n%s\n\n实际播报时间点依次为：第1秒、第10秒、第20秒、第30秒、第40秒、第50秒、第60秒、第70秒、第80秒、第90秒、第100秒。\n请输出 11 条暖场语，并按上述 11 个时间点一一对应。",
		trimmed,
		topicLine,
	)
}

// buildOpenClawWarmupSessionID 生成暖场专用的会话 ID（带 :warmup 后缀与截断的 correlationID）。
// 带上 correlationID 让上游 LLM 侧能按轮次隔离；截断前 12 位是为控制 ID 长度，
// 缺失信息时有兜底值，保证不同轮次尽量不冲突。
func buildOpenClawWarmupSessionID(sessionID string, correlationID string) string {
	base := strings.TrimSpace(sessionID)
	if base == "" {
		base = "openclaw"
	}
	correlationID = strings.TrimSpace(correlationID)
	if len(correlationID) > 12 {
		correlationID = correlationID[:12]
	}
	if correlationID == "" {
		return base + ":warmup"
	}
	return base + ":warmup:" + correlationID
}

// collectOpenClawWarmupResponse 把 LLM 的流式响应拼成一个完整字符串。
// 这里必须单独识别「错误消息」（IsLLMErrorMessage）：
// LLM 的错误经常以普通文本形式混在正常流里返回，若不识别就会被当成话术播出去。
func collectOpenClawWarmupResponse(ctx context.Context, msgChan chan *schema.Message) (string, error) {
	var builder strings.Builder

	for {
		select {
		case <-ctx.Done():
			return builder.String(), ctx.Err()
		case msg, ok := <-msgChan:
			if !ok {
				return builder.String(), nil
			}
			if msg == nil {
				continue
			}
			if llm.IsLLMErrorMessage(msg) {
				errMsg := strings.TrimSpace(llm.LLMErrorMessage(msg))
				if errMsg == "" {
					errMsg = "unknown llm error"
				}
				return builder.String(), fmt.Errorf("llm returned error: %s", errMsg)
			}
			if msg.Content != "" {
				builder.WriteString(msg.Content)
			}
		}
	}
}

// parseOpenClawWarmupPlan 解析 LLM 返回的话术计划，对 JSON 做容错。
// 为什么要容错：模型经常在 JSON 前后加一句解释、或整体包一层 Markdown 代码块，
// 直接 json.Unmarshal 会因首尾噪声整段失败。这里截取「第一个 [ 到最后一个 ]」之间的内容再解析，
// 能吃掉绝大部分这类噪声；元素格式也兼容两种（对象 {"text":...} 与纯字符串数组）。
// 彻底解析失败时返回全空计划并告警，让上层把这次暖场当成「没有话术」处理。
func parseOpenClawWarmupPlan(raw string) []string {
	lines := make([]string, openClawWarmupPlanSize)

	raw = strings.TrimSpace(raw)
	if raw == "" {
		return lines
	}

	candidate := raw
	start := strings.Index(candidate, "[")
	end := strings.LastIndex(candidate, "]")
	if start >= 0 && end > start {
		candidate = candidate[start : end+1]
	}

	var objectItems []openClawWarmupLine
	if err := json.Unmarshal([]byte(candidate), &objectItems); err == nil {
		return buildOpenClawWarmupPlanLines(objectItemsToStrings(objectItems))
	}

	var stringItems []string
	if err := json.Unmarshal([]byte(candidate), &stringItems); err == nil {
		return buildOpenClawWarmupPlanLines(stringItems)
	}

	log.Warnf("OpenClaw warmup plan parse failed, ignored: raw=%q", raw)
	return lines
}

// objectItemsToStrings 把 {"text":...} 形式的元素摊平成纯文本数组，
// 让后续统一走 buildOpenClawWarmupPlanLines 的清洗逻辑。
func objectItemsToStrings(items []openClawWarmupLine) []string {
	lines := make([]string, 0, len(items))
	for _, item := range items {
		lines = append(lines, item.Text)
	}
	return lines
}

// buildOpenClawWarmupPlanLines 把文本数组规整成固定长度（openClawWarmupPlanSize）的计划：
// 逐条清洗，多余条目丢弃、不足的留空（对应时间点跳过）。
// 固定长度是刻意设计：下标即时间点，播报循环可以按下标直接取用。
func buildOpenClawWarmupPlanLines(items []string) []string {
	lines := make([]string, openClawWarmupPlanSize)
	for idx := 0; idx < openClawWarmupPlanSize && idx < len(items); idx++ {
		if text := sanitizeOpenClawWarmupText(items[idx]); text != "" {
			lines[idx] = text
		}
	}
	return lines
}

// countOpenClawWarmupLines 统计非空话术条数，用于判断「计划是不是完全空的」。
func countOpenClawWarmupLines(lines []string) int {
	count := 0
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			count++
		}
	}
	return count
}

// sanitizeOpenClawWarmupText 清洗一条话术，返回可播报文本或空串（空串表示该条作废）。
// 逐个清洗动作的动机：
//   - 换行、首尾空白、引号括号：模型常见的格式噪声，播报出来毫无意义；
//   - 行首编号（如 "1."、"1、"）：模型即便被要求只输出 JSON 也常加编号；
//   - 合并多余空格：语音合成对连续空格的处理不一致，统一成单空格更稳；
//   - 超过 16 个汉字直接丢弃：暖场句必须短，长句会盖住正式回复的开头。
func sanitizeOpenClawWarmupText(text string) string {
	text = strings.ReplaceAll(text, "\n", " ")
	text = strings.TrimSpace(text)
	text = strings.Trim(text, "\"'`[]{}")
	text = strings.TrimLeft(text, "0123456789.、- ")
	text = strings.Join(strings.Fields(text), " ")
	if text == "" {
		return ""
	}
	if isInvalidOpenClawWarmupText(text) {
		return ""
	}

	runes := []rune(text)
	if len(runes) > 16 {
		return ""
	}
	return text
}

// isInvalidOpenClawWarmupText 判断话术里是否混入了「用户指令式」措辞（帮我/给我/告诉我…）。
// 这类句子读起来像系统在复述用户命令，不能作为暖场播报，发现即整条作废。
func isInvalidOpenClawWarmupText(text string) bool {
	for _, bad := range []string{
		"帮我",
		"给我",
		"告诉我",
		"请帮",
		"麻烦帮",
		"能帮我",
		"可以帮我",
		"帮忙查",
		"帮忙看",
		"帮忙问",
	} {
		if strings.Contains(text, bad) {
			return true
		}
	}
	return false
}

// buildOpenClawWarmupHint 从用户输入里提炼一个「名词短语主题」，供暖场话术引用。
// 为什么需要：用户说的是「帮我查一下北京后天的天气怎么样」，直接交给模型就有复述指令的风险；
// 先在本地剥掉祈使前缀和疑问后缀，得到一个中性短语（如「北京后天的天气」），
// 再让模型只围绕这个短语说「我还在看」——既不泄露答案，也不会像在念指令。
// 先做去标点是因为标点会把「帮我查一下，北京…」切成不好处理的形式。
// 最后的截断（10 字）与去尾语气词是为了让短语更短更干净，便于拼进播报句。
func buildOpenClawWarmupHint(userText string) string {
	trimmed := strings.TrimSpace(userText)
	if trimmed == "" {
		return ""
	}

	normalized := removePunctuation(trimmed)
	if normalized == "" {
		return ""
	}
	normalized = trimOpenClawWarmupCommandPrefix(normalized)
	normalized = trimOpenClawWarmupQuestionSuffix(normalized)
	if normalized == "" {
		return ""
	}

	for _, keyword := range []string{"天气", "气温", "温度", "预报"} {
		if idx := strings.Index(normalized, keyword); idx >= 0 {
			limit := idx + len([]rune(keyword))
			runes := []rune(normalized)
			if limit > len(runes) {
				limit = len(runes)
			}
			normalized = string(runes[:limit])
			break
		}
	}

	runes := []rune(normalized)
	if len(runes) > 10 {
		runes = runes[:10]
	}
	for len(runes) > 0 {
		last := runes[len(runes)-1]
		if last == '的' || last == '了' || last == '呢' {
			runes = runes[:len(runes)-1]
			continue
		}
		break
	}
	return string(runes)
}

// trimOpenClawWarmupCommandPrefix 循环剥掉句首的祈使/查询类前缀。
// 关键点：
//   - 前缀表按「长前缀在前、短前缀在后」排列，避免短前缀先把长前缀截断成半句；
//   - 做法是 for 循环而非只剥一次，因为一句话可能叠好几层
//     （「麻烦帮我查一下」「那请问…」）；
//   - 剥完即重新匹配，直到没有再能剥掉的前缀为止。
func trimOpenClawWarmupCommandPrefix(text string) string {
	trimmed := strings.TrimSpace(text)
	for {
		changed := false
		for _, prefix := range []string{
			"麻烦帮我查询一下",
			"麻烦帮我查一下",
			"麻烦帮我看一下",
			"请帮我查询一下",
			"请帮我查一下",
			"请帮我看一下",
			"帮我查询一下",
			"帮我查一下",
			"帮我看一下",
			"帮我问一下",
			"给我查询一下",
			"给我查一下",
			"给我看一下",
			"可以帮我查一下",
			"可以帮我看一下",
			"能帮我查一下",
			"能帮我看一下",
			"我想知道",
			"我想问一下",
			"我想问",
			"请问一下",
			"请问",
			"查询一下",
			"查一下",
			"看一下",
			"问一下",
			"帮我查询",
			"帮我查",
			"帮我看",
			"帮我问",
			"给我查询",
			"给我查",
			"给我看",
			"查询",
			"查",
			"看",
			"问",
		} {
			if strings.HasPrefix(trimmed, prefix) {
				trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, prefix))
				changed = true
				break
			}
		}
		if !changed {
			break
		}
	}
	return trimmed
}

// trimOpenClawWarmupQuestionSuffix 剥掉句尾的疑问词/语气词，让短语更像名词短语。
// 逐个 suffix 尝试（而不是只匹配最长的一个），是因为一句话可能同时以多个语气词结尾（「…吗呢」）。
func trimOpenClawWarmupQuestionSuffix(text string) string {
	trimmed := strings.TrimSpace(text)
	for _, suffix := range []string{
		"怎么样",
		"如何",
		"多少",
		"是什么",
		"是啥",
		"吗",
		"呢",
		"呀",
		"吧",
	} {
		trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, suffix))
	}
	return trimmed
}

// formatOpenClawWarmupTopic 把提炼出的短语整理成「定语 + 的 + 主题」的名词短语。
// 例如把「北京后天天气」整理成「北京后天的天气」：带上「的」更符合口语习惯，
// 也让模型更容易理解这是一个「可被提及的主题」而不是一条指令。
// 若「天气」等关键词已经在句首（前缀为空），说明本身就是名词短语，直接返回。
func formatOpenClawWarmupTopic(hint string) string {
	hint = strings.TrimSpace(hint)
	if hint == "" {
		return ""
	}
	for _, keyword := range []string{"天气", "气温", "温度", "预报"} {
		if idx := strings.Index(hint, keyword); idx > 0 {
			prefix := strings.TrimSpace(hint[:idx])
			if prefix == "" || strings.HasSuffix(prefix, "的") {
				return hint
			}
			return prefix + "的" + hint[idx:]
		}
	}
	return hint
}
