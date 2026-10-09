// 本文件实现 TTSManager（文本转语音管理器），是「LLM 文本 → 设备音频」这段下行链路的核心。
//
// 职责：
//  1. 接收 LLM 产出的句子（handleTextResponse / handleTts / handleStreamTts / handleDualStreamTts），
//     调用 TTS provider 合成音频帧；
//  2. 把音频帧与协议控制消息（tts_start/tts_stop、sentence_start/sentence_end）统一放进
//     会话级音频队列，由唯一的发送协程 runSenderLoop 按客户端播放缓冲做流控后发给设备；
//  3. 处理打断（barge-in）：丢弃旧代际音频、取消在途 TTS 请求、统一收口 tts_stop；
//  4. 收口一轮对话的耗时指标（tts_start / 首帧 / tts_stop / turn_end）。
//
// 核心类型与队列：
//   - TTSManager：管理器本体，挂在 ChatSession 上，每个会话一个；
//   - TTSQueueItem / ttsQueue：待合成的「句子」队列，processTTSQueue 逐条消费；
//   - AudioQueueElem / sessionAudioQueue：待发送的「音频与控制消息」队列，runSenderLoop 唯一消费；
//   - delayedSentenceQueue / delayedSentenceReadyQueue：句子控制消息的延迟对齐中转队列。
//
// 在整体流程中的位置：
//
//	LLM 文本 → handleTextResponse → ttsQueue → processTTSQueue 生成音频帧
//	        → sessionAudioQueue → runSenderLoop（流控）→ 设备
//
// 与其它文件的配合：
//   - llm.go：LLMManager 把 LLM 响应文本交给 handleTextResponse；tts_start/tts_stop 的
//     调用时机、turn-end 策略（ttsTurnEndPolicy）也由 llm.go 定义并通过 context 传递；
//   - media_player.go：媒体播放器通过 BeginExclusiveMediaPlayback 申请「独占输出」，
//     复用同一套 sessionAudioQueue + runSenderLoop 发送媒体音频（AudioQueueKindMediaFrame），
//     保证媒体声与 TTS 语音不会互相穿插；
//   - server_transport.go：真正把帧/协议消息写出去的 SendAudio / SendTtsStart / ...；
//   - session.go：TTS_OUTPUT 钩子、指标上报（TraceXxx）与欢迎语等待的收口入口。
package chat

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	. "xiaozhi-esp32-server-golang/internal/data/client"
	chathooks "xiaozhi-esp32-server-golang/internal/domain/chat/hooks"
	llm_common "xiaozhi-esp32-server-golang/internal/domain/llm/common"
	"xiaozhi-esp32-server-golang/internal/domain/tts"
	ttsstream "xiaozhi-esp32-server-golang/internal/domain/tts/streaming"
	"xiaozhi-esp32-server-golang/internal/pool"
	"xiaozhi-esp32-server-golang/internal/util"
	log "xiaozhi-esp32-server-golang/logger"
)

// 会话级全局音频队列元素类型常量
// sessionAudioQueue 是「下行数据的总入口」，里面混放了音频帧和协议控制消息，
// 用 Kind 字段区分元素含义，runSenderLoop 按 Kind 分派处理。
const (
	// AudioQueueKindFrame：TTS 合成出的音频帧（Opus），最终由 SendAudio 发给设备播放。
	AudioQueueKindFrame = 0
	// AudioQueueKindSentenceStart：一句话开始（用于端上字幕/UI）。
	// 它与音频不对齐：真正的发送会被延迟到音频缓冲之后（见 runDelayedSentenceLoop）。
	AudioQueueKindSentenceStart = 1
	// AudioQueueKindSentenceEnd：一句话结束（用于端上字幕/UI），同样会被延迟发送。
	// 其 OnEnd 回调是「本句已彻底发送完毕」的同步点，handleTts 的 isSync 等待依赖它。
	AudioQueueKindSentenceEnd = 2
	// AudioQueueKindTtsStart：通知设备进入 TTS 播放态（协议 tts state=start），
	// 同时把 ttsActive 置真、重置帧计数与播放尾指针。
	AudioQueueKindTtsStart = 3
	// AudioQueueKindTtsStop：结束本轮 TTS（协议 tts state=stop）。
	// 发送前必须等播放尾指针走完 + 保护时间，避免端上尾音被切掉。
	AudioQueueKindTtsStop = 4
	// AudioQueueKindMediaFrame：媒体播放器（music/音频资源）产出的音频帧。
	// 它和 Frame 走同一条发送路径（同样的流控与打断），区别只在于：
	//   - 不计入 TTS 指标与音频历史缓存（只有 Frame 会写 audioHistoryBuffer）；
	//   - 出错日志会标记为「媒体」，便于排查。
	// 之所以让媒体复用 TTS 队列，是为了让「谁在占用喇叭」这件事只有一个仲裁点。
	AudioQueueKindMediaFrame = 5
)

// AudioQueueElem 会话级音频队列元素，兼容 TTS/媒体音频帧与 sentence_start/sentence_end、tts_start/tts_stop。
//
// 设计动机：下行内容既有「音频数据」又有「控制消息」，且都必须严格按顺序、按流控节奏送出。
// 如果分成多条队列，就要在发送端重新做多路归并和排序；统一成一种元素后，
// 顺序天然由 channel 保证，打断时也只需清空一条队列。
type AudioQueueElem struct {
	// Kind 决定本元素的语义，取值为上面的 AudioQueueKind* 常量。
	Kind int // AudioQueueKindFrame / MediaFrame / SentenceStart / SentenceEnd / TtsStart / TtsStop
	// Data 仅在 Kind==Frame / MediaFrame 时使用。
	// 说明「拷贝后入队」：调用方（handleTts 等）在入队前已经 copy 了一份，
	// 因为上游的帧缓冲可能被 provider 复用，不能直接持有其切片。
	Data []byte // Kind==Frame 或 MediaFrame 时使用，拷贝后入队
	// Text 在 SentenceStart / SentenceEnd 时使用，是要下发给端上显示的字幕文本。
	Text string // SentenceStart/SentenceEnd 时使用
	// Err 在 SentenceEnd 时可选，表示本段句子合成/发送过程中出现的错误，
	// 会作为参数传给 OnEnd，让上层区分「正常结束」和「带错结束」。
	Err error // SentenceEnd 时可选，表示本段错误
	// IsStart 在 SentenceStart 时表示是否为整轮的首包（用于统计首帧耗时等）。
	IsStart bool // SentenceStart 时：是否为首包（用于统计）
	// Generation 是音频代际号。打断时会递增全局代际，runSenderLoop 发现元素的代际与当前不符
	// 就直接丢弃并回调 OnEnd(context.Canceled)——这样不用真的去遍历队列删除旧元素。
	Generation uint64 // 代际标识，打断后旧代际元素将被丢弃
	// MetricCycle 是 TTS 指标轮次。只有 TTS 音频帧（Frame）携带它；
	// runSenderLoop 发送首帧时用它判断「这一帧属于哪一轮」，从而记录首帧时间戳。
	MetricCycle uint64 // TTS 指标轮次，TTS 音频首帧发送时用于归属当前轮
	// DebugReason 记录该元素被投递的原因（如 "EnqueueTtsStop"），只用于日志排查。
	DebugReason string
	// OnStart 在控制消息真正被发送时回调（目前用于 TTS 播放开始的钩子）。
	OnStart func()
	// OnEnd 在元素被「消费完毕」时回调：正常发送完成、被丢弃（context.Canceled）或出错时都会调用。
	// 上层的同步等待（isSync）、指标计数收尾、消息两阶段保存都挂在这个回调上。
	OnEnd func(error)
	// OnError 仅在发送失败时回调，用于让调用方感知传输错误（与 OnEnd 可以同时触发）。
	OnError func(error)
}

// delayedSentenceTask 是延迟发送队列里的任务：把某个句子控制消息推迟到 ExecuteAt 时刻再送出。
// 存在的原因：句子控制消息要和音频播放对齐，而音频在客户端有约 120ms 缓冲，
// 所以要等这么久再发（见 sentenceControlDelay）。
type delayedSentenceTask struct {
	// Elem 是被延迟的原始元素（SentenceStart 或 SentenceEnd）。
	Elem AudioQueueElem
	// ExecuteAt 是最早可以发送的时刻。
	ExecuteAt time.Time
}

// interruptRequest 是「发给 runSenderLoop 的打断指令」。
// runSenderLoop 是唯一的队列消费者与发送者，外部想清空队列不能自己动手，
// 只能投递这个请求让发送协程自己清（避免并发操作队列）。
type interruptRequest struct {
	// done 非 nil 时表示同步打断：runSenderLoop 清空队列后会 close(done)，
	// 调用方据此得知「队列真的清干净了」。异步打断时为 nil。
	done chan struct{}
	// reason 是打断原因，仅用于日志（如 "InterruptAndClearQueueSync"）。
	reason string
}

// SessionAudioQueueCap 会话级音频队列容量，足够大以吸收预取并避免阻塞
const SessionAudioQueueCap = 150

// TTSQueueItem 是「待合成的一句话」在 ttsQueue 里的表示。
// processTTSQueue 逐条取出它，调用 TTS provider 合成音频帧。
type TTSQueueItem struct {
	// ctx 是这条合成任务的上下文（通常带会话/turn 信息，取消后应立即停止合成）。
	ctx context.Context
	// llmResponse 单条模式使用：本 item 只包含这一句文本。
	llmResponse llm_common.LLMResponseStruct // 单条模式使用
	// StreamChan 流式模式：非 nil 时优先从此 channel 读（边收文本边合成）。
	// 双流式 TTS 下，handleTextResponse 在 IsStart 时创建它、后续把 LLM 文本写进去、
	// IsEnd 时关闭，从而实现「LLM 还没说完就开始合成」的低延迟。
	StreamChan <-chan llm_common.LLMResponseStruct // 流式模式：非 nil 时优先从此 channel 读
	// enqueueSeq 是入队序号（全局递增）。打断清队列时记录一个水位 droppedTTSSeq，
	// 之后凡是序号小于等于水位的 item 即使还在队列里也被丢弃，实现「按序丢弃」。
	enqueueSeq uint64
	// generation 入队时的音频代际号，用于让合成出的音频带上正确的代际（打断后自然失效）。
	generation uint64
	// metricCycle 入队时的 TTS 指标轮次，用于把本轮耗时统计归属到正确的 turn。
	metricCycle uint64
	// onStartFunc 在首句真正开始播放时回调（如「已开始播报」钩子）。
	onStartFunc func()
	// onEndFunc 在本 item 处理结束时回调（成功/失败/被丢弃都会调用），
	// 是 isSync 同步等待与消息保存第 2 阶段的触发点。
	onEndFunc func(err error)
}

// ttsMetricState 汇总「一轮 TTS」的指标收口状态。
// 为什么需要它：一轮对话里 TTS 请求可能有多个并发/多条（多句、嵌套工具轮次），
// 而 tts_stop / turn_end 只能在「本轮所有工作都结束后」上报一次，
// 因此用这里的计数与标志位做汇聚判断（见 maybeFinalizeTtsMetricLocked）。
type ttsMetricState struct {
	// cycleID 当前指标轮次编号，0 表示还没有开启轮次（EnqueueTtsStart 时自增开启）。
	cycleID uint64
	// pendingItems 已入队但还没处理完的 TTSQueueItem 数量（含流式 item）。
	pendingItems int
	// activeRequests 已向 TTS provider 发起、还没拿到完整音频的合成请求数量。
	activeRequests int
	// turnEndRequested 上游（LLM 层）已声明「本轮逻辑输出结束」，可以准备收口。
	turnEndRequested bool
	// started 是否已记录过 tts_start 时间戳（只记第一次）。
	started bool
	// firstAudio 是否已记录过首帧时间戳（只记第一次）。
	firstAudio bool
	// ttsStopped 是否已上报过 tts_stop 指标（保证只上报一次）。
	ttsStopped bool
	// turnEnded 本轮指标是否已完全收口，收口后所有统计函数都直接返回空结果。
	turnEnded bool
	// turnEndPolicy 本轮结束时要执行的策略（如 goodbye-and-idle），由 context 携带进来。
	turnEndPolicy ttsTurnEndPolicy
}

// TTSManager 负责TTS相关的处理
// 可以根据需要扩展字段
// 目前无状态，但可后续扩展
//
// 它是「每会话一个」的管理器：持有待合成句子队列、待发送音频队列、发送协程状态、
// 打断协作状态、指标状态与音频历史缓存。所有并发点都用显式的 channel 或 mutex 保护，
// 并刻意让「发送」只有 runSenderLoop 一个执行者，以消除多协程抢发导致的乱序。

// TTSManagerOption 是 TTSManager 的可选配置项（函数式选项模式，NewTTSManager 里逐个应用）。
type TTSManagerOption func(*TTSManager)

// TTSManager 的核心字段可归为 6 组：
//  1. 依赖（clientState/session/serverTransport）；
//  2. 两条数据队列（ttsQueue 句子队列 / sessionAudioQueue 发送队列）及其延迟中转队列；
//  3. 打断协作（interruptCh / audioGeneration / audioInterruptCh / ttsActive / senderLoop*）；
//  4. 打断收口登记（interruptStop*：保证 tts_stop 只由 runSenderLoop 发一次）；
//  5. 媒体独占输出（mediaPlayback*）；
//  6. 双流式与指标状态（dualStream* / ttsMetric*）以及音频历史缓存（audioHistoryBuffer）。
type TTSManager struct {
	// clientState 设备共享状态：输出音频格式、会话上下文、统计时间戳、状态机都从这里读。
	clientState *ClientState
	// session 所属会话，用于触发 TTS_OUTPUT 钩子、上报指标、通知欢迎语播放完成。
	session *ChatSession
	// serverTransport 协议发送封装，所有帧与 tts/sentence 消息都经它下发。
	serverTransport *ServerTransport
	// ttsQueue 待合成的句子队列，由 processTTSQueue 单协程消费（容量 10）。
	ttsQueue *util.Queue[TTSQueueItem]
	// sessionAudioQueue 会话级全局音频队列，兼容帧与控制消息。runSenderLoop 是唯一消费者。
	sessionAudioQueue chan AudioQueueElem // 会话级全局音频队列，兼容帧与控制消息
	// delayedSentenceQueue 延迟任务投递口：入队时带上「最早可发送时刻」。
	delayedSentenceQueue chan delayedSentenceTask
	// delayedSentenceReadyQueue 到点后转交的队列：runSenderLoop 从这里取到「现在可以发了」的句子消息。
	delayedSentenceReadyQueue chan AudioQueueElem
	// interruptCh 打断信号：收到后 runSenderLoop 清空 sessionAudioQueue 并继续
	interruptCh chan interruptRequest
	// audioGeneration 会话级音频代际：打断时递增，旧代际元素会被发送协程丢弃
	audioGeneration atomic.Uint64
	// audioInterruptMu 保护 audioInterruptCh 的读写（读端为 TTS 合成请求，写端为打断方）。
	audioInterruptMu sync.RWMutex
	// audioInterruptCh 是一次性的「打断通知」channel：打断时被 close，
	// 在途的 TTS 合成请求通过 withAudioInterruptContext 监听到它后立刻取消，
	// 不用等整个会话 ctx 结束（否则旧请求会继续烧钱烧时间）。
	audioInterruptCh chan struct{}
	// ttsActive 当前是否存在已开始但未结束的 TTS 段
	ttsActive atomic.Bool
	// senderLoopActive runSenderLoop 是否在运行（异步打断在它不运行时可直接跳过投递）。
	senderLoopActive atomic.Bool
	// senderLoopDone runSenderLoop 退出时关闭，供同步打断在关闭路径下快速返回
	senderLoopDone chan struct{}

	// ttsQueueSeq 入队序号发生器（单调递增）。
	ttsQueueSeq atomic.Uint64
	// droppedTTSSeq 丢弃水位：序号 <= 它的 item 视为已被打断作废。
	droppedTTSSeq atomic.Uint64

	// mediaPlaybackMu 保护下面两个媒体独占字段。
	mediaPlaybackMu sync.RWMutex
	// mediaPlaybackActive 媒体是否正在独占输出（此时 TTS 合成要等它释放）。
	mediaPlaybackActive bool
	// mediaPlaybackWaitCh 媒体独占期间，等待方在此 channel 上等关闭通知。
	mediaPlaybackWaitCh chan struct{}

	// interruptStopMu 保护下面这组「待收口的 tts_stop 登记」字段。
	interruptStopMu sync.Mutex
	// interruptStopPending 是否有一次打断留下了待收口的 stop。
	interruptStopPending bool
	// interruptStopSendTtsStop 收口时是否需要真的下发协议 tts_stop。
	interruptStopSendTtsStop bool
	// interruptStopErr 收口时要上报的错误（打断原因/上下文错误）。
	interruptStopErr error
	// interruptStopReason 收口时的日志原因。
	interruptStopReason string

	// 聊天历史音频缓存：持续累积多段TTS音频（Opus帧数组）
	audioHistoryBuffer [][]byte
	// audioMutex 保护 audioHistoryBuffer（发送协程写入，会话层读取/清空）。
	audioMutex sync.Mutex

	// 双流式 TTS 内部 StreamChan：由 handleTextResponse 在 IsStart 时创建，IsEnd 时关闭
	dualStreamChan chan llm_common.LLMResponseStruct
	// dualStreamDone 双流式 isSync 等待用：StreamChan 对应的 onEndFunc 信号
	dualStreamDone chan struct{} // 双流式 isSync 等待用：StreamChan 对应的 onEndFunc 信号
	// dualStreamOwnerCtx 创建该 StreamChan 的调用方 ctx，用于拒绝「别的 turn 写错流」。
	dualStreamOwnerCtx context.Context
	// dualStreamMu 保护以上三个双流式字段。
	dualStreamMu sync.Mutex
	// dualStreamEpoch 双流式代际号：每次清队列/打断自增，用于识别「我创建的这个流是否已作废」。
	dualStreamEpoch atomic.Uint64

	// ttsMetricMu 保护 ttsMetricState（全在锁内更新，判定与收口也是）。
	ttsMetricMu sync.Mutex
	// ttsMetricState 当前轮次的指标收口状态。
	ttsMetricState ttsMetricState
}

// NewTTSManager 只接受WithClientState
//
// 构造时会预建所有 channel 并把代际号初始化为 1（0 保留给「未初始化」语义），
// 但不会启动任何协程——协程由 Start 统一拉起，便于测试与生命周期控制。
func NewTTSManager(clientState *ClientState, serverTransport *ServerTransport, session *ChatSession, opts ...TTSManagerOption) *TTSManager {
	t := &TTSManager{
		clientState:               clientState,
		session:                   session,
		serverTransport:           serverTransport,
		ttsQueue:                  util.NewQueue[TTSQueueItem](10),
		sessionAudioQueue:         make(chan AudioQueueElem, SessionAudioQueueCap),
		delayedSentenceQueue:      make(chan delayedSentenceTask, SessionAudioQueueCap),
		delayedSentenceReadyQueue: make(chan AudioQueueElem, SessionAudioQueueCap),
		interruptCh:               make(chan interruptRequest, 1),
		senderLoopDone:            make(chan struct{}),
		audioInterruptCh:          make(chan struct{}),
	}
	for _, opt := range opts {
		opt(t)
	}
	t.audioGeneration.Store(1)
	return t
}

// normalizeTTSReason 把空原因统一成 "unspecified"，避免日志里出现空字符串难以检索。
func normalizeTTSReason(reason string) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return "unspecified"
	}
	return reason
}

// debugState 汇总当前 TTS/连接的关键状态，供日志打印，方便定位「卡在哪个状态」。
func (t *TTSManager) debugState() string {
	if t == nil || t.clientState == nil {
		return "tts_manager=nil"
	}
	return fmt.Sprintf(
		"status=%s phase=%s ttsActive=%v ttsStart=%v welcomeSpeaking=%v welcomePlaying=%v",
		t.clientState.GetStatus(),
		t.clientState.GetListenPhase(),
		t.ttsActive.Load(),
		t.clientState.GetTtsStart(),
		t.clientState.IsWelcomeSpeaking,
		t.clientState.IsWelcomePlaying,
	)
}

// [5.1] 启动下行链路：起「延迟句子循环 + 唯一发送循环」，并在当前协程消费句子队列。
//
// 启动TTS队列消费协程与统一发送协程（会话级全局音频队列）
//
// 三条协程分工：
//   - runDelayedSentenceLoop：只负责把延迟到期的句子控制消息转交出去；
//   - runSenderLoop：唯一的发送协程（含流控、打断处理、tts_stop 收口）；
//   - processTTSQueue：消费句子队列，生成音频帧推入发送队列（在前台 goroutine 里跑完即返回，
//     所以 Start 本身会阻塞到 ctx 结束，由调用方放在自己的协程里）。
func (t *TTSManager) Start(ctx context.Context) {
	// [5.3 前置] 延迟句子控制消息（与音频播放时刻对齐后再发）
	go t.runDelayedSentenceLoop(ctx)
	// [5.3] 唯一发送出口：流控 + 打断处理 + tts_stop 统一收口
	go t.runSenderLoop(ctx)
	// [5.1] 生成音频帧：消费句子队列（在前台跑，故 Start 会阻塞到 ctx 结束）
	t.processTTSQueue(ctx)
}

// [5.3] 唯一发送出口：从 sessionAudioQueue 取元素按类型分发，流控集中在此；仅 ctx 取消时退出；SessionCtx 取消或收到 TurnAbort 时清空队列并继续
//
// 为什么必须「唯一」：
//   - 下行数据必须严格有序（tts_start → sentence_start → 帧… → sentence_end → tts_stop），
//     多个协程并发写同一个 websocket/udp 通道会乱序；
//   - 流控状态（playbackTail）与指标首帧判定都是「有状态且非线程安全」的，
//     集中在一个协程里就可以完全不加锁；
//   - 打断、清队列、tts_stop 收口都只有这一个执行者，避免了「谁该负责发 tts_stop」的竞态。
//
// 它只做三件事的循环：等待可发送时刻 → 发送 → 处理打断/延迟句子消息。
func (t *TTSManager) runSenderLoop(ctx context.Context) {
	t.senderLoopActive.Store(true)
	defer func() {
		t.senderLoopActive.Store(false)
		// 关闭 senderLoopDone 让同步打断方法立即返回（否则它们会一直等一个永远不会来的响应）。
		close(t.senderLoopDone)
	}()

	// frameDuration 单帧音频的播放时长（如 20ms），是流控与播放尾指针推进的最小单位。
	frameDuration := time.Duration(t.clientState.OutputAudioFormat.FrameDuration) * time.Millisecond
	// cacheFrameCount 客户端本地缓冲能容纳的帧数：按「约 120ms 缓冲」折算。
	// 提前发送量不能超过这个水位，否则端上缓冲溢出会丢帧、出现卡顿/破音。
	cacheFrameCount := 120 / t.clientState.OutputAudioFormat.FrameDuration
	// totalFrames 本轮 TTS 已发送帧数（tts_start 时清零，主要用于日志与对齐耗时估算）。
	totalFrames := 0
	// currentSentenceFrames 当前句子已发送帧数（sentence_start/end 时清零，用于字幕与音频对齐诊断）。
	currentSentenceFrames := 0
	// playbackTail 预计的「客户端播放完成时刻」：每发一帧就往后推 frameDuration。
	// 所有需要和声音对齐的动作（发 tts_stop、发字幕）都基于它计算，而不是基于「发送时刻」。
	playbackTail := time.Time{}

	// handleDelayedSentence 真正把延迟到期的句子控制消息发给设备。
	// 传入时机有两处：主循环收到 delayedSentenceReadyQueue，或 waitUntilSenderDeadline 在等待期间顺手处理。
	handleDelayedSentence := func(elem AudioQueueElem) {
		// 代际校验：这句在排队等待期间可能已经被打断，此时必须丢弃，
		// 但仍要回调 OnEnd，否则上层（如 isSync 等待、指标计数）会永远挂住。
		if elem.Generation != t.currentAudioGeneration() {
			if elem.OnEnd != nil {
				elem.OnEnd(context.Canceled)
			}
			return
		}
		switch elem.Kind {
		case AudioQueueKindSentenceStart:
			// OnStart 代表「语音真正开始播放」，因此放在发消息之前触发，
			// 让上层钩子能在同一时刻记录播报起点。
			if elem.OnStart != nil {
				elem.OnStart()
			}
			if elem.Text != "" {
				if err := t.serverTransport.SendSentenceStart(elem.Text); err != nil {
					log.Errorf("发送 TTS 文本失败: %s, %v", elem.Text, err)
					if elem.OnError != nil {
						elem.OnError(err)
					}
					if elem.OnEnd != nil {
						elem.OnEnd(err)
					}
				}
			}
		case AudioQueueKindSentenceEnd:
			// callbackErr 用来聚合「本句自身的错误」和「发送字幕失败的错误」，
			// 保证 OnEnd 一定能拿到一个能反映问题的 error。
			callbackErr := elem.Err
			if elem.Text != "" {
				if err := t.serverTransport.SendSentenceEnd(elem.Text); err != nil {
					log.Errorf("发送 TTS 文本失败: %s, %v", elem.Text, err)
					if elem.OnError != nil {
						elem.OnError(err)
					}
					if callbackErr == nil {
						callbackErr = err
					}
				}
			}
			currentSentenceFrames = 0
			// OnEnd 是本句「彻底完成」的语义：上层据此做同步返回与指标收尾，
			// 所以必须放在最后一行调用。
			if elem.OnEnd != nil {
				elem.OnEnd(callbackErr)
			}
		}
	}

	// handleInterrupt 处理一次打断：清空两条待发送队列，并（如有登记）收口 tts_stop。
	// 注意这只是「停止当前输出并继续运行」，不结束 TTSManager 本身。
	handleInterrupt := func(reason string) {
		t.drainSessionAudioQueue()
		t.drainDelayedSentenceReadyQueue()
		// 打断前如果有调用方登记了「要结束 TTS」，就在清空队列之后统一收口：
		// 此时队列已排空，发 tts_stop 不会再被旧音频"追上"。
		if pending, sendTtsStop, stopErr, stopReason := t.consumePendingInterruptStop(); pending {
			t.finishTtsStopWithReason(t.clientState.Ctx, sendTtsStop, stopErr, stopReason)
		}
		totalFrames = 0
		currentSentenceFrames = 0
		// 播放尾指针归零：打断后客户端已不再有我们发过去的待播音频。
		playbackTail = time.Time{}
		log.Infof("runSenderLoop interrupt, drained queue and continue: device=%s reason=%s state={%s}", t.clientState.DeviceID, normalizeTTSReason(reason), t.debugState())
	}

	for {
		// 第一层 select 带 default：优先排空「已经到点」的句子控制消息，再进入阻塞等待。
		// 若不加这一优先级，当音频帧持续不断时句子消息可能被长期饿死（Go select 是随机选择）。
		select {
		case elem := <-t.delayedSentenceReadyQueue:
			handleDelayedSentence(elem)
			continue
		default:
		}

		select {
		case <-ctx.Done():
			// 会话上下文结束：清空残留队列（回调 OnEnd 让上层解除等待），并以「要发 tts_stop + 错误」收口。
			t.drainSessionAudioQueue()
			t.drainDelayedSentenceReadyQueue()
			t.finishTtsStopWithReason(t.clientState.Ctx, true, ctx.Err(), "runSenderLoop: ctx done")
			log.Debugf("runSenderLoop ctx done, drained queue and exit")
			return
		case req := <-t.interruptCh:
			// 打断：清空队列后继续运行（会话不结束）。同步打断者通过 close(req.done) 得到确认。
			handleInterrupt(req.reason)
			if req.done != nil {
				close(req.done)
			}
			continue
		case elem := <-t.delayedSentenceReadyQueue:
			handleDelayedSentence(elem)
		case elem, ok := <-t.sessionAudioQueue:
			if !ok {
				return
			}
			// 代际不符 = 该元素属于被打断的上一轮，直接丢弃但必须回调 OnEnd。
			if elem.Generation != t.currentAudioGeneration() {
				if elem.OnEnd != nil {
					elem.OnEnd(context.Canceled)
				}
				continue
			}
			switch elem.Kind {
			case AudioQueueKindSentenceStart:
				currentSentenceFrames = 0
				// 不直接发送，而是转成延迟任务：等客户端缓冲播完这段再发字幕，才能和声音对齐。
				if !t.enqueueDelayedSentenceTask(ctx, elem) && elem.OnEnd != nil {
					elem.OnEnd(ctx.Err())
				}
			case AudioQueueKindFrame, AudioQueueKindMediaFrame:
				// ---- 流控核心 ----
				// 思路：不按「时间」睡固定间隔，而是维护一个「客户端播放尾时刻」playbackTail，
				// 保证任何时刻「已发出但还没播完」的音频总量不超过客户端缓冲水位（约 120ms）。
				now := time.Now()
				// 若尾指针为空（新一段开始）或已经被真实时间追上（客户端缓冲已播空），就把它对齐到当前时间。
				if playbackTail.IsZero() || now.After(playbackTail) {
					playbackTail = now
				}
				// allowedAhead：允许「提前发送」的最大时长 = 缓冲帧数 × 单帧时长（≈120ms）。
				allowedAhead := time.Duration(cacheFrameCount) * frameDuration
				// sendAt：本帧最早可以发送的时刻。现在还没到就必须等，
				// 否则发送速度超过客户端消费速度，端上缓冲溢出会丢帧/破音。
				sendAt := playbackTail.Add(-allowedAhead)
				if now.Before(sendAt) {
					// 等待期间不能"什么都不干"：
					//   - 到点的句子消息要继续发（否则字幕会越积越晚）；
					//   - 打断信号要立刻响应（否则用户插话后要等最多 120ms 才静音，体感明显）。
					// 这两件事都由 waitUntilSenderDeadline 在这个等待循环里代劳。
					waitResult, interruptReq := t.waitUntilSenderDeadline(ctx, sendAt, handleDelayedSentence)
					switch waitResult {
					case senderWaitContextDone:
						t.drainSessionAudioQueue()
						t.drainDelayedSentenceReadyQueue()
						t.finishTtsStopWithReason(t.clientState.Ctx, true, ctx.Err(), "runSenderLoop: frame send wait ctx done")
						return
					case senderWaitInterrupted:
						handleInterrupt(interruptReq.reason)
						if interruptReq.done != nil {
							close(interruptReq.done)
						}
						continue
					}
					// 等待结束后重新取时间：可能已经超过原尾指针（客户端缓冲播空），重新对齐避免累积漂移。
					now = time.Now()
					if now.After(playbackTail) {
						playbackTail = now
					}
				}
				if err := t.serverTransport.SendAudio(elem.Data); err != nil {
					// 发送失败（如连接断开）只记日志并回调 OnError，不中断整个循环：
					// 后续元素仍会继续尝试，真正的收口交给 ctx 取消路径。
					audioType := "TTS"
					if elem.Kind == AudioQueueKindMediaFrame {
						audioType = "媒体"
					}
					log.Errorf("发送%s音频失败: len: %d, %v", audioType, len(elem.Data), err)
					if elem.OnError != nil {
						elem.OnError(err)
					}
					continue
				}
				// 只有 TTS 语音帧才计入指标与历史缓存；媒体帧不进（历史回放里不该混入音乐）。
				if elem.Kind == AudioQueueKindFrame {
					t.markTtsMetricFirstAudio(t.clientState.Ctx, elem.MetricCycle)
					// 深拷贝一份存入音频历史缓存：用于「消息两阶段保存」的第 2 阶段，
					// 把本轮播报的音频附着到聊天记录上，供历史回放使用。
					t.audioMutex.Lock()
					frameCopy := make([]byte, len(elem.Data))
					copy(frameCopy, elem.Data)
					t.audioHistoryBuffer = append(t.audioHistoryBuffer, frameCopy)
					t.audioMutex.Unlock()
				}
				totalFrames++
				currentSentenceFrames++
				// 发出这一帧后，客户端的播放结束时刻自然会往后推移一个帧长。
				playbackTail = playbackTail.Add(frameDuration)
			case AudioQueueKindSentenceEnd:
				// 同 SentenceStart：延迟到音频缓冲之后再由 handleDelayedSentence 真正发送。
				if !t.enqueueDelayedSentenceTask(ctx, elem) && elem.OnEnd != nil {
					elem.OnEnd(ctx.Err())
				}
			case AudioQueueKindTtsStart:
				// 进入 TTS 播放态：先触发钩子（外部可据此做 UI/统计），再发协议消息。
				if t.session != nil {
					hookErr := t.session.hookHub.EmitTTSOutputStart(t.session.hookContext(ctx))
					if hookErr != nil {
						log.Warnf("TTS_OUTPUT_START hook 执行失败: %v", hookErr)
					}
				}
				t.ttsActive.Store(true)
				if err := t.serverTransport.SendTtsStart(); err != nil {
					log.Errorf("发送 TtsStart 失败: %v", err)
				}
				// 新语音段：重置帧计数与播放尾指针
				totalFrames = 0
				playbackTail = time.Time{}
			case AudioQueueKindTtsStop:
				log.Infof("runSenderLoop processing tts stop: device=%s reason=%s state={%s}", t.clientState.DeviceID, normalizeTTSReason(elem.DebugReason), t.debugState())
				// 等待当前播放尾指针走到最后一帧结束再发 TtsStop
				// 原因：帧"发送完"不等于"播完"，客户端还有最多约 120ms 缓冲；
				// 此时若立刻发 tts(state=stop)，端上会提前切状态，尾音被截断。
				if !playbackTail.IsZero() {
					waitResult, interruptReq := t.waitUntilSenderDeadline(ctx, playbackTail, handleDelayedSentence)
					switch waitResult {
					case senderWaitContextDone:
						t.drainSessionAudioQueue()
						t.drainDelayedSentenceReadyQueue()
						t.finishTtsStopWithReason(t.clientState.Ctx, true, ctx.Err(), "runSenderLoop: tts stop wait playback tail ctx done")
						return
					case senderWaitInterrupted:
						handleInterrupt(interruptReq.reason)
						if interruptReq.done != nil {
							close(interruptReq.done)
						}
						continue
					}
				}
				// 额外留出一小段播放完成保护时间，避免客户端尾音尚未播完就进入 turn-end 收口。
				// 这是给「网络抖动 + 端上解码调度」留的余量，属于经验值。
				waitResult, interruptReq := t.waitUntilSenderDeadline(ctx, time.Now().Add(ttsPlaybackCompletionGrace), handleDelayedSentence)
				switch waitResult {
				case senderWaitContextDone:
					t.drainSessionAudioQueue()
					t.drainDelayedSentenceReadyQueue()
					t.finishTtsStopWithReason(t.clientState.Ctx, true, ctx.Err(), "runSenderLoop: tts stop grace wait ctx done")
					return
				case senderWaitInterrupted:
					handleInterrupt(interruptReq.reason)
					if interruptReq.done != nil {
						close(interruptReq.done)
					}
					continue
				}
				// withTTSTurnPlaybackSettled 在 ctx 上打一个"播放确已结束"的标记，
				// 下游策略（goodbye-and-idle）据此决定是立刻关会话还是再等一下。
				t.finishTtsStopWithReason(withTTSTurnPlaybackSettled(t.clientState.Ctx), true, nil, elem.DebugReason)
				playbackTail = time.Time{}
				totalFrames = 0
				currentSentenceFrames = 0
			}
		}
	}
}

// drainSessionAudioQueue ctx 取消时清空队列，丢弃未发送元素
//
// 关键点：丢弃不是「静默扔掉」，必须回调 OnEnd(context.Canceled)。
// 因为上层可能正阻塞等待这个回调（如 isSync 的 done 信号、指标 pendingItems 递减），
// 不回调就会造成永久挂起或指标永远收不了口。
func (t *TTSManager) drainSessionAudioQueue() {
	for {
		select {
		case elem, ok := <-t.sessionAudioQueue:
			if !ok {
				return
			}
			if elem.OnEnd != nil {
				elem.OnEnd(context.Canceled)
			}
		default:
			return
		}
	}
}

// drainDelayedSentenceReadyQueue 清空「已到点待发送」的句子消息，同样要回调 OnEnd 解除上层等待。
func (t *TTSManager) drainDelayedSentenceReadyQueue() {
	for {
		select {
		case elem := <-t.delayedSentenceReadyQueue:
			if elem.OnEnd != nil {
				elem.OnEnd(context.Canceled)
			}
		default:
			return
		}
	}
}

// ClearSessionAudioQueue 清空会话级音频队列（可由外部在 ctx 取消时调用）
func (t *TTSManager) ClearSessionAudioQueue() {
	t.drainSessionAudioQueue()
}

// currentAudioGeneration 读取当前音频代际号（无锁，atomic）。
func (t *TTSManager) currentAudioGeneration() uint64 {
	return t.audioGeneration.Load()
}

// nextAudioGeneration 递增并返回新的音频代际号。
//
// 每次打断都调用它，于是「打断前入队的所有元素」代际都会落后于当前值，
// runSenderLoop / runDelayedSentenceLoop 见到就丢弃——用一次原子自增代替遍历删除队列。
func (t *TTSManager) nextAudioGeneration() uint64 {
	return t.audioGeneration.Add(1)
}

// currentAudioInterruptCh 读取当前这一「代」打断通知 channel。
func (t *TTSManager) currentAudioInterruptCh() <-chan struct{} {
	t.audioInterruptMu.RLock()
	defer t.audioInterruptMu.RUnlock()
	return t.audioInterruptCh
}

// rotateAudioInterruptCh 换上一个新的（未关闭的）打断通知 channel，并关闭旧的。
//
// 为什么要"轮换"而不是复用：
//   - 关闭 channel 是广播语义，适合"通知所有在途请求：现在被打断了"；
//   - 但关闭后无法再用于下一次打断，所以每次打断都要换一条新 channel；
//   - 先造新的再关旧的，保证期间拿到的监听者不会漏掉通知。
func (t *TTSManager) rotateAudioInterruptCh() {
	t.audioInterruptMu.Lock()
	defer t.audioInterruptMu.Unlock()

	oldCh := t.audioInterruptCh
	t.audioInterruptCh = make(chan struct{})
	if oldCh == nil {
		return
	}

	select {
	case <-oldCh:
	default:
		close(oldCh)
	}
}

// withAudioInterruptContext 在父 ctx 之外再套一层「可被音频打断取消」的上下文。
//
// 用途：TTS 合成请求可能耗时较长且按量计费，一旦被打断应立即取消，而不是等整个会话 ctx 结束。
// 实现：起一个只做一件事的 goroutine 监听当前代际的 audioInterruptCh，
// 一旦打断发生就 cancel()，让所有派生请求（provider 的 HTTP/流式调用）提前退出。
func (t *TTSManager) withAudioInterruptContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}

	interruptCtx, cancel := context.WithCancel(ctx)
	interruptCh := t.currentAudioInterruptCh()
	if interruptCh == nil {
		return interruptCtx, cancel
	}

	go func() {
		select {
		case <-interruptCtx.Done():
		case <-interruptCh:
			cancel()
		}
	}()

	return interruptCtx, cancel
}

// startTtsMetricCycle 开启一个新的 TTS 指标轮次（轮次号 +1 并把所有计数/标志位清零），返回新轮次号。
//
// 何时调用：EnqueueTtsStart 时（即本轮 tts_start 入队时）。
// 为什么要有"轮次"：一轮对话可能包含多句 TTS、多轮工具嵌套，指标必须在同一轮内聚合、
// 且打断/重入时旧轮次的迟到回调不能污染新轮次——cycleID 就是用来甄别迟到的旧回调的。
func (t *TTSManager) startTtsMetricCycle() uint64 {
	t.ttsMetricMu.Lock()
	defer t.ttsMetricMu.Unlock()

	t.ttsMetricState = ttsMetricState{
		cycleID: t.ttsMetricState.cycleID + 1,
	}
	return t.ttsMetricState.cycleID
}

// currentTtsMetricCycle 读取当前轮次号（入队 TTSQueueItem 时用它给 item 打标）。
func (t *TTSManager) currentTtsMetricCycle() uint64 {
	t.ttsMetricMu.Lock()
	defer t.ttsMetricMu.Unlock()
	return t.ttsMetricState.cycleID
}

// registerTTSTurnEndPolicy 把「本轮结束后要执行的策略」记到当前轮次上。
//
// 策略不是 TTS 层自己决定的，而是由上层（如主动播报链路）通过 ctx 传入，
// 这里只是把它登记下来，等真正收口时再执行（见 dispatchTTSTurnEndPolicy）。
func (t *TTSManager) registerTTSTurnEndPolicy(ctx context.Context, cycleID uint64) {
	if cycleID == 0 {
		return
	}
	policy := ttsTurnEndPolicyFromContext(ctx)
	if policy == ttsTurnEndPolicyNone {
		return
	}

	t.ttsMetricMu.Lock()
	defer t.ttsMetricMu.Unlock()

	// 轮次已变或已收口则忽略：避免迟到的登记写到新轮次上。
	if t.ttsMetricState.cycleID != cycleID || t.ttsMetricState.turnEnded {
		return
	}
	t.ttsMetricState.turnEndPolicy = policy
}

// consumeTTSTurnEndPolicy 取出并清除当前轮次的策略（保证只执行一次）。
func (t *TTSManager) consumeTTSTurnEndPolicy() ttsTurnEndPolicy {
	t.ttsMetricMu.Lock()
	defer t.ttsMetricMu.Unlock()

	policy := t.ttsMetricState.turnEndPolicy
	t.ttsMetricState.turnEndPolicy = ttsTurnEndPolicyNone
	return policy
}

// dispatchTTSTurnEndPolicy 执行本轮结束策略（例如「非自动监听模式下，播完就告别并进入空闲」）。
//
// 为什么用 go 异步派发而不是直接调用：
// 调用方是 runSenderLoop（在 finishTtsStopWithReason 里），而策略处理通常要重置会话状态/关闭会话，
// 那会再走到 stopSpeaking / Interrupt 路径，与 runSenderLoop 形成重入（自己等自己，死锁）。
// 丢到独立 goroutine 后，runSenderLoop 可以立刻继续/退出，重入链路自然被打破。
func (t *TTSManager) dispatchTTSTurnEndPolicy(ctx context.Context, stopErr error) {
	policy := t.consumeTTSTurnEndPolicy()
	if policy == ttsTurnEndPolicyNone {
		return
	}

	handler := ttsTurnEndPolicyHandlerFromContext(ctx)
	if handler == nil && t.clientState != nil {
		// 兜底：ctx 上没带 handler（例如收口用的是 clientState.Ctx），就退回设备级 ctx 里找。
		handler = ttsTurnEndPolicyHandlerFromContext(t.clientState.Ctx)
	}
	if handler == nil {
		log.Debugf("TTS turn end policy dropped: no handler, policy=%d", policy)
		return
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("TTS turn end policy handler panic: %v", r)
			}
		}()

		// 在 sender loop 内同步重置会话会与 stopSpeaking/Interrupt 路径重入，
		// 这里异步派发到 ChatManager，避免 runSenderLoop 自陷。
		handler.handleTTSTurnEndPolicy(ctx, policy, stopErr)
	}()
}

// ttsMetricCompletion 描述「一次指标收口」需要上报什么。
//
// 之所以做成返回值而不是直接上报：指标函数经常在持有 ttsMetricMu 的状态下被调用，
// 必须先把锁内计算结果带出来，出锁后再调用 session 的 TraceXxx 上报，避免持锁做 I/O。
type ttsMetricCompletion struct {
	// err 收口时的错误（nil 表示正常结束）。
	err error
	// ttsStopTs TTS 播放停止时间戳（由 clientState.MarkTtsStop 生成）。
	ttsStopTs int64
	// turnEndTs 本轮对话结束时间戳。
	turnEndTs int64
	// traceTtsStop 是否需要上报 tts_stop 指标（只有真正开始过 TTS 才需要）。
	traceTtsStop bool
	// traceTurnEnd 是否需要上报 turn_end 指标。
	traceTurnEnd bool
}

// emitTtsMetricCompletion 把收口结果真正上报给会话（TraceTtsStop / TraceTurnEnd）。
func (t *TTSManager) emitTtsMetricCompletion(ctx context.Context, completion ttsMetricCompletion) {
	if t.session == nil {
		return
	}
	if completion.traceTtsStop {
		t.session.TraceTtsStop(ctx, completion.ttsStopTs, completion.err)
	}
	if completion.traceTurnEnd {
		t.session.TraceTurnEnd(ctx, completion.turnEndTs, completion.err)
	}
}

// registerTtsMetricItem 登记「有一条 TTS 句子任务入队」，pendingItems +1。
//
// pendingItems 与 activeRequests 一起构成「本轮还有多少 TTS 工作没做完」的判据，
// 只有两者都归零、且上游已声明 turnEndRequested，才允许上报 turn_end。
// registerTtsMetricItem 见上：pendingItems +1。
// cycleID 不匹配（迟到的旧轮次回调）或本轮已收口时直接忽略，防止计数被写坏。
func (t *TTSManager) registerTtsMetricItem(cycleID uint64) {
	if cycleID == 0 {
		return
	}

	t.ttsMetricMu.Lock()
	defer t.ttsMetricMu.Unlock()

	if t.ttsMetricState.cycleID != cycleID || t.ttsMetricState.turnEnded {
		return
	}
	t.ttsMetricState.pendingItems++
}

// finishTtsMetricItem 标记「一条 TTS 句子任务处理完毕」，出锁后再上报可能产生的收口结果。
func (t *TTSManager) finishTtsMetricItem(ctx context.Context, cycleID uint64, err error) {
	t.emitTtsMetricCompletion(ctx, t.finishTtsMetricItemLocked(cycleID, err))
}

// finishTtsMetricItemLocked pendingItems -1，然后尝试收口（调用方必须已持有 ttsMetricMu）。
func (t *TTSManager) finishTtsMetricItemLocked(cycleID uint64, err error) ttsMetricCompletion {
	t.ttsMetricMu.Lock()
	defer t.ttsMetricMu.Unlock()

	if t.ttsMetricState.cycleID != cycleID || cycleID == 0 {
		return ttsMetricCompletion{}
	}
	if t.ttsMetricState.pendingItems > 0 {
		t.ttsMetricState.pendingItems--
	}
	return t.maybeFinalizeTtsMetricLocked(err)
}

// markTtsMetricRequestStart 标记「一个 TTS 合成请求开始了」：activeRequests +1。
//
// 同时，若是本轮第一次发起请求，就用 clientState.MarkTtsStart() 记下 tts_start 时间戳，
// 并通过 TraceTtsStart 上报。这样统计口径是「真正开始合成」而不是「入队」。
func (t *TTSManager) markTtsMetricRequestStart(ctx context.Context, cycleID uint64) {
	if cycleID == 0 {
		return
	}

	var startTs int64

	t.ttsMetricMu.Lock()
	if t.ttsMetricState.cycleID == cycleID && !t.ttsMetricState.turnEnded {
		t.ttsMetricState.activeRequests++
		if !t.ttsMetricState.started {
			t.clientState.MarkTtsStart()
			startTs = t.clientState.Statistic.TtsStartTs
			t.ttsMetricState.started = true
		}
	}
	t.ttsMetricMu.Unlock()

	// 上报放在锁外：TraceXxx 可能涉及外部存储/网络，不应在锁内做。
	if startTs > 0 && t.session != nil {
		t.session.TraceTtsStart(ctx, startTs)
	}
}

// markTtsMetricFirstAudio 记录本轮 TTS 的「首帧音频被发送」时间戳（只记第一次）。
// 首帧时间是端到端延迟（asr→llm→tts）里最关键的用户可感指标。
func (t *TTSManager) markTtsMetricFirstAudio(ctx context.Context, cycleID uint64) {
	if cycleID == 0 {
		return
	}

	var firstAudioTs int64

	// 额外要求 started 为真：没记录过 tts_start 就不该有首帧，属于异常时序，忽略即可。
	t.ttsMetricMu.Lock()
	if t.ttsMetricState.cycleID == cycleID && t.ttsMetricState.started && !t.ttsMetricState.firstAudio && !t.ttsMetricState.turnEnded {
		t.clientState.MarkTtsFirstFrame()
		firstAudioTs = t.clientState.Statistic.TtsFirstFrameTs
		t.ttsMetricState.firstAudio = true
	}
	t.ttsMetricMu.Unlock()

	if firstAudioTs > 0 && t.session != nil {
		t.session.TraceTtsFirstFrame(ctx, firstAudioTs)
	}
}

// finishTtsMetricRequest 标记「一个 TTS 合成请求结束」（成功或失败）。
func (t *TTSManager) finishTtsMetricRequest(ctx context.Context, cycleID uint64, err error) {
	t.emitTtsMetricCompletion(ctx, t.finishTtsMetricRequestLocked(cycleID, err))
}

// finishTtsMetricRequestLocked activeRequests -1，然后尝试收口。
func (t *TTSManager) finishTtsMetricRequestLocked(cycleID uint64, err error) ttsMetricCompletion {
	t.ttsMetricMu.Lock()
	defer t.ttsMetricMu.Unlock()

	if t.ttsMetricState.cycleID != cycleID || cycleID == 0 {
		return ttsMetricCompletion{}
	}
	if t.ttsMetricState.activeRequests > 0 {
		t.ttsMetricState.activeRequests--
	}
	return t.maybeFinalizeTtsMetricLocked(err)
}

// requestTurnEndLocked 上游声明「本轮逻辑输出结束」（LLM 不再产生新句子）。
//
// 注意它只是"申请"收口：真正上报还要等 pendingItems 与 activeRequests 都归零，
// 否则最后几句的音频可能还没合成完/发完就被判定为 turn_end。
func (t *TTSManager) requestTurnEndLocked(err error) ttsMetricCompletion {
	t.ttsMetricMu.Lock()
	defer t.ttsMetricMu.Unlock()

	if t.ttsMetricState.cycleID == 0 {
		return ttsMetricCompletion{}
	}
	t.ttsMetricState.turnEndRequested = true
	return t.maybeFinalizeTtsMetricLocked(err)
}

// forceStopTtsMetric 强制收口指标（打断、发送失败等异常路径使用）。
func (t *TTSManager) forceStopTtsMetric(ctx context.Context, err error) {
	t.emitTtsMetricCompletion(ctx, t.forceStopTtsMetricLocked(err))
}

// forceStopTtsMetricLocked 与 requestTurnEnd 的区别：它不再等计数归零，直接 finalize。
// 使用场景是「这一轮已经不可能正常结束了」（如 tts_stop 已发出/上下文取消），
// 此时若还等 pending 归零，指标可能永远不上报，监控上就会看到"缺轮次"。
func (t *TTSManager) forceStopTtsMetricLocked(err error) ttsMetricCompletion {
	t.ttsMetricMu.Lock()
	defer t.ttsMetricMu.Unlock()

	if t.ttsMetricState.cycleID == 0 || t.ttsMetricState.turnEnded {
		return ttsMetricCompletion{}
	}
	t.ttsMetricState.turnEndRequested = true
	return t.finalizeTtsMetricLocked(err)
}

// maybeFinalizeTtsMetricLocked 收口的统一判定入口（调用方必须已持有 ttsMetricMu）。
//
// 三个必要条件（缺一不可）：
//  1. turnEndRequested：上游已声明本轮文本输出结束；
//  2. pendingItems == 0：所有句子任务都已处理完；
//  3. activeRequests == 0：所有发出去的 TTS 合成请求都已返回。
//
// 这样 turn_end 才是「确实全播完了」，而不是「文本发完了」。
func (t *TTSManager) maybeFinalizeTtsMetricLocked(err error) ttsMetricCompletion {
	if !t.ttsMetricState.turnEndRequested || t.ttsMetricState.turnEnded {
		return ttsMetricCompletion{}
	}
	if t.ttsMetricState.pendingItems > 0 || t.ttsMetricState.activeRequests > 0 {
		return ttsMetricCompletion{}
	}
	return t.finalizeTtsMetricLocked(err)
}

// finalizeTtsMetricLocked 真正完成一轮指标收口：打上 turnEnded 标记并组装要上报的内容。
//
// tts_stop 时间戳的取法：若本轮确实开始过 TTS（started）且还没记过 stop，
// 就用 clientState.MarkTtsStop() 生成；否则（例如本轮压根没产生音频）直接用当前时间，
// 避免出现"有 turn_end 却没有 tts_stop 时间戳"的统计断档。
// turnEnded 是幂等保护：置真后所有统计函数都返回空结果，不会重复上报。
func (t *TTSManager) finalizeTtsMetricLocked(err error) ttsMetricCompletion {
	if t.ttsMetricState.turnEnded {
		return ttsMetricCompletion{}
	}

	completion := ttsMetricCompletion{
		err:          err,
		traceTurnEnd: true,
	}
	if t.ttsMetricState.started && !t.ttsMetricState.ttsStopped {
		t.clientState.MarkTtsStop()
		completion.ttsStopTs = t.clientState.Statistic.TtsStopTs
		completion.turnEndTs = completion.ttsStopTs
		completion.traceTtsStop = true
		t.ttsMetricState.ttsStopped = true
	} else {
		completion.turnEndTs = time.Now().UnixMilli()
	}
	t.ttsMetricState.turnEnded = true
	return completion
}

// [5.0] 文本入队：把一条句子任务入队，并同步登记到指标计数。
//
// pushTTSQueueItem 把一条句子任务入队，并同步登记到指标计数。
//
// 顺序很重要：先取序号（enqueueSeq）再 Push，这样「清队列水位」比较的序号语义一致；
// 入队成功后立刻 registerTtsMetricItem，保证 pendingItems 与实际队列内容对齐。
func (t *TTSManager) pushTTSQueueItem(item TTSQueueItem) error {
	item.enqueueSeq = t.ttsQueueSeq.Add(1)
	if err := t.ttsQueue.Push(item); err != nil {
		return err
	}
	t.registerTtsMetricItem(item.metricCycle)
	return nil
}

// shouldDropTTSQueueItem 判断该 item 是否已被「打断水位」作废。
//
// 实现思路：ClearTTSQueue 时把 droppedTTSSeq 提升到当前已发号的最大值，
// 于是所有已入队（序号 <= 水位）的旧 item 都会被判定为丢弃——
// 不用真去遍历删除队列内容，O(1) 完成"作废一批任务"。
func (t *TTSManager) shouldDropTTSQueueItem(item TTSQueueItem) bool {
	if item.enqueueSeq == 0 {
		return false
	}
	return item.enqueueSeq <= t.droppedTTSSeq.Load()
}

// dismissTTSQueueItem 丢弃一个 item 时的统一收尾：先回调 onEndFunc 通知上层，再递减指标计数。
// 两件事都不能漏：漏回调会让 isSync 永久等待，漏计数会让 turn_end 永远收不了口。
func (t *TTSManager) dismissTTSQueueItem(item TTSQueueItem, err error) {
	if item.onEndFunc != nil {
		item.onEndFunc(err)
	}
	t.finishTtsMetricItem(item.ctx, item.metricCycle, err)
}

// waitForMediaPlaybackRelease 等待媒体独占结束（若当前没有独占则立即返回）。
//
// 谁会调用：TTS 合成任务（processTTSQueue）与旧的直接发送路径（SendTTSAudio）。
// 目的：媒体播放期间不允许 TTS 语音插进来，否则设备上会出现"音乐和机器人说话同时响"。
// 实现用 waitCh 而不是轮询：EndExclusiveMediaPlayback 关闭 channel 即立刻唤醒所有等待者。
func (t *TTSManager) waitForMediaPlaybackRelease(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		t.mediaPlaybackMu.RLock()
		active := t.mediaPlaybackActive
		waitCh := t.mediaPlaybackWaitCh
		t.mediaPlaybackMu.RUnlock()

		if !active || waitCh == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-waitCh:
		}
	}
}

// [6.0] 让出输出：媒体播放器取得下行输出的独占权（清空待播 TTS 并让路）。
//
// 步骤与原因：
//  1. 先占位（设置 mediaPlaybackActive 与 waitCh），这样后续新来的 TTS 任务会立刻让路；
//  2. ClearTTSQueue 清掉还没合成的句子任务（媒体已经接管，这些语音没意义了）；
//  3. InterruptAndClearQueueSync 同步清空"已生成待发送"的音频，确保 TTS 声音立刻停下。
//
// 但注意：这里刻意不发 tts_stop。因为媒体随后会继续复用同一条发送通道，
// tts_stop 若此刻发出，端上会认为 TTS 结束而切状态，媒体音频反而播不顺；
// 真正的 tts_stop 由外层在媒体播放完成后的统一收尾阶段发送。
func (t *TTSManager) BeginExclusiveMediaPlayback(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	waitCh := make(chan struct{})

	t.mediaPlaybackMu.Lock()
	if t.mediaPlaybackActive {
		t.mediaPlaybackMu.Unlock()
		return fmt.Errorf("媒体播放已处于独占状态")
	}
	t.mediaPlaybackActive = true
	t.mediaPlaybackWaitCh = waitCh
	t.mediaPlaybackMu.Unlock()

	t.ClearTTSQueue()
	// 媒体接管时只打断并清空当前 TTS，不立即发送 tts_stop。
	// 真正的 tts_stop 由外层响应在媒体播放完成后的统一收尾阶段发送。
	if err := t.InterruptAndClearQueueSync(ctx); err != nil {
		t.EndExclusiveMediaPlayback()
		return err
	}

	return nil
}

// EndExclusiveMediaPlayback 释放媒体独占权，并唤醒所有等待中的 TTS 任务。
func (t *TTSManager) EndExclusiveMediaPlayback() {
	t.mediaPlaybackMu.Lock()
	waitCh := t.mediaPlaybackWaitCh
	t.mediaPlaybackActive = false
	t.mediaPlaybackWaitCh = nil
	t.mediaPlaybackMu.Unlock()

	if waitCh == nil {
		return
	}

	// 先探一下是否已被关闭再 close，避免重复关闭 panic。
	select {
	case <-waitCh:
	default:
		close(waitCh)
	}
}

// sentenceControlDelay 计算句子控制消息（sentence_start/end）应当延迟多久再发。
//
// 依据：客户端会先缓冲约 120ms 的音频才开始播放，所以字幕要晚于音频帧这么多发送，
// 才能和实际听到的声音对齐。实现上取「120ms 折算出的帧数 × 帧长」，
// 结果正好是 120ms 的整数倍，避免出现半帧的时间差。
func (t *TTSManager) sentenceControlDelay() time.Duration {
	frameDurationMs := t.clientState.OutputAudioFormat.FrameDuration
	if frameDurationMs <= 0 {
		return 0
	}
	cacheFrameCount := 120 / frameDurationMs
	return time.Duration(cacheFrameCount*frameDurationMs) * time.Millisecond
}

// insertDelayedSentenceTask 把任务按 ExecuteAt 升序插入切片（插入排序）。
//
// 为什么不用 heap：延迟任务的数量级只有每轮几句话，插入排序实现简单、
// 且天然保持了「同一时刻的任务按投递先后」这一稳定顺序（字幕顺序不能乱）。
func insertDelayedSentenceTask(tasks []delayedSentenceTask, task delayedSentenceTask) []delayedSentenceTask {
	insertAt := len(tasks)
	for insertAt > 0 && task.ExecuteAt.Before(tasks[insertAt-1].ExecuteAt) {
		insertAt--
	}
	tasks = append(tasks, delayedSentenceTask{})
	copy(tasks[insertAt+1:], tasks[insertAt:])
	tasks[insertAt] = task
	return tasks
}

// stopTimer 安全地停止并排空一个定时器。
//
// 直接 Stop 而不排空 channel 的话，下一次重置前可能读到上一次的过期信号，
// 导致"提前发送"。这里的非阻塞排空正好解决该问题。
func stopTimer(timer *time.Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

// enqueueDelayedSentenceTask 把一个句子控制消息登记为「延迟任务」。
//
// 返回 false 表示投递失败（ctx 已取消），调用方需要自己回调 OnEnd，
// 否则上层的同步等待会挂住。ctx 为 nil 时按无取消语义直接投递。
func (t *TTSManager) enqueueDelayedSentenceTask(ctx context.Context, elem AudioQueueElem) bool {
	task := delayedSentenceTask{
		Elem:      elem,
		ExecuteAt: time.Now().Add(t.sentenceControlDelay()),
	}
	if ctx == nil {
		t.delayedSentenceQueue <- task
		return true
	}
	if ctx.Err() != nil {
		return false
	}
	select {
	case <-ctx.Done():
		return false
	case t.delayedSentenceQueue <- task:
		return true
	}
}

// runDelayedSentenceLoop 句子控制消息的定时调度协程。
//
// 为什么需要这一层（而不是在 runSenderLoop 里 sleep 等待）：
//   - runSenderLoop 是发送协程，如果在它内部 sleep，会阻塞音频帧的发送与打断响应；
//   - 延迟时间只是"最早可发送时刻"，启动协程可以并行等到时间点再把消息放进
//     delayedSentenceReadyQueue，由 runSenderLoop 在合适的空隙真正发出。
//
// 实现要点：
//   - pending 是本地最小堆（实际用有序切片），只保存本协程私有状态，无需加锁；
//   - 用「最近到期任务的剩余时间」重设定时器，避免每来一个任务就重建计时器；
//   - 任务到点仍要校验代际：等待期间可能发生打断，此时该消息已作废。
func (t *TTSManager) runDelayedSentenceLoop(ctx context.Context) {
	var (
		// pending 按 ExecuteAt 升序保存尚未到期的任务。
		pending []delayedSentenceTask
		// timer/timerCh 指向「最早到期任务」的定时器及其 channel。
		timer   *time.Timer
		timerCh <-chan time.Time
	)

	// resetTimer 根据 pending[0] 的到期时间重新设表；队列空则停止定时器。
	resetTimer := func() {
		stopTimer(timer)
		timer = nil
		timerCh = nil
		if len(pending) == 0 {
			return
		}
		waitDuration := time.Until(pending[0].ExecuteAt)
		if waitDuration < 0 {
			waitDuration = 0
		}
		timer = time.NewTimer(waitDuration)
		timerCh = timer.C
	}

	for {
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return
		case task := <-t.delayedSentenceQueue:
			// 新任务入列：按到期时间插入，并可能把定时器提前。
			pending = insertDelayedSentenceTask(pending, task)
			resetTimer()
		case <-timerCh:
			timer = nil
			timerCh = nil
			if len(pending) == 0 {
				continue
			}
			task := pending[0]
			pending = pending[1:]
			// 到期时再校验一次代际：排队期间可能已被打断。
			if task.Elem.Generation != t.currentAudioGeneration() {
				if task.Elem.OnEnd != nil {
					task.Elem.OnEnd(context.Canceled)
				}
				resetTimer()
				continue
			}
			// 转交给 runSenderLoop；若期间 ctx 结束，则回调 OnEnd 让上层解锁后退出。
			select {
			case <-ctx.Done():
				if task.Elem.OnEnd != nil {
					task.Elem.OnEnd(ctx.Err())
				}
				return
			case t.delayedSentenceReadyQueue <- task.Elem:
			}
			resetTimer()
		}
	}
}

// senderWaitResult 表示 waitUntilSenderDeadline 的三种结束原因：
// 时间到了可以继续发送、上下文结束（该收工了）、或被打断（要先去清队列）。
type senderWaitResult int

const (
	// senderWaitReached 已到达 deadline，调用方可以继续发送。
	senderWaitReached senderWaitResult = iota
	// senderWaitContextDone ctx 结束（或中断），调用方应清空队列后退出。
	senderWaitContextDone
	// senderWaitInterrupted 期间收到了打断请求，调用方需先处理打断再继续。
	senderWaitInterrupted
)

// waitUntilSenderDeadline 等待到 deadline，但期间"顺便"处理两件不能拖的事。
//
// 为什么不能简单地 time.Sleep：
//  1. 到点的句子控制消息必须及时送出，否则字幕会越积越迟、和声音错位；
//  2. 打断必须立即响应——若等到 120ms 后才发现，用户会听到"插话后还在说"。
//
// 因此这里用一个可重入的等待循环：每轮用「距 deadline 的剩余时间」新建定时器，
// 谁先醒（ctx / 打断 / 延迟消息 / 定时器）就处理谁，处理完重新计算剩余时间再等。
// 同时把延迟消息的处理以回调形式注入（handleDelayed），让本函数不依赖 runSenderLoop 的闭包状态。
func (t *TTSManager) waitUntilSenderDeadline(ctx context.Context, deadline time.Time, handleDelayed func(AudioQueueElem)) (senderWaitResult, interruptRequest) {
	for {
		now := time.Now()
		if !now.Before(deadline) {
			return senderWaitReached, interruptRequest{}
		}

		timer := time.NewTimer(deadline.Sub(now))
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return senderWaitContextDone, interruptRequest{}
		case req := <-t.interruptCh:
			// 把打断请求原样带回给调用方：由调用方（runSenderLoop）决定如何清队列与唤醒 done。
			stopTimer(timer)
			return senderWaitInterrupted, req
		case elem := <-t.delayedSentenceReadyQueue:
			stopTimer(timer)
			handleDelayed(elem)
		case <-timer.C:
			return senderWaitReached, interruptRequest{}
		}
	}
}

// enqueueSessionElem 把元素投递到发送队列，并强行盖上当前代际号。
//
// 返回 false 的含义是「没投进去」（ctx 已取消），此时调用方必须自己回调 OnEnd，
// 否则依赖 OnEnd 的同步逻辑会挂住。ctx 为 nil 表示不允许取消（直接阻塞投递）。
func (t *TTSManager) enqueueSessionElem(ctx context.Context, generation uint64, elem AudioQueueElem) bool {
	elem.Generation = generation
	if ctx == nil {
		t.sessionAudioQueue <- elem
		return true
	}
	if ctx.Err() != nil {
		return false
	}
	select {
	case <-ctx.Done():
		return false
	case t.sessionAudioQueue <- elem:
		return true
	}
}

// [5.6] 打断：通知 runSenderLoop 清空 sessionAudioQueue 后继续运行（非阻塞）
func (t *TTSManager) InterruptAndClearQueue() {
	t.InterruptAndClearQueueWithReason("InterruptAndClearQueue")
}

// InterruptAndClearQueueWithReason 带原因的非阻塞打断。
//
// 三件事，顺序不能反：
//  1. nextAudioGeneration：先递增代际，保证此刻起仍在路上的旧音频会被发送端丢弃；
//  2. rotateAudioInterruptCh：关闭打断 channel，让在途的 TTS 合成请求立刻取消（省时省钱）；
//  3. 投递 interruptCh 通知 runSenderLoop 清空已生成待发送的音频。
//
// 注意第 3 步用的是非阻塞投递 + default：打断只需要"至少一次"通知，
// 发送协程若正忙（已经在处理一次打断）就没必要排队等待，直接返回即可。
func (t *TTSManager) InterruptAndClearQueueWithReason(reason string) {
	reason = normalizeTTSReason(reason)
	t.nextAudioGeneration()
	t.rotateAudioInterruptCh()
	if !t.senderLoopActive.Load() {
		return
	}
	select {
	case t.interruptCh <- interruptRequest{reason: reason}:
	default:
	}
}

// InterruptAndStop 用于需要立即结束当前 TTS 的场景。
// 它只登记待关闭状态，真正的 stop 与指标收口由 runSenderLoop 在清空队列后统一发出。
func (t *TTSManager) InterruptAndStop(ctx context.Context, sendTtsStop bool, stopErr error) {
	t.InterruptAndStopWithReason(ctx, sendTtsStop, stopErr, "InterruptAndStop")
}

// InterruptAndStopWithReason 「打断并结束当前 TTS」的非阻塞版本。
//
// 与 InterruptAndClearQueue（只打断、继续运行）的区别：
// 它额外把「要不要发 tts_stop / 带什么错误」登记下来，等 runSenderLoop 清空队列后再统一收口。
func (t *TTSManager) InterruptAndStopWithReason(ctx context.Context, sendTtsStop bool, stopErr error, reason string) {
	reason = normalizeTTSReason(reason)
	// 先登记再打断：顺序保证 runSenderLoop 处理打断时一定能看到这次登记，不会漏发 tts_stop。
	t.recordPendingInterruptStop(sendTtsStop, stopErr, reason)
	t.InterruptAndClearQueueWithReason(reason)
	// 兜底：如果 sender loop 已经退出（没人会来处理登记），就在这里直接收口，避免状态卡住。
	t.finishPendingInterruptStopIfSenderLoopExited(ctx)
}

// InterruptAndStopSync 触发同步打断，同时保持 TtsStop/trace/hook 只走 runSenderLoop 的统一收口。
func (t *TTSManager) InterruptAndStopSync(ctx context.Context, sendTtsStop bool, stopErr error) error {
	return t.InterruptAndStopSyncWithReason(ctx, sendTtsStop, stopErr, "InterruptAndStopSync")
}

// InterruptAndStopSyncWithReason 同步版：登记后等待 runSenderLoop 把队列清干净才返回。
// 适合调用方紧接着要复用发送通道（如切换到媒体播放）的场景——必须确保旧音频已被清空。
func (t *TTSManager) InterruptAndStopSyncWithReason(ctx context.Context, sendTtsStop bool, stopErr error, reason string) error {
	reason = normalizeTTSReason(reason)
	t.recordPendingInterruptStop(sendTtsStop, stopErr, reason)
	if err := t.InterruptAndClearQueueSyncWithReason(ctx, reason); err != nil {
		t.finishPendingInterruptStopIfSenderLoopExited(ctx)
		return err
	}
	t.finishPendingInterruptStopIfSenderLoopExited(ctx)
	return nil
}

// recordPendingInterruptStop 登记「这次打断需要收口一个 tts_stop」。
//
// 为什么要集中登记而不是各自直接发 tts_stop：
// 只有 runSenderLoop 知道队列是否真的清空了、播放尾指针走到哪了。
// 让所有打断路径都只做"登记"，把"何时真正发 tts_stop"这件事唯一化，
// 就能避免 tts_stop 被并发发出多次、或在队列还有音频时就提前发出。
//
// 多次登记时的合并规则（都是"取更严格/更早"）：
//   - sendTtsStop 取逻辑或：只要有一次要求发，就发；
//   - stopErr 保留第一个非 nil：优先反映最初的失败原因；
//   - reason 保留第一个有意义的原因，便于日志追溯是谁引发的。
func (t *TTSManager) recordPendingInterruptStop(sendTtsStop bool, stopErr error, reason string) {
	t.interruptStopMu.Lock()
	defer t.interruptStopMu.Unlock()

	reason = normalizeTTSReason(reason)
	if t.interruptStopPending {
		t.interruptStopSendTtsStop = t.interruptStopSendTtsStop || sendTtsStop
		if t.interruptStopErr == nil {
			t.interruptStopErr = stopErr
		}
		if t.interruptStopReason == "" || t.interruptStopReason == "unspecified" {
			t.interruptStopReason = reason
		}
	} else {
		t.interruptStopPending = true
		t.interruptStopSendTtsStop = sendTtsStop
		t.interruptStopErr = stopErr
		t.interruptStopReason = reason
	}
	log.Infof("record pending tts interrupt stop: device=%s reason=%s sendTtsStop=%v stopErr=%v state={%s}", t.clientState.DeviceID, reason, sendTtsStop, stopErr, t.debugState())
}

// consumePendingInterruptStop 取出并清空待收口的登记（"取走即消费"，保证只会收口一次）。
func (t *TTSManager) consumePendingInterruptStop() (bool, bool, error, string) {
	t.interruptStopMu.Lock()
	defer t.interruptStopMu.Unlock()

	if !t.interruptStopPending {
		return false, false, nil, ""
	}

	sendTtsStop := t.interruptStopSendTtsStop
	stopErr := t.interruptStopErr
	reason := normalizeTTSReason(t.interruptStopReason)
	t.interruptStopPending = false
	t.interruptStopSendTtsStop = false
	t.interruptStopErr = nil
	t.interruptStopReason = ""
	return true, sendTtsStop, stopErr, reason
}

// finishPendingInterruptStopIfSenderLoopExited 处理"发送协程已经不在了"的兜底收口。
//
// 正常路径下登记会由 runSenderLoop 在清队列时消费掉；但如果发送协程已经退出
// （会话结束/上下文取消），就没人来消费了，登记会永久残留，导致 ttsActive 等状态卡住。
// 所以每次登记后都检查一下：发送协程不在，就由调用方自己完成收口。
func (t *TTSManager) finishPendingInterruptStopIfSenderLoopExited(ctx context.Context) {
	if t.senderLoopActive.Load() {
		return
	}
	if pending, sendTtsStop, stopErr, reason := t.consumePendingInterruptStop(); pending {
		t.finishTtsStopWithReason(ctx, sendTtsStop, stopErr, reason)
	}
}

// InterruptAndClearQueueSync 触发打断并等待 runSenderLoop 完成清队列后再返回。
func (t *TTSManager) InterruptAndClearQueueSync(ctx context.Context) error {
	return t.InterruptAndClearQueueSyncWithReason(ctx, "InterruptAndClearQueueSync")
}

// InterruptAndClearQueueSyncWithReason 同步打断：投递请求并等 runSenderLoop 回执。
//
// 两段等待都同时监听 senderLoopDone，是为了避免死锁：
// 如果发送协程在我们投递前/等待中退出了，req.done 永远不会被关闭，
// 此时靠 senderLoopDone 立刻返回，而不是干等 ctx 超时。
func (t *TTSManager) InterruptAndClearQueueSyncWithReason(ctx context.Context, reason string) error {
	if ctx == nil {
		ctx = context.Background()
	}

	reason = normalizeTTSReason(reason)
	t.nextAudioGeneration()
	t.rotateAudioInterruptCh()
	if !t.senderLoopActive.Load() {
		return nil
	}

	req := interruptRequest{
		done:   make(chan struct{}),
		reason: reason,
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.senderLoopDone:
		return nil
	case t.interruptCh <- req:
	}

	// 请求已送达，等对方清完队列后关闭 req.done。
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.senderLoopDone:
		return nil
	case <-req.done:
		return nil
	}
}

// finishTtsStop 结束当前 TTS 段（是否下发协议 tts_stop 由 sendTtsStop 决定）。
func (t *TTSManager) finishTtsStop(ctx context.Context, sendTtsStop bool, stopErr error) bool {
	return t.finishTtsStopWithReason(ctx, sendTtsStop, stopErr, "finishTtsStop")
}

// finishTtsStopWithReason 负责「结束当前 TTS 段」的全部收尾动作，是 tts_stop 的唯一出口。
//
// 返回 true 表示本次确实收口了一个活跃的 TTS 段，false 表示本来就没有活跃段（幂等，重复调用无副作用）。
//
// 收尾顺序及原因：
//  1. ttsActive.Swap(false)：原子占位，保证并发调用里只有一个执行真正的收尾；
//  2. 若需要发协议 tts_stop：先 DrainPendingAudio（有错误时把客户端尚未播的音频清掉，避免播到一半的残音）；
//  3. 把客户端状态切回 ListenStop、清 TtsStart 标记——设备此时应回到"等待下一轮 listen"；
//  4. 通知欢迎语等待方（避免握手期间主动播报的等待一直挂着）；
//  5. 触发 TTS_OUTPUT_STOP 钩子（外部统计/UI）；
//  6. forceStopTtsMetric 收口指标，dispatchTTSTurnEndPolicy 执行 turn-end 策略。
//
// 时机上的关键点：调用方（runSenderLoop 处理 AudioQueueKindTtsStop）已经在发本函数前
// 等过 playbackTail 与 ttsPlaybackCompletionGrace，所以这里再发 tts_stop 时，
// 端上音频已经真正播完，不会出现"状态切了但声音还在响"的割裂感。
func (t *TTSManager) finishTtsStopWithReason(ctx context.Context, sendTtsStop bool, stopErr error, reason string) bool {
	reason = normalizeTTSReason(reason)
	log.Infof("finish tts stop: device=%s reason=%s sendTtsStop=%v stopErr=%v state={%s}", t.clientState.DeviceID, reason, sendTtsStop, stopErr, t.debugState())
	// Swap(false) 返回旧值：旧值为 false 说明本轮没有活跃 TTS，只需顺手把状态/空闲时钟对齐即可返回。
	if !t.ttsActive.Swap(false) {
		if t.clientState != nil {
			if sendTtsStop && t.clientState.IsRealTime() {
				t.clientState.SetStatus(ClientStatusListenStop)
				t.clientState.SetTtsStart(false)
			}
			if t.clientState.UsesAudioIdleClock() {
				t.clientState.StartAudioIdleWindow(time.Now())
			}
		}
		return false
	}

	if ctx == nil {
		ctx = context.Background()
	}

	sentTtsStop := false
	if sendTtsStop {
		// 出错结束（如传输异常）时先丢弃尚未送出的音频，避免客户端播到一半的残音。
		if stopErr != nil && t.serverTransport != nil {
			t.serverTransport.DrainPendingAudio()
		}
		if err := t.serverTransport.SendTtsStop(); err != nil {
			// 发送失败不覆盖更早的错误原因，只在原本没有错误时记录本次失败。
			if stopErr == nil {
				stopErr = err
			}
			log.Errorf("发送 TtsStop 失败: %v", err)
		} else {
			sentTtsStop = true
		}
	}
	// 只有"没真正发出去 tts_stop"时才需要本地兜底把状态切回监听态；
	// 若已发出，设备侧收到消息后自己会切，服务端无需重复设置（保持状态来源单一）。
	if !sentTtsStop && t.clientState != nil {
		t.clientState.SetStatus(ClientStatusListenStop)
		t.clientState.SetTtsStart(false)
	}
	if t.clientState != nil && t.clientState.UsesAudioIdleClock() {
		t.clientState.StartAudioIdleWindow(time.Now())
	}
	// 主动播报（welcome/说话请求）在等待"播完"，这里告知结果。
	if t.session != nil {
		t.session.completeWelcomePlaybackWait(stopErr == nil)
	}
	if t.session != nil {
		hookErr := t.session.hookHub.EmitTTSOutputStop(t.session.hookContext(ctx), chathooks.TTSOutputStopData{Err: stopErr})
		if hookErr != nil {
			log.Warnf("TTS_OUTPUT_STOP hook 执行失败: %v", hookErr)
		}
	}

	t.forceStopTtsMetric(ctx, stopErr)
	t.dispatchTTSTurnEndPolicy(ctx, stopErr)

	return true
}

// FinishTtsWithoutProtocolStop 结束 TTS 但不下发协议 tts_stop（只做本地状态与指标收口）。
// 用于上层自己会负责通知设备的场景。
func (t *TTSManager) FinishTtsWithoutProtocolStop(ctx context.Context, stopErr error) bool {
	return t.finishTtsStop(ctx, false, stopErr)
}

// EnqueueTtsStart 向会话级音频队列投递 TtsStart，由 runSenderLoop 统一发送；队列满时阻塞直到入队或 ctx.Done
func (t *TTSManager) EnqueueTtsStart(ctx context.Context) {
	t.EnqueueTtsStartWithReason(ctx, "EnqueueTtsStart")
}

// EnqueueTtsStartWithReason 开始新一轮 TTS：开启指标轮次、登记 turn-end 策略，再投递 tts_start 元素。
//
// 顺序有讲究：先 startTtsMetricCycle() 拿到本轮的 cycleID，后续所有 TTS 元素都会带上它；
// 而 registerTTSTurnEndPolicy 必须在收口之前完成，否则策略会丢。
func (t *TTSManager) EnqueueTtsStartWithReason(ctx context.Context, reason string) {
	cycleID := t.startTtsMetricCycle()
	t.registerTTSTurnEndPolicy(ctx, cycleID)
	reason = normalizeTTSReason(reason)
	log.Debugf("enqueue tts start: device=%s reason=%s generation=%d state={%s}", t.clientState.DeviceID, reason, t.currentAudioGeneration(), t.debugState())
	t.enqueueSessionElem(ctx, t.currentAudioGeneration(), AudioQueueElem{Kind: AudioQueueKindTtsStart, DebugReason: reason})
}

// [5.5] 轮次收口：标记当前轮逻辑输出结束；实际 turn_end 会在所有 TTS 音频收完后发出。
func (t *TTSManager) RequestTurnEnd(ctx context.Context, err error) {
	t.emitTtsMetricCompletion(ctx, t.requestTurnEndLocked(err))
}

// EnqueueTtsStop 向会话级音频队列投递 TtsStop，由 runSenderLoop 统一发送；队列满时阻塞直到入队或 ctx.Done
func (t *TTSManager) EnqueueTtsStop(ctx context.Context) {
	t.EnqueueTtsStopWithReason(ctx, "EnqueueTtsStop")
}

// EnqueueTtsStopWithReason 投递 tts_stop 元素。
//
// 注意它只是"排队"：runSenderLoop 收到后会先等播放尾指针走完 + 保护时间，再真正发送，
// 因此从调用到设备收到 tts_stop 之间会有一个符合音频长度的自然延迟。
func (t *TTSManager) EnqueueTtsStopWithReason(ctx context.Context, reason string) {
	reason = normalizeTTSReason(reason)
	log.Infof("enqueue tts stop: device=%s reason=%s generation=%d state={%s}", t.clientState.DeviceID, reason, t.currentAudioGeneration(), t.debugState())
	t.enqueueSessionElem(ctx, t.currentAudioGeneration(), AudioQueueElem{Kind: AudioQueueKindTtsStop, DebugReason: reason})
}

// enqueueSessionElemWithError 是 enqueueSessionElem 的错误版：把"投递失败"转成具体 error 返回。
// 若 ctx 本身已取消就返回 ctx.Err()，否则统一返回 context.Canceled（例如队列在关闭期被丢弃）。
func (t *TTSManager) enqueueSessionElemWithError(ctx context.Context, generation uint64, elem AudioQueueElem) error {
	if t.enqueueSessionElem(ctx, generation, elem) {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return context.Canceled
}

// EnqueueMediaSentenceStart 供媒体播放器插入一条 sentence_start（例如歌词/标题字幕）。
// 它和 TTS 的句子消息共享同一套延迟对齐机制，因此字幕依然能和音频同步。
func (t *TTSManager) EnqueueMediaSentenceStart(ctx context.Context, text string, onError func(error)) error {
	return t.enqueueSessionElemWithError(ctx, t.currentAudioGeneration(), AudioQueueElem{
		Kind:    AudioQueueKindSentenceStart,
		Text:    text,
		OnError: onError,
	})
}

// EnqueueMediaSentenceEnd 供媒体播放器插入一条 sentence_end。
// 若投递失败会主动回调 onEnd，避免调用方（可能正在同步等待）被挂住。
func (t *TTSManager) EnqueueMediaSentenceEnd(ctx context.Context, text string, onError func(error), onEnd func(error)) error {
	err := t.enqueueSessionElemWithError(ctx, t.currentAudioGeneration(), AudioQueueElem{
		Kind:    AudioQueueKindSentenceEnd,
		Text:    text,
		OnEnd:   onEnd,
		OnError: onError,
	})
	if err != nil && onEnd != nil {
		onEnd(err)
	}
	return err
}

// EnqueueMediaFrame 供媒体播放器投递一帧音频（MediaFrame 走与 TTS 帧相同的发送与流控路径）。
// 入队前先深拷贝：上层可能复用缓冲区，直接持有切片会导致数据被覆盖。
func (t *TTSManager) EnqueueMediaFrame(ctx context.Context, frame []byte, onError func(error)) error {
	frameCopy := make([]byte, len(frame))
	copy(frameCopy, frame)
	return t.enqueueSessionElemWithError(ctx, t.currentAudioGeneration(), AudioQueueElem{
		Kind:    AudioQueueKindMediaFrame,
		Data:    frameCopy,
		OnError: onError,
	})
}

// [5.1] 生成音频：句子队列的消费循环：把 TTSQueueItem 变成音频帧并推入发送队列。
//
// processTTSQueue 句子队列的消费循环：把 TTSQueueItem 变成音频帧并推入发送队列。
//
// 它是「生成侧」的串行执行者：虽然合成请求本身在 provider 内部可能是并发的，
// 但"从队列取哪个 item、何时取"是串行的，这保证了音频的先后顺序与文本顺序一致。
func (t *TTSManager) processTTSQueue(ctx context.Context) {
	for {
		// [5.1] 取队列：阻塞等待下一条待合成句子（0 = 不设超时）
		item, err := t.ttsQueue.Pop(ctx, 0) // 阻塞式
		if err != nil {
			// ctx 结束才退出；其它错误（极少见）继续尝试下一条，避免整条链路因单次异常停摆。
			if err == util.ErrQueueCtxDone {
				return
			}
			continue
		}

		// 已被打断水位的 item 直接丢弃（dismiss 内部会回调 onEnd 并递减指标计数）。
		if t.shouldDropTTSQueueItem(item) {
			t.dismissTTSQueueItem(item, context.Canceled)
			continue
		}

		// 派生一个"可被音频打断取消"的上下文：打断时能提前终止在途的合成请求，节省时间与费用。
		itemCtx, cancel := t.withAudioInterruptContext(item.ctx)
		item.ctx = itemCtx
		// 媒体正在独占输出时先等它结束，否则会出现音乐和人声同时播。
		waitErr := t.waitForMediaPlaybackRelease(item.ctx)
		if waitErr != nil {
			cancel()
			t.dismissTTSQueueItem(item, waitErr)
			continue
		}
		// 等待期间可能发生打断，需要再查一次水位。
		if t.shouldDropTTSQueueItem(item) {
			cancel()
			t.dismissTTSQueueItem(item, context.Canceled)
			continue
		}

		itemErr := error(nil)
		if item.StreamChan != nil {
			// 流式模式：文本来自 channel，边收边合成。
			log.Debugf("processTTSQueue start, stream mode")
			itemErr = t.handleStreamTts(item)
			t.finishTtsMetricItem(item.ctx, item.metricCycle, itemErr)
			cancel()
			log.Debugf("processTTSQueue end, stream mode")
			continue
		}

		// 非流式：由 handleTts 生成并推送 SentenceStart → Frame… → SentenceEnd
		log.Debugf("processTTSQueue start, text: %s", item.llmResponse.Text)
		itemErr = t.handleTts(item.ctx, item.generation, item.metricCycle, item.llmResponse, item.onStartFunc, item.onEndFunc)
		t.finishTtsMetricItem(item.ctx, item.metricCycle, itemErr)
		cancel()
		log.Debugf("processTTSQueue end, text: %s (pushed)", item.llmResponse.Text)
	}
}

// ClearTTSQueue 清空句子队列（打断/媒体接管时调用）。
//
// 三步：
//  1. droppedTTSSeq 提升到当前已发号的最大值 → 队列里现有 item 全部判为作废；
//  2. dualStreamEpoch 自增并关闭当前双流 channel → 让正在写入的 LLM 文本流知道"这一轮废了"；
//  3. ClearAndDrain 真正取空队列，逐个 dismiss（回调 onEnd + 递减指标计数）。
//
// 注意：这里只处理"还没合成"的任务；"已生成待发送"的音频由 sessionAudioQueue 的那一套
// （audioGeneration + interruptCh）负责，两者配合才是一次完整的打断。
func (t *TTSManager) ClearTTSQueue() {
	t.droppedTTSSeq.Store(t.ttsQueueSeq.Load())
	t.dualStreamEpoch.Add(1)

	t.dualStreamMu.Lock()
	dualStreamChan := t.dualStreamChan
	t.dualStreamChan = nil
	t.dualStreamDone = nil
	t.dualStreamOwnerCtx = nil
	t.dualStreamMu.Unlock()
	safeCloseLLMResponseStream(dualStreamChan)

	drained := t.ttsQueue.ClearAndDrain()
	for _, item := range drained {
		t.dismissTTSQueueItem(item, context.Canceled)
	}
}

// handleTts 单条 TTS：生成并向 sessionAudioQueue 推送 SentenceStart → Frame… → SentenceEnd
//
// 这是「一句话的完整生命周期」：字幕开始 → N 个音频帧 → 字幕结束。
// 它会把 provider 的释放函数（release）与指标请求（finishRequest）在每条退出路径上都收尾，
// 所以下面每个分支都有 release()/finishRequest()/onEndFunc() 的成组调用。
func (t *TTSManager) handleTts(ctx context.Context, generation uint64, metricCycle uint64, llmResponse llm_common.LLMResponseStruct, onStartFunc func(), onEndFunc func(error)) error {
	if strings.TrimSpace(llmResponse.Text) == "" {
		// 空文本：没有可合成的内容，但仍要回调 onEnd 让上层继续（不能静默返回）。
		if onEndFunc != nil {
			onEndFunc(nil)
		}
		return nil
	}
	outChan, release, genErr := t.generateTtsOnly(ctx, metricCycle, llmResponse)
	if genErr != nil {
		log.Errorf("handleTts gen err, text: %s, err: %v", llmResponse.Text, genErr)
		if onEndFunc != nil {
			onEndFunc(genErr)
		}
		return genErr
	}
	if outChan == nil {
		// provider 表示无需合成（如纯标点）：释放资源后按正常结束处理。
		if release != nil {
			release()
		}
		if onEndFunc != nil {
			onEndFunc(nil)
		}
		return nil
	}
	// requestActive/finishRequest 保证「指标上的合成请求」只减一次，无论从哪个分支退出。
	requestActive := true
	firstAudioReported := false
	finishRequest := func(err error) {
		if requestActive {
			t.finishTtsMetricRequest(ctx, metricCycle, err)
			requestActive = false
		}
	}
	// 先发 sentence_start（会走延迟队列与音频对齐），再逐帧发送。
	if !t.enqueueSessionElem(ctx, generation, AudioQueueElem{
		Kind:    AudioQueueKindSentenceStart,
		Text:    llmResponse.Text,
		IsStart: llmResponse.IsStart,
		OnStart: onStartFunc,
	}) {
		if release != nil {
			release()
		}
		finishRequest(ctx.Err())
		if onEndFunc != nil {
			onEndFunc(ctx.Err())
		}
		return ctx.Err()
	}
	for {
		select {
		case <-ctx.Done():
			// 被打断或上下文结束：释放 provider、收尾指标，并回调 onEnd 让上层解锁。
			if release != nil {
				release()
			}
			finishRequest(ctx.Err())
			if onEndFunc != nil {
				onEndFunc(ctx.Err())
			}
			return ctx.Err()
		case frame, ok := <-outChan:
			if !ok {
				// 音频流结束：释放资源、收尾指标，再补一条 sentence_end。
				// 注意 sentence_end 才是本句的完成信号（它的 OnEnd 挂在 onEndFunc 上）。
				if release != nil {
					release()
				}
				finishRequest(nil)
				if !t.enqueueSessionElem(ctx, generation, AudioQueueElem{
					Kind:  AudioQueueKindSentenceEnd,
					Text:  llmResponse.Text,
					OnEnd: onEndFunc,
				}) && onEndFunc != nil {
					onEndFunc(ctx.Err())
				}
				return ctx.Err()
			}
			// 首帧要单独记一次指标（端到端延迟的关键节点）。
			if !firstAudioReported {
				t.markTtsMetricFirstAudio(ctx, metricCycle)
				firstAudioReported = true
			}
			// 深拷贝：provider 可能复用帧缓冲，入队后不能被它覆盖。
			frameCopy := make([]byte, len(frame))
			copy(frameCopy, frame)
			if !t.enqueueSessionElem(ctx, generation, AudioQueueElem{Kind: AudioQueueKindFrame, Data: frameCopy, MetricCycle: metricCycle}) {
				if release != nil {
					release()
				}
				finishRequest(ctx.Err())
				if onEndFunc != nil {
					onEndFunc(ctx.Err())
				}
				return ctx.Err()
			}
		}
	}
}

// ttsSyncWaitTimeout 同步等待 TTS 完成的兜底超时。
// 之所以需要它：如果上游某个回调因异常未被触发，isSync 调用方会永久阻塞，
// 有超时至少能让请求返回错误而不是把协程/连接一起拖死。
const ttsSyncWaitTimeout = 30 * time.Second

// signalDone 向已缓冲的 done 发送一次完成信号，多次调用仅首次生效
//
// done 的缓冲大小为 1，因此"发送成功"只会有一次；重复调用走 default 直接丢弃，
// 起到幂等作用（多种结束路径可能都会尝试 signal）。
func signalDone(done chan<- struct{}) {
	select {
	case done <- struct{}{}:
	default:
	}
}

// safeCloseLLMResponseStream 安全关闭 LLM 响应 channel。
// 用 recover 包住的原因：关闭一个已被别人关闭的 channel 会 panic，
// 而"谁负责关闭双流 channel"存在天然竞争（打断、IsEnd、被顶替的新 IsStart），
// 这里选择"尽力而为止、重复关闭不致命"的策略。
func safeCloseLLMResponseStream(ch chan llm_common.LLMResponseStruct) {
	if ch == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	close(ch)
}

// sendLLMResponseToDualStream 把一段 LLM 文本写入双流式 channel。
// 同样用 recover 兜住"向已关闭 channel 发送"的 panic（发送方与关闭方是不同协程），
// 此时视为正常结束（err = nil），因为流已作废。
func sendLLMResponseToDualStream(ctx context.Context, ch chan llm_common.LLMResponseStruct, llmResponse llm_common.LLMResponseStruct) (err error) {
	if ch == nil {
		return nil
	}

	defer func() {
		if recover() != nil {
			err = nil
		}
	}()

	select {
	case ch <- llmResponse:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("TTS 处理上下文已取消")
	}
}

// chainTTSOnEndFuncs 把多个 onEnd 回调串成一个：按顺序依次执行。
//
// 存在的原因：同一个 TTSQueueItem 的结束可能要触发好几件事
// （业务回调 + isSync 的 done 信号），但 item 只支持一个 onEndFunc，
// 所以在入队前把它们链起来。全部为 nil 时返回 nil，避免产生无意义的闭包。
func chainTTSOnEndFuncs(funcs ...func(error)) func(error) {
	filtered := make([]func(error), 0, len(funcs))
	for _, fn := range funcs {
		if fn != nil {
			filtered = append(filtered, fn)
		}
	}
	if len(filtered) == 0 {
		return nil
	}

	return func(err error) {
		for _, fn := range filtered {
			fn(err)
		}
	}
}

// waitForSync 同步等待完成信号，支持 ctx 取消与超时
func (t *TTSManager) waitForSync(ctx context.Context, done <-chan struct{}) error {
	timer := time.NewTimer(ttsSyncWaitTimeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("TTS 处理上下文已取消")
	case <-timer.C:
		return fmt.Errorf("TTS 处理超时")
	}
}

// handleTextResponse 处理文本响应（异步 TTS 入队）。调用方按句多次调用，内部根据 SupportsDualStream() 自动决定：
//   - 不支持双流式：每次 Push 一个单条 TTSQueueItem（与原逻辑一致）。
//   - 支持双流式：IsStart 时创建内部 StreamChan 并 Push 一个流式 item，后续调用写入该 channel，IsEnd 时 close。
//
// 这是 LLM 层进入 TTS 层的唯一入口（llm.go 调用），也是「按句驱动」的入口：
// 上层每分出一句就调一次，isSync=true 时本函数会阻塞到该句真正播报结束。
func (t *TTSManager) handleTextResponse(ctx context.Context, llmResponse llm_common.LLMResponseStruct, isSync bool) error {
	return t.handleTextResponseWithHooks(ctx, llmResponse, isSync, nil, nil)
}

// [5.0] 文本入队：把 LLM 文本切成句子投进 ttsQueue（双流式则投流式 item）。
//
// handleTextResponseWithHooks 带钩子的实现：onTTSItemEnqueued 在真正入队时返回一个完成回调，
// onTTSPlaybackStart 在首句开始播放时触发。
//
// 语义约定（llm.go 侧的约定）：
//   - IsStart 表示这一轮 TTS 的第一段文本，用来建流/记起点；
//   - IsEnd 表示这一轮文本已经全部产出完毕，可以关流；
//   - 正常情形下应成对出现（IsStart → 若干中间段 → IsEnd），但上游异常时可能缺失。
//
// 这里的容错设计：只有文本、没有 IsStart/IsEnd 也接受（走单条入队），
// 避免因为上游少发一个标记就整轮播报不出来。
func (t *TTSManager) handleTextResponseWithHooks(ctx context.Context, llmResponse llm_common.LLMResponseStruct, isSync bool, onTTSItemEnqueued func() func(error), onTTSPlaybackStart func()) error {
	hasText := strings.TrimSpace(llmResponse.Text) != ""
	// 既无文本、也不是首/尾标记：没有信息量的空响应，直接忽略。
	if !hasText && !llmResponse.IsEnd && !llmResponse.IsStart {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		log.Debugf("handleTextResponse: ignore response because ctx already canceled")
		return nil
	}

	// TTS_INPUT hook
	// 允许外部（业务钩子）改写文本或调整首/尾标记，甚至要求停止本轮流程。
	if t.session != nil {
		payload, stop, hookErr := t.session.hookHub.EmitTTSInput(t.session.hookContext(ctx), chathooks.TTSInputData{Text: llmResponse.Text, IsStart: llmResponse.IsStart, IsEnd: llmResponse.IsEnd})
		if hookErr != nil {
			log.Warnf("TTS_INPUT hook 执行失败: %v", hookErr)
		}
		llmResponse.Text = payload.Text
		llmResponse.IsStart = payload.IsStart
		llmResponse.IsEnd = payload.IsEnd
		if stop {
			log.Infof("TTS_INPUT hook 请求停止当前流程")
			return nil
		}
	}

	// 重新检查 hasText，因为 hook 可能修改了文本
	hasText = strings.TrimSpace(llmResponse.Text) != ""

	if !t.SupportsDualStream() {
		// ---- 非双流式：一句一个 item，交给 processTTSQueue 串行合成 ----
		if !hasText {
			return nil
		}
		gen := t.currentAudioGeneration()
		metricCycle := t.currentTtsMetricCycle()
		var done chan struct{}
		onEndFunc := func(error) {}
		if onTTSItemEnqueued != nil {
			onEndFunc = onTTSItemEnqueued()
		}
		if isSync {
			// 同步模式：把 done 链接到 onEndFunc 上，等这一句真正发完才返回。
			done = make(chan struct{}, 1)
			onEndFunc = chainTTSOnEndFuncs(onEndFunc, func(error) { signalDone(done) })
		}
		if err := t.pushTTSQueueItem(TTSQueueItem{
			ctx:         ctx,
			llmResponse: llmResponse,
			generation:  gen,
			metricCycle: metricCycle,
			onStartFunc: onTTSPlaybackStart,
			onEndFunc:   onEndFunc,
		}); err != nil {
			if onEndFunc != nil {
				onEndFunc(err)
			}
			return err
		}
		if isSync {
			return t.waitForSync(ctx, done)
		}
		return nil
	}

	// 双流式模式
	// 与单条模式的区别：整轮只 Push 一个"流式 item"，它内部持有 streamChan，
	// 由 handleDualStreamTts 边读文本边合成，首帧延迟更低（不用等整句分完）。
	var streamChan chan llm_common.LLMResponseStruct
	var streamOwnerCtx context.Context
	if llmResponse.IsStart {
		// 新一轮开始：先把可能残留的旧流顶掉并关闭，避免文本被写进上一轮的流。
		streamEpoch := t.dualStreamEpoch.Load()
		t.dualStreamMu.Lock()
		oldStreamChan := t.dualStreamChan
		t.dualStreamChan = nil
		t.dualStreamDone = nil
		t.dualStreamOwnerCtx = nil
		t.dualStreamMu.Unlock()
		safeCloseLLMResponseStream(oldStreamChan)

		streamChan = make(chan llm_common.LLMResponseStruct, 16)
		var done chan struct{}
		var onEndFunc func(error)
		if onTTSItemEnqueued != nil {
			onEndFunc = onTTSItemEnqueued()
		}
		if isSync {
			done = make(chan struct{}, 1)
			onEndFunc = chainTTSOnEndFuncs(onEndFunc, func(error) { signalDone(done) })
		}
		if err := t.pushTTSQueueItem(TTSQueueItem{
			ctx:         ctx,
			StreamChan:  streamChan,
			generation:  t.currentAudioGeneration(),
			metricCycle: t.currentTtsMetricCycle(),
			onStartFunc: onTTSPlaybackStart,
			onEndFunc:   onEndFunc,
		}); err != nil {
			safeCloseLLMResponseStream(streamChan)
			if onEndFunc != nil {
				onEndFunc(err)
			}
			return err
		}
		// epoch 校验：入队这一步可能耗时，期间队列被清空/打断（epoch 自增）了，
		// 那么这个流已经没人会消费，直接关掉返回，避免协程泄漏。
		if t.dualStreamEpoch.Load() != streamEpoch {
			safeCloseLLMResponseStream(streamChan)
			return nil
		}
		t.dualStreamMu.Lock()
		// 上锁后再校验一次，防止"检查通过→上锁之前"被打断（双检锁）。
		if t.dualStreamEpoch.Load() != streamEpoch {
			t.dualStreamMu.Unlock()
			safeCloseLLMResponseStream(streamChan)
			return nil
		}
		// 记下"当前活跃流"，后续中间文本段就写进它。
		t.dualStreamChan = streamChan
		t.dualStreamDone = done
		t.dualStreamOwnerCtx = ctx
		t.dualStreamMu.Unlock()
		log.Debugf("handleTextResponse: dual stream, created StreamChan and pushed item")
	} else {
		// 非 IsStart 的文本段：取出当前活跃流写入。
		t.dualStreamMu.Lock()
		streamChan = t.dualStreamChan
		streamOwnerCtx = t.dualStreamOwnerCtx
		t.dualStreamMu.Unlock()
		// owner ctx 校验：不同 ctx 意味着这是另一个 turn 的迟到片段，不能混进本轮的音频流。
		if streamChan != nil && streamOwnerCtx != ctx {
			log.Debugf("handleTextResponse: dual stream fragment ignored because active stream belongs to another context")
			return nil
		}
	}

	if streamChan != nil && hasText {
		// 双流式正常路径：文本写进流，由 handleDualStreamTts 边收边合成。
		if err := sendLLMResponseToDualStream(ctx, streamChan, llmResponse); err != nil {
			return err
		}
	} else if streamChan == nil && hasText {
		// 降级：未收到 IsStart 就来了数据，按单条入队
		// 触发场景：上游异常/首包丢失/IsStart 被 hook 清掉。
		// 宁可退化成"逐句合成"也不能丢掉这段文本，否则用户会听到答非所问的残缺回答。
		gen := t.currentAudioGeneration()
		var done chan struct{}
		var onEndFunc func(error)
		if onTTSItemEnqueued != nil {
			onEndFunc = onTTSItemEnqueued()
		}
		if isSync {
			done = make(chan struct{}, 1)
			onEndFunc = chainTTSOnEndFuncs(onEndFunc, func(error) { signalDone(done) })
		}
		if err := t.pushTTSQueueItem(TTSQueueItem{
			ctx:         ctx,
			llmResponse: llmResponse,
			generation:  gen,
			metricCycle: t.currentTtsMetricCycle(),
			onStartFunc: onTTSPlaybackStart,
			onEndFunc:   onEndFunc,
		}); err != nil {
			if onEndFunc != nil {
				onEndFunc(err)
			}
			return err
		}
		log.Debugf("handleTextResponse: dual stream fallback, no active stream, pushed single item")
		if isSync {
			return t.waitForSync(ctx, done)
		}
	}

	if llmResponse.IsEnd && streamChan != nil {
		// 一轮结束：关闭流（消费方 handleStreamTts/handleDualStreamTts 会读到关闭并收尾）。
		var done chan struct{}
		t.dualStreamMu.Lock()
		// 只有"确实是我这个 ctx 的流"才由我关闭并清理，避免误关后来者的流。
		if t.dualStreamChan == streamChan && t.dualStreamOwnerCtx == ctx {
			done = t.dualStreamDone
			t.dualStreamChan = nil
			t.dualStreamDone = nil
			t.dualStreamOwnerCtx = nil
		}
		t.dualStreamMu.Unlock()
		safeCloseLLMResponseStream(streamChan)
		// isSync 时等"整轮音频发送完毕"（onEndFunc 触发）再返回。
		if isSync && done != nil {
			return t.waitForSync(ctx, done)
		}
	}

	return nil
}

// getEffectiveTTSConfig 返回当前生效的 TTS 配置：有声纹则用声纹配置，否则用设备默认 TTS 配置（与 getTTSProviderInstance 一致）
//
// 为什么返回拷贝：返回的 map 会被调用方读取甚至（理论上）修改，
// 若直接返回 clientState 里的引用，就可能把设备配置改脏，影响后续所有轮次。
func (t *TTSManager) getEffectiveTTSConfig() map[string]interface{} {
	if t.clientState.SpeakerTTSConfig != nil && len(t.clientState.SpeakerTTSConfig) > 0 {
		config := make(map[string]interface{})
		for k, v := range t.clientState.SpeakerTTSConfig {
			config[k] = v
		}
		return config
	}
	return t.clientState.DeviceConfig.Tts.Config
}

// SupportsDualStream 判断当前 TTS 是否支持双流式：TTS 输入与输出均为流式（边收文本边合成输出），与 LLM 无关；由配置 double_stream 与 TTS provider 绑定。
//
// 之所以按"当前生效配置"判断（而非设备默认配置）：声纹切换音色时可能换成了不支持的 provider，
// 此时必须退回单条模式，否则会走进双流分支却拿不到 DualStreamProvider。
// 配置值兼容 bool 与字符串两种写法（"true"/"1"），因为配置文件里两种都可能出现。
func (t *TTSManager) SupportsDualStream() bool {
	config := t.getEffectiveTTSConfig()
	if config == nil {
		return false
	}
	v, ok := config["double_stream"]
	if !ok {
		return false
	}
	if b, ok := v.(bool); ok {
		return b
	}
	if s, ok := v.(string); ok {
		return s == "true" || s == "1"
	}
	return false
}

// getTTSProviderInstance 获取TTS Provider实例（使用provider+音色作为资源池唯一key）
//
// 为什么池 key 用「provider+音色」而不是只用 provider：
// TTS 实例内部通常绑定了一个具体音色/音色 ID。若同一 provider 的多个音色共用池 key，
// 拿到的实例可能是别人音色的，播报声音就串了。加上 voiceID 后，
// 声纹切换（SpeakerTTSConfig 变化）会自然命中另一个池槽位，从而立即换声。
func (t *TTSManager) getTTSProviderInstance() (*pool.ResourceWrapper[tts.TTSProvider], error) {
	// 获取TTS配置和provider
	var ttsConfig map[string]interface{}
	var ttsProvider string

	if t.clientState.SpeakerTTSConfig != nil && len(t.clientState.SpeakerTTSConfig) > 0 {
		// 使用声纹TTS配置
		if provider, ok := t.clientState.SpeakerTTSConfig["provider"].(string); ok {
			ttsProvider = provider
		} else {
			log.Warnf("声纹TTS配置中缺少 provider，使用默认配置")
			ttsProvider = t.clientState.DeviceConfig.Tts.Provider
			ttsConfig = t.clientState.DeviceConfig.Tts.Config
		}
		// 深拷贝配置
		// 注意：无论上面是否走了兜底分支，这里都会用 SpeakerTTSConfig 重建配置，
		// 保证 provider 与 config 来自同一份设置，不会出现"provider 是默认、config 是声纹"的错配。
		ttsConfig = make(map[string]interface{})
		for k, v := range t.clientState.SpeakerTTSConfig {
			ttsConfig[k] = v
		}
	} else {
		// 使用默认TTS配置
		ttsProvider = t.clientState.DeviceConfig.Tts.Provider
		ttsConfig = t.clientState.DeviceConfig.Tts.Config
	}

	// 逻辑标识（用于日志与指纹计算）：provider 或 provider:voiceID
	voiceID := extractVoiceID(ttsConfig)
	providerLabel := ttsProvider
	if voiceID != "" {
		providerLabel = fmt.Sprintf("%s:%s", ttsProvider, voiceID)
	}

	// 从资源池获取TTS资源（池 key 由配置指纹决定，host/voice 等变更会自动换池）
	// 用完后必须调用 pool.Release，因此 handleTts/generateTtsOnly 都会把 release 一路传下去。
	ttsWrapper, err := pool.Acquire[tts.TTSProvider]("tts", providerLabel, ttsConfig)
	if err != nil {
		log.Errorf("获取TTS资源失败: %v", err)
		return nil, fmt.Errorf("获取TTS资源失败: %v", err)
	}

	return ttsWrapper, nil
}

// extractVoiceID 从配置中提取音色ID
//
// 不同 provider 的音色字段名不同，这里做统一映射：
//   - cosyvoice 用 spk_id；
//   - minimax 等其余 provider 用 voice。
//
// 提取不到（字段缺失/类型不符/为空）时返回空串，池 key 就退化为纯 provider 名。
func extractVoiceID(config map[string]interface{}) string {
	if config == nil {
		return ""
	}

	// 尝试从config中获取provider类型
	provider, _ := config["provider"].(string)

	// cosyvoice使用spk_id字段
	if provider == "cosyvoice" {
		if spkID, ok := config["spk_id"].(string); ok && spkID != "" {
			return spkID
		}
		return ""
	}

	// minimax和其他provider：使用voice
	if voice, ok := config["voice"].(string); ok && voice != "" {
		return voice
	}

	return ""
}

// [5.2] 合成：仅做 TTS 生成、不发送；返回音频 channel 与发送完成后需调用的 ReleaseFunc
//
// 为什么要"生成与发送分离"（方案 C）：
//   - 生成侧只关心"拿到音频帧"（handleTts/handleStreamTts 都在生成侧）；
//   - 发送侧统一由 runSenderLoop 做流控与打断；
//
// 这样插入媒体播放、打断、重排都不会影响 provider 调用逻辑。
//
// 返回约定：
//   - 空文本时直接返回 (nil, nil, nil)，调用方按"无需处理"对待；
//   - releaseFunc 必须被调用（无论成功失败），否则池资源泄漏;
//   - 一旦发起请求就 markTtsMetricRequestStart，失败时会自己 finish，成功时由调用方负责 finish。
func (t *TTSManager) generateTtsOnly(ctx context.Context, metricCycle uint64, llmResponse llm_common.LLMResponseStruct) (outputChan <-chan []byte, releaseFunc func(), err error) {
	if strings.TrimSpace(llmResponse.Text) == "" {
		return nil, nil, nil
	}
	ttsWrapper, err := t.getTTSProviderInstance()
	if err != nil {
		log.Errorf("获取TTS Provider实例失败: %v", err)
		return nil, nil, err
	}
	ttsProviderInstance := ttsWrapper.GetProvider()
	t.markTtsMetricRequestStart(ctx, metricCycle)
	// 传入输出音频格式（采样率/声道/帧长），保证合成结果与设备可播放的格式一致。
	ch, err := ttsProviderInstance.TextToSpeechStream(ctx, llmResponse.Text, t.clientState.OutputAudioFormat.SampleRate, t.clientState.OutputAudioFormat.Channels, t.clientState.OutputAudioFormat.FrameDuration)
	if err != nil {
		pool.Release(ttsWrapper)
		t.finishTtsMetricRequest(ctx, metricCycle, err)
		log.Errorf("生成 TTS 音频失败: %v", err)
		return nil, nil, fmt.Errorf("生成 TTS 音频失败: %v", err)
	}
	return ch, func() { pool.Release(ttsWrapper) }, nil
}

// handleDualStreamTts 真正的双流式 TTS：将 StreamChan 里的文本流式输入给 TTS provider，同时流式输出音频。
// 返回 true 表示已处理（成功或出错），false 表示 provider 不支持双流式需要降级。
//
// 与 handleTts（单条）的本质区别：
//   - 单条：先等一整句文本，再一次性合成；
//   - 双流：文本还没写完就开始合成，provider 每凑够一段就吐音频，
//     因此首帧延迟显著更低（用户感觉"回答得更快"）。
//
// 代价是 provider 必须实现 DualStreamProvider（文本/音频双向流式），
// 所以这里有两层类型断言，任何一层不满足就返回 false 让调用方降级。
func (t *TTSManager) handleDualStreamTts(item TTSQueueItem) (bool, error) {
	ttsWrapper, err := t.getTTSProviderInstance()
	if err != nil {
		log.Errorf("双流式 TTS 获取 provider 失败: %v", err)
		return false, nil
	}
	defer pool.Release(ttsWrapper)

	provider := ttsWrapper.GetProvider()
	// 第一层：需要 provider 是适配器类型（适配器暴露了内部真正的 provider）。
	adapter, ok := provider.(*tts.ContextTTSAdapter)
	if !ok {
		return false, nil
	}
	// 第二层：内部 provider 必须声明自己支持双流式。
	dp, ok := adapter.Provider.(tts.DualStreamProvider)
	if !ok {
		return false, nil
	}

	// textChan 是"文本进"的通道：下面的 goroutine 从 item.StreamChan 读 LLM 文本写进来。
	textChan := make(chan string, 16)
	t.markTtsMetricRequestStart(item.ctx, item.metricCycle)
	// eventChan 是"事件出"的通道：每批音频 + 句子边界信号都从这里回来。
	eventChan, err := dp.StreamingSynthesize(item.ctx, textChan,
		t.clientState.OutputAudioFormat.SampleRate,
		t.clientState.OutputAudioFormat.Channels,
		t.clientState.OutputAudioFormat.FrameDuration)
	if err != nil {
		close(textChan)
		t.finishTtsMetricRequest(item.ctx, item.metricCycle, err)
		log.Errorf("双流式 TTS StreamingSynthesize 失败: %v", err)
		return false, nil
	}
	requestActive := true
	firstAudioReported := false
	finishRequest := func(err error) {
		if requestActive {
			t.finishTtsMetricRequest(item.ctx, item.metricCycle, err)
			requestActive = false
		}
	}

	// 从 StreamChan 读 LLM 响应文本并喂给 TTS provider。
	// 用独立 goroutine 是因为："读 LLM 文本"与"收 TTS 音频"必须同时进行，
	// 否则会互相阻塞（provider 等文本、而我们等音频）。
	// textChan 在退出时关闭，作为"文本结束"的信号告诉 provider 可以收尾了。
	go func() {
		defer close(textChan)
		for {
			select {
			case <-item.ctx.Done():
				return
			case resp, ok := <-item.StreamChan:
				if !ok {
					return
				}
				text := strings.TrimSpace(resp.Text)
				if text == "" {
					continue
				}
				select {
				case textChan <- text:
				case <-item.ctx.Done():
					return
				}
			}
		}
	}()

	firstSentence := true
	// 消费 provider 的事件流直到它关闭（关闭意味着合成结束）。
	for event := range eventChan {
		// SentenceSignals 是 provider 识别出的句子边界：用于同步发送字幕消息，
		// 这样双流式下字幕依然能和音频对齐（由 delayed sentence 机制兜住延迟）。
		for _, signal := range event.SentenceSignals {
			switch signal.Type {
			case ttsstream.SentenceSignalEnd:
				if !t.enqueueSessionElem(item.ctx, item.generation, AudioQueueElem{
					Kind: AudioQueueKindSentenceEnd,
					Text: signal.Text,
				}) {
					finishRequest(item.ctx.Err())
					if item.onEndFunc != nil {
						item.onEndFunc(item.ctx.Err())
					}
					return true, item.ctx.Err()
				}
			case ttsstream.SentenceSignalStart:
				startElem := AudioQueueElem{
					Kind:    AudioQueueKindSentenceStart,
					Text:    signal.Text,
					IsStart: firstSentence,
				}
				// OnStart（播报开始钩子）只在整轮的第一句上挂一次。
				if firstSentence {
					startElem.OnStart = item.onStartFunc
					firstSentence = false
				}
				if !t.enqueueSessionElem(item.ctx, item.generation, startElem) {
					finishRequest(item.ctx.Err())
					if item.onEndFunc != nil {
						item.onEndFunc(item.ctx.Err())
					}
					return true, item.ctx.Err()
				}
			}
		}

		if len(event.Audio) > 0 {
			if !firstAudioReported {
				t.markTtsMetricFirstAudio(item.ctx, item.metricCycle)
				firstAudioReported = true
			}
			// 深拷贝：event 结构可能被 provider 复用。
			frameCopy := make([]byte, len(event.Audio))
			copy(frameCopy, event.Audio)
			if !t.enqueueSessionElem(item.ctx, item.generation, AudioQueueElem{Kind: AudioQueueKindFrame, Data: frameCopy, MetricCycle: item.metricCycle}) {
				finishRequest(item.ctx.Err())
				if item.onEndFunc != nil {
					item.onEndFunc(item.ctx.Err())
				}
				return true, item.ctx.Err()
			}
		}
	}

	// 事件流正常结束：收尾指标，并补一条 sentence_end 把 onEndFunc 交给发送侧触发。
	finishRequest(nil)
	if !t.enqueueSessionElem(item.ctx, item.generation, AudioQueueElem{Kind: AudioQueueKindSentenceEnd, OnEnd: item.onEndFunc}) && item.onEndFunc != nil {
		item.onEndFunc(nil)
	}
	return true, item.ctx.Err()
}

// [5.2] 合成（流式）：从 item.StreamChan 读并逐条 generateTtsOnly，推 SentenceStart → Frame… → SentenceEnd。
//
// handleStreamTts 流式 TTS：从 item.StreamChan 读并逐条 generateTtsOnly，向 sessionAudioQueue 推送 SentenceStart → Frame… → SentenceEnd
//
// 它是"流式 item"的总入口，内部有两种实现：
//   - 优先尝试双流式（真正边收文本边合成）；
//   - 若 provider 不支持（handleDualStreamTts 返回 false），则退化为
//     "逐条读文本 → 逐条整句合成"，即下面这个循环。
//
// 因此流式 item 的存在意义不只是双流式：即使降级，也只需一次入队，
// 后续文本通过 channel 源源不断送来，不必每句都走一遍 Push/指标登记。
func (t *TTSManager) handleStreamTts(item TTSQueueItem) error {
	if t.SupportsDualStream() {
		handled, err := t.handleDualStreamTts(item)
		if handled {
			return err
		}
	}

	// firstSegment 用于把 OnStart（播报开始）只挂在第一句上。
	firstSegment := true
	for {
		select {
		case <-item.ctx.Done():
			// 被打断：发一条带错误的 sentence_end，把 onEndFunc 交给发送侧触发后退出。
			if !t.enqueueSessionElem(item.ctx, item.generation, AudioQueueElem{Kind: AudioQueueKindSentenceEnd, OnEnd: item.onEndFunc, Err: item.ctx.Err()}) && item.onEndFunc != nil {
				item.onEndFunc(item.ctx.Err())
			}
			return item.ctx.Err()
		case resp, ok := <-item.StreamChan:
			if !ok {
				// 文本流关闭（LLM 已输出完）：补最后一条 sentence_end 收尾。
				if !t.enqueueSessionElem(item.ctx, item.generation, AudioQueueElem{Kind: AudioQueueKindSentenceEnd, OnEnd: item.onEndFunc}) && item.onEndFunc != nil {
					item.onEndFunc(nil)
				}
				return item.ctx.Err()
			}
			// 逐条整句合成（降级路径的"流式"只体现在文本按句陆续到达）。
			outChan, release, genErr := t.generateTtsOnly(item.ctx, item.metricCycle, resp)
			if genErr != nil {
				// 首句就失败时也要补一条 sentence_start，让上层钩子/统计能看到"曾经尝试开始"。
				if firstSegment {
					if !t.enqueueSessionElem(item.ctx, item.generation, AudioQueueElem{Kind: AudioQueueKindSentenceStart, OnStart: item.onStartFunc}) {
						if item.onEndFunc != nil {
							item.onEndFunc(item.ctx.Err())
						}
						return item.ctx.Err()
					}
				}
				if !t.enqueueSessionElem(item.ctx, item.generation, AudioQueueElem{Kind: AudioQueueKindSentenceEnd, OnEnd: item.onEndFunc, Err: genErr}) && item.onEndFunc != nil {
					item.onEndFunc(genErr)
				}
				return genErr
			}
			if outChan == nil {
				// provider 认为这条文本无需合成（如纯标点）：释放后继续读下一句。
				if release != nil {
					release()
				}
				continue
			}
			requestActive := true
			firstAudioReported := false
			finishRequest := func(err error) {
				if requestActive {
					t.finishTtsMetricRequest(item.ctx, item.metricCycle, err)
					requestActive = false
				}
			}
			// 每句都发一对 sentence_start/end，端上字幕才能逐句显示。
			startElem := AudioQueueElem{
				Kind:    AudioQueueKindSentenceStart,
				Text:    resp.Text,
				IsStart: resp.IsStart,
			}
			if firstSegment {
				startElem.OnStart = item.onStartFunc
				firstSegment = false
			}
			if !t.enqueueSessionElem(item.ctx, item.generation, startElem) {
				if release != nil {
					release()
				}
				finishRequest(item.ctx.Err())
				if item.onEndFunc != nil {
					item.onEndFunc(item.ctx.Err())
				}
				return item.ctx.Err()
			}
			for {
				select {
				case <-item.ctx.Done():
					if release != nil {
						release()
					}
					finishRequest(item.ctx.Err())
					if item.onEndFunc != nil {
						item.onEndFunc(item.ctx.Err())
					}
					return item.ctx.Err()
				case frame, ok := <-outChan:
					if !ok {
						// 本句音频结束：释放资源、收尾指标，发 sentence_end 后跳到外层继续下一句。
						if release != nil {
							release()
						}
						finishRequest(nil)
						if !t.enqueueSessionElem(item.ctx, item.generation, AudioQueueElem{Kind: AudioQueueKindSentenceEnd, Text: resp.Text}) && item.onEndFunc != nil {
							item.onEndFunc(item.ctx.Err())
						}
						goto nextResp
					}
					if !firstAudioReported {
						t.markTtsMetricFirstAudio(item.ctx, item.metricCycle)
						firstAudioReported = true
					}
					frameCopy := make([]byte, len(frame))
					copy(frameCopy, frame)
					if !t.enqueueSessionElem(item.ctx, item.generation, AudioQueueElem{Kind: AudioQueueKindFrame, Data: frameCopy, MetricCycle: item.metricCycle}) {
						if release != nil {
							release()
						}
						finishRequest(item.ctx.Err())
						if item.onEndFunc != nil {
							item.onEndFunc(item.ctx.Err())
						}
						return item.ctx.Err()
					}
				}
			}
			// nextResp 是"本句已处理完，继续处理下一句文本"的跳转点。
			// 用 goto 而不是把内层循环包成函数，是为了让所有错误分支都能直接 return，
			// 不必额外维护一层闭包状态（代价是标签看起来略突兀）。
		nextResp:
		}
	}
}

// getAlignedDuration 计算当前时间与开始时间的差值，向上对齐到frameDuration
//
// "向上对齐"的含义：把耗时向上取整到帧长的整数倍。
// 因为音频是按整帧发送/播放的，未满一帧的部分实际也占了一帧的时间，
// 向下取整会导致计算出的播放时长偏短（进而过早判定播放结束）。
func getAlignedDuration(startTime time.Time, frameDuration time.Duration) time.Duration {
	elapsed := time.Since(startTime)
	// 向上对齐到frameDuration
	alignedMs := ((elapsed.Milliseconds() + frameDuration.Milliseconds() - 1) / frameDuration.Milliseconds()) * frameDuration.Milliseconds()
	return time.Duration(alignedMs) * time.Millisecond
}

// [5.4] 发音频帧（旧直连路径）：不等统一发送协程，自己按滑动窗口流控发音频。
//
// 与 runSenderLoop 的关系：它服务于旧的直连路径（SendTTSAudio / SendMediaAudio），
// 思路同为"让对端始终缓存 cacheFrameCount 帧"，但用绝对时间基准 startTime 推算每帧发送时刻，
// 而不是维护 playbackTail。新代码优先走 sessionAudioQueue + runSenderLoop，
// 这里保留是为了兼容旧调用方与历史行为。
//
// recordHistory=true 时，每一帧都会额外存一份到 audioHistoryBuffer（用于聊天历史音频）。
func (t *TTSManager) sendAudioStream(ctx context.Context, audioChan <-chan []byte, isStart bool, recordHistory bool) error {
	totalFrames := 0 // 跟踪已发送的总帧数

	isStatistic := true
	//首次发送180ms音频, 根据outputAudioFormat.FrameDuration计算
	cacheFrameCount := 120 / t.clientState.OutputAudioFormat.FrameDuration
	/*if cacheFrameCount > 20 || cacheFrameCount < 3 {
		cacheFrameCount = 5
	}*/

	// 记录开始发送的时间戳
	startTime := time.Now()

	// 基于绝对时间的精确流控
	frameDuration := time.Duration(t.clientState.OutputAudioFormat.FrameDuration) * time.Millisecond

	log.Debugf("SendTTSAudio 开始，缓存帧数: %d, 帧时长: %v", cacheFrameCount, frameDuration)

	// 使用滑动窗口机制，确保对端始终缓存 cacheFrameCount 帧数据
	for {
		// 计算下一帧应该发送的时间点
		// 推导：第 N 帧"应该已经发出"的时刻 = startTime + (N - cacheFrameCount) × 帧长，
		// 也就是说先提前送出 cacheFrameCount 帧（填满对端缓冲），之后按实时节奏发送。
		nextFrameTime := startTime.Add(time.Duration(totalFrames-cacheFrameCount) * frameDuration)
		now := time.Now()

		// 如果下一帧时间还没到，需要等待
		if now.Before(nextFrameTime) {
			sleepDuration := nextFrameTime.Sub(now)
			//log.Debugf("SendTTSAudio 流控等待: %v", sleepDuration)
			time.Sleep(sleepDuration)
		}

		// 尝试获取并发送下一帧
		select {
		case <-ctx.Done():
			log.Debugf("SendTTSAudio context done, exit")
			return nil
		case frame, ok := <-audioChan:
			if !ok {
				// 通道已关闭，所有帧已处理完毕
				// 为确保终端播放完成：等待已发送帧的总时长与从开始发送以来的实际耗时之间的差值
				// 即"已经发出去但还在对端缓冲里没播完"的那部分时间，等完才算真正播完。
				elapsed := time.Since(startTime)
				totalDuration := time.Duration(totalFrames) * frameDuration
				if totalDuration > elapsed {
					waitDuration := totalDuration - elapsed
					log.Debugf("SendTTSAudio 等待客户端播放剩余缓冲: %v (totalFrames=%d, frameDuration=%v)", waitDuration, totalFrames, frameDuration)
					time.Sleep(waitDuration)
				}

				log.Debugf("SendTTSAudio audioChan closed, exit, 总共发送 %d 帧", totalFrames)
				return nil
			}
			// 发送当前帧
			if err := t.serverTransport.SendAudio(frame); err != nil {
				log.Errorf("发送 TTS 音频失败: 第 %d 帧, len: %d, 错误: %v", totalFrames, len(frame), err)
				return fmt.Errorf("发送 TTS 音频 len: %d 失败: %v", len(frame), err)
			}

			if recordHistory {
				// 累积音频数据到历史缓存（每一帧作为独立的[]byte）
				// 注意必须深拷贝：frame 的所有权在上游，可能被复用。
				t.audioMutex.Lock()
				frameCopy := make([]byte, len(frame))
				copy(frameCopy, frame)
				t.audioHistoryBuffer = append(t.audioHistoryBuffer, frameCopy)
				t.audioMutex.Unlock()
			}

			totalFrames++
			if totalFrames%100 == 0 {
				log.Debugf("SendTTSAudio 已发送 %d 帧", totalFrames)
			}

			// 统计信息记录（仅在开始时记录一次）
			if isStart && isStatistic && totalFrames == 1 {
				log.Debugf("从接收音频结束 asr->llm->tts首帧 整体 耗时: %d ms", t.clientState.GetAsrLlmTtsDuration())
				isStatistic = false
			}
		}
	}
}

// SendTTSAudio 旧版"直接发送 TTS 音频"入口：等待媒体独占释放后，按滑动窗口发帧并记录历史音频。
func (t *TTSManager) SendTTSAudio(ctx context.Context, audioChan <-chan []byte, isStart bool) error {
	if err := t.waitForMediaPlaybackRelease(ctx); err != nil {
		return err
	}
	return t.sendAudioStream(ctx, audioChan, isStart, true)
}

// SendMediaAudio 发送媒体音频：不记历史音频（媒体内容不应混进聊天历史回放），也不做首帧统计。
func (t *TTSManager) SendMediaAudio(ctx context.Context, audioChan <-chan []byte) error {
	return t.sendAudioStream(ctx, audioChan, false, false)
}

// ClearAudioHistory 清空TTS音频历史缓存
//
// 使用时机：开始新一轮对话/清理会话时，避免把上一轮的音频错误地附着到新消息上。
func (t *TTSManager) ClearAudioHistory() {
	t.audioMutex.Lock()
	defer t.audioMutex.Unlock()
	t.audioHistoryBuffer = nil
}

// GetAndClearAudioHistory 获取并清空TTS音频历史缓存
//
// 这是「消息两阶段保存」第 2 阶段的取数口：文本先存（第 1 阶段），
// 播报结束后再把累积的音频取走并更新到同一条消息上，取完即清空以便下一轮重新累积。
func (t *TTSManager) GetAndClearAudioHistory() [][]byte {
	t.audioMutex.Lock()
	defer t.audioMutex.Unlock()
	data := t.audioHistoryBuffer
	t.audioHistoryBuffer = nil
	return data
}
