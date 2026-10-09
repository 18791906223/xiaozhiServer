// llm.go —— 中段链路：用户文本 → LLM → （文本给 TTS / 工具调用并发执行）
//
// 文件职责：
//  1. 组装请求：把 system prompt（设备 prompt + 全局 prompt + 时间 + 记忆 + 声纹 prompt + 知识库策略）
//     与历史消息拼成 LLM 请求（GetMessages）；
//  2. 发起请求：从资源池借 LLM provider，拿到流式响应 channel（handleLLMWithContextAndTools）；
//  3. 流变换：响应先经过 streamtransform.Pipeline（插件式“输出流变换”，例如按句子切分）；
//  4. 消费响应：文本喂给 TTS（tts.go），工具调用交给 tool.go 并发执行，工具结果可以再“回问”一轮 LLM；
//  5. 落库：两阶段保存 assistant 消息（先存文本，播完后补音频），用户打断时也会保存半截回复。
//
// 核心类型：LLMManager —— 挂在 ChatSession 上的“经理”，持有 responder 队列（llmResponseQueue）
// 与最近消息 ID（lastMessageID，用于两阶段保存）。
//
// 在整体流程中的位置：
//
//	chatTextQueue → processChatText/actionDoChat → 【本文件: DoLLmRequest → handleLLMResponse】 → TTS → 下行音频
//
// 与其它文件的配合：
//   - session.go：ChatSession 创建本 Manager，决定何时调用 DoLLmRequest（actionDoChat），
//     并提供 hookHub（LLM_INPUT / LLM_OUTPUT_RAW 钩子）、声纹结果、指标回调；
//   - tts.go：本文件把文本片段与“一轮开始/结束”的语义交给 TTSManager；
//   - tool.go：newToolCallExecutor 创建工具执行器，handleToolCallResponse 汇总执行结果；
//   - plugins/（streamtransform）与 domain/llm（provider）：前者变换输出流，后者真正调模型 API；
//   - domain/eventbus：消息保存以事件形式发布（TopicAddMessage），由订阅方写数据库。
//
// 并发模型：LLM 响应在独立 goroutine 里被读取并推入 responseChannel；
// 本文件内部统一用 context 传递“这一轮对话的临时状态”（nest、fullText、TTS 轮次追踪器等），
// 而不是把这些状态挂在结构体上——因为工具调用会产生“嵌套的多轮请求”，每轮的临时状态必须隔离。
package chat

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	. "xiaozhi-esp32-server-golang/internal/data/client"
	chathooks "xiaozhi-esp32-server-golang/internal/domain/chat/hooks"
	"xiaozhi-esp32-server-golang/internal/domain/chat/streamtransform"
	config_types "xiaozhi-esp32-server-golang/internal/domain/config/types"
	"xiaozhi-esp32-server-golang/internal/domain/eventbus"
	"xiaozhi-esp32-server-golang/internal/domain/llm"
	llm_common "xiaozhi-esp32-server-golang/internal/domain/llm/common"
	"xiaozhi-esp32-server-golang/internal/domain/speaker"
	"xiaozhi-esp32-server-golang/internal/pool"
	"xiaozhi-esp32-server-golang/internal/util"
	log "xiaozhi-esp32-server-golang/logger"

	"github.com/cloudwego/eino/schema"
	"github.com/spf13/viper"
)

const (
	// MaxMessageCount 组装历史上下文时最多带多少条消息（太长会让 prompt 变贵且稀释重点）。
	MaxMessageCount = 10

	// McpReadResourcePageSize / McpReadResourceStreamDoneFlag：MCP 资源读取的分页大小与结束标记。
	McpReadResourcePageSize       = 100 * 1024
	McpReadResourceStreamDoneFlag = "[DONE]"
)

// Context key 类型用于避免冲突
// 用自定义类型（而不是 string）做 context key，可以避免与其它包的 key 撞名。
type contextKey int

// 下面这些常量分两类：
//   - ttsPlaybackCompletionGrace：发 tts_stop 前额外等待的保护时间（见 tts.go 的发送循环）；
//   - 其余是 context key。之所以大量用 context 传递“本轮对话的临时状态”，是因为一次用户请求
//     可能触发“工具调用 → 再问 LLM”的嵌套多轮，这些状态必须按“轮”隔离，不能放在 LLMManager 字段上
//     （否则嵌套轮会互相覆盖）：fullText、TTS 轮次追踪器、播放开始钩子、轮次结束策略、策略处理器、
//     “播放已落定”标记，各自解决一个具体问题（详见各自的 withXxx/xxxFromContext 函数注释）。
const (
	ttsPlaybackCompletionGrace time.Duration = 150 * time.Millisecond
	fullTextKey                contextKey    = iota
	toolRoundMessagesKey
	ttsTurnTrackerKey
	ttsPlaybackStartHookKey
	ttsTurnEndPolicyKey
	ttsTurnEndPolicyHandlerKey
	ttsTurnPlaybackSettledKey
)

// Extra 字段名常量：被打断的 assistant 消息会带这些标记一起保存。
// 它们的作用是让“历史回放/下一次请求”知道这条消息是被截断的，从而在拼接上下文时
// 给内容加上“ [用户打断]”后缀（见 decorateInterruptedContent），避免模型把半截话当成完整回复。
const (
	interruptExtraKey      = "interrupt"
	interruptByExtraKey    = "interrupt_by"
	interruptStageExtraKey = "interrupt_stage"
	interruptContentSuffix = " [用户打断]"
)

// GetLastMessageID 获取最近保存的消息的 MessageID（用于两阶段保存）
//
// 两阶段保存的衔接点：第一阶段保存文本时把 MessageID 记到 map（AddMessage 里做），
// 第二阶段（TTS 播完）再按角色取回同一个 ID，用同一 ID 去“更新”那条消息补上音频。
// 加读锁是因为写入发生在 LLM 处理协程，读取发生在 TTS 收尾回调里。
func (l *LLMManager) GetLastMessageID(role string) (string, bool) {
	l.lastMessageIDMu.RLock()
	defer l.lastMessageIDMu.RUnlock()
	id, ok := l.lastMessageID[role]
	return id, ok
}

// LLMResponseChannelItem 是投进 llmResponseQueue 的工作单元：
// “对一个 LLM 响应 channel 做处理”，并带上这一轮的启动/收尾钩子。
// 为什么要排队而不是直接就地处理：processLLMResponseQueue 是单协程串行消费，
// 保证同一个会话的 LLM 响应不会并发处理（TTS/消息保存都要求顺序）。
//
// 字段说明：
//   - ctx：本轮上下文（携带 fullText / nest / TTS 轮次追踪器）；
//   - userMessage：触发本轮的用户消息（主动播报等场景可能是占位消息）；
//   - responseChan：LLM 的流式响应，处理完会被关闭；
//   - onStartFunc：开始处理前调用（发 tts_start、清音频缓存等）；
//   - onEndFunc：处理结束后调用（发 tts_stop、保存音频、打点等），err 与 result 来自处理结果。
type LLMResponseChannelItem struct {
	ctx          context.Context
	userMessage  *schema.Message
	responseChan chan llm_common.LLMResponseStruct
	onStartFunc  func(args ...any)
	onEndFunc    func(err error, args ...any)
}

// llmHandleResult 是 handleLLMResponse 的结果摘要。
//   - ok：本轮是否正常结束（通道关闭/正常收到 IsEnd）；
//   - suppressProtocolTtsStop：是否“抑制协议级 tts_stop”——媒体工具（如播音乐）会自己接管输出，
//     这时不能按常规流程发 tts_stop，否则会把媒体的音频状态切断。
type llmHandleResult struct {
	ok                      bool
	suppressProtocolTtsStop bool
}

// llmHandleResultFromArgs 从 onEndFunc 的变参里取回 llmHandleResult。
// 因为 onEndFunc 的签名是 func(err error, args ...any)（为了能被通用地串联），
// 结果只能以 args 传递，这里做一次安全断言，取不到就返回零值。
func llmHandleResultFromArgs(args []any) llmHandleResult {
	if len(args) == 0 {
		return llmHandleResult{}
	}
	result, ok := args[0].(llmHandleResult)
	if !ok {
		return llmHandleResult{}
	}
	return result
}

// finishTTSTurn 结束一轮 TTS：固定 reason 的便捷封装。
func (l *LLMManager) finishTTSTurn(ctx context.Context, stopErr error, result llmHandleResult) {
	l.finishTTSTurnWithReason(ctx, stopErr, result, "LLMManager.finishTTSTurn")
}

// finishTTSTurnWithReason 通知 TTS 这一轮“没有更多文本了”：
//   - EnqueueTtsStopWithReason：排入 tts_stop（真正的发送由 tts.go 的发送循环按播放尾指针时机发出）；
//   - RequestTurnEnd：告诉轮次追踪器本轮结束（stopErr 会带到等待方，用于区分正常结束与被取消）。
//
// 关于 suppressProtocolTtsStop：被置位时我们仍然走到这里（说明媒体播放已经结束、要回到常规收尾），
// 所以这里只打一条日志、依然发送 tts_stop —— 目的是把客户端从“说话中”状态解救出来。
func (l *LLMManager) finishTTSTurnWithReason(ctx context.Context, stopErr error, result llmHandleResult, reason string) {
	if l == nil || l.ttsManager == nil {
		return
	}

	if result.suppressProtocolTtsStop {
		// 媒体工具会等待播放完成后再回到这里收尾，此时仍需补发协议级 tts_stop，
		// 否则客户端会停留在“说话中”状态。
		log.Debugf("媒体输出已完成，沿用常规 TTS 收尾发送 tts stop")
	}

	l.ttsManager.EnqueueTtsStopWithReason(ctx, reason)
	l.ttsManager.RequestTurnEnd(ctx, stopErr)
}

// llmResponseChannelOptions 描述“把 LLM 响应交给本 Manager 处理”时的可选行为。
// 存在的意义：同一条处理链路要服务多种调用场景（正常对话、主动播报 AddTextToTTSQueue、
// 工具调用后的嵌套请求），各自需要的额外钩子/开关不同，所以用选项对象而不是加一串参数。
//
// 字段说明：
//   - disableTTSCommands：禁发 tts_start/tts_stop（例如媒体播放自己管理输出状态）；
//   - onStartFunc / onEndFunc：调用方附加的开始/结束钩子，会与内部钩子串联执行；
//   - onTTSPlaybackStart：TTS 真正开始播放时的通知（给“等播放开始再继续”的逻辑用）；
//   - ttsTurnEndPolicy：轮次结束后的收尾策略（例如“说再见后进入空闲”）。
type llmResponseChannelOptions struct {
	disableTTSCommands bool
	onStartFunc        func(args ...any)
	onEndFunc          func(err error, args ...any)
	onTTSPlaybackStart func()
	ttsTurnEndPolicy   ttsTurnEndPolicy
}

// ttsPlaybackStartHook 播放开始钩子的函数类型（单独定义是为了能作为 context value 断言与包装）。
type ttsPlaybackStartHook func()

// ttsTurnEndPolicy 一轮 TTS 结束后的收尾策略。
// 目前只用到一个业务场景：助手说了“再见/退出”类内容时，除了正常收尾还要把会话切到空闲态。
type ttsTurnEndPolicy uint8

const (
	ttsTurnEndPolicyNone ttsTurnEndPolicy = iota
	ttsTurnEndPolicyGoodbyeAndIdle
)

// ttsTurnEndPolicyHandler 策略的执行者（由 ChatSession 或上层实现），
// 这样本文件不必知道“再见后要做什么”的具体业务。
type ttsTurnEndPolicyHandler interface {
	handleTTSTurnEndPolicy(ctx context.Context, policy ttsTurnEndPolicy, stopErr error)
}

// withTTSPlaybackStartHook 把“播放开始钩子”放进 context。
//
// 为什么用 sync.Once 包一层：同一个钩子可能被多个路径（首帧、双流式收尾等）触发，
// 但业务上只应生效一次，所以在这里做去重，而不是要求每个调用点自己小心。
func withTTSPlaybackStartHook(ctx context.Context, hook func()) context.Context {
	if ctx == nil || hook == nil {
		return ctx
	}

	var once sync.Once
	return context.WithValue(ctx, ttsPlaybackStartHookKey, ttsPlaybackStartHook(func() {
		once.Do(hook)
	}))
}

// ttsPlaybackStartHookFromContext 读取播放开始钩子；不存在时返回 nil（调用方需判空）。
func ttsPlaybackStartHookFromContext(ctx context.Context) func() {
	if ctx == nil {
		return nil
	}
	hook, ok := ctx.Value(ttsPlaybackStartHookKey).(ttsPlaybackStartHook)
	if !ok || hook == nil {
		return nil
	}
	return func() {
		hook()
	}
}

// withTTSTurnEndPolicy 写入轮次结束策略；None 视为“没设置”，直接返回原 ctx（省一次分配）。
func withTTSTurnEndPolicy(ctx context.Context, policy ttsTurnEndPolicy) context.Context {
	if ctx == nil || policy == ttsTurnEndPolicyNone {
		return ctx
	}
	return context.WithValue(ctx, ttsTurnEndPolicyKey, policy)
}

// ttsTurnEndPolicyFromContext 读取轮次结束策略，默认 None。
func ttsTurnEndPolicyFromContext(ctx context.Context) ttsTurnEndPolicy {
	if ctx == nil {
		return ttsTurnEndPolicyNone
	}
	policy, ok := ctx.Value(ttsTurnEndPolicyKey).(ttsTurnEndPolicy)
	if !ok {
		return ttsTurnEndPolicyNone
	}
	return policy
}

// withTTSTurnEndPolicyHandler 写入策略执行者（谁来实现“再见后切空闲”）。
func withTTSTurnEndPolicyHandler(ctx context.Context, handler ttsTurnEndPolicyHandler) context.Context {
	if ctx == nil || handler == nil {
		return ctx
	}
	return context.WithValue(ctx, ttsTurnEndPolicyHandlerKey, handler)
}

// withTTSTurnPlaybackSettled 标记“本轮播放已经落定”。
// 用途：收尾逻辑需要区分“只是文本发完了”和“客户端确实播完了”，
// 后者才允许执行某些幂等收口动作（避免重复触发）。
func withTTSTurnPlaybackSettled(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, ttsTurnPlaybackSettledKey, true)
}

// ttsTurnPlaybackSettledFromContext 读取“播放已落定”标记，未设置时返回 false。
func ttsTurnPlaybackSettledFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	settled, ok := ctx.Value(ttsTurnPlaybackSettledKey).(bool)
	return ok && settled
}

// ttsTurnEndPolicyHandlerFromContext 读取策略执行者，未设置时返回 nil。
func ttsTurnEndPolicyHandlerFromContext(ctx context.Context) ttsTurnEndPolicyHandler {
	if ctx == nil {
		return nil
	}
	handler, ok := ctx.Value(ttsTurnEndPolicyHandlerKey).(ttsTurnEndPolicyHandler)
	if !ok {
		return nil
	}
	return handler
}

// ttsTurnTracker 追踪“本轮送进 TTS 的条目是否全部处理完”。
//
// 解决什么问题：LLM 的文本是一段段推给 TTS 的，而 TTS 在自己的队列/协程里异步生成音频。
// 如果 LLM 这边不等 TTS 出完队就继续往下走（例如立刻发起工具调用后的下一轮请求、或者立刻
// 做消息收尾），就会出现“上一句还在合成，下一轮的音频已经插进来”的乱序。
// 所以用计数器 + 关闭通道的方式提供一次“等本轮 TTS 排空”的能力：
//   - Add() 每入队一个 TTS 条目就 +1，并返回一个“完成回调”，条目被处理完时调用它（-1）；
//   - Wait() 阻塞到 pending 归零（或 ctx 取消）。
//
// 注意“轮”的边界由 context 承载（见 ensureTTSTurnTrackerInContext / waitForTTSTurnDrainIfRoot），
// 嵌套的工具轮（nest>1）会复用同一个 tracker，但只有最外层才等待。
//
// 字段说明：
//   - pending：当前未完成的 TTS 条目数；
//   - doneCh：pending 归零时被 close，用来唤醒正在 Wait 的协程。
type ttsTurnTracker struct {
	mu      sync.Mutex
	pending int
	doneCh  chan struct{}
}

// newTTSTurnTracker 创建一个初始“已完成”状态的 tracker。
// doneCh 预先 close，是为了让“没有任何条目时的 Wait”直接返回，而不用特判。
func newTTSTurnTracker() *ttsTurnTracker {
	doneCh := make(chan struct{})
	close(doneCh)
	return &ttsTurnTracker{doneCh: doneCh}
}

// Add 登记一个 TTS 条目，返回其“完成回调”。
//
// 细节：
//   - 只有当 pending 从 0 变 1 时才新建 doneCh：保证多批条目可以共用一个“本轮结束”信号，
//     也让 Wait 每次等待的都是“当前这批”。
//   - 返回的回调被 sync.Once 保护：同一个条目可能被多条路径收尾（正常/取消/错误），
//     只允许计数一次，否则 pending 会变成负数、doneCh 永远不会关闭。
func (t *ttsTurnTracker) Add() func(error) {
	if t == nil {
		return func(error) {}
	}

	t.mu.Lock()
	if t.pending == 0 {
		t.doneCh = make(chan struct{})
	}
	t.pending++
	t.mu.Unlock()

	var once sync.Once
	return func(error) {
		once.Do(func() {
			t.mu.Lock()
			defer t.mu.Unlock()
			if t.pending == 0 {
				return
			}
			t.pending--
			if t.pending == 0 {
				close(t.doneCh)
			}
		})
	}
}

// Wait 等待本轮所有已登记的 TTS 条目完成。
// 返回 ctx.Err() 表示等待被取消（此时不保证 TTS 已经排空，调用方需自行处理被打断的情况）。
func (t *ttsTurnTracker) Wait(ctx context.Context) error {
	if t == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	t.mu.Lock()
	pending := t.pending
	doneCh := t.doneCh
	t.mu.Unlock()

	// 已经排空（或从未 Add 过）时立即返回，避免无意义地等一个已关闭的 channel
	// （虽然读已关闭 channel 不会阻塞，但提前返回语义更清晰）。
	if pending == 0 {
		return nil
	}

	select {
	case <-doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// LLMManager 管理单个会话的 LLM 请求与响应处理。
//
// 生命周期：与 ChatSession 同生命周期；随会话创建而创建，随会话关闭而停止消费（queue 的 ctx）。
// 并发：llmResponseQueue 本身并发安全（util.Queue）；lastMessageID 用读写锁保护；
// 其余字段是创建后不变的只读引用。
//
// 字段说明：
//   - clientState：客户端状态（会话/设备配置、消息历史、指标时间戳）；
//   - session：所属会话（提供 hookHub、声纹、收尾策略等能力）；
//   - serverTransport：协议消息发送封装（留给直发场景/扩展）；
//   - ttsManager：下游，把文本片段变成音频并发送；
//   - transformRegistry：LLM 输出流变换插件的注册表；
//   - einoTools：本次请求可用的工具（Eino 格式），由 actionDoChat 组装后传入；
//   - llmResponseQueue：待处理的 LLM 响应，单协程串行消费。
type LLMManager struct {
	clientState       *ClientState
	session           *ChatSession
	serverTransport   *ServerTransport
	ttsManager        *TTSManager
	transformRegistry *streamtransform.Registry

	einoTools []*schema.ToolInfo

	llmResponseQueue *util.Queue[LLMResponseChannelItem]

	// 存储最近保存的消息的 MessageID（用于两阶段保存）
	// key: role (user/assistant), value: MessageID
	lastMessageID   map[string]string
	lastMessageIDMu sync.RWMutex // 保护 lastMessageID 的并发访问
}

// NewLLMManager 创建管理器。队列容量 10：
// 满时 Push 会失败（调用方丢弃该响应），这是刻意的——积压的 LLM 响应早已过期，
// 与其阻塞生产者不如快速失败，避免把 LLM goroutine 卡死。
func NewLLMManager(clientState *ClientState, serverTransport *ServerTransport, ttsManager *TTSManager, session *ChatSession, transformRegistry *streamtransform.Registry) *LLMManager {
	return &LLMManager{
		clientState:       clientState,
		session:           session,
		serverTransport:   serverTransport,
		ttsManager:        ttsManager,
		transformRegistry: transformRegistry,
		llmResponseQueue:  util.NewQueue[LLMResponseChannelItem](10),
		lastMessageID:     make(map[string]string),
	}
}

// openOutputPipeline 为本次 LLM 请求打开一条“输出流变换管线”。
//
// 为什么要有这层：像“把连续文本切成适合播报的句子”“过滤 markdown 标记”这类逻辑与具体 LLM 无关，
// 拆成插件后可以按设备配置开关，主流程只负责把 LLM 的原始片段 Push 进去、取出变换后的 Item。
// 没有注册表时返回空 Pipeline（Push 直接原样返回输入），保证主流程永远可用。
//
// RequestID 用 sessionID + 纳秒时间戳，保证同会话的每次请求都有唯一标识（插件可用于日志关联）。
func (l *LLMManager) openOutputPipeline(ctx context.Context) (*streamtransform.Pipeline, error) {
	if l == nil || l.transformRegistry == nil {
		return &streamtransform.Pipeline{}, nil
	}

	sessionID := ""
	deviceID := ""
	if l.clientState != nil {
		sessionID = l.clientState.SessionID
		deviceID = l.clientState.DeviceID
	}

	return l.transformRegistry.Open(streamtransform.Context{
		Ctx:       ctx,
		SessionID: sessionID,
		DeviceID:  deviceID,
		RequestID: fmt.Sprintf("%s-%d", sessionID, time.Now().UnixNano()),
	})
}

// emitLLMOutputRaw 触发 LLM_OUTPUT_RAW 钩子：在“流变换之前”把 LLM 的原始片段给上层看一眼。
// 上层可以改写 Delta/工具调用，也可以返回 stop 要求终止本轮（例如命中敏感内容直接截断）。
// 返回 (改写后的数据, 是否停止, 错误)。
func (l *LLMManager) emitLLMOutputRaw(ctx context.Context, data chathooks.LLMOutputRawData) (chathooks.LLMOutputRawData, bool, error) {
	if l == nil || l.session == nil || l.session.hookHub == nil {
		return data, false, nil
	}
	return l.session.hookHub.EmitLLMOutputRaw(l.session.hookContext(ctx), data)
}

// handleLLMWithContextAndTools 使用上下文控制来处理LLM响应（兼容带工具和不带工具）
// 内部自动管理 LLM 资源的获取和释放
//
// 数据流（本方法是“发起 + 变换”阶段，返回的 channel 由 handleLLMResponse 消费）：
//
//	pool.Acquire("llm") → provider.ResponseWithContext(流式) → msgChan
//	msgChan → LLM_OUTPUT_RAW 钩子 → streamtransform.Pipeline.Push → responseChannel
//
// 参数：
//
//	ctx      —— 本轮上下文（取消即停止读取上游，并释放资源）；
//	dialogue —— 组装好的消息列表（system + 历史 + 当前用户输入）；
//	tools    —— 可用工具定义。
//
// 返回：responseChannel（调用方消费；它会在 goroutine 结束时被 close）。
// 资源释放：由内部 goroutine 的 defer 完成，所以调用方不需要（也不应该）再释放。
func (l *LLMManager) handleLLMWithContextAndTools(
	ctx context.Context,
	dialogue []*schema.Message,
	tools []*schema.ToolInfo,
) (chan llm_common.LLMResponseStruct, error) {
	// 获取 LLM 资源
	// 和 ASR 一样走资源池：LLM provider 可能持有 HTTP 连接/鉴权状态，复用比每次新建便宜得多。
	llmWrapper, err := pool.Acquire[llm.LLMProvider](
		"llm",
		l.clientState.DeviceConfig.Llm.Provider,
		l.clientState.DeviceConfig.Llm.Config,
	)
	if err != nil {
		return nil, fmt.Errorf("获取LLM资源失败: %w", err)
	}

	// 获取 provider
	llmProvider := llmWrapper.GetProvider()

	// [3.3] 请求 LLM：调用 LLM provider
	// ResponseWithContext 立即返回一个流式 channel，模型输出会持续写入；
	// 这里把 sessionID 传下去是为了让 provider 侧能按会话做日志/统计（或复用会话级连接）。
	msgChan := llmProvider.ResponseWithContext(ctx, l.clientState.SessionID, dialogue, tools)

	// 管线创建失败要立刻归还资源，否则会泄漏一个池化实例。
	pipeline, err := l.openOutputPipeline(ctx)
	if err != nil {
		pool.Release(llmWrapper)
		return nil, fmt.Errorf("创建LLM输出流变换管线失败: %w", err)
	}

	// 创建响应 channel
	// 容量只有 2：下游（工具执行/TTS）处理不过来时应该让上游阻塞，形成天然背压，
	// 而不是把几百个分片堆在内存里。
	responseChannel := make(chan llm_common.LLMResponseStruct, 2)
	startTs := time.Now().UnixMilli()
	// firstSegment 只用于“首句耗时”指标（只统计第一次出现非空文本的时刻）。
	var firstSegment bool
	// rawFullText 记录“钩子和管线处理之前”的原始全文，仅用于日志排查（真正用于落库的全文在 context 的 fullTextKey 里）。
	var rawFullText strings.Builder

	// 启动 goroutine 处理响应
	go func() {
		// 一个 defer 负责三件事，顺序不能乱：
		// 1) close(responseChannel) 让消费方知道流结束；
		// 2) 关闭管线（释放插件资源）；
		// 3) 归还 pool 资源。
		// 即使中途因为 ctx 取消/钩子要求停止而 return，也一定会走到这里——这是资源不泄漏的保证。
		defer func() {
			log.Debugf("full Response with %d tools, fullText: %s", len(tools), rawFullText.String())
			close(responseChannel)
			if closeErr := pipeline.Close(); closeErr != nil {
				log.Warnf("关闭 LLM 输出流变换管线失败: %v", closeErr)
			}
			// 释放资源
			pool.Release(llmWrapper)
			log.Debugf("LLM资源已释放")
		}()

		isFirstOutput := true
		llmFirstTokenMarked := false

		// emitResponse 做“最后一个环节”：把管线产出的 Item 翻译成下游认识的
		// LLMResponseStruct 并写进 responseChannel。返回值 false 表示“应当停止整个处理”
		// （ctx 已取消），true 表示继续。
		//
		// 这里同时承担两项指标/语义工作：
		//   - IsStart：标记“这是本轮的第一个有效输出”，下游（TTS）用它决定何时清缓存、发 tts_start；
		//   - 首句耗时：第一次出现非空文本时打点，用于观测 LLM 的响应速度。
		emitResponse := func(item streamtransform.Item) bool {
			response := llm_common.LLMResponseStruct{
				IsEnd: item.IsEnd,
			}

			switch item.Kind {
			case streamtransform.ItemKindToolCalls:
				response.ToolCalls = item.ToolCalls
				if len(item.ToolCalls) > 0 {
					response.IsStart = isFirstOutput
				}
			case streamtransform.ItemKindTextDelta, streamtransform.ItemKindTextSegment:
				response.Text = item.Text
				if strings.TrimSpace(item.Text) != "" {
					response.IsStart = isFirstOutput
					if !firstSegment {
						firstSegment = true
						// MarkLlmFirstSentenceAt 返回 true 表示“这是首次打点”，避免指标被重复记录。
						firstSentenceTs := time.Now().UnixMilli()
						if l.clientState.MarkLlmFirstSentenceAt(firstSentenceTs) && l.session != nil {
							l.session.TraceLlmFirstSentence(ctx, firstSentenceTs)
						}
						log.Infof("耗时统计: llm首句: %d ms", firstSentenceTs-startTs)
					}
					if isFirstOutput {
						isFirstOutput = false
					}
				}
			default:
				// 不认识的 Item 类型直接忽略（保持前向兼容：插件新增类型不会打死主流程）。
				return true
			}

			// 过滤“没有内容的空包”：既无文本也无工具调用、又不是结束标记的分片没有传递价值。
			if strings.TrimSpace(response.Text) == "" && len(response.ToolCalls) == 0 && !response.IsEnd {
				return true
			}

			// 写 channel 时同时监听 ctx：下游卡住时能靠取消及时退出，避免 goroutine 泄漏。
			select {
			case <-ctx.Done():
				log.Infof("上下文已取消，停止LLM响应处理: %v, context done, exit", ctx.Err())
				return false
			case responseChannel <- response:
				return true
			}
		}

		// pushToPipeline 把原始 Item 交给管线，并把管线产出的所有子 Item 依次交给 emitResponse。
		// 返回的 stop 来自插件（例如 output_segmenter 在收到结束标记后要求收口）。
		pushToPipeline := func(item streamtransform.Item) (bool, error) {
			items, stop, err := pipeline.Push(item)
			if err != nil {
				return false, err
			}
			for _, out := range items {
				if !emitResponse(out) {
					return true, nil
				}
			}
			return stop, nil
		}

		// pushRawText 处理“文本分片”路径：先过 LLM_OUTPUT_RAW 钩子（可改写/拦截），
		// 再作为 TextDelta 进入管线。rawFullText 累积的是钩子改写后的文本，用于排障日志。
		pushRawText := func(delta string, isEnd bool, errVal error) (bool, error) {
			payload, stop, hookErr := l.emitLLMOutputRaw(ctx, chathooks.LLMOutputRawData{
				Delta:    delta,
				FullText: rawFullText.String(),
				IsEnd:    isEnd,
				Err:      errVal,
			})
			if hookErr != nil {
				log.Warnf("LLM_OUTPUT_RAW hook 执行失败: %v", hookErr)
			}
			if stop {
				log.Infof("LLM_OUTPUT_RAW hook 请求停止当前流程")
				return true, nil
			}
			if payload.Delta != "" {
				rawFullText.WriteString(payload.Delta)
			}
			return pushToPipeline(streamtransform.Item{
				Kind:  streamtransform.ItemKindTextDelta,
				Text:  payload.Delta,
				IsEnd: payload.IsEnd,
			})
		}

		// pushRawToolCalls 处理“工具调用”路径：同样先过钩子（可增删/改写工具调用），
		// 钩子把工具调用清空时视为“不要执行工具”，直接返回不推管线。
		pushRawToolCalls := func(toolCalls []schema.ToolCall) (bool, error) {
			payload, stop, hookErr := l.emitLLMOutputRaw(ctx, chathooks.LLMOutputRawData{
				FullText:  rawFullText.String(),
				ToolCalls: toolCalls,
			})
			if hookErr != nil {
				log.Warnf("LLM_OUTPUT_RAW hook 执行失败: %v", hookErr)
			}
			if stop {
				log.Infof("LLM_OUTPUT_RAW hook 请求停止当前流程")
				return true, nil
			}
			if len(payload.ToolCalls) == 0 {
				return false, nil
			}
			return pushToPipeline(streamtransform.Item{
				Kind:      streamtransform.ItemKindToolCalls,
				ToolCalls: payload.ToolCalls,
			})
		}

		// 主读取循环：把 provider 的流式输出逐条“翻译”成管线输入。
		// 一个 message 可能同时带文本和工具调用（有些模型会先说话再调工具），所以两段 if 是并列的而不是 else。
		for {
			select {
			case <-ctx.Done():
				log.Infof("上下文已取消，停止LLM响应处理: %v, context done, exit", ctx.Err())
				return
			case message, ok := <-msgChan:
				if !ok {
					// 上游正常关闭：补一个 isEnd=true 的空文本，让管线有机会 flush()
					// （插件可能把最后不足一句的残留文本吐出来），下游也能收到结束信号。
					stop, pushErr := pushRawText("", true, nil)
					if pushErr != nil {
						log.Errorf("处理 LLM 结束流失败: %v", pushErr)
					}
					if stop || pushErr != nil {
						return
					}
					return
				}
				if message == nil {
					continue
				}
				// provider 用一个特殊消息承载错误：按普通文本推给下游并把 IsEnd 置真，
				// 这样 TTS 能收到结束信号、收尾逻辑不被卡住（错误内容也会被播报/记录，便于发现）。
				if llm.IsLLMErrorMessage(message) {
					errMsg := llm.LLMErrorMessage(message)
					log.Warnf("LLM 返回错误: %s", errMsg)
					stop, pushErr := pushRawText(errMsg, true, nil)
					if pushErr != nil {
						log.Errorf("处理 LLM 错误输出失败: %v", pushErr)
					}
					if stop || pushErr != nil {
						return
					}
					return
				}
				if message.Content != "" {
					// 首 token 指标：只打点一次（MarkLlmFirstToken 内部保证幂等）。
					if !llmFirstTokenMarked {
						firstTokenTs := time.Now().UnixMilli()
						l.clientState.MarkLlmFirstToken()
						if l.session != nil {
							l.session.TraceLlmFirstToken(ctx, firstTokenTs)
						}
						llmFirstTokenMarked = true
					}
					stop, pushErr := pushRawText(message.Content, false, nil)
					if pushErr != nil {
						log.Errorf("处理 LLM 文本流失败: %v", pushErr)
						return
					}
					if stop {
						return
					}
				}
				if len(message.ToolCalls) > 0 {
					log.Infof("处理工具调用: %+v", message.ToolCalls)
					stop, pushErr := pushRawToolCalls(message.ToolCalls)
					if pushErr != nil {
						log.Errorf("处理 LLM 工具流失败: %v", pushErr)
						return
					}
					if stop {
						return
					}
				}
			}
		}
	}()

	return responseChannel, nil
}

// Start 启动 LLM 响应消费者。
// 注意它是同步调用（内部是死循环）：调用方（ChatSession）需要自己 `go l.llmManager.Start(ctx)`。
func (l *LLMManager) Start(ctx context.Context) {
	l.processLLMResponseQueue(ctx)
}

// [3.4] 消费响应：单协程串行消费 llmResponseQueue（保证轮次与协议消息的顺序）。
//
// processLLMResponseQueue 单协程串行消费响应队列。
//
// 为什么必须串行：TTS 的文本/音频顺序、消息落库的顺序、协议消息（tts_start/stop）的顺序
// 都依赖“一轮处理完再处理下一轮”。如果并发消费，工具轮与主轮的输出可能交错。
//
// 一轮的完整处理：onStartFunc（发 tts_start/清缓存）→ handleLLMResponse（消费流）→
// 等 TTS 排空（仅最外层）→ onEndFunc（发 tts_stop/保存音频）。
func (l *LLMManager) processLLMResponseQueue(ctx context.Context) {
	for {
		// [3.4] 取队列：阻塞等待下一条待处理响应
		item, err := l.llmResponseQueue.Pop(ctx, 0) // 阻塞式
		if err != nil {
			if err == util.ErrQueueCtxDone {
				// ctx 结束（会话关闭）：正常退出。
				return
			}
			// 其他错误
			continue
		}

		log.Debugf("processLLMResponseQueue item: %+v", item)
		if item.onStartFunc != nil {
			item.onStartFunc()
		}

		// 调用 handleLLMResponse，它会从 context 中获取 fullText 和 toolCalls 并填充
		result, err := l.handleLLMResponse(item.ctx, item.userMessage, item.responseChan)
		// 只有最外层（nest<=1）才等 TTS 排空：工具轮（nest>1）复用同一批 TTS 资源，
		// 在这里等会导致死等（工具轮的音频要等最外层统一收尾）。
		if waitErr := waitForTTSTurnDrainIfRoot(item.ctx); err == nil && waitErr != nil {
			err = waitErr
		}

		if item.onEndFunc != nil {
			item.onEndFunc(err, result)
		}
	}
}

// ClearLLMResponseQueue 清空待处理的 LLM 响应。
// 打断/停止说话时调用：已经无意义的在途响应不必再喂给 TTS。
func (l *LLMManager) ClearLLMResponseQueue() {
	l.llmResponseQueue.Clear()
}

// AddTextToTTSQueue 直接把一段文本送去播报（不经过 LLM）。
// 典型用途：主动播报（speak_request）、系统提示音、暖场话术。
func (l *LLMManager) AddTextToTTSQueue(text string) error {
	return l.AddTextToTTSQueueWithOptions(text, llmResponseChannelOptions{})
}

// AddTextToTTSQueueWithOptions 带选项版本的“直接播报”。
//
// 实现技巧：构造一个“只有一条文本 + IsStart/IsEnd 都为真”的假 LLM 响应 channel，
// 然后复用正常的 HandleLLMResponseChannelAsync 链路——
// 这样 TTS 启动、音频落库、tts_start/stop 收尾全都自动走同一条代码路径，不需要为播报单独写一套。
// msg 的角色写成 User 只是占位（此路径不会真正落用户消息）。
func (l *LLMManager) AddTextToTTSQueueWithOptions(text string, options llmResponseChannelOptions) error {
	log.Debugf("AddTextToTTSQueue text: %s", text)
	msg := &schema.Message{
		Role:    schema.User,
		Content: text,
	}
	// 容量 10 足够放下这两条（一条数据 + close 后的读取），不会阻塞。
	llmResponseChan := make(chan llm_common.LLMResponseStruct, 10)
	llmResponseChan <- llm_common.LLMResponseStruct{
		IsStart: true,
		IsEnd:   true,
		Text:    text,
	}
	close(llmResponseChan)

	// 取“本轮对话上下文”（SessionCtx 派生），而不是连接级 ctx：
	// 这样播报也能被正常的打断流程（cancel 轮次上下文）掐掉。
	sessionCtx := l.clientState.SessionCtx.Get(l.clientState.Ctx)
	ctx := l.clientState.AfterAsrSessionCtx.Get(sessionCtx)
	ctx = withTTSPlaybackStartHook(ctx, options.onTTSPlaybackStart)
	ctx = withTTSTurnEndPolicy(ctx, options.ttsTurnEndPolicy)
	if err := l.HandleLLMResponseChannelAsyncWithOptions(ctx, msg, llmResponseChan, options); err != nil {
		log.Warnf("AddTextToTTSQueue enqueue failed: %v", err)
		return err
	}

	return nil
}

// chainLLMResponseStartHooks 把多个“开始钩子”串成一个。
// 为什么要串联：内部逻辑（发 tts_start、清缓存）和调用方传入的额外钩子都要执行，
// 且不应互相覆盖，所以按顺序调用；nil 钩子直接过滤掉（简化调用点的判空）。
func chainLLMResponseStartHooks(hooks ...func(args ...any)) func(args ...any) {
	filtered := make([]func(args ...any), 0, len(hooks))
	for _, hook := range hooks {
		if hook != nil {
			filtered = append(filtered, hook)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return func(args ...any) {
		for _, hook := range filtered {
			hook(args...)
		}
	}
}

// chainLLMResponseEndHooks 同 chainLLMResponseStartHooks，用于“结束钩子”。
// 串行执行保证“内部收尾（tts_stop/落库）先做，外部钩子后做”，顺序对调用方是可预期的。
func chainLLMResponseEndHooks(hooks ...func(err error, args ...any)) func(err error, args ...any) {
	filtered := make([]func(err error, args ...any), 0, len(hooks))
	for _, hook := range hooks {
		if hook != nil {
			filtered = append(filtered, hook)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return func(err error, args ...any) {
		for _, hook := range filtered {
			hook(err, args...)
		}
	}
}

// HandleLLMResponseChannelAsync 异步处理一个 LLM 响应：入队后立即返回，
// 实际处理由 processLLMResponseQueue 在另一个协程串行完成。
func (l *LLMManager) HandleLLMResponseChannelAsync(ctx context.Context, userMessage *schema.Message, responseChan chan llm_common.LLMResponseStruct) error {
	return l.handleLLMResponseChannelAsync(ctx, userMessage, responseChan, llmResponseChannelOptions{})
}

// HandleLLMResponseChannelAsyncWithOptions 带选项版本（额外钩子、是否发 TTS 命令等）。
func (l *LLMManager) HandleLLMResponseChannelAsyncWithOptions(ctx context.Context, userMessage *schema.Message, responseChan chan llm_common.LLMResponseStruct, options llmResponseChannelOptions) error {
	return l.handleLLMResponseChannelAsync(ctx, userMessage, responseChan, options)
}

// handleLLMResponseChannelAsync 异步链路的核心：准备上下文状态、构造开始/结束钩子、入队。
//
// 这里最关键的三个 context 值：
//   - "nest"：嵌套层级。最外层是 1，工具调用后发起的下一轮被置为 2。
//     只有 nest<=1 才发 tts_start/tts_stop、才清 TTS 音频缓存、才做第二阶段落库——
//     目的是保证“一轮对话只对外表现成一个 TTS 会话”，嵌套轮只是这一轮内部的追加内容。
//   - fullTextKey：本轮累积的 assistant 全文。嵌套轮复用同一个 builder，
//     所以最终落库的是一整段完整回复，而不是碎片。
//   - ttsTurnTrackerKey：本轮 TTS 条目计数（用于等排空）。
func (l *LLMManager) handleLLMResponseChannelAsync(ctx context.Context, userMessage *schema.Message, responseChan chan llm_common.LLMResponseStruct, options llmResponseChannelOptions) error {
	ctx = ensureTTSTurnTrackerInContext(ctx)
	ctx = withTTSPlaybackStartHook(ctx, options.onTTSPlaybackStart)
	ctx = withTTSTurnEndPolicy(ctx, options.ttsTurnEndPolicy)

	// needSendTtsCmd：是否由这一轮负责发协议级 tts_start/tts_stop。
	// 规则：nest>1（工具调用的追加轮）不发，由最外层统一收发；
	// 或者调用方显式要求禁用（例如媒体播放自己管理状态）。
	needSendTtsCmd := true
	val := ctx.Value("nest")
	nest := 0
	log.Debugf("AddLLMResponseChannel nest: %+v", val)
	if n, ok := val.(int); ok {
		nest = n
		if nest > 1 {
			needSendTtsCmd = false
		}
	}
	if options.disableTTSCommands {
		needSendTtsCmd = false
	}

	// 在 context 中初始化或复用 fullText（用于聊天历史）
	// 如果 context 中已有 fullText（工具调用后继续LLM请求），则复用；否则创建新的
	// 注意必须通过 context 传递而不是用 LLMManager 字段：嵌套轮的读取是并发的，
	// 放在字段上会互相覆盖。
	var fullText *strings.Builder
	if existingFullText, ok := ctx.Value(fullTextKey).(*strings.Builder); ok && existingFullText != nil {
		fullText = existingFullText
		log.Debugf("复用已有的 fullText，当前长度: %d", fullText.Len())
	} else {
		fullText = &strings.Builder{}
		ctx = context.WithValue(ctx, fullTextKey, fullText)
		log.Debugf("创建新的 fullText")
	}

	var onStartFunc func(...any)
	var onEndFunc func(err error, args ...any)

	// 只有“负责发 TTS 命令”的那一轮才注册内部开始/结束钩子（否则嵌套轮会重复收发协议消息）。
	if needSendTtsCmd {
		onStartFunc = func(...any) {
			// 判断是否为首次LLM调用（通过context的nest值），仅首次调用时清空TTS音频缓存
			// 为什么要清缓存：音频缓存是“本轮要落库的音频”，新一轮开始时必须先清掉上一轮的残留，
			// 否则会把上一条回复的音频拼到这一条消息上。
			val := ctx.Value("nest")
			if nest, ok := val.(int); !ok || nest <= 1 {
				// 首次调用或没有nest值，清空TTS音频缓存
				l.ttsManager.ClearAudioHistory()
				log.Debugf("onStartFunc 首次调用，已清空TTS音频缓存")
			}
			l.ttsManager.EnqueueTtsStartWithReason(ctx, "LLMManager.handleLLMResponseChannelAsync onStart")
		}
		onEndFunc = func(err error, args ...any) {
			// handleResult 由 handleLLMResponse 通过 args 带出来（suppressProtocolTtsStop 等）。
			handleResult := llmHandleResultFromArgs(args)
			l.clientState.MarkLlmEnd()
			if l.session != nil {
				l.session.TraceLlmEnd(ctx, time.Now().UnixMilli(), err)
			}
			strFullText := fullText.String()

			l.finishTTSTurnWithReason(ctx, err, handleResult, "LLMManager.handleLLMResponseChannelAsync onEnd")

			// 从 closure 中获取 fullText
			// 收尾时才取音频：GetAndClearAudioHistory 会把本轮累积的音频帧取走并清空，
			// 保证同一条消息的音频只被取一次。
			audioData := l.ttsManager.GetAndClearAudioHistory()

			// 计算总音频大小（所有帧的字节数之和）
			audioSize := 0
			for _, frame := range audioData {
				audioSize += len(frame)
			}

			// 只有在首次调用（nest<=1）时才发送事件
			// 这是“两阶段保存”的第二阶段：用第一阶段拿到的 MessageID 去更新同一条消息，补上音频。
			if nest <= 1 {
				// 从 LLMManager 中获取 MessageID（Assistant 角色）
				// 如果没有找到 MessageID，说明第一阶段保存未完成，不进行第二阶段更新
				messageID, ok := l.GetLastMessageID(string(schema.Assistant))
				if !ok {
					log.Warnf("TTS 完成时未找到 MessageID，跳过第二阶段音频更新")
					return
				}

				// 发布事件：第二阶段（更新音频）
				assistantMsg := schema.AssistantMessage(strFullText, nil)
				eventbus.Get().Publish(eventbus.TopicAddMessage, &eventbus.AddMessageEvent{
					ClientState: l.clientState,
					Msg:         *assistantMsg,
					MessageID:   messageID,
					AudioData:   audioData, // 第二阶段：有音频
					AudioSize:   audioSize,
					SampleRate:  l.clientState.OutputAudioFormat.SampleRate,
					Channels:    l.clientState.OutputAudioFormat.Channels,
					Timestamp:   time.Now(),
					IsUpdate:    true, // 更新消息
				})
			}
		}
	}

	onStartFunc = chainLLMResponseStartHooks(onStartFunc, options.onStartFunc)
	onEndFunc = chainLLMResponseEndHooks(onEndFunc, options.onEndFunc)

	item := LLMResponseChannelItem{
		ctx:          ctx,
		userMessage:  userMessage,
		responseChan: responseChan,
		onStartFunc:  onStartFunc,
		onEndFunc:    onEndFunc,
	}

	// [3.3] 入队：队列满了直接失败并让调用方丢弃：丢一条过期的回复远好于阻塞整个 LLM 链路。
	err := l.llmResponseQueue.Push(item)
	if err != nil {
		log.Warnf("llmResponseQueue 已满或已关闭, 丢弃消息")
		return fmt.Errorf("llmResponseQueue 已满或已关闭, 丢弃消息")
	}
	return nil
}

// HandleLLMResponseChannelSync 同步处理一个 LLM 响应：调用方原地等待本轮处理完毕。
//
// 与 Async 版本的区别：
//   - 不经过 llmResponseQueue，直接调 handleLLMResponse，因此调用方会被阻塞到本轮结束；
//   - 开始/结束逻辑（tts_start、落库第二阶段）本函数内联完成，而不是交给队列项的钩子执行。
//
// 使用场景：需要“拿到本轮最终结果再继续”的链路（如 isSync=true 的请求、工具调用后的追加轮）。
// 返回值：ok 表示本轮是否正常结束；err 表示处理过程中出现的错误。
func (l *LLMManager) HandleLLMResponseChannelSync(ctx context.Context, userMessage *schema.Message, llmResponseChannel chan llm_common.LLMResponseStruct, einoTools []*schema.ToolInfo) (bool, error) {
	ctx = ensureTTSTurnTrackerInContext(ctx)

	// 与 Async 版一致：nest>1 的追加轮不负责发协议级 TTS 命令，避免一轮对话里 tts 状态反复重置。
	needSendTtsCmd := true
	val := ctx.Value("nest")
	nest := 0
	log.Debugf("AddLLMResponseChannel nest: %+v", val)
	if n, ok := val.(int); ok {
		nest = n
		if nest > 1 {
			needSendTtsCmd = false
		}
	}

	// 在 context 中初始化或复用 fullText（用于聊天历史）
	// 如果 context 中已有 fullText（工具调用后继续LLM请求），则复用；否则创建新的
	var fullText *strings.Builder
	if existingFullText, ok := ctx.Value(fullTextKey).(*strings.Builder); ok && existingFullText != nil {
		fullText = existingFullText
		log.Debugf("复用已有的 fullText，当前长度: %d", fullText.Len())
	} else {
		fullText = &strings.Builder{}
		ctx = context.WithValue(ctx, fullTextKey, fullText)
		log.Debugf("创建新的 fullText")
	}

	if needSendTtsCmd {
		// 判断是否为首次LLM调用（通过context的nest值），仅首次调用时清空TTS音频缓存
		if nest <= 1 {
			// 首次调用或没有nest值，清空TTS音频缓存
			l.ttsManager.ClearAudioHistory()
			log.Debugf("HandleLLMResponseChannelSync 首次调用，已清空TTS音频缓存")
		}
		l.ttsManager.EnqueueTtsStartWithReason(ctx, "LLMManager.HandleLLMResponseChannelSync start")
	}

	// 同步消费整条响应流（内部可能触发工具执行、甚至再次嵌套调用本链路）。
	result, err := l.handleLLMResponse(ctx, userMessage, llmResponseChannel)
	// 等本轮 TTS 真正排空再收尾：否则下一轮回复可能和本轮尾音交错。
	if waitErr := waitForTTSTurnDrainIfRoot(ctx); err == nil && waitErr != nil {
		err = waitErr
	}
	l.clientState.MarkLlmEnd()
	if l.session != nil {
		l.session.TraceLlmEnd(ctx, time.Now().UnixMilli(), err)
	}
	strFullText := fullText.String()

	if needSendTtsCmd {
		l.finishTTSTurnWithReason(ctx, err, result, "LLMManager.HandleLLMResponseChannelSync end")

		// 收集TTS音频并发送聊天历史事件
		// 注意：工具调用后的LLM响应（nest > 1）也会累积音频到缓存中，但不会清空
		// 只有在首次调用（nest<=1）时才清空缓存并发送事件
		audioData := l.ttsManager.GetAndClearAudioHistory()

		// 计算总音频大小（所有帧的字节数之和）
		audioSize := 0
		for _, frame := range audioData {
			audioSize += len(frame)
		}

		// 只有在首次调用（nest<=1）时才发送事件
		if nest <= 1 {
			// 从 LLMManager 中获取 MessageID（Assistant 角色）
			// 如果没有找到 MessageID，说明第一阶段保存未完成，不进行第二阶段更新
			messageID, ok := l.GetLastMessageID(string(schema.Assistant))
			if !ok {
				log.Warnf("TTS 完成时未找到 MessageID，跳过第二阶段音频更新")
				return result.ok, err
			}

			// 发布事件：第二阶段（更新音频）
			// 注意这里没有设置 IsUpdate=true（与 Async 版不同），语义由事件订阅方处理：
			// MessageID 已存在时按“更新同一条消息”处理。
			assistantMsg := schema.AssistantMessage(strFullText, nil)
			eventbus.Get().Publish(eventbus.TopicAddMessage, &eventbus.AddMessageEvent{
				ClientState: l.clientState,
				Msg:         *assistantMsg,
				MessageID:   messageID,
				AudioData:   audioData, // 第二阶段：有音频
				AudioSize:   audioSize,
				SampleRate:  l.clientState.OutputAudioFormat.SampleRate,
				Channels:    l.clientState.OutputAudioFormat.Channels,
				Timestamp:   time.Now(),
			})
		}
	} else {
		// nest > 1 的情况：虽然不发送TTS命令，但音频数据仍然会累积到缓存中
		// 这些音频会在首次响应结束时（nest <= 1）一起收集
		log.Debugf("工具调用后的LLM响应（nest=%d），音频数据将累积到缓存中", nest)
	}

	return result.ok, err
}

// [3.4] 消费响应：逐条读 llmResponseChannel，文本喂 TTS、工具调用交执行器。
//
// handleLLMResponse 处理LLM响应
//
// 这是“消费阶段”的核心，做的事：
//  1. 逐条读 llmResponseChannel：
//     · 文本 → ttsManager.handleTextResponseWithHooks（喂给 TTS，同时用 tracker 登记在途条目）；
//     · 工具调用 → 收集起来并 Submit 给 toolExecutor 并发执行；
//  2. 把响应文本累积到 context 里的 fullText（供落库与嵌套轮复用）；
//  3. 收到 IsEnd 时收敛：
//     · 无工具 → 保存 assistant 消息（两阶段保存的“第一阶段”，只有文本）；
//     · 有工具 → handleToolCallResponse 等工具执行完、可能带上结果再问一轮 LLM。
//
// 参数：
//
//	ctx                —— 本轮上下文（含 nest/fullText/tracker）；
//	userMessage        —— 触发本轮的用户消息（可能为 nil，例如主动播报）；
//	llmResponseChannel —— 要消费的响应流。
//
// 返回：llmHandleResult（含是否抑制协议级 tts_stop）+ error。
// 关于“抑制 tts_stop”：当工具产生了媒体输出（放音乐等），输出通道被媒体接管，
// 这一轮不能按常规发 tts_stop，否则会把媒体状态切断；调用方拿到该标记后自行处理。
func (l *LLMManager) handleLLMResponse(ctx context.Context, userMessage *schema.Message, llmResponseChannel chan llm_common.LLMResponseStruct) (llmHandleResult, error) {
	log.Debugf("handleLLMResponse start")
	defer log.Debugf("handleLLMResponse end")

	// 从 context 中获取 fullText（用于聊天历史）
	// 这里可以直接断言非 nil：调用方（Sync/Async 两处）都保证已经放进 context。
	fullText := ctx.Value(fullTextKey).(*strings.Builder)
	state := l.clientState
	// toolCalls 使用局部变量（内部工具调用逻辑，不涉及聊天历史）
	// 收集本轮所有工具调用，等 IsEnd 时统一执行（而不是边收边执行：
	// 模型可能把一次调用拆成多个分片，提前执行会重复调用同一个工具）。
	var toolCalls []schema.ToolCall
	// toolExecCtx 是给“工具执行 + 工具后追加轮”准备的子上下文：
	//   · nest=2：标记这是嵌套轮（避免重复发 tts_start/tts_stop、避免重复落库）；
	//   · 带上同一个 fullText：工具结果回来后模型续写的文本要拼进同一条消息；
	//   · 透传播放开始钩子；
	//   · MemoryModeNone（无记忆模式）下把当前用户消息放进 toolRoundMessages，
	//     否则模型看不到“用户刚刚说了什么”（因为无记忆模式不留历史），工具参数会缺上下文。
	toolExecCtx := context.WithValue(ctx, "nest", 2)
	toolExecCtx = context.WithValue(toolExecCtx, fullTextKey, fullText)
	if speechStartHook := ttsPlaybackStartHookFromContext(ctx); speechStartHook != nil {
		toolExecCtx = withTTSPlaybackStartHook(toolExecCtx, speechStartHook)
	}
	if l.clientState.GetMemoryMode() == MemoryModeNone && userMessage != nil {
		toolExecCtx = appendToolRoundMessagesToContext(toolExecCtx, []*schema.Message{userMessage})
	}
	// ttsTracker：本轮 TTS 条目计数器。onTTSItemEnqueued 是“入队登记函数”，
	// 交给 TTS 层在每次入队一条文本时调用，返回的闭包在条目处理完时被调用（见 ttsTurnTracker）。
	ttsTracker := ttsTurnTrackerFromContext(ctx)
	var onTTSItemEnqueued func() func(error)
	onTTSPlaybackStart := ttsPlaybackStartHookFromContext(ctx)
	if ttsTracker != nil {
		onTTSItemEnqueued = ttsTracker.Add
	}
	toolExecutor := newToolCallExecutor(l, toolExecCtx)
	// assistantSaved 保证 assistant 消息只保存一次（正常结束保存一次；用户打断时由
	// saveInterruptedAssistant 保存半截内容，二者互斥）。
	assistantSaved := false
	result := llmHandleResult{}

	// saveInterruptedAssistant 在“用户打断”时保存已经生成的那半截回复。
	//
	// 为什么需要它：正常路径只在收到 IsEnd 时才落库，但打断意味着永远等不到 IsEnd，
	// 已生成的内容就会丢失（历史里看不到助手说过什么，下一轮上下文也不完整）。
	// 触发条件：ctx 已取消（打断会 cancel 轮次上下文）且确实攒到了文本。
	// Extra 里的标记用于让历史拼接时给这条消息加“ [用户打断]”后缀（见 decorateInterruptedContent）。
	saveInterruptedAssistant := func() {
		if assistantSaved {
			return
		}
		// ctx 未取消 = 不是打断场景，交给正常路径保存。
		if ctx.Err() == nil {
			return
		}
		text := strings.TrimSpace(fullText.String())
		if text == "" {
			return
		}
		msg := schema.AssistantMessage(text, nil)
		msg.Extra = map[string]any{
			interruptExtraKey:      true,
			interruptByExtraKey:    "user",
			interruptStageExtraKey: "llm",
		}
		if err := l.AddLlmMessage(ctx, msg); err != nil {
			log.Errorf("保存打断助手消息失败: %v", err)
			return
		}
		assistantSaved = true
	}

	// 进入循环前先查一次取消：可能刚入队就被打断，避免白跑一轮。
	select {
	case <-ctx.Done():
		saveInterruptedAssistant()
		log.Debugf("handleLLMResponse ctx done, return")
		return result, nil
	default:
	}

	for {
		select {
		case <-ctx.Done():
			// 上下文已取消，优先处理取消逻辑
			// 之所以在外层先查一次：内层 select 里“读到响应”和“ctx 取消”是两个 case，
			// Go 的 select 在两者同时就绪时是随机选，会导致取消后仍处理一条响应；
			// 这个嵌套结构让“取消优先”。
			saveInterruptedAssistant()
			log.Infof("%s 上下文已取消，停止处理LLM响应, context done, exit", state.DeviceID)
			return result, nil
		default:
			// 非阻塞检查，如果ctx没有Done，继续处理LLM响应
			select {
			case llmResponse, ok := <-llmResponseChannel:
				if !ok {
					// 通道已关闭，退出协程
					// 通道关闭 = 上游 goroutine 正常收尾（handleLLMWithContextAndTools 的 defer），
					// 视为本轮成功结束。
					log.Infof("LLM 响应通道已关闭，退出协程")
					result.ok = true
					return result, nil
				}
				if ctx.Err() != nil {
					// 取消之后才到达的分片必须丢弃：此时 TTS/队列可能已被清空，
					// 再喂进去会把“上一轮的话”混到新一轮。
					saveInterruptedAssistant()
					log.Infof("%s LLM分片到达时上下文已取消，丢弃晚到响应并退出", state.DeviceID)
					return result, nil
				}

				log.Debugf("LLM 响应: %+v", llmResponse)

				// 工具调用：收集 + 立即提交执行（Submit 只负责启动，不等结果）。
				// 并发执行的意义：模型可能一次调用多个工具（查天气+放音乐），
				// 串行会导致等待时间叠加；结果在 IsEnd 时统一 Wait 汇总。
				if len(llmResponse.ToolCalls) > 0 {
					log.Debugf("获取到工具: %+v", llmResponse.ToolCalls)
					toolCalls = append(toolCalls, llmResponse.ToolCalls...)
					toolExecutor.Submit(llmResponse.ToolCalls)
				}

				hasText := strings.TrimSpace(llmResponse.Text) != ""
				// 判断条件里的 IsStart/IsEnd 是必须的：双流式 TTS 依赖“空文本但 IsEnd=true”
				// 作为收尾信号，如果只在有文本时才转发，TTS 侧的流就永远不会结束。
				if hasText || llmResponse.IsStart || llmResponse.IsEnd {
					// 双流式收尾依赖空文本的 IsEnd 信号，不能只在有文本时才传给 TTS。
					// onTTSItemEnqueued：TTS 每登记一个条目就 +1，条目处理完后 -1（用于轮次排空等待）。
					// [5.0] 交接 TTS：把这段文本喂给 TTSManager（同时登记在途条目，供轮次排空等待）
					if err := l.ttsManager.handleTextResponseWithHooks(ctx, llmResponse, false, onTTSItemEnqueued, onTTSPlaybackStart); err != nil {
						result.ok = true
						return result, err
					}
				}
				if hasText {
					// 累积本轮（含嵌套轮）的全部 assistant 文本，最终用于落库与日志。
					fullText.WriteString(llmResponse.Text)
				}

				// IsEnd 是“本轮收敛点”：到这里模型已经说完了（或决定调用工具了）。
				if llmResponse.IsEnd {
					if len(toolCalls) == 0 {
						//写到redis中
						// 这是一段历史遗留的注释+被注释掉的代码：upstream 曾在“无工具”分支里
						// 再次保存用户消息，后来因为 ASR 阶段已经保存过用户消息而废弃。
						// 保留原样以便追溯，勿删（删除会改变函数体行数/历史）。
						if userMessage != nil {
							if userMessage.Role == schema.User {
								// 检查用户消息是否已经保存过（ASR 处理时已经保存）
								// 通过检查最后一条消息是否是用户消息且内容匹配来判断
								/*messages := l.clientState.GetMessages(1)
								shouldSave := true
								if len(messages) > 0 {
									lastMsg := messages[len(messages)-1]
									if lastMsg.Role == schema.User && lastMsg.Content == userMessage.Content {
										// 用户消息已经保存过了（ASR 处理时保存的），跳过
										shouldSave = false
										log.Debugf("用户消息已在 ASR 处理时保存，跳过重复保存: %s", userMessage.Content)
									}
								}
								if shouldSave {
									if err := l.AddLlmMessage(ctx, userMessage); err != nil {
										log.Errorf("保存用户消息失败: %v", err)
									}
								}*/
							}
						}
						// 两阶段保存的第一阶段：先存文本（音频等 TTS 播完由 onEndFunc 补）。
						// 为什么先存文本：即使播报中途被打断/进程重启，历史里至少还有这段文字。
						strFullText := fullText.String()
						if strings.TrimSpace(strFullText) != "" || len(toolCalls) > 0 {
							if err := l.AddLlmMessage(ctx, schema.AssistantMessage(strFullText, toolCalls)); err != nil {
								log.Errorf("保存助手消息失败: %v", err)
							} else {
								assistantSaved = true
							}
						}
					}
					if len(toolCalls) > 0 {
						// 工具轮：等待所有工具执行完，保存工具结果，必要时带上结果再问一轮 LLM。
						// 注意这里传入的 assistant 消息是“调用工具的意图 + 参数”，
						// 它是下一轮请求的必要上下文（模型需要看到自己刚才调了什么）。
						toolSummary, err := l.handleToolCallResponse(toolExecCtx, schema.AssistantMessage(fullText.String(), toolCalls), toolCalls, toolExecutor)
						if err != nil {
							log.Errorf("处理工具调用响应失败: %v", err)
							result.ok = true
							return result, fmt.Errorf("处理工具调用响应失败: %v", err)
						}
						// 工具有媒体输出（例如放音乐）时，输出状态由媒体接管，本轮不要发 tts_stop。
						result.suppressProtocolTtsStop = toolSummary.hasMediaOutput
						// 工具执行失败时把模型已经生成的那段文本补播出去：
						// 通常模型会先说“好的，我帮你看一下……”再调工具，如果工具挂了就直接静默会显得很怪。
						if !toolSummary.invokeToolSuccess && strings.TrimSpace(llmResponse.Text) != "" {
							if err := l.ttsManager.handleTextResponseWithHooks(ctx, llmResponse, false, nil, onTTSPlaybackStart); err != nil {
								result.ok = true
								return result, err
							}
							fullText.WriteString(llmResponse.Text)
						}
					}

					result.ok = true
					return result, nil
				}
			case <-ctx.Done():
				// 上下文已取消，退出协程
				saveInterruptedAssistant()
				log.Infof("%s 上下文已取消，停止处理LLM响应, context done, exit", state.DeviceID)
				return result, nil
			}
		}
	}
}

// [3.3] 请求 LLM：一次完整 LLM 请求的入口（由 session.go 的 actionDoChat 调用）。
//
// 流程：
//
//	GetMessages（拼 system prompt + 历史）→ LLM_INPUT 钩子（可改写/拦截）
//	→ 打点/置状态 → handleLLMWithContextAndTools（发起请求）
//	→ HandleLLMResponseChannelSync/Async（消费响应）
//
// 参数：
//
//	ctx           —— 本轮上下文（打断即取消）；
//	userMessage   —— 当前用户消息；
//	einoTools     —— 可用工具（可能为空 = 纯聊天）；
//	isSync        —— 是否同步等待本轮处理完（同步会被当前 goroutine 阻塞）；
//	speakerResult —— 声纹识别结果（用于往 system prompt 里补充“说话人是谁”的信息）。
func (l *LLMManager) DoLLmRequest(ctx context.Context, userMessage *schema.Message, einoTools []*schema.ToolInfo, isSync bool, speakerResult *speaker.IdentifyResult) error {
	log.Debugf("发送带工具的 LLM 请求, seesionID: %s, requestEinoMessages: %+v", l.clientState.SessionID, userMessage)
	clientState := l.clientState

	// 记录到实例上，便于诊断/其他方法（如工具执行）读取本次可用工具。
	l.einoTools = einoTools

	//组装历史消息和当前用户的消息
	// GetMessages 负责“把这一轮该给模型看的东西”全都拼好（system prompt、时间、记忆、声纹、知识库策略…）。
	// [3.2] 组装消息：system prompt + 历史 + 当前用户消息
	requestMessages := l.GetMessages(ctx, userMessage, MaxMessageCount, speakerResult)

	// LLM_INPUT 钩子是“发出去之前最后一道可编程干预”：上层可以修改消息列表、工具集，
	// 甚至直接 stop（返回 nil 表示本轮不问了，例如被本地指令拦截）。
	if l.session != nil {
		payload, stop, hookErr := l.session.hookHub.EmitLLMInput(l.session.hookContext(ctx), chathooks.LLMInputData{
			UserMessage:     userMessage,
			RequestMessages: requestMessages,
			Tools:           einoTools,
		})
		if hookErr != nil {
			log.Warnf("LLM_INPUT hook 执行失败: %v", hookErr)
		}
		userMessage = payload.UserMessage
		requestMessages = payload.RequestMessages
		einoTools = payload.Tools
		if stop {
			log.Infof("LLM_INPUT hook 请求停止当前流程")
			return nil
		}
	}

	// 打点 + 置状态：状态位会影响其他协程的行为（例如 ASR 循环判断“此时能否重启 ASR”）。
	clientState.SetStartLlmTs()
	if l.session != nil {
		l.session.TraceLlmStart(ctx, time.Now().UnixMilli())
	}
	clientState.SetStatus(ClientStatusLLMStart)

	// 调用内部方法处理 LLM 响应，资源在方法内部管理
	responseSentences, err := l.handleLLMWithContextAndTools(
		ctx,
		requestMessages,
		einoTools,
	)
	if err != nil {
		log.Errorf("发送带工具的 LLM 请求失败, seesionID: %s, error: %v", l.clientState.SessionID, err)
		return fmt.Errorf("发送带工具的 LLM 请求失败: %v", err)
	}

	log.Debugf("DoLLmRequest goroutine开始 - SessionID: %s, context状态: %v", l.clientState.SessionID, ctx.Err())

	if isSync {
		// 同步处理：资源会在 handleLLMWithContextAndTools 的 defer 中自动释放
		// 同步路径下本函数会阻塞到本轮（含工具轮）全部结束。
		_, err := l.HandleLLMResponseChannelSync(ctx, userMessage, responseSentences, einoTools)
		if err != nil {
			log.Errorf("处理 LLM 响应失败, seesionID: %s, error: %v", l.clientState.SessionID, err)
			return err
		}
	} else {
		// 异步处理：资源会在 handleLLMWithContextAndTools 的 defer 中自动释放
		// 只负责入队，实际消费由 processLLMResponseQueue 串行完成。
		err = l.HandleLLMResponseChannelAsync(ctx, userMessage, responseSentences)
		if err != nil {
			log.Errorf("处理 LLM 响应失败, seesionID: %s, error: %v", l.clientState.SessionID, err)
		}
	}

	log.Debugf("DoLLmRequest 结束 - SessionID: %s", l.clientState.SessionID)

	return nil
}

// AddMessage 添加消息到聊天历史（统一入口，适用于所有消息类型）
//
// 它做两件事：
//  1. 同步写进内存（ClientState.AddMessage）——供后续请求组装上下文时读取；
//  2. 通过 eventbus 发布 TopicAddMessage 事件——由订阅方异步落库（解耦存储）。
//
// “两阶段保存”在这里完成第一阶段：
//
//	User/Assistant → 生成 MessageID 并暂存到 l.lastMessageID（供 TTS 结束后补音频）；
//	Tool          → 一次性保存（工具消息没有音频，不需要第二阶段）。
//
// 注意用户消息在 ASR 阶段也会被保存一次（消息 ID 由 asr.go 生成，算法一致），
// 这里主要承载 assistant/tool 消息。
func (l *LLMManager) AddMessage(ctx context.Context, msg *schema.Message) error {
	if msg == nil {
		log.Warnf("尝试添加 nil 消息到聊天历史")
		return fmt.Errorf("消息不能为 nil")
	}

	// 生成 MessageID（使用 MD5 哈希缩短长度，避免超过数据库 varchar(64) 限制）
	// 原始格式：{SessionID}-{Role}-{Timestamp}
	// 与 asr.go 中生成用户消息 ID 的算法保持一致：SessionID 过长 + 时间戳容易超列宽，
	// MD5 十六进制固定 32 字符，安全落在 varchar(64) 内。
	rawMessageID := fmt.Sprintf("%s-%s-%d",
		l.clientState.SessionID,
		msg.Role,
		time.Now().UnixMilli())
	// 使用 MD5 哈希生成固定32字符的十六进制字符串
	hash := md5.Sum([]byte(rawMessageID))
	messageID := hex.EncodeToString(hash[:])

	// 同步添加到内存中
	l.clientState.AddMessage(msg)

	// Tool 角色消息：直接保存，不涉及两阶段保存（无音频）
	if msg.Role == schema.Tool {
		eventbus.Get().Publish(eventbus.TopicAddMessage, &eventbus.AddMessageEvent{
			ClientState: l.clientState,
			Msg:         *msg,
			MessageID:   messageID,
			AudioData:   nil, // Tool 角色无音频
			AudioSize:   0,
			SampleRate:  0,
			Channels:    0,
			Timestamp:   time.Now(),
			IsUpdate:    false, // 一次性保存
		})
		return nil
	}

	// User/Assistant 角色：两阶段保存
	// 将 MessageID 存储到 LLMManager 中，供后续音频更新使用
	// 这里就是 GetLastMessageID 的写入端：TTS 收尾时靠它找到同一条消息补音频。
	if msg.Role == schema.User || msg.Role == schema.Assistant {
		l.lastMessageIDMu.Lock()
		l.lastMessageID[string(msg.Role)] = messageID
		l.lastMessageIDMu.Unlock()
	}

	// 发布事件：第一阶段（仅文本，无音频）
	// IsUpdate=false 表示“新增消息”；第二阶段会用同一个 MessageID 发 IsUpdate=true 来更新它。
	eventbus.Get().Publish(eventbus.TopicAddMessage, &eventbus.AddMessageEvent{
		ClientState: l.clientState,
		Msg:         *msg,
		MessageID:   messageID,
		AudioData:   nil, // 第一阶段：无音频
		AudioSize:   0,
		SampleRate:  0,
		Channels:    0,
		Timestamp:   time.Now(),
		IsUpdate:    false, // 新增消息
	})

	return nil
}

// AddLlmMessage 保持向后兼容，委托给 AddMessage
// （旧代码调用 AddLlmMessage，语义与本文件的 AddMessage 完全相同。）
func (l *LLMManager) AddLlmMessage(ctx context.Context, msg *schema.Message) error {
	return l.AddMessage(ctx, msg)
}

// [3.2] 组装消息：拼出 [system prompt] + [历史消息] + [当前用户消息]。
//
// GetMessages 组装本次发给 LLM 的完整消息列表。
//
// 结构固定为：[system prompt] + [历史消息] + [当前用户消息]。
//
// system prompt 的拼接顺序（顺序影响模型注意力，前面更“权威”）：
//  1. 全局 prompt（chat.global_system_prompt，运维级统一人设）；
//  2. 设备 prompt（SystemPrompt，每个设备/角色自己的设定）；
//  3. 当前时间和日期（让模型能回答“今天/现在”类问题）；
//  4. 长记忆（MemoryModeLong 时的用户个性化信息）；
//  5. 声纹信息（识别到说话人时，补充“当前对话人是谁”的描述）；
//  6. 记忆检索结果（用当前问题去搜历史，作为“历史关联信息”）；
//  7. 知识库检索路由策略（告诉模型何时该调 search_knowledge、怎么选知识库）。
//
// 参数：
//
//	ctx           —— 本轮上下文（MemoryModeNone 时会从这里取工具轮的临时消息）；
//	userMessage   —— 当前用户消息（为 nil 表示不追加，例如纯工具续写轮）；
//	count         —— 最多带多少条历史；
//	speakerResult —— 声纹结果（可能为 nil）。
func (l *LLMManager) GetMessages(ctx context.Context, userMessage *schema.Message, count int, speakerResult *speaker.IdentifyResult) []*schema.Message {
	memoryMode := l.clientState.GetMemoryMode()
	// includeHistory：只有“无记忆模式”才不带历史。其它模式都要带，保证多轮对话连贯。
	includeHistory := memoryMode != MemoryModeNone

	// 从 dialogue 中获取上下文；none 模式下仅允许携带当前工具调用链的临时消息
	messageList := make([]*schema.Message, 0)
	if includeHistory {
		messageList = l.clientState.GetMessages(count)
		if userMessage != nil {
			// 去掉末尾的 user 消息：当前用户消息在函数末尾会单独追加，
			// 不先剔掉就会重复（且可能出现两条 content 相同的 user 消息，模型容易混乱）。
			messageList = trimTrailingUserMessages(messageList)
		}
	} else if toolRoundMessages := toolRoundMessagesFromContext(ctx); len(toolRoundMessages) > 0 {
		// 无记忆模式下不留历史，但工具调用需要“用户问题 + 工具调用 + 工具结果”这些临时消息，
		// 否则模型无法继续推理，所以从 context 里取这一轮的临时上下文。
		messageList = toolRoundMessages
	}

	// 构建 system prompt
	systemPrompt := l.clientState.SystemPrompt
	globalSystemPrompt := strings.TrimSpace(viper.GetString("chat.global_system_prompt"))
	if globalSystemPrompt != "" {
		if systemPrompt != "" {
			systemPrompt = globalSystemPrompt + "\n\n" + systemPrompt
		} else {
			systemPrompt = globalSystemPrompt
		}
	}

	// 添加当前时间和日期信息
	// 模型本身不知道“现在几点”，不注入的话时间类问题会瞎答。
	now := time.Now()
	systemPrompt += fmt.Sprintf("\n当前时间和日期: %s %s", now.Format("2006年01月02日 15:04:05"), now.Format("Monday"))

	if memoryMode == MemoryModeLong && l.clientState.MemoryContext != "" {
		systemPrompt += fmt.Sprintf("\n用户个性化信息: \n%s", l.clientState.MemoryContext)
	}

	log.Debugf("speakerResult: %+v, voiceIdentify: %+v", speakerResult, l.clientState.DeviceConfig.VoiceIdentify)

	// 整合说话人识别结果到 systemPrompt
	if speakerResult != nil && speakerResult.Identified {
		// 根据 speakerResult 匹配 userConfig 中的 speakerGroup 信息
		if l.clientState.DeviceConfig.VoiceIdentify != nil {
			// 优先使用 SpeakerName 匹配（VoiceIdentify 的 key 是 speakerGroup.Name）
			if speakerGroupInfo, found := l.clientState.DeviceConfig.VoiceIdentify[speakerResult.SpeakerName]; found {
				// 如果找到匹配的 speakerGroup，将描述整合到 systemPrompt
				// 场景：家庭里不同成员有不同称呼/偏好，识别出是谁后让模型按那个人的设定回答。
				if speakerGroupInfo.Prompt != "" {
					systemPrompt += fmt.Sprintf("\n基于声纹识别到对话人信息: \n%s", speakerGroupInfo.Prompt)
				}
			}
		}
	}

	//search memory
	// 长记忆模式下做一次“相关记忆检索”：把和当前问题相关的历史片段注入 prompt，
	// 相当于模型自己看不到的长期记忆。失败不影响主流程（只记日志）。
	if memoryMode == MemoryModeLong && l.clientState.MemoryProvider != nil && userMessage != nil {
		memoryContext, err := l.clientState.MemoryProvider.Search(ctx, l.clientState.GetDeviceIDOrAgentID(), userMessage.Content, 10, 180)
		if err != nil {
			log.Errorf("搜索记忆失败: %v", err)
		}
		log.Debugf("搜索记忆成功, 输入内容: %s, 记忆内容: %s", userMessage.Content, memoryContext)
		if memoryContext != "" {
			systemPrompt += fmt.Sprintf("\n历史关联信息: \n%s", memoryContext)
		}
	}

	// 知识库路由策略：把“有哪些知识库、什么时候该检索”写进 prompt，
	// 让模型自己决定是否调用 search_knowledge（没有可用知识库时返回空串）。
	systemPrompt += buildKnowledgeSearchRoutingPolicy(l.clientState.DeviceConfig.KnowledgeBases)

	retMessage := make([]*schema.Message, 0)
	retMessage = append(retMessage, &schema.Message{
		Role:    schema.System,
		Content: systemPrompt,
	})
	// 过滤掉空的assistant消息，避免发送给LLM API时出现400错误
	// 空的assistant消息（Content为空且ToolCalls为空）会导致API错误
	// 产生原因：消息被打断/落库时只剩壳（例如只调了工具没说话），这类消息必须剔除。
	for _, msg := range messageList {
		if msg != nil && msg.Role == schema.Assistant && msg.Content == "" && len(msg.ToolCalls) == 0 {
			log.Debugf("过滤掉空的assistant消息，避免发送给LLM API")
			continue
		}
		// 深拷贝：请求组装过程会改写 Content（加打断后缀），不能污染内存里的历史消息。
		msgCopy := cloneMessageForRequest(msg)
		// 被打断的消息加上“ [用户打断]”标记，让模型知道这句话没说完，
		// 从而不会把它当成完整意图去延续。
		if isInterruptedMessage(msgCopy) {
			msgCopy.Content = decorateInterruptedContent(msgCopy.Content)
		}
		retMessage = append(retMessage, msgCopy)
	}
	if userMessage != nil {
		// 检查 retMessage 的最后一条消息是否已经是相同的用户消息，避免重复添加
		// （某些调用路径会把当前用户消息先写进内存历史，这时就不该再追加一次。）
		shouldAdd := true
		if len(retMessage) > 0 {
			lastMsg := retMessage[len(retMessage)-1]
			if lastMsg.Role == schema.User && lastMsg.Content == userMessage.Content {
				// 最后一条消息已经是相同的用户消息，跳过添加
				shouldAdd = false
				//log.Debugf("最后一条消息已经是相同的用户消息，跳过重复添加: %s", userMessage.Content)
			}
		}
		if shouldAdd {
			retMessage = append(retMessage, userMessage)
		}
	}
	return retMessage
}

// buildKnowledgeSearchRoutingPolicy 生成“知识库检索路由策略”文本，注入 system prompt。
//
// 为什么需要它：knowledge 检索是工具调用（search_knowledge），模型必须知道
// “有哪些知识库可选、什么情况下该调、不能提到检索过程”。把这些规则写进 prompt，
// 比在代码里硬判定更灵活（模型能理解自然语言问题是否需要文档依据）。
//
// 过滤规则（任一不满足就跳过该知识库）：
//   - Status 为 inactive；ExternalKBID 或 ID 为空（没配置好的库调了也没用）；
//   - 名称与 ExternalKBID 都为空（模型无法指名）。
//
// 最多列 8 个：prompt 空间有限，列太多反而稀释注意力。
// 返回空字符串表示“没有可用知识库”，此时调用方也不会注册该工具。
func buildKnowledgeSearchRoutingPolicy(knowledgeBases []config_types.KnowledgeBaseRef) string {
	if len(knowledgeBases) == 0 {
		return ""
	}

	availableKBs := make([]string, 0, len(knowledgeBases))
	for _, kb := range knowledgeBases {
		if strings.EqualFold(strings.TrimSpace(kb.Status), "inactive") {
			continue
		}
		if strings.TrimSpace(kb.ExternalKBID) == "" {
			continue
		}
		name := strings.TrimSpace(kb.Name)
		if name == "" {
			name = strings.TrimSpace(kb.ExternalKBID)
		}
		if name == "" {
			continue
		}
		if kb.ID == 0 {
			continue
		}
		desc := strings.TrimSpace(kb.Description)
		if desc == "" {
			desc = "无描述"
		}
		// 格式 "$ID: 名称=$name; 描述=$desc"：模型用 ID 传 knowledge_base_ids 参数，
		// 名称与描述供它判断语义相关性。
		availableKBs = append(availableKBs, fmt.Sprintf("%d: 名称=%s; 描述=%s", kb.ID, name, desc))
		if len(availableKBs) >= 8 {
			break
		}
	}
	if len(availableKBs) == 0 {
		return ""
	}

	return fmt.Sprintf(
		"\n知识库检索规则（工具: search_knowledge）:\n可用知识库(id:名称+描述): %s\n"+
			"1. 触发条件: 用户询问事实、流程、参数、规则、定义、条款、对比等需要文档依据的问题，或用户明确要求“按知识库/文档回答”。\n"+
			"2. 不触发条件: 闲聊问候、情绪陪伴、纯创作、纯主观建议。\n"+
			"3. 调用方式: 每轮最多调用1次，query提炼用户问题核心关键词，top_k默认5；如可判断具体知识库，请传 knowledge_base_ids（可多个）。\n"+
			"4. 选择规则: 只传与当前问题语义最相关的知识库ID；若无法判断可不传 knowledge_base_ids。\n"+
			"5. 信息不足处理: 若证据不足，不得编造，直接请用户补充更具体关键词。\n"+
			"6. 输出要求: 回答时禁止提及“知识库”“检索”“MCP”“工具调用”“命中结果”等来源或过程信息。",
		strings.Join(availableKBs, "、"),
	)
}

// trimTrailingUserMessages 去掉消息列表末尾连续的 user 消息。
//
// 场景：当前用户消息在 GetMessages 末尾会单独追加，而内存历史里可能已经把这条
// （ASR 落库时写入的）user 消息也存进去了；如果不去掉，就会出现两条相同/相邻的 user 消息，
// 部分模型会因此“重复回答”或直接报格式错误（要求 user/assistant 交替）。
func trimTrailingUserMessages(messages []*schema.Message) []*schema.Message {
	end := len(messages)
	for end > 0 {
		msg := messages[end-1]
		if msg == nil || msg.Role != schema.User {
			break
		}
		end--
	}
	return messages[:end]
}

// isInterruptedMessage 判断一条消息是否带“被打断”标记。
// 兼容 bool（内存中构造）与 string（从数据库/JSON 读回时可能是字符串"true"）两种形态。
func isInterruptedMessage(msg *schema.Message) bool {
	if msg == nil || msg.Extra == nil {
		return false
	}
	v, ok := msg.Extra[interruptExtraKey]
	if !ok || v == nil {
		return false
	}
	switch t := v.(type) {
	case bool:
		return t
	case string:
		return strings.EqualFold(strings.TrimSpace(t), "true")
	default:
		return false
	}
}

// decorateInterruptedContent 给被打断的内容加上后缀标记（幂等：已加过就不再重复加）。
// 这样模型看到“ [用户打断]”就知道那句话没说完，不会把半截话当完整意图继续发挥。
func decorateInterruptedContent(content string) string {
	if strings.TrimSpace(content) == "" {
		return content
	}
	if strings.HasSuffix(content, interruptContentSuffix) {
		return content
	}
	return content + interruptContentSuffix
}

// cloneMessagesForRequest 批量深拷贝消息（nil 元素会被跳过）。
// 用途：把 context 里暂存的工具轮消息取出时复制一份，避免调用方改写影响暂存内容。
func cloneMessagesForRequest(messages []*schema.Message) []*schema.Message {
	if len(messages) == 0 {
		return nil
	}

	cloned := make([]*schema.Message, 0, len(messages))
	for _, msg := range messages {
		if msg == nil {
			continue
		}
		cloned = append(cloned, cloneMessageForRequest(msg))
	}

	return cloned
}

// toolRoundMessagesFromContext 读取“本轮工具调用链的临时消息”（无记忆模式下用）。
// 返回深拷贝，防止调用方修改污染 context 中的原始数据。
func toolRoundMessagesFromContext(ctx context.Context) []*schema.Message {
	if ctx == nil {
		return nil
	}

	messages, ok := ctx.Value(toolRoundMessagesKey).([]*schema.Message)
	if !ok || len(messages) == 0 {
		return nil
	}

	return cloneMessagesForRequest(messages)
}

// ttsTurnTrackerFromContext 读取本轮 TTS 轮次追踪器；未设置时为 nil。
// 为 nil 意味着调用方不关心“等 TTS 排空”（例如主动播报路径）。
func ttsTurnTrackerFromContext(ctx context.Context) *ttsTurnTracker {
	if ctx == nil {
		return nil
	}

	tracker, ok := ctx.Value(ttsTurnTrackerKey).(*ttsTurnTracker)
	if !ok {
		return nil
	}

	return tracker
}

// ensureTTSTurnTrackerInContext 保证 context 里有追踪器（有则复用，无则新建）。
// “复用”是嵌套轮能共用同一个 tracker 的关键：工具轮登记的 TTS 条目会被最外层的
// Wait 一起等，不会出现“外层以为播完了、其实工具轮的音频还在生成”的情况。
func ensureTTSTurnTrackerInContext(ctx context.Context) context.Context {
	if ttsTurnTrackerFromContext(ctx) != nil {
		return ctx
	}
	return context.WithValue(ctx, ttsTurnTrackerKey, newTTSTurnTracker())
}

// waitForTTSTurnDrainIfRoot 只在“最外层轮”等待 TTS 排空。
//
// 为什么嵌套轮（nest>1）不能等：嵌套轮与最外层共用同一个 tracker，而最外层的 TTS 收尾
// （发 tts_stop、清音频缓存）要等最外层的 onEndFunc 才发生；如果嵌套轮在这里等 pending 归零，
// 就会和“最外层还没开始收尾”互相等待，造成长阻塞。
func waitForTTSTurnDrainIfRoot(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if nest, ok := ctx.Value("nest").(int); ok && nest > 1 {
		return nil
	}

	tracker := ttsTurnTrackerFromContext(ctx)
	if tracker == nil {
		return nil
	}

	return tracker.Wait(ctx)
}

// appendToolRoundMessagesToContext 把消息追加到“工具轮临时消息”列表（累积，不去重）。
// 无记忆模式下，这些消息是模型能看到本轮的完整证据链（用户问题 + 工具调用 + 工具结果）。
// 内容做深拷贝后存进 context，避免后续改写影响原消息。
func appendToolRoundMessagesToContext(ctx context.Context, messages []*schema.Message) context.Context {
	if len(messages) == 0 {
		return ctx
	}

	combined := toolRoundMessagesFromContext(ctx)
	combined = append(combined, cloneMessagesForRequest(messages)...)
	if len(combined) == 0 {
		return ctx
	}

	return context.WithValue(ctx, toolRoundMessagesKey, combined)
}

// cloneMessageForRequest 深拷贝单条消息。
//
// 为什么必须深拷贝：请求组装会改写 Content（加打断后缀）、工具执行可能改写 ToolCalls，
// 而这些消息对象同时被内存历史（ClientState）持有。浅拷贝会让“为了发请求做的修改”
// 污染本地历史，导致下一轮上下文出错。所以切片与 map 字段都要复制一份。
func cloneMessageForRequest(msg *schema.Message) *schema.Message {
	if msg == nil {
		return nil
	}
	msgCopy := *msg

	if msg.ToolCalls != nil {
		msgCopy.ToolCalls = append([]schema.ToolCall(nil), msg.ToolCalls...)
	}
	if msg.MultiContent != nil {
		msgCopy.MultiContent = append([]schema.ChatMessagePart(nil), msg.MultiContent...)
	}
	if msg.Extra != nil {
		msgCopy.Extra = make(map[string]any, len(msg.Extra))
		for k, v := range msg.Extra {
			msgCopy.Extra[k] = v
		}
	}
	if msg.ResponseMeta != nil {
		respMetaCopy := *msg.ResponseMeta
		msgCopy.ResponseMeta = &respMetaCopy
	}

	return &msgCopy
}
