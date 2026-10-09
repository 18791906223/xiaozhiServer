// chat.go 是 chat 包的「连接管理层」，实现 ChatManager。
//
// 职责：
//   - 维护一条设备连接（websocket 或 mqtt+udp）的生命周期：Start / Close / OnClose；
//   - 处理协议层 JSON 控制消息（hello / listen / abort / iot / mcp / goodbye / speak_ready），
//     以及上行二进制音频（Opus）的分发；
//   - 完成 hello 握手：校验传输类型、协商输入/输出音频格式、回复 hello；
//   - 惰性创建 ChatSession，并在设备 goodbye 后把会话「保留」一段时间再彻底清理；
//   - 为 MQTT 链路维护「主动播报链路」（speak_request / speak_ready 握手）。
//
// 在整体流程中的位置：
//
//	设备连接 → 本文件（握手、消息分发、会话创建）→ session.go（会话内 ASR/LLM/TTS）
//
// 与哪些文件配合：
//   - session.go：ChatSession 由本文件创建与持有（sessionMu 保护）；
//   - server_transport.go：所有协议消息的实际收发都走 ServerTransport；
//   - mcp.go：IoT over MCP 的桥接（McpTransport）；
//   - common.go / types.go：会话级动作与 ChatSessionOperator 接口（工具层回调会话能力的契约）。
//
// 并发模型：ChatManager 会被多个 goroutine 共享——两个收包循环（cmd / audio）、
// 定时器回调、上层注入消息的 goroutine 等。因此每块可变状态都有专门的锁或原子变量，
// 具体原因见各字段的注释。
package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/viper"

	"xiaozhi-esp32-server-golang/constants"
	"xiaozhi-esp32-server-golang/internal/app/server/auth"
	"xiaozhi-esp32-server-golang/internal/app/server/chat/plugins"
	types_conn "xiaozhi-esp32-server-golang/internal/app/server/types"
	types_audio "xiaozhi-esp32-server-golang/internal/data/audio"
	. "xiaozhi-esp32-server-golang/internal/data/client"
	. "xiaozhi-esp32-server-golang/internal/data/msg"
	chathooks "xiaozhi-esp32-server-golang/internal/domain/chat/hooks"
	"xiaozhi-esp32-server-golang/internal/domain/chat/streamtransform"
	userconfig "xiaozhi-esp32-server-golang/internal/domain/config"
	"xiaozhi-esp32-server-golang/internal/domain/mcp"
	"xiaozhi-esp32-server-golang/internal/domain/openclaw"
	pkghooks "xiaozhi-esp32-server-golang/internal/pkg/hooks"
	log "xiaozhi-esp32-server-golang/logger"
)

// ChatManager 管理「一条设备连接」的全部状态，是协议层与对话层之间的边界。
//
// 生命周期：NewChatManager 创建 → Start 开始收包 → 连接断开或上层关闭时 shutdown。
// 它最多持有一个活跃的 ChatSession（session 字段），且会话是按需创建的（惰性创建）。
type ChatManager struct {
	// DeviceID 设备唯一标识，由接入层传入；后续配置查询、状态记录、会话创建都以它为主键。
	DeviceID string
	// transport 底层连接抽象（websocket / mqtt+udp）。这里只用于关闭连接和注册 OnClose 回调；
	// 发送协议消息统一走 serverTransport（见下），以保证格式与状态一致。
	transport types_conn.IConn

	// clientState 与 ChatManager 同生命周期的共享状态：设备配置、音频格式、VAD 状态、时间戳等。
	// ChatSession 持有同一份指针，所以这里做的修改会立即被会话看到（这是刻意的设计）。
	clientState *ClientState
	// serverTransport 协议消息的发送/接收封装（hello / tts / stt / iot / mcp / goodbye…）。
	serverTransport *ServerTransport
	// mcpTransport IoT over MCP 的桥接层：转发设备与 MCP runtime 之间的消息。
	mcpTransport *McpTransport
	// hookHub 事件钩子中心（ASR/LLM/TTS 生命周期与指标埋点），会话与各 Manager 都向它上报。
	hookHub *chathooks.Hub
	// transformRegistry LLM 输出流的变换插件注册表（例如分句），会传给 ChatSession → LLMManager。
	transformRegistry *streamtransform.Registry

	// sessionMu 保护 session / startingSession / startingSessionDone / needFreshHello。
	// 用 RWMutex 是因为读远多于写：GetSession 在每次收音频时都会被调用，而创建/关闭会话很少。
	sessionMu sync.RWMutex
	// session 当前活跃会话，可能为 nil（尚未创建，或已关闭）。
	session *ChatSession

	// startingSession 记录「正在启动中」的会话。
	// 为什么需要它：ChatSession.Start 要初始化 ASR/LLM/TTS，耗时不为零；这段时间里 session
	// 尚未赋值。若不做占位记录，同时到达的 hello 与 listen 会各自创建一个会话，造成资源重复
	// 与协议混乱。有了它 + startingSessionDone，并发调用者会等待同一轮启动结果，然后复用。
	startingSession *ChatSession
	// startingSessionDone 在 startingSession 启动流程结束时被 close，
	// 用于唤醒所有正在等待该次启动完成的 ensureSession 调用者。
	startingSessionDone chan struct{}

	// ctx 是 ChatManager 级根上下文：两个收包循环、后台初始化、等待链路的超时都挂在它下面。
	// cancel 在 shutdown 时调用，用于统一取消所有后台 goroutine。
	ctx    context.Context
	cancel context.CancelFunc

	// helloMu 串行化 hello 处理。hello 会同时读写 clientState、helloInited、mcpInitState，
	// 且可能是并发/重复到达的（例如 MQTT 重连后重发 hello），因此必须整体加锁；
	// 另外 scheduleMcpInitLocked / finishMcpInit 也共用这把锁来保护 mcpInitState。
	helloMu sync.Mutex
	// helloInited 表示是否已完成首次 hello（即已通过 auth 拿到 SessionID）。
	// 未完成前不允许创建 ChatSession：因为 VAD/ASR 都依赖 hello 中携带的输入音频格式。
	helloInited bool
	// mcpInitState 设备 MCP runtime 的初始化状态机（idle / in-flight / ready）。
	// 既用于防止重复初始化，也用于检测「状态漂移」（已 ready 但底层 transport 变了）后重试。
	mcpInitState chatMcpInitState

	// speakRequestMu 保护主动播报握手的 pending 请求。
	// speak_request 与 speak_ready 是一问一答，双方可能来自不同 goroutine，必须有锁。
	speakRequestMu sync.Mutex
	// pendingSpeakRequest 当前等待设备 speak_ready 回应的请求；nil 表示没有在等待。
	pendingSpeakRequest *pendingSpeakRequest
	// lastSpeakPathWarmAt 上一次「播报链路可用」的时间戳（毫秒）。
	// 背景：MQTT 下服务端要下发音频必须先完成一次 speak 握手（见 prepareSpeakPathForInjectedSpeech）。
	// 如果刚刚握手成功过，短期内可以复用，省掉一次网络往返。用 atomic 是因为它被多个
	// goroutine 无锁更新（CAS 取最大值），且在多处被读取。
	lastSpeakPathWarmAt atomic.Int64
	// speakReadyTimeout 等待 speak_ready 的超时时间，可通过 ChatManagerOption 覆盖（默认 5s）。
	speakReadyTimeout time.Duration

	// Close 保护，防止多次关闭
	closeOnce sync.Once
	// managerClosing 标记「正在主动关闭链路」。用于区分主动 shutdown 与被动断连：
	// 主动关闭时不应再触发 goodbye，避免重复发送（见 shutdown / OnClose）。
	managerClosing atomic.Bool
	// needFreshHello 表示会话已结束、需要设备重新发 hello 才能再建会话。
	// 典型场景：服务端主动关闭会话后，音频链路（例如 MQTT 下的 UDP 绑定）已经失效。
	// 它在 sessionMu 保护下读写（见 requiresFreshHello / setNeedFreshHello）。
	needFreshHello bool

	// mqttRebootstrapPending 标记 MQTT 链路刚重建（收到 online 广播或 duplicate_hello）。
	// 此时旧会话状态（音频通道、MCP runtime）已不可信，需要做一次归一化；
	// 用 atomic 是为了让收包热路径能无锁快速判断。
	mqttRebootstrapPending atomic.Bool

	// retainedSessionCleanupMu 保护下面两个字段：保留态会话的清理定时器及其目标会话。
	retainedSessionCleanupMu sync.Mutex
	// retainedSessionCleanupTimer goodbye 后延迟销毁会话的定时器；nil 表示没有待清理任务。
	retainedSessionCleanupTimer *time.Timer
	// retainedSessionCleanupTarget 定时器要清理的会话。
	// 保存目标是为了在定时器触发时校验「期间会话是否已被替换」，避免误杀新会话。
	retainedSessionCleanupTarget *ChatSession
	// retainedSessionIdleTimeout 保留态时长（默认 10 分钟），可由 option 或配置覆盖。
	retainedSessionIdleTimeout time.Duration
}

// pendingSpeakRequest 表示一次「等待设备 speak_ready」的主动播报握手。
//
// 为什么需要它：MQTT 链路下，服务端想主动播报（例如上层注入一条语音）时不能直接下发音频，
// 必须先发 speak_request 请设备准备 UDP 通道，设备就绪后回 speak_ready，音频才能下发。
// 一次请求只允许被完成一次，且必须有超时兜底，所以用 once + done channel + timer 配合实现。
type pendingSpeakRequest struct {
	// sessionID 发起请求时的会话 ID，用于校验回包 speak_ready 是否属于本次请求（防止握手错配）。
	sessionID string
	// done 在请求被 resolve 时关闭；等待方 select 它即可被唤醒。
	done chan struct{}
	// timer 超时兜底：即使设备始终不回 speak_ready，也会在超时后 resolve 一个错误，
	// 避免调用方（例如注入播报的 goroutine）永久阻塞。
	timer *time.Timer

	// once 保证 resolve 只生效一次。超时定时器与设备应答可能几乎同时发生，先到者胜。
	once sync.Once
	// mu 保护 err 字段，因为 resolve 与 Err() 可能运行在不同 goroutine 上。
	mu sync.Mutex
	// err 请求结果：nil 表示成功握手，非 nil 为失败原因。
	err error
}

// resolve 完成本次请求并记录错误。用 once 保证幂等：无论超时先到还是设备先应答，
// 第二次调用都是空操作，不会重复关闭 channel 导致 panic。
func (p *pendingSpeakRequest) resolve(err error) {
	if p == nil {
		return
	}
	p.once.Do(func() {
		if p.timer != nil {
			p.timer.Stop()
		}
		p.mu.Lock()
		p.err = err
		p.mu.Unlock()
		close(p.done)
	})
}

// Err 读取请求结果。加锁是因为 resolve 可能在其他 goroutine 中写 err。
func (p *pendingSpeakRequest) Err() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

// chatMcpInitState 描述设备 MCP runtime 的初始化进度。
// 之所以要状态机：MCP 初始化是异步且可能失败的，需要防止并发重复发起，
// 并在「已 ready 但底层 transport 变了」时检测漂移后重新初始化。
type chatMcpInitState uint8

const (
	// chatMcpInitStateIdle 未初始化（或上次失败，等待下一次 hello 重试）。
	chatMcpInitStateIdle chatMcpInitState = iota
	// chatMcpInitStateInFlight 正在初始化中，此时重复的 hello 不应再次发起初始化。
	chatMcpInitStateInFlight
	// chatMcpInitStateReady 已就绪，正常情况下无需再初始化。
	chatMcpInitStateReady
)

const (
	// defaultSpeakRequestReuseWindow 「热」播报链路的复用窗口。
	// 若在这么短时间内刚握手成功过，就认为通道仍然可用，可直接播报、跳过 speak_request。
	defaultSpeakRequestReuseWindow = 60 * time.Second
	// defaultSpeakReadyTimeout 等待设备 speak_ready 的默认超时时间。
	defaultSpeakReadyTimeout = 5 * time.Second
	// defaultRetainedSessionIdleTTL 设备 goodbye 后会话的默认保留时长。
	// 保留而不立刻销毁，是为了设备再次唤醒时能直接复用会话、省掉重复初始化 ASR/LLM/TTS 的开销。
	defaultRetainedSessionIdleTTL = 10 * time.Minute
)

// brokerOnlineAwareTransport 由支持「broker 是否在线」查询的传输实现（目前是 MQTT）。
// ChatManager.GetTransportType 借助它判断链路是否真的可用：MQTT 的连接对象可能还在，
// 但 broker 已离线，这时不应把设备当作在线可通信。
type brokerOnlineAwareTransport interface {
	IsBrokerOnline() bool
}

// ChatManagerOption 新建 ChatManager 时的可选配置（函数式选项）。
// 这样可以在不改变构造签名的前提下注入超时等参数，便于测试与定制。
type ChatManagerOption func(*ChatManager)

var (
	// chatHookAsyncExecutorOnce 保证下面的共享执行器只初始化一次。
	chatHookAsyncExecutorOnce sync.Once
	// chatHookAsyncExecutor 全局共享的异步钩子执行器。
	// 为什么共享：每条连接都新建一个执行器会放大 goroutine 与队列数量；
	// 而钩子大多是轻量旁路逻辑，共用一个有界队列更可控。
	chatHookAsyncExecutor *pkghooks.AsyncExecutor
)

// sharedChatHookAsyncExecutor 懒加载返回全局共享的异步钩子执行器（首次调用时按配置创建）。
func sharedChatHookAsyncExecutor() *pkghooks.AsyncExecutor {
	chatHookAsyncExecutorOnce.Do(func() {
		asyncCfg := pkghooks.AsyncConfig{
			QueueSize:    viper.GetInt("chat_hooks.async.queue_size"),
			WorkerCount:  viper.GetInt("chat_hooks.async.worker_count"),
			DropWhenFull: viper.GetBool("chat_hooks.async.drop_when_full"),
			Timeout:      time.Duration(viper.GetInt("chat_hooks.async.timeout_ms")) * time.Millisecond,
		}
		chatHookAsyncExecutor = pkghooks.NewAsyncExecutor(context.Background(), asyncCfg)
		log.Infof("初始化全局共享 chat hook observer executor: queue_size=%d worker_count=%d drop_when_full=%v timeout=%s", asyncCfg.QueueSize, asyncCfg.WorkerCount, asyncCfg.DropWhenFull, asyncCfg.Timeout)
	})
	return chatHookAsyncExecutor
}

// newChatHookHub 为一条连接创建钩子中心（chathooks.Hub）。
// 它复用全局共享的异步执行器，但每个 hub 拥有自己的同步配置与插件实例，
// 因此不同连接之间的钩子状态互不干扰。
func newChatHookHub(parent context.Context) *chathooks.Hub {
	asyncCfg := pkghooks.AsyncConfig{
		QueueSize:    viper.GetInt("chat_hooks.async.queue_size"),
		WorkerCount:  viper.GetInt("chat_hooks.async.worker_count"),
		DropWhenFull: viper.GetBool("chat_hooks.async.drop_when_full"),
		Timeout:      time.Duration(viper.GetInt("chat_hooks.async.timeout_ms")) * time.Millisecond,
	}
	hub := chathooks.NewHub(parent, pkghooks.WithAsyncConfig(asyncCfg), pkghooks.WithAsyncExecutor(sharedChatHookAsyncExecutor()))
	stats := hub.Stats()
	log.Infof("初始化 chat hook hub: queue_size=%d worker_count=%d drop_when_full=%v timeout=%s dropped_async=%d", asyncCfg.QueueSize, asyncCfg.WorkerCount, asyncCfg.DropWhenFull, asyncCfg.Timeout, stats.DroppedAsync)
	return hub
}

// chatHookBuiltinOverrides 从全局配置（chat_hooks.plugins.<name>.enabled / .priority）
// 读出对内置钩子插件的覆盖项。
// 目的：让运维可以通过配置开关或调整某个内置钩子的优先级，而不必改代码。
func chatHookBuiltinOverrides() map[string]chathooks.BuiltinPluginConfig {
	overrides := map[string]chathooks.BuiltinPluginConfig{}
	for _, reg := range chathooks.BuiltinRegistrations() {
		path := "chat_hooks.plugins." + reg.Meta.Name
		cfg := chathooks.BuiltinPluginConfig{}
		if viper.IsSet(path + ".enabled") {
			enabled := viper.GetBool(path + ".enabled")
			cfg.Enabled = &enabled
		}
		if viper.IsSet(path + ".priority") {
			cfg.Priority = viper.GetInt(path + ".priority")
		}
		overrides[reg.Meta.Name] = cfg
	}
	return overrides
}

// [0.0] 连接建立：为一条新接入的设备连接创建 ChatManager（一条连接一个实例）。
//
// NewChatManager 为一条新接入的设备连接创建 ChatManager。
//
// 步骤顺序是有依赖的，不可随意调换：
//  1. 先把 ChatManager 自身作为 ChatSessionOperator 放进 ctx（key 为 chat_session_operator），
//     这样后续创建的工具/子模块都能通过 ctx 取回「操作会话」的能力而不直接依赖 ChatSession
//     （解耦，避免循环依赖，接口定义见 types.go）；
//  2. 注册 TTS 轮次结束策略处理器（见 handleTTSTurnEndPolicy）；
//  3. 生成 clientState（读设备配置、校验激活状态、创建音频缓冲与时钟）；
//  4. 创建 serverTransport 与 mcpTransport；
//  5. 注册底层 OnClose 回调，保证连接断开时能统一回收资源；
//  6. 创建 hookHub 与输出流变换插件注册表。
//
// 任何一步失败都要主动关闭底层连接，否则会泄漏一条「已死但未释放」的连接。
func NewChatManager(deviceID string, transport types_conn.IConn, options ...ChatManagerOption) (*ChatManager, error) {
	cm := &ChatManager{
		DeviceID:          deviceID,
		transport:         transport,
		speakReadyTimeout: defaultSpeakReadyTimeout,
	}

	for _, option := range options {
		option(cm)
	}

	// 把自身注入 ctx，供工具层通过 ctx.Value("chat_session_operator") 回调会话能力。
	ctx := context.WithValue(context.Background(), "chat_session_operator", ChatSessionOperator(cm))
	ctx = withTTSTurnEndPolicyHandler(ctx, cm)
	cm.ctx, cm.cancel = context.WithCancel(ctx)

	clientState, err := GenClientState(cm.ctx, cm.DeviceID)
	if err != nil {
		log.Errorf("初始化客户端状态失败: %v", err)
		_ = cm.transport.Close()
		return nil, err
	}
	cm.clientState = clientState

	cm.serverTransport = NewServerTransport(cm.transport, clientState)
	cm.mcpTransport = &McpTransport{
		Client:          clientState,
		ServerTransport: cm.serverTransport,
	}

	cm.transport.OnClose(cm.OnClose)

	cm.hookHub = newChatHookHub(cm.ctx)
	if !viper.IsSet("chat_hooks.enabled") || viper.GetBool("chat_hooks.enabled") {
		if err := chathooks.RegisterBuiltinPlugins(cm.hookHub, chatHookBuiltinOverrides()); err != nil {
			log.Errorf("注册 chat hook builtin plugins 失败: %v", err)
			_ = cm.transport.Close()
			return nil, err
		}
		log.Infof("已加载 chat hook plugins: %+v", cm.hookHub.PluginMetas())
	}

	// 输出流变换插件注册表（如分句）：ChatSession 会把它传给 LLMManager，
	// 让「怎么切分/过滤 LLM 输出」这件事与具体 LLM 实现解耦。
	cm.transformRegistry = streamtransform.NewRegistry()
	plugins.Init(cm.transformRegistry)

	return cm, nil
}

// GenClientState 读取设备配置并构造与该连接同生命周期的 ClientState。
//
// 这里做的事情与「为什么」：
//   - 归一化 memory / speaker 模式，避免上层到处判断非法值；
//   - 兜底静音阈值（chat_max_silence_duration，默认 400ms）：VAD 用它判断「说完了」；
//   - 查询激活状态（auth.enable 时才真正有意义），未激活设备后续会被拦截在 listen 之前；
//   - OpusAudioBuffer 容量固定 100：音频是可丢的实时数据，缓冲满了应丢弃而不是阻塞收包协程；
//   - 最后调用 applyOutputAudioFormatForTTS 决定下行音频格式（TTS 播放参数）。
func GenClientState(pctx context.Context, deviceID string) (*ClientState, error) {
	configProvider, err := userconfig.GetProvider(viper.GetString("config_provider.type"))
	if err != nil {
		log.Errorf("获取 用户配置提供者失败: %+v", err)
		return nil, err
	}
	deviceConfig, err := configProvider.GetUserConfig(pctx, deviceID)
	if err != nil {
		log.Errorf("获取 设备 %s 配置失败: %+v", deviceID, err)
		return nil, err
	}
	deviceConfig.MemoryMode = NormalizeMemoryMode(deviceConfig.MemoryMode)
	deviceConfig.SpeakerChatMode = NormalizeSpeakerChatMode(deviceConfig.SpeakerChatMode)

	ctx, cancel := context.WithCancel(pctx)

	maxSilenceDuration := viper.GetInt64("chat.chat_max_silence_duration")
	if !viper.IsSet("chat.chat_max_silence_duration") {
		maxSilenceDuration = 400
	}

	isDeviceActivated, err := configProvider.IsDeviceActivated(ctx, deviceID, "")
	if err != nil {
		log.Errorf("检查设备激活状态失败: %v", err)
	}

	clientState := &ClientState{
		IsActivated:       isDeviceActivated,
		Dialogue:          &Dialogue{},
		Abort:             false,
		ListenMode:        "auto",          // 默认拾音模式；设备会在 listen 消息里覆盖它
		ListenPhase:       ListenPhaseIdle, // listen start 流程状态：idle/starting/listening
		DeviceID:          deviceID,
		AgentID:           deviceConfig.AgentId,
		Ctx:               ctx,
		Cancel:            cancel,
		SystemPrompt:      deviceConfig.SystemPrompt,
		DeviceConfig:      deviceConfig,
		OutputAudioFormat: types_audio.AudioFormat{}, // 由 applyOutputAudioFormatForTTS 填充
		// 上行 Opus 帧的接收缓冲；容量 100 且写满即丢，用于保护收包协程不被慢消费者拖死。
		OpusAudioBuffer: make(chan []byte, 100),
		// VAD 解码后的 PCM（float32）累积缓冲，受 AudioBufferMutex 保护。
		AsrAudioBuffer: &AsrAudioBuffer{
			PcmData:          make([]float32, 0),
			AudioBufferMutex: sync.RWMutex{},
		},
		// VAD 判定所需的状态；SilenceThresholdTime 决定「停顿多久算说完」。
		VoiceStatus: VoiceStatus{
			HaveVoice:            false,
			HaveVoiceLastTime:    0,
			VoiceStop:            false,
			SilenceThresholdTime: maxSilenceDuration,
		},
		// SessionCtx 是一次对话的上下文容器（惰性创建），由会话在 listen start 时使用。
		SessionCtx: Ctx{},
	}
	applyOutputAudioFormatForTTS(clientState)

	return clientState, nil
}

// applyOutputAudioFormatForTTS 决定「下行音频帧」的格式（TTS 播放参数的默认值）。
//
// 为什么要有这个特例：小智自带 TTS 通道固定用 24kHz / 20ms 帧，与通用默认格式不同，
// 若沿用默认格式，设备侧解码会得到错误语速/噪声。因此在 hello 与配置刷新后都要重算一次。
func applyOutputAudioFormatForTTS(clientState *ClientState) {
	clientState.OutputAudioFormat = types_audio.AudioFormat{
		SampleRate:    types_audio.SampleRate,
		Channels:      types_audio.Channels,
		FrameDuration: types_audio.FrameDuration,
		Format:        types_audio.Format,
	}
	ttsType := clientState.DeviceConfig.Tts.Provider
	if ttsType == constants.TtsTypeXiaozhi {
		clientState.OutputAudioFormat.SampleRate = 24000
		clientState.OutputAudioFormat.FrameDuration = 20
	}
}

// ReloadDeviceConfig 重新拉取设备配置并就地刷新 clientState（不重启会话）。
// 供上层在「设备配置被后台修改」后热更新使用。
//
// 两个容易忽略的点：
//   - SpeakerTTSConfig 必须清空：它属于上一轮声纹识别结果，配置变了就必须重新计算；
//   - OpenClaw 模式记录是按 (agentID, deviceID) 存的，agent 变更后旧记录不会自动失效，
//     所以这里对新旧两个 agent 都调用一次 ExitMode，避免「换了 agent 却还在旧模式里」。
func (c *ChatManager) ReloadDeviceConfig(ctx context.Context) error {
	configProvider, err := userconfig.GetProvider(viper.GetString("config_provider.type"))
	if err != nil {
		return fmt.Errorf("获取配置提供者失败: %w", err)
	}

	deviceConfig, err := configProvider.GetUserConfig(ctx, c.DeviceID)
	if err != nil {
		return fmt.Errorf("获取设备配置失败: %w", err)
	}
	deviceConfig.MemoryMode = NormalizeMemoryMode(deviceConfig.MemoryMode)
	deviceConfig.SpeakerChatMode = NormalizeSpeakerChatMode(deviceConfig.SpeakerChatMode)

	oldAgentID := c.clientState.AgentID
	c.clientState.AgentID = deviceConfig.AgentId
	c.clientState.DeviceConfig = deviceConfig
	c.clientState.SystemPrompt = deviceConfig.SystemPrompt
	c.clientState.SpeakerTTSConfig = nil
	openclaw.GetManager().ExitMode(oldAgentID, c.DeviceID)
	openclaw.GetManager().ExitMode(c.clientState.AgentID, c.DeviceID)
	applyOutputAudioFormatForTTS(c.clientState)
	log.Infof("设备 %s 配置已刷新，当前agent=%s", c.DeviceID, deviceConfig.AgentId)
	return nil
}

// [0.1] 启动收包：起两个常驻接收循环（控制消息 / 音频），然后阻塞到连接结束。
//
// Start 启动连接的两个收包循环并阻塞，直到 ChatManager 被关闭。
//
// 为什么拆成两个循环：控制消息是 JSON 文本、音频是二进制 Opus，两者在协议层就是不同通道，
// 分开读取可以避免慢的音频解码阻塞握手/打断等控制指令。
// 这里阻塞在当前 goroutine 上，通常由接入层用 go 调用。
func (c *ChatManager) Start() error {
	go c.cmdMessageLoop(c.ctx)
	go c.audioMessageLoop(c.ctx)

	<-c.ctx.Done()
	return nil
}

// handleLoopExit 是收包循环的 defer 收尾函数，兜住 panic 并关闭 ChatManager。
//
// 为什么要 recover：循环里任何未捕获的 panic 都会直接终结 goroutine，而不会关闭连接，
// 结果是设备侧看到「连接还在但没有任何响应」的僵尸会话。这里兜住后统一走 Close。
// 两个提前 return 的条件（上下文已取消 / transport 已关闭）用于区分「正常退出」：
// 正常退出时不该打 warn，也不该重复关闭。
func (c *ChatManager) handleLoopExit(loopName string, ctx context.Context) {
	if r := recover(); r != nil {
		log.Errorf("设备 %s %s loop panic: %v\n%s", c.DeviceID, loopName, r, string(debug.Stack()))
	}
	if ctx == nil || ctx.Err() != nil {
		return
	}
	if c.serverTransport != nil && c.serverTransport.IsClosed() {
		return
	}
	log.Warnf("设备 %s %s loop 异常退出，关闭 ChatManager", c.DeviceID, loopName)
	if err := c.Close(); err != nil {
		log.Warnf("设备 %s %s loop 退出后关闭 ChatManager 失败: %v", c.DeviceID, loopName, err)
	}
}

// cmdMessageLoop 持续读取并分发 JSON 控制消息（hello / listen / abort / iot / mcp / goodbye）。
//
// recvFailCount 的作用是「僵尸连接保护」：RecvCmd 带 120s 读超时，若连续多次超时仍无数据，
// 说明链路实际已失效（例如半开连接），主动退出并交由 handleLoopExit 关闭连接。
// 收到一条有效消息就清零计数，因此正常空闲但偶尔有心跳的连接不会被误杀。
func (c *ChatManager) cmdMessageLoop(ctx context.Context) {
	defer c.handleLoopExit("cmd", ctx)

	recvFailCount := 0
	for {
		select {
		case <-ctx.Done():
			log.Infof("设备 %s recvCmd context cancel", c.DeviceID)
			return
		default:
		}

		if recvFailCount > 3 {
			log.Errorf("设备 %s recv cmd timeout 超过阈值", c.DeviceID)
			return
		}

		message, err := c.serverTransport.RecvCmd(ctx, 120)
		if err != nil {
			log.Errorf("recv cmd error: %v", err)
			recvFailCount++
			continue
		}
		if message == nil {
			continue
		}

		recvFailCount = 0
		log.Infof("收到文本消息: %s", string(message))
		if err := c.handleTextMessage(message); err != nil {
			log.Errorf("处理文本消息失败: %v, 消息内容: %s", err, string(message))
		}
	}
}

// [1.0] 读取音频：读上行 Opus 二进制帧，转交给当前会话。
//
// audioMessageLoop 读取上行二进制音频（Opus）并转交给当前会话。
//
// 多个「丢弃音频」的分支都是刻意的，理解它们是理解上行链路稳定性的关键：
//   - 无活跃会话：音频早于 hello/listen 到达时，不应为了音频去创建会话；
//   - 存在待完成的 speak_request：说明正在做主动播报握手，此时链路状态正在重建，
//     把音频喂进去会让 VAD/ASR 处理来历不明的数据；
//   - 未激活 + auth.enable：未激活设备不该走识别链路（避免无谓的资源消耗）；
//   - ClientVoiceStop：客户端已表示停止说话（例如 VAD 已判静音/已收口），
//     后续残留音频直接跳过，避免把尾巴数据再送进 VAD。
func (c *ChatManager) audioMessageLoop(ctx context.Context) {
	defer c.handleLoopExit("audio", ctx)

	for {
		select {
		case <-ctx.Done():
			log.Debugf("设备 %s recvAudio context cancel", c.DeviceID)
			return
		default:
		}
		// [1.0] 读取音频帧：阻塞等下一帧 Opus（600ms 超时）
		message, err := c.serverTransport.RecvAudio(ctx, 600)
		if err != nil {
			log.Errorf("recv audio error: %v", err)
			return
		}
		if message == nil {
			continue
		}

		session := c.GetSession()
		if session == nil {
			log.Debugf("设备 %s 当前无活动 ChatSession，丢弃音频数据", c.DeviceID)
			continue
		}
		if c.hasPendingSpeakRequest() {
			log.Debugf("设备 %s 当前存在待完成 speak_request，丢弃音频数据", c.DeviceID)
			continue
		}

		log.Debugf("收到音频数据，大小: %d 字节", len(message))
		isAuth := viper.GetBool("auth.enable")
		if isAuth && !c.clientState.IsActivated {
			log.Debugf("设备 %s 未激活, 跳过音频数据", c.clientState.DeviceID)
			continue
		}
		if c.clientState.GetClientVoiceStop() {
			log.Debug("客户端停止说话, 跳过音频数据")
			continue
		}

		// [1.0] 交给会话：写入 OpusAudioBuffer，等 VAD 循环消费（缓冲满则丢弃本帧）
		if ok := session.HandleAudioMessage(message); !ok {
			log.Warnf("音频缓冲区已满，丢弃音频数据")
		}
	}
}

// handleTextMessage 解析控制消息并按类型分发到对应处理器。
//
// 注意第一段逻辑：除了 goodbye 之外，任何设备活动消息都视为「设备还活着」，
// 于是取消保留态会话的清理定时器（延长会话寿命），避免设备仍在交互却把会话回收掉。
// goodbye 本身就是「进入保留态」的信号，所以特意排除它。
func (c *ChatManager) handleTextMessage(message []byte) error {
	var clientMsg ClientMessage
	if err := json.Unmarshal(message, &clientMsg); err != nil {
		log.Errorf("解析消息失败: %v", err)
		return fmt.Errorf("解析消息失败: %v", err)
	}

	if clientMsg.Type != MessageTypeGoodBye {
		c.cancelRetainedSessionCleanup("收到设备活动消息")
	}

	switch clientMsg.Type {
	case MessageTypeHello:
		return c.HandleHelloMessage(&clientMsg)
	case MessageTypeSpeakReady:
		return c.HandleSpeakReadyMessage(&clientMsg)
	case MessageTypeListen:
		return c.HandleListenMessage(&clientMsg)
	case MessageTypeAbort:
		return c.HandleAbortMessage(&clientMsg)
	case MessageTypeIot:
		return c.HandleIoTMessage(&clientMsg)
	case MessageTypeMcp:
		return c.HandleMcpMessage(&clientMsg)
	case MessageTypeGoodBye:
		return c.HandleGoodByeMessage(&clientMsg)
	default:
		return fmt.Errorf("未知消息类型: %s", clientMsg.Type)
	}
}

// [0.2] 握手：处理 hello，协商输入/输出音频格式（后续 VAD/ASR/TTS 全部依赖它）。
//
// HandleHelloMessage 处理设备的 hello 握手。这是所有后续流程的前置条件：
// ASR/VAD 依赖 hello 里的输入音频格式，设备解码依赖我们回复的输出音频格式。
//
// 关键点与顺序原因：
//  1. 先校验传输类型（websocket / mqtt_udp），避免不支持的链路继续走下去；
//  2. 首次 hello 需要向 auth 申请 SessionID（EnsureSession），并置 helloInited；
//     非首次（duplicate hello，常见于 MQTT 重连/重复投递）只刷新设备配置，
//     不重新申请会话，以免历史/路由状态被无谓重置；
//  3. ensureSessionForHello 会按需创建或复用一个 ChatSession；
//  4. 发回 hello 响应（带输出音频格式，MQTT 下还带 UDP 参数）；
//  5. MCP 初始化放在最后且是异步的：它耗时较长，不能阻塞握手。
//
// 失败时的 needFreshHello 处理：如果「首轮/需要新 hello」的握手在创建会话或回包阶段失败，
// 就把 needFreshHello 置为 true，让设备重发 hello 重试，同时关闭半初始化的会话，
// 避免留下一个「helloInited 但没有可用会话」的中间态。
func (c *ChatManager) HandleHelloMessage(msg *ClientMessage) error {
	if msg.AudioParams == nil {
		return fmt.Errorf("hello消息缺少audio_params")
	}
	transportType := strings.TrimSpace(msg.Transport)
	if transportType == "" && c.serverTransport != nil {
		transportType = c.serverTransport.GetTransportType()
	}
	switch transportType {
	case types_conn.TransportTypeWebsocket, types_conn.TransportTypeMqttUdp:
	default:
		return fmt.Errorf("不支持的传输类型: %s", transportType)
	}

	c.helloMu.Lock()
	defer c.helloMu.Unlock()

	clientState := c.clientState
	// hello 带来的输入音频格式，供 VAD/ASR 使用（握手后才会被用到）。
	clientState.InputAudioFormat = *msg.AudioParams
	isFirstHello := !c.helloInited
	requiresFreshHello := c.requiresFreshHello()
	// duplicate hello：非首次、也不要求新 hello，且是 MQTT 链路。
	// MQTT 下重投 hello 往往意味着链路刚重建，需要做一次「会话状态归一化」。
	isDuplicateMqttHello := !isFirstHello && !requiresFreshHello &&
		c.serverTransport != nil && c.serverTransport.GetTransportType() == types_conn.TransportTypeMqttUdp
	if c.helloInited {
		// 非首次 hello：刷新设备配置（后台可能改过 agent/音色等），并清理 OpenClaw 模式残留。
		prevAgentID := clientState.AgentID
		if err := c.refreshDeviceConfigOnHello(); err != nil {
			log.Warnf("设备 %s duplicate hello 刷新配置失败，降级继续: %v", clientState.DeviceID, err)
		}
		c.resetOpenClawModeOnHello(prevAgentID, clientState.AgentID)
	} else {
		c.resetOpenClawModeOnHello(clientState.AgentID)
	}

	if isFirstHello || requiresFreshHello {
		// 只有在「首次」或「明确要求新 hello」时才重新申请认证会话。
		// 首次允许沿用 clientState 里可能已存在的 SessionID（例如从 Redis 恢复的场景）。
		preferredSessionID := ""
		if isFirstHello {
			preferredSessionID = strings.TrimSpace(clientState.SessionID)
		}
		session, err := auth.A().EnsureSession(msg.DeviceID, preferredSessionID)
		if err != nil {
			return fmt.Errorf("创建会话失败: %v", err)
		}
		clientState.SessionID = session.ID
		c.helloInited = true
	}
	if isDuplicateMqttHello {
		log.Infof("设备 %s 收到 duplicate_hello，执行 hello 重协商", clientState.DeviceID)
		c.markMqttConversationStateStale("duplicate_hello")
	}

	chatSession, err := c.ensureSessionForHello()
	if err != nil {
		if isFirstHello || requiresFreshHello {
			c.setNeedFreshHello(true)
		}
		return err
	}
	if err := c.sendHelloResponse(msg); err != nil {
		if isFirstHello || requiresFreshHello {
			c.setNeedFreshHello(true)
			if chatSession != nil {
				// 回包失败意味着这次握手没完成，半初始化的会话直接关闭，
				// 让设备重发 hello 后重新走一遍干净的流程。
				chatSession.CloseWithReason(chatSessionCloseReasonFatalError)
			}
		}
		return err
	}
	// 握手成功：如果底层已经有活跃的 UDP 绑定，说明播报链路是热的，记录时间戳以便后续复用。
	c.refreshSpeakPathWarmFromTransport()
	// MCP 初始化放到最后：它耗时长，必须是异步的，不能拖慢握手。
	c.scheduleMcpInitOnHelloLocked(msg)
	if !isFirstHello && !requiresFreshHello {
		log.Infof("设备 %s duplicate_hello 处理完成，hello 重协商已刷新", clientState.DeviceID)
	}
	return nil
}

// scheduleMcpInitOnHelloLocked 在设备声明支持 MCP 时触发 MCP runtime 初始化。
// 调用方必须已持有 helloMu。
func (c *ChatManager) scheduleMcpInitOnHelloLocked(msg *ClientMessage) {
	if !c.hasMcpFeature(msg) {
		return
	}
	c.scheduleMcpInitLocked()
}

// scheduleMcpInitLocked 按状态机决定是否发起一次异步的 MCP runtime 初始化。
// 调用方必须已持有 helloMu。
//
// 三重防护，缺一不可：
//   - 正在初始化（InFlight）→ 直接返回，避免重复发起；
//   - 底层不需要初始化（transport 不支持）→ 返回；
//   - 已 Ready 但仍需初始化 → 说明状态漂移（例如 broker 重连后 transport 变了），
//     打日志后重新初始化，否则设备会一直用着一个已失效的 MCP runtime。
//
// 初始化在独立 goroutine 中执行，完成后由 finishMcpInit 回写状态。
func (c *ChatManager) scheduleMcpInitLocked() {
	if c.mcpTransport == nil {
		return
	}
	if c.mcpInitState == chatMcpInitStateInFlight {
		return
	}
	if !shouldScheduleDeviceMcpRuntimeInit(c.clientState.DeviceID, c.mcpTransport) {
		return
	}
	if c.mcpInitState == chatMcpInitStateReady {
		log.Warnf(
			"设备 %s MCP 状态漂移: ChatManager 已ready，但 transport=%s 需要重新初始化",
			c.DeviceID,
			strings.TrimSpace(c.mcpTransport.GetMcpTransportType()),
		)
	}

	c.mcpInitState = chatMcpInitStateInFlight
	deviceID := c.clientState.DeviceID
	transportType := strings.TrimSpace(c.mcpTransport.GetMcpTransportType())
	go func() {
		err := initMcp(deviceID, c.mcpTransport)
		c.finishMcpInit(transportType, err)
	}()
}

// finishMcpInit 回写异步 MCP 初始化的结果。必须重新加锁，因为初始化期间世界可能已变化。
//
// 这里的几个「直接放弃」判断都是为了避免把过期的结果写进新状态：
//   - manager 已关闭/ctx 已取消 → 不再改状态；
//   - transport 类型与发起时不一致 → 说明期间发生了链路切换，本次结果作废；
//   - 初始化失败 → 回到 Idle，等下一次 hello 再重试（不阻塞当前握手）。
func (c *ChatManager) finishMcpInit(transportType string, err error) {
	c.helloMu.Lock()
	defer c.helloMu.Unlock()

	if c.ctx.Err() != nil || c.managerClosing.Load() {
		return
	}
	if c.mcpTransport == nil {
		c.mcpInitState = chatMcpInitStateIdle
		return
	}
	currentTransportType := strings.TrimSpace(c.mcpTransport.GetMcpTransportType())
	if currentTransportType != strings.TrimSpace(transportType) {
		return
	}

	if err != nil {
		c.mcpInitState = chatMcpInitStateIdle
		log.Warnf("设备 %s MCP 初始化失败，等待后续 hello 重试: %v", c.DeviceID, err)
		return
	}

	c.mcpInitState = chatMcpInitStateReady
}

// hasMcpFeature 判断设备的 hello 里是否声明了 features.mcp=true。
// 只有声明了才需要为其准备 IoT over MCP 的 runtime，否则跳过初始化、省资源。
func (c *ChatManager) hasMcpFeature(msg *ClientMessage) bool {
	if msg == nil || msg.Features == nil {
		return false
	}
	isMcp, ok := msg.Features["mcp"]
	return ok && isMcp
}

// [0.3] 回握手：把下行音频格式（MQTT 下还带 UDP 参数）回给设备。
//
// sendHelloResponse 回复 hello。
// 回包里的 OutputAudioFormat 告诉设备下行音频该怎么解码；
// MQTT 链路额外带上 UDP 参数（地址/端口/密钥/nonce），因为音频走 UDP 而非 MQTT。
func (c *ChatManager) sendHelloResponse(msg *ClientMessage) error {
	transportType := strings.TrimSpace(msg.Transport)
	if transportType == "" {
		transportType = c.serverTransport.GetTransportType()
	}

	switch transportType {
	case types_conn.TransportTypeWebsocket:
		return c.serverTransport.SendHello(types_conn.TransportTypeWebsocket, &c.clientState.OutputAudioFormat, nil)
	case types_conn.TransportTypeMqttUdp:
		udpConfig, err := c.buildMqttHelloUdpConfig()
		if err != nil {
			return err
		}
		return c.serverTransport.SendHello(types_conn.TransportTypeMqttUdp, &c.clientState.OutputAudioFormat, udpConfig)
	default:
		return fmt.Errorf("不支持的传输类型: %s", transportType)
	}
}

// buildMqttHelloUdpConfig 组装 MQTT 模式下 hello 响应里的 UDP 参数。
// 音频数据不走 MQTT 而走 UDP，因此需要把对外可达的地址端口、以及加密所需的
// aes_key / full_nonce（由传输层在握手过程中协商得到）一并下发给设备。
func (c *ChatManager) buildMqttHelloUdpConfig() (*UdpConfig, error) {
	udpExternalHost := viper.GetString("udp.external_host")
	udpExternalPort := viper.GetInt("udp.external_port")

	aesKey, err := c.serverTransport.GetData("aes_key")
	if err != nil {
		return nil, fmt.Errorf("获取aes_key失败: %v", err)
	}
	fullNonce, err := c.serverTransport.GetData("full_nonce")
	if err != nil {
		return nil, fmt.Errorf("获取full_nonce失败: %v", err)
	}

	strAesKey, ok := aesKey.(string)
	if !ok {
		return nil, fmt.Errorf("aes_key不是字符串")
	}
	strFullNonce, ok := fullNonce.(string)
	if !ok {
		return nil, fmt.Errorf("full_nonce不是字符串")
	}

	return &UdpConfig{
		Server: udpExternalHost,
		Port:   udpExternalPort,
		Key:    strAesKey,
		Nonce:  strFullNonce,
	}, nil
}

// HandleListenMessage 处理设备的 listen 控制消息（start / stop / detect）。
//
// 第一步的「依赖 hello 引导」检查很关键：如果当前会话已关闭或 hello 尚未完成，
// 这条 listen 是无意义的（音频链路不可用），直接忽略并等设备发新 hello，
// 避免在无效状态下创建会话、占用资源。
func (c *ChatManager) HandleListenMessage(msg *ClientMessage) error {
	if c.requiresHelloBootstrapForSession() {
		log.Infof(
			"设备 %s 当前会话已关闭或尚未完成 hello，忽略 listen %s，等待新 hello",
			c.DeviceID,
			strings.TrimSpace(msg.State),
		)
		return nil
	}
	session, err := c.ensureSession()
	if err != nil {
		return err
	}
	c.clearMqttRebootstrapPending("listen_message")
	return session.HandleListenMessage(msg)
}

// HandleAbortMessage 处理设备的 abort（用户主动打断）。
// 无活跃会话时无需处理：没有会话就没有正在产生的输出，忽略即可（不报错，避免噪声日志）。
func (c *ChatManager) HandleAbortMessage(msg *ClientMessage) error {
	session := c.GetSession()
	if session == nil {
		log.Debugf("设备 %s 当前无活动 ChatSession，忽略 abort", c.DeviceID)
		return nil
	}
	return session.HandleAbortMessage(msg)
}

// HandleIoTMessage 处理设备上报的物联网指令。
// 当前实现是「原样回显」给设备，作为协议层的应答/回环（真正的 IoT 控制逻辑走 MCP 路径）。
func (c *ChatManager) HandleIoTMessage(msg *ClientMessage) error {
	if err := c.serverTransport.SendIot(msg); err != nil {
		return fmt.Errorf("发送响应失败: %v", err)
	}
	log.Infof("设备 %s 物联网指令: %s", msg.DeviceID, msg.Text)
	return nil
}

// HandleMcpMessage 把设备发来的 MCP 消息转交给 IoT over MCP 的处理入口。
// 这里按「设备 ID + transport 类型」定位对应的 MCP 运行时，实现多设备隔离。
func (c *ChatManager) HandleMcpMessage(msg *ClientMessage) error {
	return mcp.HandleDeviceIotMcpMessage(c.clientState.DeviceID, c.mcpTransport.GetMcpTransportType(), msg.PayLoad)
}

// HandleGoodByeMessage 处理设备端 goodbye（设备主动结束本轮会话）。
//
// 为什么要「保留」而不是「销毁」会话：设备说完再见后往往会再次唤醒，
// 如果立刻销毁就要重建 ASR/LLM/TTS 资源，延迟高且浪费。
// 因此这里把会话「重置为静默态」并启动一个定时清理（默认 10 分钟），
// 期间设备回来可直接复用；超时未活动才由 runRetainedSessionCleanup 彻底关闭。
//
// 同时要重置主动播报链路状态：链路相关的 UDP 绑定/热标记都随 goodbye 失效。
func (c *ChatManager) HandleGoodByeMessage(msg *ClientMessage) error {
	session := c.GetSession()
	if session != nil {
		log.Infof("设备 %s 收到设备端 goodbye，保留 ChatSession 并重置为静默态", c.DeviceID)
		session.ResetToSilentState()
		c.scheduleRetainedSessionCleanup(session, "peer_goodbye")
	} else {
		log.Infof("设备 %s 收到设备端 goodbye，但当前无活动 ChatSession，仅清理音频链路", c.DeviceID)
	}

	c.resetSpeakPathAfterGoodbye()
	return c.transport.CloseAudioChannel()
}

// HandleSpeakReadyMessage 处理设备对 speak_request 的应答（仅 MQTT 链路有意义）。
//
// 这是「主动播报链路」的握手第二步：设备收到 speak_request 后准备好 UDP 接收通道，
// 回一条 speak_ready，此后服务端才能下发音频。
//
// 校验顺序（都是为了不误判握手成功）：
//  1. 非 MQTT 链路直接忽略（websocket 本就不需要握手）；
//  2. state 必须是 ready；
//  3. udp_config.ready=false 说明设备侧没准备好，忽略；
//  4. 必须存在待处理请求（否则是过期/重复的应答）；
//  5. session_id 必须与发起时一致（防止把上一轮的应答当成这一轮的）。
//
// 校验通过后才标记链路为「热」、清除 MQTT 重建标记，并唤醒等待中的调用方。
func (c *ChatManager) HandleSpeakReadyMessage(msg *ClientMessage) error {
	if msg == nil {
		return nil
	}
	if c.serverTransport == nil || c.serverTransport.GetTransportType() != types_conn.TransportTypeMqttUdp {
		return nil
	}
	if msg.State != "" && msg.State != MessageStateReady {
		log.Debugf("设备 %s speak_ready 状态不是 ready，忽略: %+v", c.DeviceID, msg)
		return nil
	}
	if msg.SpeakUDPConfig != nil && !msg.SpeakUDPConfig.Ready {
		log.Warnf("设备 %s speak_ready udp_config.ready=false，忽略", c.DeviceID)
		return nil
	}

	c.speakRequestMu.Lock()
	pending := c.pendingSpeakRequest
	c.speakRequestMu.Unlock()
	if pending == nil {
		log.Debugf("设备 %s 收到无待处理请求的 speak_ready，忽略", c.DeviceID)
		return nil
	}
	if pending.sessionID != "" && strings.TrimSpace(msg.SessionID) != pending.sessionID {
		log.Warnf("设备 %s speak_ready session_id 不匹配: got=%s want=%s", c.DeviceID, msg.SessionID, pending.sessionID)
		return nil
	}

	c.markSpeakPathWarm(time.Now())
	c.clearMqttRebootstrapPending("speak_ready")
	c.finishPendingSpeakRequest(pending, nil)

	reuseExisting := false
	if msg.SpeakUDPConfig != nil {
		reuseExisting = msg.SpeakUDPConfig.ReuseExisting
	}
	log.Infof("设备 %s speak_ready 已就绪，reuse_existing=%v", c.DeviceID, reuseExisting)
	return nil
}

// ensureSession 取当前会话，没有就创建一个（不允许顺带完成 hello 引导）。
// 用于 listen 等「必须已有 hello」的路径。
func (c *ChatManager) ensureSession() (*ChatSession, error) {
	return c.ensureSessionInternal(false)
}

// [0.4] 惰性建会话：hello 路径下按需创建/复用 ChatSession。
//
// ensureSessionForHello 与 ensureSession 相同，但允许在「需要新 hello」的状态下顺带完成初始化。
// hello 处理路径用这个版本，因为 hello 本身就是重交涉入口。
func (c *ChatManager) ensureSessionForHello() (*ChatSession, error) {
	return c.ensureSessionInternal(true)
}

// ensureSessionInternal 是 ChatSession 的唯一创建入口，实现「惰性创建 + 并发去重」。
//
// 并发去重是这里最核心的设计（用 sessionMu + startingSession + startingSessionDone 三个字段）：
// 一条连接上 listen、hello、注入播报等路径可能同时要求「给我会话」，如果各自
// 直接 NewChatSession 就会创建出多条会话，导致音频被多个 VAD/ASR 消费、协议消息重复下发。
// 因此约定：
//   - 已存在 session → 直接返回（若它正在关闭则报错让调用方稍后重试）；
//   - 已有 startingSession → 不再创建，等它的 done channel 关闭后重试循环；
//   - 都没有 → 先占位（写入 startingSession），再在锁外执行耗时的 Start，
//     最后用 finishSessionStart 收敛状态并唤醒等待者。
//
// allowFreshHello 控制第 3 个准入条件：needFreshHello 为真时必须由 hello 路径来触发重建，
// 普通路径（listen 等）不允许，以免在会话刚关闭、链路已失效时悄悄又建一个。
func (c *ChatManager) ensureSessionInternal(allowFreshHello bool) (*ChatSession, error) {
	for {
		c.sessionMu.Lock()
		if c.session != nil {
			session := c.session
			c.sessionMu.Unlock()
			if session.IsClosing() {
				return nil, fmt.Errorf("ChatSession 正在关闭，稍后再试")
			}
			return session, nil
		}
		if c.startingSession != nil {
			// 有人正在启动会话：等待它结束再重试，而不是自己也去创建一个。
			waitCh := c.startingSessionDone
			c.sessionMu.Unlock()
			if waitCh == nil {
				// 极少见的窗口：状态已被收敛但 done 尚未读到，让调用方稍后重试。
				return nil, fmt.Errorf("ChatSession 正在启动，稍后再试")
			}
			<-waitCh
			continue
		}
		if !c.helloInited {
			c.sessionMu.Unlock()
			return nil, fmt.Errorf("hello尚未初始化，无法创建ChatSession")
		}
		if c.needFreshHello && !allowFreshHello {
			c.sessionMu.Unlock()
			return nil, fmt.Errorf("ChatSession 已退出，请先重新发送hello")
		}

		// 先占位，再在锁外执行耗时的 Start（Start 内部会初始化 ASR/LLM/TTS 并起多个 goroutine，
		// 不能在持锁状态下做，否则会阻塞 GetSession 等读路径）。
		session := NewChatSession(
			c.clientState,
			c.serverTransport,
			c.hookHub,
			c.transformRegistry,
			WithChatSessionCloseHandler(c.handleSessionClosed),
		)
		c.startingSession = session
		c.startingSessionDone = make(chan struct{})
		c.sessionMu.Unlock()

		err := session.Start(c.ctx)
		if err != nil {
			session.CloseWithReason(chatSessionCloseReasonFatalError)
		}
		// 收敛状态：成功则把 session 正式挂上，失败则保持无会话（由调用方决定是否要求新 hello）。
		c.finishSessionStart(session, allowFreshHello, err)
		if err != nil {
			return nil, err
		}
		if session.IsClosing() {
			return nil, fmt.Errorf("ChatSession 正在关闭，稍后再试")
		}
		return session, nil
	}
}

// requiresFreshHello 读取 needFreshHello（加读锁，因为写入方在 hello 处理与会话关闭路径）。
func (c *ChatManager) requiresFreshHello() bool {
	c.sessionMu.RLock()
	defer c.sessionMu.RUnlock()
	return c.needFreshHello
}

// setNeedFreshHello 设置「需要设备重发 hello」标记（写锁保护，与读取方 requiresFreshHello 配对）。
func (c *ChatManager) setNeedFreshHello(required bool) {
	c.sessionMu.Lock()
	c.needFreshHello = required
	c.sessionMu.Unlock()
}

// needsMqttRebootstrap 查询「MQTT 链路待重建」标记。
// 播报链路判定与热链路刷新都依赖它：链路待重建时不能复用旧的热标记。
func (c *ChatManager) needsMqttRebootstrap() bool {
	if c == nil {
		return false
	}
	return c.mqttRebootstrapPending.Load()
}

// clearMqttRebootstrapPending 清除「MQTT 链路待重建」标记。
// 用 Swap 而不是 Store，是为了只有「确实从未标记变为已清除」时才打日志，避免重复刷屏。
func (c *ChatManager) clearMqttRebootstrapPending(reason string) {
	if c == nil {
		return
	}
	if c.mqttRebootstrapPending.Swap(false) {
		log.Debugf("设备 %s 清除 MQTT 会话重建标记: reason=%s", c.DeviceID, reason)
	}
}

// hasPendingSpeakRequest 判断是否正在等待设备 speak_ready。
// 收音频循环与播报链路判定都会用到（握手期间不处理音频、也不刷新热标记）。
func (c *ChatManager) hasPendingSpeakRequest() bool {
	if c == nil {
		return false
	}
	c.speakRequestMu.Lock()
	defer c.speakRequestMu.Unlock()
	return c.pendingSpeakRequest != nil
}

// resetSpeakPathAfterMqttRebootstrap 在 MQTT 链路重建后让播报链路回到「冷」状态：
// 清掉热标记，并用错误唤醒（而不是挂起）可能存在的等待者。
// 必须唤醒等待者而不是丢弃请求：否则发起播报的 goroutine 会一直阻塞到超时。
func (c *ChatManager) resetSpeakPathAfterMqttRebootstrap(reason string) {
	if c == nil {
		return
	}
	c.lastSpeakPathWarmAt.Store(0)

	c.speakRequestMu.Lock()
	pending := c.pendingSpeakRequest
	c.speakRequestMu.Unlock()
	if pending != nil {
		c.finishPendingSpeakRequest(pending, fmt.Errorf("MQTT链路重建导致主动播报链路已重置: %s", reason))
	}
}

// markMqttConversationStateStale 在 MQTT 链路重建时把「会话状态归一化」：
// 当前会话重置为静默态并进入保留期（可复用），没有会话则清理残留的 clientState。
//
// 一个重要的例外：如果是 duplicate_hello 且正在等待 speak_ready，说明这次 hello 其实是
// 主动播报握手的重协商（设备侧为了拿 UDP 参数重发了 hello），此时断言「状态过期」
// 会把正在进行的握手打乱，因此跳过归一化，交给播报链路自己收尾。
func (c *ChatManager) markMqttConversationStateStale(reason string) {
	if c == nil || c.serverTransport == nil || c.serverTransport.GetTransportType() != types_conn.TransportTypeMqttUdp {
		return
	}
	if c.managerClosing.Load() {
		return
	}
	if reason == "duplicate_hello" && c.hasPendingSpeakRequest() {
		log.Infof("设备 %s 收到 duplicate_hello，检测到待完成 speak_request，按主动播报握手重协商处理，跳过会话重建归一化", c.DeviceID)
		return
	}

	alreadyPending := c.mqttRebootstrapPending.Swap(true)
	if alreadyPending {
		log.Debugf("设备 %s MQTT 会话重建标记已存在，跳过重复归一化: reason=%s", c.DeviceID, reason)
		return
	}

	session := c.GetSession()
	if session != nil && !session.IsClosing() {
		log.Infof("设备 %s MQTT 链路重建，重置当前会话状态: reason=%s", c.DeviceID, reason)
		session.ResetToSilentState()
		c.scheduleRetainedSessionCleanup(session, "mqtt_transport_rebootstrap")
	} else if c.clientState != nil {
		log.Infof("设备 %s MQTT 链路重建，清理残留客户端状态: reason=%s", c.DeviceID, reason)
		c.clientState.Destroy()
		c.clientState.Abort = false
		c.clientState.IsWelcomeSpeaking = false
		c.clientState.IsWelcomePlaying = false
	}

	c.resetSpeakPathAfterMqttRebootstrap(reason)
}

// resetMcpRuntimeOnMqttTransportReady 在 MQTT 链路重新可用时销毁旧的 IoT MCP runtime。
// 为什么销毁而不复用：MQTT 重连后设备与 broker 的会话已经变了，旧 runtime 里的
// 话题订阅/请求状态都已失效，必须重新初始化，否则设备指令会发进已死的话题。
func (c *ChatManager) resetMcpRuntimeOnMqttTransportReady() {
	if c == nil {
		return
	}

	c.helloMu.Lock()
	defer c.helloMu.Unlock()

	c.mcpInitState = chatMcpInitStateIdle
	if c.clientState == nil || c.mcpTransport == nil {
		return
	}

	log.Infof("设备 %s 收到 MQTT online 广播，销毁现有 IoT MCP runtime，准备重新初始化", c.DeviceID)
	closeDeviceMcpRuntime(c.clientState.DeviceID, c.mcpTransport)
}

// HandleMqttTransportReady 是 MQTT 链路重新就绪（收到 online 广播）时的统一入口：
// 会话状态归一化 + MCP runtime 重置。两者都必须做，缺一个就会留下与旧链路绑定的脏状态。
func (c *ChatManager) HandleMqttTransportReady() {
	c.markMqttConversationStateStale("transport_ready")
	c.resetMcpRuntimeOnMqttTransportReady()
}

// resetSpeakPathAfterGoodbye 设备 goodbye 后重置播报链路（热标记失效 + 唤醒等待者）。
func (c *ChatManager) resetSpeakPathAfterGoodbye() {
	c.resetSpeakPathAfterSessionReset(fmt.Errorf("设备端goodbye导致主动播报链路已重置"))
}

// resetSpeakPathAfterServerSessionClose 服务端关闭会话后重置播报链路。
// 与 goodbye 版本分开命名，是为了日志里能区分「谁触发的重置」，便于排障。
func (c *ChatManager) resetSpeakPathAfterServerSessionClose(reason string) {
	c.resetSpeakPathAfterSessionReset(fmt.Errorf("服务端关闭会话导致主动播报链路已重置: %s", reason))
}

// resetSpeakPathAfterSessionReset 是上述 reset 的统一实现：把热标记清零，
// 并用传入的错误完成（唤醒）尚未结束的 speak_request。
// 注意这里不会主动发送任何协议消息，只是让本地的握手状态与「链路已不可用」这一事实一致。
func (c *ChatManager) resetSpeakPathAfterSessionReset(err error) {
	if c == nil {
		return
	}
	c.lastSpeakPathWarmAt.Store(0)

	c.speakRequestMu.Lock()
	pending := c.pendingSpeakRequest
	c.speakRequestMu.Unlock()
	if pending != nil {
		c.finishPendingSpeakRequest(pending, err)
	}
}

// getRetainedSessionIdleTimeout 返回会话保留态时长：优先用实例字段（测试/option 覆盖），
// 其次读配置 chat.retained_session_idle_timeout_ms，都没有就用默认 10 分钟。
// 非正值一律回退到默认值，避免配置写错导致会话被立刻回收。
func (c *ChatManager) getRetainedSessionIdleTimeout() time.Duration {
	if c != nil && c.retainedSessionIdleTimeout > 0 {
		return c.retainedSessionIdleTimeout
	}
	if !viper.IsSet("chat.retained_session_idle_timeout_ms") {
		return defaultRetainedSessionIdleTTL
	}
	ms := viper.GetInt64("chat.retained_session_idle_timeout_ms")
	if ms <= 0 {
		return defaultRetainedSessionIdleTTL
	}
	return time.Duration(ms) * time.Millisecond
}

// cancelRetainedSessionCleanup 取消「保留态会话清理」定时器。
// 调用场景：设备又有活动（说明会话还将被复用）、会话已被主动关闭、或要重新安排一个新的清理任务。
// 先把字段置空再 Stop，保证即使定时器此刻已触发，也不会再引用到旧目标。
func (c *ChatManager) cancelRetainedSessionCleanup(reason string) {
	if c == nil {
		return
	}
	c.retainedSessionCleanupMu.Lock()
	timer := c.retainedSessionCleanupTimer
	c.retainedSessionCleanupTimer = nil
	c.retainedSessionCleanupTarget = nil
	c.retainedSessionCleanupMu.Unlock()

	if timer != nil {
		timer.Stop()
		log.Debugf("设备 %s 取消 ChatSession 保留期清理: reason=%s", c.DeviceID, reason)
	}
}

// scheduleRetainedSessionCleanup 让一个会话进入「保留态」：保留 timeout 后若无活动则彻底关闭。
//
// 为什么需要：设备 goodbye 后马上又唤醒是很常见的（连续对话），立刻销毁会话会带来
// 重新初始化 ASR/LLM/TTS 的延迟；但一直留着又浪费资源，所以用定时器延迟回收。
// 先 cancel 再安排，保证同时只有一个待执行的清理任务（重复 goodbye 不会叠加定时器）。
func (c *ChatManager) scheduleRetainedSessionCleanup(session *ChatSession, reason string) {
	if c == nil || session == nil || session.IsClosing() {
		return
	}

	timeout := c.getRetainedSessionIdleTimeout()
	c.cancelRetainedSessionCleanup("reschedule")

	c.retainedSessionCleanupMu.Lock()
	c.retainedSessionCleanupTarget = session
	c.retainedSessionCleanupTimer = time.AfterFunc(timeout, func() {
		c.runRetainedSessionCleanup(session, timeout)
	})
	c.retainedSessionCleanupMu.Unlock()

	log.Infof(
		"设备 %s ChatSession 已进入保留态: timeout=%s reason=%s",
		c.DeviceID,
		timeout,
		reason,
	)
}

// runRetainedSessionCleanup 是保留态定时器的回调：确认「状态没变」后才真正关闭会话。
//
// 两道校验都是为了防止误杀：
//   - 清理目标必须仍是本次定时器的 target（期间可能已重新安排或已取消）；
//   - 它必须仍是当前活跃会话且未在关闭中（期间设备可能已重连并换了新会话）。
func (c *ChatManager) runRetainedSessionCleanup(session *ChatSession, timeout time.Duration) {
	if c == nil || session == nil {
		return
	}

	c.retainedSessionCleanupMu.Lock()
	if c.retainedSessionCleanupTarget != session {
		c.retainedSessionCleanupMu.Unlock()
		return
	}
	c.retainedSessionCleanupTimer = nil
	c.retainedSessionCleanupTarget = nil
	c.retainedSessionCleanupMu.Unlock()

	c.sessionMu.RLock()
	currentSession := c.session
	c.sessionMu.RUnlock()
	if currentSession != session || session.IsClosing() {
		return
	}

	log.Infof("设备 %s ChatSession 空闲超过 %s，执行彻底清理", c.DeviceID, timeout)
	session.CloseWithReason(chatSessionCloseReasonRetainedIdleTimeout)
}

// finishSessionStart 收敛一次会话启动的结果，并唤醒所有等待它的调用者。
//
// 为什么要判断 c.startingSession == session：可能存在「启动过程中会话被另一个路径关闭」
// 的情况，此时 startingSession 已被 handleSessionClosed 清空，这里就不该再把 session 挂上去
// （否则会挂上一个已关闭的会话）。waitCh != nil 表示确实是本函数负责收尾，才 close 通知等待者。
func (c *ChatManager) finishSessionStart(session *ChatSession, allowFreshHello bool, startErr error) {
	var waitCh chan struct{}

	c.sessionMu.Lock()
	if c.startingSession == session {
		waitCh = c.startingSessionDone
		c.startingSession = nil
		c.startingSessionDone = nil
		if startErr == nil && !session.IsClosing() {
			c.session = session
			if allowFreshHello {
				c.needFreshHello = false
			}
		}
	}
	c.sessionMu.Unlock()

	if waitCh != nil {
		close(waitCh)
	}
}

// handleSessionClosed 是 ChatSession 关闭时回调到 ChatManager 的收尾逻辑
// （通过 NewChatSession 的 WithChatSessionCloseHandler 注册）。
//
// 它做三件事，顺序有讲究：
//  1. 取消失话的保留态清理任务（会话都没了，清理任务必须先失效）；
//  2. 从 session / startingSession 中摘除该会话，让 ensureSession 后续能重建；
//     若它是在「启动中」被关闭的，还要唤醒等待者并直接返回；
//  3. 按传输类型决定后续动作（见下方分支）。
func (c *ChatManager) handleSessionClosed(session *ChatSession, reason string) {
	var waitCh chan struct{}

	c.cancelRetainedSessionCleanup("session_closed")

	c.sessionMu.Lock()
	switch {
	case c.session == session:
		c.session = nil
	case c.startingSession == session:
		waitCh = c.startingSessionDone
		c.startingSession = nil
		c.startingSessionDone = nil
	default:
		c.sessionMu.Unlock()
		log.Debugf("设备 %s 收到过期 ChatSession close 回调，忽略后续清理", c.DeviceID)
		return
	}
	c.sessionMu.Unlock()

	if waitCh != nil {
		close(waitCh)
		log.Debugf("设备 %s ChatSession 在启动阶段关闭，已清理启动状态", c.DeviceID)
		return
	}

	if reason == chatSessionCloseReasonManagerShutdown {
		// manager_shutdown 既可能来自服务端主动 shutdown(true)，也可能来自底层链路
		// 已断开后的资源清理 shutdown(false)。主动 shutdown(true) 场景仍由
		// ServerTransport.Close() 发送 goodbye，避免在被动断连路径误发。
		return
	}

	if c.serverTransport == nil {
		return
	}

	switch c.serverTransport.GetTransportType() {
	case types_conn.TransportTypeWebsocket:
		// websocket 下会话关闭通常意味着整个连接已无意义（没有第二条承载通道），
		// 因此直接关闭整条链路。goodbye 由 ServerTransport.Close() 负责发送。
		if err := c.shutdown(true); err != nil {
			log.Warnf("关闭 websocket transport 失败: %v", err)
		}
	case types_conn.TransportTypeMqttUdp:
		// MQTT 下连接（信令）要保留：只有会话与音频链路需要重建。
		// 保留态超时属于「正常的空闲回收」，不该给设备发 goodbye（设备并没有主动退出对话），
		// 只需标记需要新的 hello，并重置播报链路。
		if reason == chatSessionCloseReasonRetainedIdleTimeout {
			c.setNeedFreshHello(true)
			c.resetSpeakPathAfterServerSessionClose(reason)
			return
		}
		// 其他原因也并非都要发 goodbye：只有明确的退出/致命错误/音频空闲超时才是
		// 「需要告知设备」的终止（见 shouldSendMqttGoodbyeOnSessionClose）。
		if !shouldSendMqttGoodbyeOnSessionClose(reason) {
			return
		}
		c.setNeedFreshHello(true)
		c.resetSpeakPathAfterServerSessionClose(reason)
		if err := c.serverTransport.SendMqttGoodbye(); err != nil {
			log.Warnf("发送 mqtt goodbye 失败: %v", err)
		}
	}
}

// shouldSendMqttGoodbyeOnSessionClose 决定 MQTT 链路下会话关闭时是否需要给设备发 goodbye。
// 白名单方式（而不是黑名单）是刻意的：宁可少发也不能多发给设备造成状态混乱。
//   - explicit_exit：用户/服务端明确结束对话，设备应知道；
//   - fatal_error：发生不可恢复错误，需要让设备结束本轮；
//   - audio_idle_timeout：长时间没声音，主动结束以释放资源。
//
// 而 manager_shutdown（链路本身要关）、retained_idle_timeout（纯服务端空闲回收）都不发。
func shouldSendMqttGoodbyeOnSessionClose(reason string) bool {
	switch reason {
	case chatSessionCloseReasonExplicitExit,
		chatSessionCloseReasonFatalError,
		chatSessionCloseReasonAudioIdleTimeout:
		return true
	default:
		return false
	}
}

// shutdown 关闭 ChatManager。用 closeOnce 保证「关一次」，因为主动关闭与被动断连可能同时到达。
//
// 参数 closeTransport 是理解本函数的关键：
//   - true：主动关闭，连底层链路一起关，并全程置 managerClosing=true，
//     让此时触发的 OnClose 回调知道「这是自己关的」而不再重复清理；
//   - false：底层链路已断开（通过 OnClose 进入），只做本地资源回收，
//     调用 CloseWithoutTransport 避免对已断开的连接再发 goodbye。
//
// 清理顺序：先停保留态定时器 → 关会话（含启动中的）→ 关 MCP → 关钩子 hub →
// 关传输 → 最后 cancel 根上下文（让所有后台 goroutine 退出）。
func (c *ChatManager) shutdown(closeTransport bool) error {
	var shutdownErr error

	c.closeOnce.Do(func() {
		c.cancelRetainedSessionCleanup("manager_shutdown")

		if c.clientState != nil {
			log.Infof("关闭 ChatManager, 设备 %s", c.clientState.DeviceID)
		}

		c.sessionMu.RLock()
		session := c.session
		startingSession := c.startingSession
		c.sessionMu.RUnlock()

		// 会话与「启动中的会话」都要关：只关一个会留下另一条还持有着资源与 goroutine。
		// 判等是为了避免同一条会话被关闭两次（虽然 CloseWithReason 内部有 once 保护）。
		if session != nil {
			session.CloseWithReason(chatSessionCloseReasonManagerShutdown)
		}
		if startingSession != nil && startingSession != session {
			startingSession.CloseWithReason(chatSessionCloseReasonManagerShutdown)
		}

		if c.clientState != nil && c.mcpTransport != nil {
			mcp.CloseDeviceIotOverMcp(c.clientState.DeviceID, c.mcpTransport)
		}

		if c.hookHub != nil {
			c.hookHub.Close()
		}

		if closeTransport {
			// 标记「主动关闭中」并 defer 复位：期间底层触发的 OnClose 回调据此跳过重复回收。
			c.managerClosing.Store(true)
			defer c.managerClosing.Store(false)

			if c.serverTransport != nil {
				shutdownErr = c.serverTransport.Close()
			} else if c.transport != nil {
				shutdownErr = c.transport.Close()
			}
		} else if c.serverTransport != nil {
			if err := c.serverTransport.CloseWithoutTransport(); err != nil {
				log.Warnf("关闭 server transport 包装层失败: %v", err)
			}
		}

		if c.cancel != nil {
			c.cancel()
		}
	})

	return shutdownErr
}

// Close 主动关闭 ChatManager（含底层连接）。
func (c *ChatManager) Close() error {
	return c.shutdown(true)
}

// OnClose 是底层连接断开时（由 transport 回调）的收尾入口。
//
// managerClosing 的判断是防重入的关键：如果是我们主动 shutdown(true) 引起的回调，
// 资源已经清理过了，这里直接返回，避免重复关闭与重复发送 goodbye。
// 若是被动断连（设备掉线等），则走 shutdown(false)，只回收本地资源。
func (c *ChatManager) OnClose(deviceId string) {
	log.Infof("设备 %s 断开连接", deviceId)
	if c.managerClosing.Load() {
		return
	}
	if err := c.shutdown(false); err != nil {
		log.Warnf("连接关闭后的资源清理失败: %v", err)
	}
}

// GetClientState 返回设备共享状态。返回的是内部指针（不加锁），
// 调用方只应读取其中的并发安全字段/方法，不能随意改写结构。
func (c *ChatManager) GetClientState() *ClientState {
	return c.clientState
}

// GetDeviceId 返回设备 ID。
func (c *ChatManager) GetDeviceId() string {
	return c.clientState.DeviceID
}

// GetTransportType 返回「当前确实可用」的传输类型；不可用时返回空串。
//
// 为什么不是直接返回 serverTransport 的类型：MQTT 下连接对象可能还在，但根上下文已取消、
// 或 transport 包装层已关闭、或 broker 已离线。上层（例如 MCP 工具注册）若据此判断设备在线，
// 就必须拿到「真实可用性」而不是「曾经的类型」，否则会给已失联的设备发消息。
func (c *ChatManager) GetTransportType() string {
	if c == nil || c.serverTransport == nil {
		return ""
	}
	if c.ctx != nil && c.ctx.Err() != nil {
		return ""
	}
	if c.serverTransport.IsClosed() {
		return ""
	}
	if awareTransport, ok := c.transport.(brokerOnlineAwareTransport); ok && !awareTransport.IsBrokerOnline() {
		return ""
	}
	return c.serverTransport.GetTransportType()
}

// WarmupMcp 提前触发 MCP runtime 初始化（用于在设备真正用 MCP 前先暖好，降低首次调用延迟）。
// 必须持 helloMu，因为要读写 mcpInitState。
func (c *ChatManager) WarmupMcp() {
	c.helloMu.Lock()
	defer c.helloMu.Unlock()
	c.scheduleMcpInitLocked()
}

// GetSession 返回当前活跃会话（可能为 nil）。
// 用读锁而不是 atomic：session 的写入需要与 startingSession 一起原子地判断，读写锁更自然。
func (c *ChatManager) GetSession() *ChatSession {
	c.sessionMu.RLock()
	defer c.sessionMu.RUnlock()
	return c.session
}

// InjectMessage 让服务端主动「说话」：把一段文本注入对话链路，让设备播报出来。
// 典型用途：后台推送通知、计划任务播报、上层系统想让设备说话。
//
// 与普通对话的区别在于「谁先说话」：正常流程是设备开口（listen），而这里是服务端主动发起，
// 所以必须先确保音频下发通道可用（prepareSpeakPathForInjectedSpeech，MQTT 下要握 speak_request）。
//
// 参数：
//   - message：要播报的文本；
//   - skipLlm：true 直接把文本交给 TTS（跳过 LLM），false 则把文本当作"用户说的话"
//     送进 LLM（用 AddAsrResultToQueue），让模型基于它生成回复；
//   - autoListen：播完后是否保持监听（决定 TTS 轮次结束策略，见 injectedSpeechTTSTurnEndPolicy）。
func (c *ChatManager) InjectMessage(message string, skipLlm bool, autoListen bool) error {
	c.cancelRetainedSessionCleanup("inject_message")
	if err := c.prepareSpeakPathForInjectedSpeech(message, autoListen); err != nil {
		return err
	}
	session, err := c.ensureSession()
	if err != nil {
		return err
	}
	options := llmResponseChannelOptions{
		onTTSPlaybackStart: c.newInjectedSpeechStartHook(),
		ttsTurnEndPolicy:   injectedSpeechTTSTurnEndPolicy(autoListen),
	}
	if skipLlm {
		return session.AddTextToTTSQueueWithOptions(message, options)
	}
	return session.AddAsrResultToQueueWithOptions(message, nil, options)
}

// prepareSpeakPathForInjectedSpeech 在主动播报前把音频下发通道准备好。
//
// 这是「主动播报链路」存在的根本原因：MQTT 链路本身只传信令，音频走 UDP。
// 设备端要先知道 UDP 参数并准备好接收，服务端才能下发音频——这个准备过程就是
// speak_request → 设备回 speak_ready。websocket 不需要这步（通道本来就是常连的）。
//
// 三个分支：
//   - 非 MQTT：直接跳过；
//   - 无需发送（已有热链路可复用）：跳过握手；
//   - 需要发送：发 speak_request 并等待 speak_ready（有超时兜底）。
//
// 另外，如果当前处于「依赖新 hello 才能建会话」的状态，还要额外等会话建立完成，
// 否则音频准备好了却没有会话去承载它。
func (c *ChatManager) prepareSpeakPathForInjectedSpeech(previewText string, autoListen bool) error {
	if c == nil || c.serverTransport == nil {
		return nil
	}
	if c.serverTransport.GetTransportType() != types_conn.TransportTypeMqttUdp {
		log.Debugf("设备 %s 注入消息跳过 speak_request: transport=%s", c.DeviceID, c.serverTransport.GetTransportType())
		return nil
	}
	if !c.shouldSendSpeakRequest(time.Now()) {
		log.Debugf("设备 %s 注入消息复用现有播报链路，跳过 speak_request", c.DeviceID)
		return nil
	}
	if _, err := c.ensureClientSessionID(); err != nil {
		return err
	}

	needSessionBootstrap := c.requiresHelloBootstrapForSession()
	pending, created := c.getOrCreatePendingSpeakRequest()
	if created {
		if err := c.serverTransport.SendSpeakRequest(previewText, autoListen); err != nil {
			c.finishPendingSpeakRequest(pending, err)
			return err
		}
		log.Infof("设备 %s 已发送 speak_request，session_id=%s", c.DeviceID, pending.sessionID)
	}

	waitCtx := c.ctx
	if waitCtx == nil {
		waitCtx = context.Background()
	}
	if err := c.waitPendingSpeakRequest(waitCtx, pending); err != nil {
		return err
	}
	if needSessionBootstrap {
		if err := c.waitForInjectedSpeechSession(waitCtx); err != nil {
			return err
		}
	}
	return nil
}

// shouldSendSpeakRequest 判断本次主动播报是否需要重新做 speak_request 握手。
// 每条判定都对应一种「链路状态不可信/不可复用」的情形：
//   - 需要新 hello 才能建会话 → 旧链路肯定失效，必须重新握手；
//   - MQTT 链路待重建 → 同上；
//   - 当前正处于会话中（监听/LLM/TTS 活动）→ 说明链路本来就是通的，跳过握手以免打断；
//   - 热标记缺失 → 没有可复用的握手记录，要发；
//   - 热标记存在但已超过复用窗口 → 通道可能已被 broker 回收，要重新握手。
func (c *ChatManager) shouldSendSpeakRequest(now time.Time) bool {
	if c == nil || c.serverTransport == nil {
		return false
	}
	if c.serverTransport.GetTransportType() != types_conn.TransportTypeMqttUdp {
		log.Debugf("设备 %s speak_request 判定: transport=%s，无需发送", c.DeviceID, c.serverTransport.GetTransportType())
		return false
	}
	if c.requiresHelloBootstrapForSession() {
		log.Debugf("设备 %s speak_request 判定: ChatSession 依赖新 hello 建立，需发送", c.DeviceID)
		return true
	}
	if c.needsMqttRebootstrap() {
		log.Debugf("设备 %s speak_request 判定: MQTT 链路待重建，需发送", c.DeviceID)
		return true
	}
	if c.isConversationActive() {
		log.Debugf("设备 %s speak_request 判定: 当前处于会话中，跳过发送", c.DeviceID)
		return false
	}

	warmAt := c.currentSpeakPathWarmAt()
	if warmAt <= 0 {
		log.Debugf("设备 %s speak_request 判定: 无可复用热链路，需发送", c.DeviceID)
		return true
	}
	reuseWindow := speakRequestReuseWindow()
	idleFor := now.Sub(time.UnixMilli(warmAt))
	if idleFor <= reuseWindow {
		log.Debugf("设备 %s speak_request 判定: 热链路仍有效 idle_for=%s reuse_window=%s，跳过发送", c.DeviceID, idleFor, reuseWindow)
		return false
	}
	log.Debugf("设备 %s speak_request 判定: 热链路已过期 idle_for=%s reuse_window=%s，需发送", c.DeviceID, idleFor, reuseWindow)
	return true
}

// requiresHelloBootstrapForSession 判断「要建立会话是否必须先等一次新的 hello」。
// 已有会话时返回 false（可复用）；没有会话且 hello 未完成或明确要求新 hello 时返回 true。
func (c *ChatManager) requiresHelloBootstrapForSession() bool {
	if c == nil {
		return false
	}
	if c.GetSession() != nil {
		return false
	}
	if !c.helloInited {
		return true
	}
	return c.requiresFreshHello()
}

// ensureClientSessionID 保证 clientState 上有一个可用的认证会话 ID。
// 主动播报可能发生在设备还没握过 hand 的时候（服务端先说话），此时 SessionID 可能为空，
// 需要先补一个，否则 speak_request 里没有 session_id，设备无法关联这次播报。
func (c *ChatManager) ensureClientSessionID() (string, error) {
	if c == nil || c.clientState == nil {
		return "", fmt.Errorf("clientState 未初始化")
	}
	sessionID := strings.TrimSpace(c.clientState.SessionID)
	if sessionID != "" {
		return sessionID, nil
	}
	session, err := auth.A().CreateSession(c.DeviceID)
	if err != nil {
		return "", fmt.Errorf("创建会话失败: %v", err)
	}
	c.clientState.SessionID = session.ID
	return session.ID, nil
}

// waitForInjectedSpeechSession 轮询等待 ChatSession 建立完成（仅在需要新 hello 时使用）。
//
// 为什么是轮询而不是通知：会话建立是被 hello 消息驱动的（来自收包循环），
// 这里没有可订阅的事件，只能以较小间隔（20ms）重试，并用超时兜底避免永久等待。
// shouldRetryInjectedSpeechSessionWait 决定哪些错误属于「还没好，继续等」，
// 其余错误（无法恢复的）立即返回，不做无意义的重试。
func (c *ChatManager) waitForInjectedSpeechSession(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if _, err := c.ensureSession(); err == nil {
		return nil
	} else if !shouldRetryInjectedSpeechSessionWait(err) {
		return err
	}

	if ctx == nil {
		ctx = context.Background()
	}
	timeout := c.speakReadyTimeout
	if timeout <= 0 {
		timeout = defaultSpeakReadyTimeout
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	for {
		if _, err := c.ensureSession(); err == nil {
			return nil
		} else {
			lastErr = err
			if !shouldRetryInjectedSpeechSessionWait(err) {
				return err
			}
		}

		select {
		case <-waitCtx.Done():
			if waitCtx.Err() == context.DeadlineExceeded {
				if lastErr != nil {
					return fmt.Errorf("等待 ChatSession 建立超时: %w", lastErr)
				}
				return fmt.Errorf("等待 ChatSession 建立超时")
			}
			return waitCtx.Err()
		case <-ticker.C:
		}
	}
}

// shouldRetryInjectedSpeechSessionWait 判断 ensureSession 的错误是否属于「暂时性、可等待重试」。
// 用错误文案匹配是无奈之举（ensureSession 返回的是普通 error），但把可重试的几种
// 中间态（hello 未完成 / 需重发 hello / 正在启动 / 正在关闭）显式列出，语义是清楚的：
// 这些状态都会随时间自行消失。
func shouldRetryInjectedSpeechSessionWait(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "hello尚未初始化") ||
		strings.Contains(msg, "重新发送hello") ||
		strings.Contains(msg, "正在启动") ||
		strings.Contains(msg, "正在关闭")
}

// isConversationActive 判断设备当前是否处于「正在对话」中。
// 这是 shouldSendSpeakRequest 的依据之一：对话进行中说明音频链路一直是通的，
// 主动播报可以直接插入，不必再握手。三种活动迹象：监听相位非 idle、
// 状态处于 listening/llmStart/ttsStart、或会话的 TTS 正在输出。
func (c *ChatManager) isConversationActive() bool {
	if c == nil || c.clientState == nil {
		return false
	}
	if phase := c.clientState.GetListenPhase(); phase != "" && phase != ListenPhaseIdle {
		return true
	}
	switch c.clientState.GetStatus() {
	case ClientStatusListening, ClientStatusLLMStart, ClientStatusTTSStart:
		return true
	}
	session := c.GetSession()
	return session != nil && session.IsTTSActive()
}

// getOrCreatePendingSpeakRequest 取得等待中的播报握手请求；没有就新建一个并启动超时兜底。
//
// 返回 created 表示「本次是新建的」，调用方据此决定是否需要真正发送 speak_request：
// 如果已有待处理请求（created=false），说明已经有人发过了，直接复用同一个请求等待即可，
// 这样并发注入的多条播报不会各自发一次握手。
func (c *ChatManager) getOrCreatePendingSpeakRequest() (*pendingSpeakRequest, bool) {
	c.speakRequestMu.Lock()
	defer c.speakRequestMu.Unlock()

	if c.pendingSpeakRequest != nil {
		return c.pendingSpeakRequest, false
	}

	sessionID := ""
	if c.clientState != nil {
		sessionID = strings.TrimSpace(c.clientState.SessionID)
	}
	pending := &pendingSpeakRequest{
		sessionID: sessionID,
		done:      make(chan struct{}),
	}
	timeout := c.speakReadyTimeout
	if timeout <= 0 {
		timeout = defaultSpeakReadyTimeout
	}
	pending.timer = time.AfterFunc(timeout, func() {
		c.finishPendingSpeakRequest(pending, fmt.Errorf("等待 speak_ready 超时"))
	})
	c.pendingSpeakRequest = pending
	return pending, true
}

// waitPendingSpeakRequest 阻塞等待握手结果。
// 同时监听 ctx：即使请求还没被 resolve（例如上层要求取消），也不会卡住调用方。
func (c *ChatManager) waitPendingSpeakRequest(ctx context.Context, pending *pendingSpeakRequest) error {
	if pending == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-pending.done:
		return pending.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// finishPendingSpeakRequest 结束一次握手：先把它从 ChatManager 上摘下（仅当仍是同一个请求），
// 再 resolve 唤醒等待者。
// 判等很重要：可能已经有人把 pendingSpeakRequest 换成了新请求，那就不能误清掉新请求的状态。
func (c *ChatManager) finishPendingSpeakRequest(pending *pendingSpeakRequest, err error) {
	if pending == nil {
		return
	}
	c.speakRequestMu.Lock()
	if c.pendingSpeakRequest == pending {
		c.pendingSpeakRequest = nil
	}
	c.speakRequestMu.Unlock()
	pending.resolve(err)
}

// refreshSpeakPathWarmFromTransport 在 hello 成功后，依据底层 UDP 绑定情况刷新「热链路」时间戳。
// 为什么优先用 transport 的时间戳而不是 now：它反映的是「上一次真实音频活动」的时刻，
// 更接近通道实际可用的证据，从而让复用窗口的判定更准确。
// 若存在待处理握手或链路待重建，则说明通道状态正在变化，不刷新（避免给出过期的"热"结论）。
func (c *ChatManager) refreshSpeakPathWarmFromTransport() {
	if c == nil || c.serverTransport == nil || !c.serverTransport.HasActiveUDPBinding() {
		return
	}
	if c.hasPendingSpeakRequest() {
		log.Debugf("设备 %s 当前存在待完成 speak_request，跳过刷新热链路", c.DeviceID)
		return
	}
	if c.needsMqttRebootstrap() {
		log.Debugf("设备 %s 当前存在 MQTT 会话重建标记，跳过刷新热链路", c.DeviceID)
		return
	}
	if ts := c.serverTransport.GetUDPLastActiveTs(); ts > 0 {
		c.updateSpeakPathWarmAt(ts)
		return
	}
	c.markSpeakPathWarm(time.Now())
}

// currentSpeakPathWarmAt 取「热链路」时间戳，取本地记录与 transport 上报值的较大者
// （两个来源都可能更新，取最大表示「最近一次确凿的可达证据」）。
func (c *ChatManager) currentSpeakPathWarmAt() int64 {
	if c == nil {
		return 0
	}
	latest := c.lastSpeakPathWarmAt.Load()
	if c.serverTransport != nil {
		if transportTs := c.serverTransport.GetUDPLastActiveTs(); transportTs > latest {
			latest = transportTs
		}
	}
	return latest
}

// markSpeakPathWarm 以「当前时间」标记热链路（零值时间会被替换为 now，避免写入 0 造成误判）。
func (c *ChatManager) markSpeakPathWarm(ts time.Time) {
	if ts.IsZero() {
		ts = time.Now()
	}
	c.updateSpeakPathWarmAt(ts.UnixMilli())
}

// updateSpeakPathWarmAt 用 CAS 单调更新热链路时间戳。
// 为什么要单调（只允许变大）：多个 goroutine 会并发标记（收到 speak_ready、收到音频、
// hello 成功等），若不单调，一个较早的时间戳可能覆盖掉更新的，导致热链路被误判为过期。
func (c *ChatManager) updateSpeakPathWarmAt(ts int64) {
	if c == nil || ts <= 0 {
		return
	}
	for {
		current := c.lastSpeakPathWarmAt.Load()
		if current >= ts {
			return
		}
		if c.lastSpeakPathWarmAt.CompareAndSwap(current, ts) {
			return
		}
	}
}

// speakRequestReuseWindow 读取配置 chat.speak_request_reuse_window_ms，非法值回退到默认 60s。
func speakRequestReuseWindow() time.Duration {
	if !viper.IsSet("chat.speak_request_reuse_window_ms") {
		return defaultSpeakRequestReuseWindow
	}
	ms := viper.GetInt64("chat.speak_request_reuse_window_ms")
	if ms <= 0 {
		return defaultSpeakRequestReuseWindow
	}
	return time.Duration(ms) * time.Millisecond
}

// newInjectedSpeechStartHook 返回一个「TTS 真正开始播放」时只执行一次的回调，
// 用于把链路标记为热（说明音频确实送达过）。
// 用 once 而不是普通函数：注入的这条播报可能被 TTS 分多句处理，只在第一句真正开播时才需要标记。
func (c *ChatManager) newInjectedSpeechStartHook() func() {
	if c == nil {
		return nil
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			if c.serverTransport == nil || c.serverTransport.GetTransportType() != types_conn.TransportTypeMqttUdp {
				return
			}
			c.markSpeakPathWarm(time.Now())
		})
	}
}

// injectedSpeechTTSTurnEndPolicy 决定主动播报结束后要做什么：
//   - 保持监听（autoListen=true）：什么都不做，等设备继续说话；
//   - 不保持监听：走 goodbye + 回到静默（ttsTurnEndPolicyGoodbyeAndIdle），
//     即播完就把会话收起，避免一条「单向播报」长期占着会话资源。
func injectedSpeechTTSTurnEndPolicy(autoListen bool) ttsTurnEndPolicy {
	if autoListen {
		return ttsTurnEndPolicyNone
	}
	return ttsTurnEndPolicyGoodbyeAndIdle
}

// handleTTSTurnEndPolicy 是「TTS 一轮播报结束」后的策略执行者（由 TTS 侧回调，见 withTTSTurnEndPolicyHandler）。
//
// 目前只有一种策略 ttsTurnEndPolicyGoodbyeAndIdle：仅用于 MQTT 链路下的主动播报，
// 语义是「播完就把会话结束掉」。两个细节很关键：
//   - 只有当播放已经落定（ttsTurnPlaybackSettledFromContext）时才直接收尾，
//     否则先等一小段保护时间（ttsPlaybackCompletionGrace），避免客户端尾音未播完就切会话；
//   - 等待期间也要监听 ChatManager 的 ctx 与「是否正在关闭」，因为这是异步回调，
//     期间链路可能已断开，此时不应再操作会话。
func (c *ChatManager) handleTTSTurnEndPolicy(ctx context.Context, policy ttsTurnEndPolicy, stopErr error) {
	if c == nil || policy == ttsTurnEndPolicyNone {
		return
	}
	if stopErr != nil {
		log.Debugf("设备 %s TTS turn end policy skipped: policy=%d err=%v", c.DeviceID, policy, stopErr)
		return
	}

	switch policy {
	case ttsTurnEndPolicyGoodbyeAndIdle:
		if c.serverTransport == nil || c.serverTransport.GetTransportType() != types_conn.TransportTypeMqttUdp {
			return
		}
		if !ttsTurnPlaybackSettledFromContext(ctx) {
			timer := time.NewTimer(ttsPlaybackCompletionGrace)
			defer stopTimer(timer)

			select {
			case <-timer.C:
			case <-c.ctx.Done():
				log.Debugf("设备 %s TTS turn end policy delayed goodbye canceled: %v", c.DeviceID, c.ctx.Err())
				return
			}
		}
		if c.managerClosing.Load() || c.serverTransport == nil || c.serverTransport.GetTransportType() != types_conn.TransportTypeMqttUdp {
			return
		}

		session := c.GetSession()
		if session == nil || session.IsClosing() {
			log.Debugf("设备 %s TTS turn end policy skipped: session already closed", c.DeviceID)
			return
		}
		session.CloseWithReason(chatSessionCloseReasonExplicitExit)
	}
}

// InjectOpenClawResponse 把 OpenClaw 侧产生的回复注入到会话的 TTS 播报链路。
// 先取消保留态清理定时器：OpenClaw 有回复说明设备仍在交互中，不该被回收；
// 再复用普通会话路径（惰性创建会话），保证沿用同一套 TTS/媒体播报机制。
func (c *ChatManager) InjectOpenClawResponse(event openclaw.ResponseDelivery) error {
	c.cancelRetainedSessionCleanup("openclaw_response")
	session, err := c.ensureSession()
	if err != nil {
		return err
	}
	return session.InjectOpenClawResponse(event)
}

// ExitChat 让当前会话执行「退出对话」（说再见语后关闭会话）。
// 无会话时直接返回 nil：没有会话就没有正在进行的对话需要退出。
func (c *ChatManager) ExitChat() error {
	session := c.GetSession()
	if session == nil {
		return nil
	}
	session.DoExitChat()
	return nil
}

// resetOpenClawModeOnHello 在 hello 之后把设备的 OpenClaw 模式重置掉。
// 为什么必须做：OpenClaw 模式是「会话内的临时状态」（按 agentID + deviceID 记录），
// 设备重连/重发 hello 意味着上一段对话已经结束，若不清掉，设备会在新会话里
// 莫名其妙地处于「已进入 OpenClaw 模式」的状态。
// 可传多个 agentID（旧/新），并去重，避免同一次 hello 重复退出。
func (c *ChatManager) resetOpenClawModeOnHello(agentIDs ...string) {
	deviceID := strings.TrimSpace(c.clientState.DeviceID)
	if deviceID == "" {
		return
	}

	openclawManager := openclaw.GetManager()
	seen := make(map[string]struct{}, len(agentIDs))
	for _, agentID := range agentIDs {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		if _, exists := seen[agentID]; exists {
			continue
		}
		seen[agentID] = struct{}{}
		if openclawManager.ExitMode(agentID, deviceID) {
			log.Infof("设备 %s 在 hello 后重置OpenClaw模式: agent=%s", deviceID, agentID)
		}
	}
}

// refreshDeviceConfigOnHello 在（非首次）hello 时重新拉取设备配置并就地刷新 clientState。
// 与 ReloadDeviceConfig 的区别：本函数不处理 OpenClaw 模式退出（由调用方 resetOpenClawModeOnHello 负责），
// 只做「配置 + 派生状态」的刷新：agent、prompt、以及依赖 TTS 类型的输出音频格式。
// SpeakerTTSConfig 置 nil 是因为它由声纹识别结果决定，配置变了必须重新计算。
func (c *ChatManager) refreshDeviceConfigOnHello() error {
	configProvider, err := userconfig.GetProvider(viper.GetString("config_provider.type"))
	if err != nil {
		return fmt.Errorf("获取配置提供者失败: %w", err)
	}

	deviceConfig, err := configProvider.GetUserConfig(c.clientState.Ctx, c.clientState.DeviceID)
	if err != nil {
		return fmt.Errorf("获取设备配置失败: %w", err)
	}
	deviceConfig.MemoryMode = NormalizeMemoryMode(deviceConfig.MemoryMode)
	deviceConfig.SpeakerChatMode = NormalizeSpeakerChatMode(deviceConfig.SpeakerChatMode)

	prevAgentID := c.clientState.AgentID
	c.clientState.AgentID = deviceConfig.AgentId
	c.clientState.DeviceConfig = deviceConfig
	c.clientState.SystemPrompt = deviceConfig.SystemPrompt
	c.clientState.SpeakerTTSConfig = nil
	applyOutputAudioFormatForTTS(c.clientState)

	log.Infof("设备 %s hello 刷新配置成功，agent: %s -> %s", c.clientState.DeviceID, prevAgentID, deviceConfig.AgentId)
	return nil
}
