// session.go 是 chat 包的「会话层」，实现 ChatSession。
//
// 职责：
//   - 承载一次对话从「开始听话」到「回复播完」的完整链路：listen 处理、VAD/ASR 启动、
//     ASR 文本入队、OpenClaw 路由、打断、结束清理；
//   - 持有并协调三个子管理器：ASRManager（音频→文本）、LLMManager（文本→文本/工具）、
//     TTSManager（文本→音频帧），以及声纹识别 SpeakerManager 与媒体播放器 SessionMediaPlayer；
//   - 管理会话级上下文的「代际」（listenStartSeq）与静默/保留态。
//
// 在整体流程中的位置：
//
//	chat.go（握手/消息分发）→ 本文件（会话内流程编排）→ asr.go / llm.go / tts.go（各链路实现）
//
// 与哪些文件配合：
//   - chat.go：ChatSession 由 ChatManager 惰性创建与持有，关闭时通过 closeHandler 回调通知；
//   - common.go：stopSpeaking 系列与 ResetToSilentState 的具体实现在那里；
//   - asr.go / llm.go / tts.go：本文件只做编排与状态判断，具体链路逻辑在各自文件里；
//   - media_player.go / openclaw_warmup.go：媒体播放与 OpenClaw 暖场话术。
//
// 生命周期：一次 listen start 触发一轮「听→答」，而会话本身可跨多轮复用；
// 设备 goodbye 后会话不销毁，而是被重置为静默态并进入保留期（见 chat.go 的 scheduleRetainedSessionCleanup）。
package chat

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
	"github.com/spf13/viper"

	. "xiaozhi-esp32-server-golang/internal/data/client"
	"xiaozhi-esp32-server-golang/internal/data/history"
	. "xiaozhi-esp32-server-golang/internal/data/msg"
	chathooks "xiaozhi-esp32-server-golang/internal/domain/chat/hooks"
	"xiaozhi-esp32-server-golang/internal/domain/chat/streamtransform"
	user_config "xiaozhi-esp32-server-golang/internal/domain/config"
	"xiaozhi-esp32-server-golang/internal/domain/config/types"
	"xiaozhi-esp32-server-golang/internal/domain/eventbus"
	"xiaozhi-esp32-server-golang/internal/domain/llm"
	llm_common "xiaozhi-esp32-server-golang/internal/domain/llm/common"
	"xiaozhi-esp32-server-golang/internal/domain/mcp"
	"xiaozhi-esp32-server-golang/internal/domain/memory"
	"xiaozhi-esp32-server-golang/internal/domain/memory/llm_memory"
	"xiaozhi-esp32-server-golang/internal/domain/openclaw"
	"xiaozhi-esp32-server-golang/internal/domain/speaker"
	"xiaozhi-esp32-server-golang/internal/util"
	log "xiaozhi-esp32-server-golang/logger"
)

// AsrResponseChannelItem 是 chatTextQueue（ASR→LLM 的交接队列）里的元素。
//
// 为什么要连 ctx 一起带上：排队意味着可能等一会儿才被处理，而等待期间「本轮对话」
// 可能已被打断/重置（ctx 被 cancel）。把当时的 ctx 随元素保存，处理时就能通过
// ctx.Done() 判断这条消息是否已经过期，从而丢弃而不是「用旧上下文说新话」。
type AsrResponseChannelItem struct {
	ctx           context.Context
	text          string
	speakerResult *speaker.IdentifyResult
}

// detectLLMDebounceDuration listen detect 到真正送入 LLM 之间的缓冲时长。
// 为什么需要：detect 常是「设备刚听到一点前导语音」的信号，紧接着通常会收到正式的
// listen start。用很短的窗口等一等，让正式监听有机会接管，避免同一句话既被 detect 触发、
// 又被正式 ASR 识别，导致重复回复。
const detectLLMDebounceDuration = 300 * time.Millisecond

// detectAction 是 listen detect 文本被判定后要执行的动作。
type detectAction string

const (
	// detectActionSilent 忽略这段文本（例如重复唤醒词、空文本）。
	detectActionSilent detectAction = "silent"
	// detectActionWelcome 播放欢迎语（首次唤醒）。
	detectActionWelcome detectAction = "welcome"
	// detectActionLLM 按普通文本交给 LLM 处理。
	detectActionLLM detectAction = "llm"
)

// welcomePlaybackResult 表示一次欢迎语播放等待的结局。
// natural=true 表示欢迎语完整播完；false 表示被打断/取消。
// realtime 场景据此决定是否继续进入 listen（见 waitForWelcomePlaybackCompletion）。
type welcomePlaybackResult struct {
	natural bool
}

// ChatSession 关闭原因常量。它们不只是日志文案，还决定了后续动作：
// 例如 chat.go 的 shouldSendMqttGoodbyeOnSessionClose 只对其中一部分发送 mqtt goodbye。
const (
	// chatSessionCloseReasonManagerShutdown ChatManager 整体关闭（连接要断了）。
	chatSessionCloseReasonManagerShutdown = "manager_shutdown"
	// chatSessionCloseReasonExplicitExit 明确退出对话（用户退出词、上层要求退出）。
	chatSessionCloseReasonExplicitExit = "explicit_exit"
	// chatSessionCloseReasonFatalError 不可恢复错误（ASR/链路异常），需要断开会话。
	chatSessionCloseReasonFatalError = "fatal_error"
	// chatSessionCloseReasonAudioIdleTimeout 长时间既无语音也无识别结果，主动回收。
	chatSessionCloseReasonAudioIdleTimeout = "audio_idle_timeout"
	// chatSessionCloseReasonRetainedIdleTimeout 保留态超时（纯服务端空闲回收，设备无关）。
	chatSessionCloseReasonRetainedIdleTimeout = "retained_idle_timeout"
)

// ChatSession 是一次「对话会话」：把 ASR / LLM / TTS / 声纹 / 媒体播放串成一条完整链路。
//
// 与 ChatManager 的关系：ChatManager 管「连接」，ChatSession 管「对话」。
// 一条连接上同一时刻最多一个会话，但会话可以跨多轮复用。
type ChatSession struct {
	// 五个子管理器/依赖，各自拥有独立 goroutine，本结构负责编排它们。
	clientState     *ClientState
	asrManager      *ASRManager
	ttsManager      *TTSManager
	llmManager      *LLMManager
	speakerManager  *SpeakerManager
	mediaPlayer     *SessionMediaPlayer
	serverTransport *ServerTransport

	// ctx 是会话级根上下文：会话内所有后台 goroutine（VAD 循环、文本队列、
	// LLM/TTS 队列处理等）都挂在它下面；cancel 在 CloseWithReason 时调用。
	ctx    context.Context
	cancel context.CancelFunc

	// chatTextQueue 是「ASR 结果 → LLM」的交接队列（容量 10）。
	// 用队列而不是直接调用：ASR 循环不应被 LLM 的耗时阻塞，两者通过队列解耦。
	chatTextQueue *util.Queue[AsrResponseChannelItem]

	// 声纹识别结果暂存（带锁保护）
	//
	// 为什么需要暂存 + 通知：声纹识别是「静音后才开始算」的异步操作（见构造函数中
	// OnVoiceSilenceSpeakerCallback 的回调），而 ASR 最终文本可能先一步到达。
	// 因此这里采用「结果寄存 + 就绪信号」的方式让两条异步链路汇合（见 getSpeakerResult）。
	speakerResultMu        sync.RWMutex
	pendingSpeakerResult   *speaker.IdentifyResult
	speakerResultReady     chan struct{} // 仅用于通知就绪，不传数据
	turnSpeakerInterrupted atomic.Bool   // 本轮是否因声纹相关原因被打断（影响是否允许该说话人继续对话）

	// vadLoopStarted 保证 VAD 常驻循环只启动一次（会话可跨多轮复用，Start 可能被多次调用语义覆盖）。
	vadLoopStarted bool
	// listenStartSeq 是「listen start 代际号」。每次 listen start 自增，
	// 异步的启动流程会沿途校验自己是否仍是当前代际；不是则丢弃结果（见 OnListenStart）。
	// 这样连点/重复的 listen start 不会让旧的启动流程干扰新的。
	listenStartSeq atomic.Uint64
	// realtimeListenSessionActive 标记 realtime 下是否已有一次监听会话在进行，
	// 用于忽略重复的 listen start（realtime 是长驻监听，重复包没有意义）。
	realtimeListenSessionActive atomic.Bool

	// 未激活设备高频触发时，短时间内复用最近一次“未激活”判定，避免频繁打接口。
	// 设备未激活时会不断重发 listen，如果每次都查一次激活状态接口会造成无谓压力。
	activationCheckMu     sync.Mutex
	lastActivationFalseAt time.Time

	// Close 保护，防止多次关闭
	closeOnce sync.Once
	// closing 表示会话正在（或已经）关闭。用 atomic 让任意 goroutine 都能无锁判断。
	closing atomic.Bool

	// stopSpeaking 保护，防止与 AddAsrResultToQueue/HandleWelcome 并发冲突
	//
	// 这把锁的意义：StopSpeaking（打断）与「往文本队列塞新消息」如果交错执行，
	// 可能出现「刚清空队列又被塞进一条」的竞态，于是用同一把锁把两者串起来。
	// AddAsrResultToQueue 还会用 TryLock 来「探测」打断是否正在进行（见该方法）。
	stopSpeakingMu sync.Mutex

	// welcomePlaybackMu 保护 welcomePlaybackDoneCh：欢迎语播放等待通道。
	// 生成新通道时旧通道会被以 natural=false 收尾，保证等待方不会永久阻塞。
	welcomePlaybackMu     sync.Mutex
	welcomePlaybackDoneCh chan welcomePlaybackResult

	// detectLLMDebounceMu 保护 detectLLMDebounceTimer，避免并发重排导致定时器泄漏。
	detectLLMDebounceMu    sync.Mutex
	detectLLMDebounceTimer *time.Timer

	// openClawStreamMu 保护 openClawStreams：OpenClaw 流式回复的「correlation_id → 通道」映射。
	openClawStreamMu sync.Mutex
	openClawStreams  map[string]chan llm_common.LLMResponseStruct

	// openClawWarmupMu 保护 openClawWarmup：等待 OpenClaw 回复期间的暖场话术任务。
	openClawWarmupMu sync.Mutex
	openClawWarmup   *openClawWarmupTask

	// hookHub 事件钩子中心（与 ChatManager 共用同一实例）。
	hookHub *chathooks.Hub
	// closeHandler 会话关闭后的回调，由 ChatManager 注册（见 WithChatSessionCloseHandler），
	// 用于让 ChatManager 摘除会话、决定要不要发 goodbye 等。
	closeHandler func(session *ChatSession, reason string)
}

// ChatSessionOption 新建 ChatSession 时的可选配置（函数式选项）。
type ChatSessionOption func(*ChatSession)

// WithChatSessionCloseHandler 注册会话关闭回调。
// 由 ChatManager 使用，让会话关闭时能通知到「连接层」做后续处理。
func WithChatSessionCloseHandler(handler func(session *ChatSession, reason string)) ChatSessionOption {
	return func(s *ChatSession) {
		s.closeHandler = handler
	}
}

// NewChatSession 构造一个会话。只是「组装对象」，不做任何耗时初始化：
// 真正启动 ASR/LLM/TTS 与后台循环的是 Start，这样构造过程不会阻塞调用方（例如 hello 处理）。
//
// 这里的两个「回调注入」是 chat 包与 data 层解耦的关键：
// ClientState 位于 internal/data/client，不能反向依赖 chat 包，因此它只声明
// 「OnVoiceSilenceMetricCallback / OnAsrFirstTextCallback / OnVoiceSilenceSpeakerCallback」
// 这类函数字段；本构造函数把会话里的具体实现塞进去，形成从底层往上调用的钩子。
func NewChatSession(clientState *ClientState, serverTransport *ServerTransport, hookHub *chathooks.Hub, transformRegistry *streamtransform.Registry, opts ...ChatSessionOption) *ChatSession {
	s := &ChatSession{
		clientState:        clientState,
		serverTransport:    serverTransport,
		chatTextQueue:      util.NewQueue[AsrResponseChannelItem](10),
		speakerResultReady: make(chan struct{}, 1), // 缓冲为1，避免阻塞
		openClawStreams:    make(map[string]chan llm_common.LLMResponseStruct),
		hookHub:            hookHub,
	}
	for _, opt := range opts {
		opt(s)
	}

	s.asrManager = NewASRManager(clientState, serverTransport)
	// 让 ASR 侧能回调会话（异常处理、打断判定等），同时保持 data 层不反向依赖 chat 包。
	s.asrManager.session = s // 设置 session 引用
	s.ttsManager = NewTTSManager(clientState, serverTransport, s)
	s.mediaPlayer = NewSessionMediaPlayer(s)
	s.llmManager = NewLLMManager(clientState, serverTransport, s.ttsManager, s, transformRegistry)

	// 把「静音」指标埋点桥接到会话的 Trace 方法（VAD 判定语音结束时上报一次）。
	clientState.OnVoiceSilenceMetricCallback = func(ctx context.Context, ts int64) {
		s.TraceVoiceSilence(ctx, ts)
	}

	// 如果启用声纹识别，创建声纹管理器
	if clientState.IsSpeakerEnabled() {
		// 从系统配置（viper）获取声纹服务地址
		baseURL := viper.GetString("voice_identify.base_url")
		if baseURL != "" {
			// 设置服务地址和阈值到配置中
			speakerConfig := map[string]interface{}{
				"base_url": baseURL,
			}
			// 读取阈值配置，如果未配置则使用默认值 0.6
			if viper.IsSet("voice_identify.threshold") {
				threshold := viper.GetFloat64("voice_identify.threshold")
				speakerConfig["threshold"] = threshold
			}

			provider, err := speaker.GetSpeakerProvider(speakerConfig)
			if err != nil {
				log.Warnf("创建声纹识别提供者失败: %v", err)
			} else {
				clientState.SpeakerProvider = provider
				s.speakerManager = NewSpeakerManager(provider)
				log.Debugf("设备 %s 启用声纹识别", clientState.DeviceID)

				// 设置异步获取声纹结果的回调
				// 触发时机：VAD 判定「本轮说话结束」时
				// （见 client.ClientState.OnVoiceSilence，那里在收口 ASR 的同时触发它）。
				//
				// 为什么要异步、为什么只存不发：
				//   - 声纹识别要拿本轮音频去算，耗时不可控，若同步等待会拖慢 ASR 收口；
				//   - 而 ASR 最终文本何时到达同样不确定。两者是两条独立的异步链，
				//     因此这里只把结果「寄存」到 pendingSpeakerResult，并关闭 speakerResultReady
				//     作就绪信号，由 ASR 侧在需要时来取（getSpeakerResult 会短暂等待）。
				clientState.OnVoiceSilenceSpeakerCallback = func(ctx context.Context) {
					log.Debugf("[声纹识别] OnVoiceSilenceSpeakerCallback 被调用, deviceID: %s", clientState.DeviceID)

					// 异步获取声纹结果
					go func() {
						log.Debugf("[声纹识别] 开始异步获取声纹识别结果, deviceID: %s", clientState.DeviceID)

						// 检查 speakerManager 是否激活
						if !s.speakerManager.IsActive() {
							//log.Warnf("[声纹识别] speakerManager 未激活，无法获取识别结果")
							return
						}
						// 先清空上一轮可能残留的结果与就绪信号：声纹是「按轮」的概念，
						// 若不清，ASR 侧可能取到上一轮说话人的结果，导致音色/权限判断错人。
						// 清空之前的结果
						s.speakerResultMu.Lock()
						oldResult := s.pendingSpeakerResult
						s.pendingSpeakerResult = nil
						s.speakerResultMu.Unlock()
						if oldResult != nil {
							log.Debugf("[声纹识别] 清空之前的识别结果: identified=%v, speaker_id=%s", oldResult.Identified, oldResult.SpeakerID)
						}

						// 清空就绪通知（非阻塞）
						select {
						case <-s.speakerResultReady:
							log.Debugf("[声纹识别] 清空就绪通知通道")
						default:
							log.Debugf("[声纹识别] 就绪通知通道已为空")
						}

						result, err := s.speakerManager.FinishAndIdentify(ctx)
						if err != nil {
							log.Warnf("[声纹识别] 获取声纹识别结果失败: %v, deviceID: %s", err, clientState.DeviceID)
							// 声纹识别失败不影响主流程，存储 nil 结果
							s.speakerResultMu.Lock()
							s.pendingSpeakerResult = nil
							s.speakerResultMu.Unlock()
							log.Debugf("[声纹识别] 已存储 nil 结果（识别失败）")
						} else if result != nil && result.Identified {
							log.Infof("[声纹识别] 识别到说话人: %s (置信度: %.4f, 阈值: %.4f), deviceID: %s",
								result.SpeakerName, result.Confidence, result.Threshold, clientState.DeviceID)
							log.Debugf("[声纹识别] 识别结果详情: speaker_id=%s, speaker_name=%s, confidence=%.4f, threshold=%.4f",
								result.SpeakerID, result.SpeakerName, result.Confidence, result.Threshold)
							s.speakerResultMu.Lock()
							s.pendingSpeakerResult = result
							s.speakerResultMu.Unlock()
							log.Debugf("[声纹识别] 已存储识别结果（已识别）")
						} else {
							// 未识别到说话人，也存储结果
							if result != nil {
								log.Debugf("[声纹识别] 未识别到说话人: identified=%v, confidence=%.4f, threshold=%.4f, deviceID: %s",
									result.Identified, result.Confidence, result.Threshold, clientState.DeviceID)
							} else {
								log.Debugf("[声纹识别] 识别结果为 nil, deviceID: %s", clientState.DeviceID)
							}
							s.speakerResultMu.Lock()
							s.pendingSpeakerResult = result
							s.speakerResultMu.Unlock()
							log.Debugf("[声纹识别] 已存储识别结果（未识别）")
						}

						// 通知结果就绪
						select {
						case s.speakerResultReady <- struct{}{}:
							log.Debugf("[声纹识别] 已发送结果就绪通知, deviceID: %s", clientState.DeviceID)
						default:
							log.Warnf("[声纹识别] 结果就绪通知通道已满，无法发送通知, deviceID: %s", clientState.DeviceID)
						}
					}()
				}
			}
		}
	}

	// 设置 ASR 首次返回字符的回调
	// 触发时机：ASR 侧拿到第一个非空文本时。
	// 这里的每一步都有明确目的，改动时不要随意删：
	//   - MarkTextReceived：告诉 ASR 循环「本轮有文本了」，用于空结果保护
	//     （避免上游异常时把「一直没结果」误判成需要断连）；
	//   - ClearAudioIdleTimeoutPending / PauseAudioIdleWindow：用户已经在说话了，
	//     必须暂停「长时间无语音就回收会话」的空闲计时，否则会把正常对话误杀；
	//   - MarkAsrFirstText / TraceAsrFirstText：埋点，用于统计首字延迟；
	//   - realtime_mode=4：realtime 模式下「ASR 首字」就打断当前播报（最低延迟的 barge-in）。
	//     但若此时正在播 MCP 媒体（realtime 媒体门控激活），则不能打断——否则音乐会被语音误停。
	clientState.OnAsrFirstTextCallback = func(text string, isFinal bool) {
		clientState.Asr.MarkTextReceived()
		clientState.ClearAudioIdleTimeoutPending()
		clientState.PauseAudioIdleWindow(time.Now())
		log.Debugf("ASR首次返回字符: device=%s, text=%s, isFinal=%v", clientState.DeviceID, text, isFinal)
		clientState.MarkAsrFirstText()
		s.TraceAsrFirstText(clientState.Ctx, time.Now().UnixMilli())
		if clientState.IsRealTime() && viper.GetInt("chat.realtime_mode") == 4 {
			if s.isRealtimeMcpAudioGateActive() {
				log.Debugf("设备 %s realtime媒体播放门控激活，跳过ASR首字打断: text=%s", clientState.DeviceID, text)
				return
			}
			s.StopAssistantOutputAfterAsrWithReason(true, "ChatSession.OnAsrFirstTextCallback realtime_mode=4")
		}
	}

	return s
}

// Start 启动会话：先补上音频格式前置校验，再初始化资源并起一批后台循环。
//
// 前置校验「输入音频格式未初始化」是必要的一道闸：输入格式来自 hello，
// 若没有它，VAD/ASR 无法正确解码音频（会静默地产生垃圾识别结果）。
//
// 三个循环/启动项各自的理由：
//   - runAudioIdleTimeoutWatchdog：会话级空闲看门狗，必须独立于单次 ASR 循环存在，
//     这样 auto 模式在一轮成功结束后仍能继续统计「连接空闲了多久」；
//   - processChatText：消费 chatTextQueue，把 ASR 文本送进 LLM（本文件的 actionDoChat）；
//   - llmManager.Start / ttsManager.Start：消费 LLM 响应队列与 TTS 句子队列；
//   - initHistoryMessages 放异步：加载历史（可能走 Redis/Manager 网络请求）不该拖慢会话启动。
func (s *ChatSession) Start(pctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(pctx)

	if s.clientState.InputAudioFormat.SampleRate <= 0 || s.clientState.InputAudioFormat.Channels <= 0 {
		return fmt.Errorf("输入音频格式未初始化，请先完成 hello 握手")
	}

	err := s.InitAsrLlmTts()
	if err != nil {
		log.Errorf("初始化ASR/LLM/TTS失败: %v", err)
		return err
	}

	// 异步加载历史消息，不阻塞会话启动
	go func() {
		err := s.initHistoryMessages()
		if err != nil {
			log.Errorf("初始化对话历史失败: %v", err)
		}
	}()

	if !s.vadLoopStarted {
		// Session 级 idle watchdog 需要独立于单次 ASR loop 生命周期存在，
		// 这样 auto 模式在一轮成功结束后仍能继续统计连接空闲时间。
		go s.asrManager.runAudioIdleTimeoutWatchdog(s.ctx)
		// [1.1] VAD 识别循环：消费 OpusAudioBuffer → 解码 PCM → VAD 检测 → 喂给 ASR
		s.asrManager.ProcessVadAudio(s.ctx)
		s.vadLoopStarted = true
	}

	// 三条常驻消费循环在此启动，串起「中段 + 下行」：
	//   [3.0] 消费 chatTextQueue（ASR 文本 → LLM 路由）
	//   [3.4] 消费 llmResponseQueue（LLM 响应 → TTS / 工具）
	//   [5.1] 消费 ttsQueue（文本 → 音频帧 → 发送队列）
	go s.processChatText(s.ctx)  //处理 asr后 的对话消息
	go s.llmManager.Start(s.ctx) //处理 llm后 的一系列返回消息
	go s.ttsManager.Start(s.ctx) //处理 tts的 消息队列
	if s.mediaPlayer != nil {
		s.mediaPlayer.AttachSession()
	}

	return nil
}

// 初始化历史对话记录到内存中
//
// 为什么要异步加载：历史可能来自 Redis 或远程 Manager，是网络请求，放到会话启动路径上
// 会直接增加首轮响应延迟，因此 Start 里用 goroutine 调用它。
//
// 数据源由 config_provider.type 决定（redis / manager），两者不是优先级关系而是二选一；
// memory 模式为 none 时直接跳过（省掉无意义的请求）。加载到的消息写入 ClientState 的内存
// Dialogue，后续 LLM 组 prompt 时会用到（见 llm.go）。
func (s *ChatSession) initHistoryMessages() error {
	var historyMessages []*schema.Message
	var err error

	if s.clientState.GetMemoryMode() == MemoryModeNone {
		log.Debugf("设备 %s 记忆模式=none，跳过历史消息加载", s.clientState.DeviceID)
		return nil
	}

	// 根据配置选择数据源（无优先级关系，直接选择）
	useRedis := s.shouldUseRedis()
	useManager := s.shouldUseManager()

	// 验证必要字段：DeviceID 不能为空
	if s.clientState.DeviceID == "" {
		log.Debugf("DeviceID 为空，跳过历史消息加载（可能在 hello 消息之前调用）")
		return nil
	}

	// 根据配置选择数据源（无优先级关系，直接选择）
	if useRedis {
		// 从 Redis 加载
		historyMessages, err = llm_memory.Get().GetMessages(
			s.ctx,
			s.clientState.DeviceID,
			s.clientState.AgentID,
			20)
		if err != nil {
			log.Warnf("从 Redis 加载历史消息失败: %v", err)
			return err
		}
		log.Infof("从 Redis 加载了 %d 条历史消息", len(historyMessages))
	} else if useManager {
		// 从 Manager 加载
		historyMessages, err = s.loadFromManager()
		if err != nil {
			log.Warnf("从 Manager 加载历史消息失败: %v", err)
			return err
		}
		log.Infof("从 Manager 加载了 %d 条历史消息", len(historyMessages))
	} else {
		// 两个数据源都未配置，不加载历史消息
		log.Debugf("Redis 和 Manager 都未配置，跳过历史消息加载")
		return nil
	}

	if len(historyMessages) > 0 {
		s.clientState.InitMessages(historyMessages)
		log.Infof("成功加载 %d 条历史消息", len(historyMessages))
	} else {
		log.Debugf("未加载到历史消息（可能没有历史记录）")
	}

	return nil
}

// shouldUseRedis 判断是否使用 Redis 作为数据源
// shouldUseRedis 判断是否从 Redis 取历史消息（依据 config_provider.type）。
func (s *ChatSession) shouldUseRedis() bool {
	// 根据 config_provider.type 判断
	providerType := viper.GetString("config_provider.type")
	return providerType == "redis"
}

// shouldUseManager 判断是否使用 Manager 作为数据源
// shouldUseManager 判断是否从远程 Manager 取历史消息（依据 config_provider.type）。
func (s *ChatSession) shouldUseManager() bool {
	// 根据 config_provider.type 判断
	providerType := viper.GetString("config_provider.type")
	return providerType == "manager"
}

// loadFromManager 从 Manager 数据库加载历史消息
// loadFromManager 从远程 Manager 的 HTTP 接口拉取最近 20 条历史消息，
// 并转换成 Eino 的 schema.Message 供 LLM 使用。
// 这里逐条 switch role 是为了保证转换出的消息类型正确（user/assistant/tool/system），
// 未知角色直接跳过并告警，避免把脏数据塞进 prompt 造成模型行为异常。
func (s *ChatSession) loadFromManager() ([]*schema.Message, error) {
	// 创建 HistoryClient
	historyCfg := history.HistoryClientConfig{
		BaseURL:   util.GetBackendURL(),
		AuthToken: util.GetManagerAuthToken(),
		Timeout:   viper.GetDuration("manager.history_timeout"),
		Enabled:   true,
	}
	client := history.NewHistoryClient(historyCfg)

	if s.clientState.DeviceID == "" || s.clientState.AgentID == "" {
		return []*schema.Message{}, nil
	}

	req := &history.GetMessagesRequest{
		DeviceID:  s.clientState.DeviceID,
		AgentID:   s.clientState.AgentID,
		SessionID: s.clientState.SessionID,
		Limit:     20,
	}

	resp, err := client.GetMessages(s.ctx, req)
	if err != nil {
		return nil, err
	}

	// 转换为 schema.Message 格式
	messages := make([]*schema.Message, 0, len(resp.Messages))
	for _, item := range resp.Messages {
		var msg *schema.Message
		switch item.Role {
		case "user":
			msg = schema.UserMessage(item.Content)
		case "assistant":
			msg = schema.AssistantMessage(item.Content, item.ToolCalls)
		case "tool":
			msg = schema.ToolMessage(item.Content, item.ToolCallID)
		case "system":
			msg = schema.SystemMessage(item.Content)
		default:
			log.Warnf("未知的消息角色: %s", item.Role)
			continue
		}

		messages = append(messages, msg)
	}

	for _, msg := range messages {
		log.Debugf("历史消息: %+v", msg)
	}

	return messages, nil
}

// 在mqtt 收到type: listen, state: start后进行
//
// InitAsrLlmTts 初始化会话所需的 ASR 结构与记忆（memory）提供者。
//
// 为什么 memory 在这里初始化而不是随连接创建：记忆是按「会话」使用的（要取 memory context
// 注入 prompt），且只有长记忆模式才需要真正建 provider 与加载上下文，
// 非长记忆模式统一降级为 none，避免为不需要的设备付出代价。
func (c *ChatSession) InitAsrLlmTts() error {
	//初始化asr结构
	c.clientState.InitAsr()

	// 初始化memory（memory不在资源池中）
	memoryMode := c.clientState.GetMemoryMode()
	memoryConfig := c.clientState.DeviceConfig.Memory
	memoryType := memory.MemoryType(memoryConfig.Provider)
	if memoryMode != MemoryModeLong {
		memoryType = memory.MemoryTypeNone
	}

	memoryProvider, err := memory.GetProvider(memoryType, memoryConfig.Config)
	if err != nil {
		return fmt.Errorf("创建 Memory 提供者失败: %v", err)
	}
	c.clientState.MemoryProvider = memoryProvider

	if memoryMode == MemoryModeLong {
		// 初始化memory context（仅长记忆模式）
		context, err := memoryProvider.GetContext(c.ctx, c.clientState.GetDeviceIDOrAgentID(), 500)
		if err != nil {
			log.Warnf("初始化memory context失败: %v", err)
		}
		c.clientState.MemoryContext = context
	} else {
		c.clientState.MemoryContext = ""
	}

	return nil
}

// [1.0] 读取音频（入缓冲）：把 Opus 帧塞进 OpusAudioBuffer，等 VAD 循环消费。
//
// HandleAudioMessage 处理音频消息
// 把一帧上行 Opus 音频放进接收缓冲，满了就丢弃并返回 false。
//
// 为什么用「非阻塞发送 + 丢弃」而不是阻塞等待：调用方是 ChatManager 的收包协程，
// 一旦阻塞，后续的控制消息（打断、goodbye）都会延迟处理，问题比丢一帧音频严重得多。
// 音频是可丢的实时数据，丢旧帧好过让整条链路卡住。
func (c *ChatSession) HandleAudioMessage(data []byte) bool {
	select {
	case c.clientState.OpusAudioBuffer <- data:
		return true
	default:
		log.Warnf("音频缓冲区已满, 丢弃音频数据")
	}
	return false
}

// handleListenMessage 处理监听消息
// 按 listen 消息的 state 分发到 start / stop / detect 三个处理分支。
//
// 三个 state 语义不同：start=开始拾音、stop=结束拾音（我方收口）、detect=设备检测到
// 一段可能的前导文本（不等于正式拾音）。因此后面的处理路径也完全不同。
func (s *ChatSession) HandleListenMessage(msg *ClientMessage) error {
	// 根据状态处理
	switch msg.State {
	case MessageStateStart:
		s.HandleListenStart(msg)
	case MessageStateStop:
		s.HandleListenStop()
	case MessageStateDetect:
		s.HandleListenDetect(msg)
	}

	// 记录日志
	log.Infof("设备 %s 更新音频监听状态: %s", msg.DeviceID, msg.State)
	return nil
}

// beginListenStart 开启一次新的 listen 起点：递增代际号、把相位置为 starting，
// 并返回本次的代际号供异步启动流程沿途校验。
//
// 代际号（listenStartSeq）解决的竞态：listen start 是异步处理的（要在 goroutine 里做
// 资源初始化），期间可能又来了新的 listen start 或打断，旧流程若不管不顾地继续，
// 就会把相位/ASR 状态改回旧的样子。因此约定「只有代际号仍等于当前值时才允许推进」。
func (s *ChatSession) beginListenStart() uint64 {
	startSeq := s.listenStartSeq.Add(1)
	if s.clientState.IsRealTime() {
		s.realtimeListenSessionActive.Store(true)
	}
	s.clientState.SetListenPhase(ListenPhaseStarting)
	return startSeq
}

// invalidateListenStart 使当前 listen 会话失效（listen stop 或打断时调用）：
// 递增代际号让在途的异步启动全部作废，取消 realtime 监听标记，并把相位复位为 idle。
func (s *ChatSession) invalidateListenStart() {
	s.listenStartSeq.Add(1)
	s.realtimeListenSessionActive.Store(false)
	s.clientState.SetListenPhase(ListenPhaseIdle)
}

// isCurrentListenStart 判断给定代际号是否仍是当前代际，即「这次启动还没被作废」。
func (s *ChatSession) isCurrentListenStart(startSeq uint64) bool {
	return startSeq == s.listenStartSeq.Load()
}

// isRealtimeListenSessionActive 查询 realtime 监听会话是否已在进行中。
// 用于忽略重复的 realtime listen start（realtime 是长驻监听，重复包只会造成干扰）。
func (s *ChatSession) isRealtimeListenSessionActive() bool {
	return s.realtimeListenSessionActive.Load()
}

// shouldIgnoreListenStartError 判断 listen start 启动过程中的错误是否属于「正常中断、可忽略」。
//
// 三种情况都不是真故障，而是「这轮启动已经被放弃」的正常表现：
//   - 代际已过期（又来了一次 listen start 或被打断）；
//   - 传入的 ctx 已被取消；
//   - 客户端状态 ctx 已取消（链路/会话在收尾）；
//   - 错误本身是 context.Canceled。
//
// 若不区分，就会把用户打断导致的取消当成致命错误，误关会话（进而影响下次唤醒的复用）。
func (s *ChatSession) shouldIgnoreListenStartError(startSeq uint64, ctx context.Context, err error) bool {
	if !s.isCurrentListenStart(startSeq) {
		return true
	}
	if ctx != nil && ctx.Err() != nil {
		return true
	}
	if s.clientState.Ctx.Err() != nil {
		return true
	}
	return errors.Is(err, context.Canceled)
}

// shouldIgnoreAsrLoopError 与 shouldIgnoreListenStartError 同理，
// 用于判断 ASR 识别循环的退出错误是否只是「本轮被重置/退出」而非真故障。
// 二者判断条件相同但职责不同（一个管启动、一个管循环），故分开保留以便日后独立调整。
func (s *ChatSession) shouldIgnoreAsrLoopError(startSeq uint64, ctx context.Context, err error) bool {
	if !s.isCurrentListenStart(startSeq) {
		return true
	}
	if ctx != nil && ctx.Err() != nil {
		return true
	}
	if s.clientState.Ctx.Err() != nil {
		return true
	}
	return errors.Is(err, context.Canceled)
}

// isAutoListenActive 判断 auto 模式下的自动监听是否正在进行
// （相位为 starting 或 listening）。用户已唤醒并处于拾音中时，重复的唤醒词就该被忽略。
func isAutoListenActive(state *ClientState) bool {
	if state == nil || state.ListenMode != "auto" {
		return false
	}
	phase := state.GetListenPhase()
	return phase == ListenPhaseStarting || phase == ListenPhaseListening
}

// shouldIgnoreListenStartDuringWelcome 判断是否应忽略这条 listen start：
// auto/manual 模式下，欢迎语还在播时设备常会自动补发 listen start，
// 这条包不应抢占欢迎语，因此直接忽略。（realtime 走的是「等待欢迎语结束」而非忽略。）
func shouldIgnoreListenStartDuringWelcome(mode string, welcomePlaying bool) bool {
	return mode != "realtime" && welcomePlaying
}

// shouldWaitRealtimeListenStartDuringWelcome 目前恒为 false，
// 即「realtime 首次 listen start 时等待欢迎语播完」这条路径被显式关闭（保留判断函数便于日后恢复）。
// 注意区分：忽略（ignore）与等待（wait）是两种不同策略——忽略是丢弃本条 listen start，
// 等待是推迟本次 listen start 的启动。
func shouldWaitRealtimeListenStartDuringWelcome(mode string, welcomePlaying bool) bool {
	return false
}

// shouldInterruptOutputOnListenStart 判断 listen start 是否需要打断当前输出。
// 唯一的例外是 realtime + 欢迎语播放中：此时不应打断欢迎语（realtime 是长驻监听，
// 设备可能在上电时同时发生欢迎语播报与 listen start，打断会让欢迎语残缺）。
// 其余情况（包括 auto/manual 与 realtime 非欢迎语场景）都返回 true，让「听」优先于「说」。
func shouldInterruptOutputOnListenStart(mode string, welcomePlaying bool) bool {
	if mode == "realtime" && welcomePlaying {
		return false
	}
	return true
}

// completeWelcomePlaybackWaitCh 把结果写入等待通道并关闭它。
// 「先非阻塞发送、再 close」是刻意的组合：
//   - 通道带 1 个缓冲，正常路径下接收方能拿到 natural 值；
//   - close 则保证「即使没人接收」等待方也能立刻解除阻塞，不会永久挂住；
//   - 用 select+default 而非阻塞发送，是为了在缓冲已满时也不卡住调用方。
func completeWelcomePlaybackWaitCh(ch chan welcomePlaybackResult, natural bool) {
	if ch == nil {
		return
	}
	select {
	case ch <- welcomePlaybackResult{natural: natural}:
	default:
	}
	close(ch)
}

// beginWelcomePlaybackWait 开启一次「欢迎语播放等待」：新建等待通道，
// 并把上一轮的旧通道以 natural=false（被打断）收尾。
// 为什么要收尾旧通道：旧等待者还阻塞在它上面，若不管它就会永久挂住。
func (s *ChatSession) beginWelcomePlaybackWait() {
	if s == nil {
		return
	}

	s.welcomePlaybackMu.Lock()
	staleCh := s.welcomePlaybackDoneCh
	s.welcomePlaybackDoneCh = make(chan welcomePlaybackResult, 1)
	s.welcomePlaybackMu.Unlock()

	if staleCh != nil {
		completeWelcomePlaybackWaitCh(staleCh, false)
	}
}

// completeWelcomePlaybackWait 结束当前等待：摘出通道（置 nil，表示不再有等待）
// 并以给定结局唤醒等待方。natural=true 表示欢迎语自然播完，false 表示被打断/取消。
func (s *ChatSession) completeWelcomePlaybackWait(natural bool) {
	if s == nil {
		return
	}

	s.welcomePlaybackMu.Lock()
	ch := s.welcomePlaybackDoneCh
	s.welcomePlaybackDoneCh = nil
	s.welcomePlaybackMu.Unlock()

	completeWelcomePlaybackWaitCh(ch, natural)
}

// currentWelcomePlaybackWaitCh 返回当前等待通道（可能为 nil，表示没有正在等待的欢迎语）。
// 读取时加锁，因为等待通道可能被其他 goroutine 并发替换。
func (s *ChatSession) currentWelcomePlaybackWaitCh() <-chan welcomePlaybackResult {
	if s == nil {
		return nil
	}

	s.welcomePlaybackMu.Lock()
	ch := s.welcomePlaybackDoneCh
	s.welcomePlaybackMu.Unlock()
	return ch
}

// waitForWelcomePlaybackCompletion 阻塞等待欢迎语播放结束，返回「是否可以继续」。
//
// 三种返回 false 的情形都应理解为「本次等待被取消」而不是错误：
//   - 等待通道被关闭（说明有人重新开始了等待/取消了它）；
//   - 欢迎语被打断（natural=false，用户已经开口，继续 listen 更合适）；
//   - 会话/客户端上下文被取消（链路在收尾，没必要继续等）。
//
// 返回 true 表示欢迎语完整播完，调用方可安全地进入 realtime listen。
func (s *ChatSession) waitForWelcomePlaybackCompletion() bool {
	if s == nil {
		return true
	}

	doneCh := s.currentWelcomePlaybackWaitCh()
	if doneCh == nil {
		return true
	}

	var sessionDone <-chan struct{}
	if s.ctx != nil {
		sessionDone = s.ctx.Done()
	}

	log.Infof("设备 %s realtime listen start 等待欢迎语 TTS 结束", s.clientState.DeviceID)

	select {
	case result, ok := <-doneCh:
		if !ok {
			log.Infof("设备 %s 欢迎语等待通道已关闭，取消 realtime listen start", s.clientState.DeviceID)
			return false
		}
		if !result.natural {
			log.Infof("设备 %s 欢迎语被打断，取消本次 realtime listen start", s.clientState.DeviceID)
			return false
		}
		log.Infof("设备 %s 欢迎语播放完成，继续 realtime listen start", s.clientState.DeviceID)
		return true
	case <-s.clientState.Ctx.Done():
		log.Debugf("设备 %s client ctx 已取消，终止 realtime listen start 等待", s.clientState.DeviceID)
		return false
	case <-sessionDone:
		log.Debugf("设备 %s session ctx 已取消，终止 realtime listen start 等待", s.clientState.DeviceID)
		return false
	}
}

// resolveDetectAction 是 listen detect 的核心决策函数，把「一段前导文本」映射成一个动作。
//
// 决策表（为什么这样分）：
//   - 空文本 → silent：没内容可做，直接忽略；
//   - 允许欢迎语 && 是唤醒词：
//     · 还没说过欢迎语 → welcome（首次唤醒，打个招呼）；
//     · 已在 auto 监听中 → silent（重复唤醒，别打断正在进行的拾音）；
//     · 其他（例如欢迎语已说过但当前不在监听）→ llm（可能用户确实在跟助手说话）；
//   - 非唤醒词 → llm（普通内容，交给模型）。
func resolveDetectAction(text string, enableGreeting bool, welcomeAlreadySpoken bool, autoListenActive bool) detectAction {
	if text == "" {
		return detectActionSilent
	}
	if enableGreeting && isWakeupWord(text) {
		if !welcomeAlreadySpoken {
			return detectActionWelcome
		}
		if autoListenActive {
			return detectActionSilent
		}
		return detectActionLLM
	}
	return detectActionLLM
}

// cancelPendingDetectLLM 取消尚未触发的 detect→LLM 去抖任务。
// 新 detect 到来、收到正式 listen start、会话关闭等场景都要调用它，
// 否则旧的前导文本会在稍后被重复送进 LLM 队列（用户会听到重复回答）。
func (s *ChatSession) cancelPendingDetectLLM() {
	if s == nil {
		return
	}

	s.detectLLMDebounceMu.Lock()
	timer := s.detectLLMDebounceTimer
	s.detectLLMDebounceTimer = nil
	s.detectLLMDebounceMu.Unlock()

	if timer != nil {
		timer.Stop()
	}
}

// scheduleDetectLLM 把 detect 文本延迟 detectLLMDebounceDuration 后送入 LLM 队列。
//
// 去抖（debounce）的目的：给紧随其后的正式 listen start 一个「抢跑」的机会。
// 若期间收到了 listen start，正式监听会接管这段语音，本次定时任务要么被取消（新的 detect
// 会 cancel 掉旧的），要么到时发现监听相位已非 idle 而主动跳过。
// 定时器回调里的三重校验（定时器仍是当前那个、会话未关闭、相位仍为 idle）都是为了
// 保证「只有一切都没变时才真正入队」。
func (s *ChatSession) scheduleDetectLLM(text string) {
	if s == nil {
		return
	}

	text = strings.TrimSpace(text)
	if text == "" {
		return
	}

	s.cancelPendingDetectLLM()

	var timer *time.Timer
	timer = time.AfterFunc(detectLLMDebounceDuration, func() {
		s.detectLLMDebounceMu.Lock()
		if s.detectLLMDebounceTimer != timer {
			s.detectLLMDebounceMu.Unlock()
			return
		}
		s.detectLLMDebounceTimer = nil
		s.detectLLMDebounceMu.Unlock()

		if s.IsClosing() || s.clientState == nil {
			return
		}
		if s.clientState.Ctx != nil && s.clientState.Ctx.Err() != nil {
			return
		}

		if phase := s.clientState.GetListenPhase(); phase != ListenPhaseIdle {
			log.Debugf("Detect LLM debounce skipped because listen phase=%s", phase)
			return
		}

		if err := s.AddAsrResultToQueue(text, nil); err != nil {
			log.Errorf("Detect LLM debounce enqueue failed: %v", err)
		}
	})

	s.detectLLMDebounceMu.Lock()
	s.detectLLMDebounceTimer = timer
	s.detectLLMDebounceMu.Unlock()
}

// HandleListenDetect 处理设备的 listen detect 消息（检测到一段可能可用的前导文本）。
//
// 注意它「不是」正式拾音的开始，而是一个探路信号，因此这里只做三件事：
// 检查激活 → 用 resolveDetectAction 决策 → 按动作播欢迎语 / 忽略 / 延迟走 LLM。
// 内部已有详细的行内说明，这里补充整体意图：把「可能即将开始的对话」尽快处理掉，
// 同时用 debounce 与「先取消旧的 detect 任务」来避免与正式 listen start 打架。
func (s *ChatSession) HandleListenDetect(msg *ClientMessage) error {
	// 新的 detect 到来时，先取消上一条尚未触发的 detect->LLM debounce，
	// 避免旧前导文本在稍后被重复送入 LLM 队列。
	s.cancelPendingDetectLLM()

	// 检查设备激活状态
	isActivated, err := s.CheckDeviceActivated()
	if err != nil {
		log.Errorf("检查设备激活状态失败: %v", err)
		return err
	}
	if !isActivated {
		return nil
	}

	// 先拿“本条 detect 到来前”的命令历史快照，再记录当前 detect，
	// 这样后续日志里看到的 history 才是上一条命令，而不是当前这条 detect 自己。
	now := time.Now()
	prevHistory := s.clientState.GetCommandHistorySnapshot()
	s.clientState.RecordCommandArrival(CommandTypeDetect, now)

	// listen detect 代表“设备检测到一段可能可用的前导文本”，
	// 这里不直接进入正式监听，而是先判断它应该触发欢迎语、静默忽略，还是延迟进入 LLM。
	if msg.Text != "" {
		text := removePunctuation(msg.Text)
		enableGreeting := viper.GetBool("enable_greeting")
		autoListenActive := isAutoListenActive(s.clientState)
		// 对唤醒词的处理分三类：
		// 1. 首次唤醒且允许欢迎语：播欢迎语；
		// 2. 欢迎语播过且当前已在 auto listen：忽略重复唤醒；
		// 3. 其他情况：按普通文本走 detect -> LLM 的缓冲路径。
		action := resolveDetectAction(text, enableGreeting, s.clientState.IsWelcomeSpeaking, autoListenActive)

		log.Debugf(
			"Detect recv: device=%s text=%q action=%s autoListenActive=%v history={%s} welcomeSpeaking=%v welcomePlaying=%v",
			msg.DeviceID,
			text,
			action,
			autoListenActive,
			prevHistory.DebugString(now),
			s.clientState.IsWelcomeSpeaking,
			s.clientState.IsWelcomePlaying,
		)

		if action == detectActionSilent {
			return nil
		}

		// detect 决定要播欢迎语或接管对话时，要先停掉当前残留输出，
		// 避免旧一轮 TTS/LLM 与新一轮 detect 动作交叉。
		s.StopSpeakingWithReason(true, fmt.Sprintf("HandleListenDetect action=%s text=%q", action, text))

		if action == detectActionWelcome {
			s.HandleWelcome()
			return nil
		}

		if action == detectActionLLM {
			// detect 阶段的文本先做一个很短的 debounce；
			// 如果后面马上收到 listen start，就会由正式监听接管。
			s.scheduleDetectLLM(text)
		}
	}
	return nil
}

// HandleNotActivated 对未激活设备播报「请先激活」的提示，并给出激活码。
//
// 为什么绕开 LLM 直接走 TTS：设备未激活时不该消耗模型资源，也不需要理解能力，
// 播一句话即可。这里手动 EnqueueTtsStart/Stop 包住 TTS 调用，是为了让设备侧
// 正确进入/退出播报状态（否则端上 UI 会一直显示在播）。
func (s *ChatSession) HandleNotActivated() {
	configProvider, err := user_config.GetProvider(viper.GetString("config_provider.type"))
	if err != nil {
		log.Errorf("获取配置提供者失败: %v", err)
		return
	}

	code, challenge, message, timeoutMs := configProvider.GetActivationInfo(s.clientState.Ctx, s.clientState.DeviceID, "client_id")
	if code == "" {
		log.Errorf("获取激活信息失败: %v", err)
		return
	}

	log.Infof("激活码: %s, 挑战码: %s, 消息: %s, 超时时间: %d", code, challenge, message, timeoutMs)

	s.ttsManager.EnqueueTtsStartWithReason(s.clientState.Ctx, "HandleNotActivated")
	defer s.ttsManager.EnqueueTtsStopWithReason(s.clientState.Ctx, "HandleNotActivated")

	sessionCtx := s.clientState.SessionCtx.Get(s.clientState.Ctx)
	ctx := s.clientState.AfterAsrSessionCtx.Get(sessionCtx)
	err = s.ttsManager.handleTextResponse(ctx, llm_common.LLMResponseStruct{
		Text: fmt.Sprintf("请在后台添加设备，激活码: %s", code),
	}, false)
	s.ttsManager.RequestTurnEnd(ctx, err)

}

// HandleWelcome 播放欢迎语（设备首次唤醒时）。
//
// 几个设计点：
//   - 全程持有 stopSpeakingMu（直到函数返回）：欢迎语是一次「抢占式输出」，
//     必须与打断流程串行，否则可能出现「欢迎语刚开始播就被另一条路径清队列」；
//     注意 goroutine 里的实际播放是在锁释放之后进行的，锁只覆盖状态设置与启动；
//   - IsWelcomeSpeaking / IsWelcomePlaying 两个标记分别表示「说过欢迎语」与「正在播」，
//     后者被 listen start 用来判断要不要忽略/打断；
//   - 使用 AfterAsr 上下文（而非会话上下文）：欢迎语属于「ASR 之后的输出阶段」，
//     打断/重置只需取消该阶段即可，不影响监听本身；
//   - beginWelcomePlaybackWait 建立等待通道，供 realtime 的 listen start 等它播完。
func (s *ChatSession) HandleWelcome() {
	greetingText := s.GetRandomGreeting()

	s.stopSpeakingMu.Lock()
	defer s.stopSpeakingMu.Unlock()

	if s.clientState.Ctx.Err() != nil {
		log.Debugf("HandleWelcome client ctx 已取消，跳过欢迎语")
		return
	}

	sessionCtx := s.clientState.SessionCtx.Get(s.clientState.Ctx)
	ctx := s.clientState.AfterAsrSessionCtx.Get(sessionCtx)
	if ctx.Err() != nil {
		log.Debugf("HandleWelcome afterAsr ctx 已取消，跳过欢迎语")
		return
	}

	s.clientState.IsWelcomeSpeaking = true
	s.clientState.IsWelcomePlaying = true
	s.beginWelcomePlaybackWait()

	go func(ctx context.Context, greetingText string) {
		if ctx.Err() != nil || s.clientState.Ctx.Err() != nil {
			s.completeWelcomePlaybackWait(false)
			return
		}

		s.ttsManager.EnqueueTtsStartWithReason(s.clientState.Ctx, "HandleWelcome")
		err := s.ttsManager.handleTextResponse(ctx, llm_common.LLMResponseStruct{Text: greetingText}, true)
		s.ttsManager.EnqueueTtsStopWithReason(s.clientState.Ctx, "HandleWelcome natural end")
		s.ttsManager.RequestTurnEnd(ctx, err)
	}(ctx, greetingText)
}

// checkExitWords 判断文本是否包含退出词，用于「用户说再见就让会话退出」。
// 用「包含」而不是「相等」是故意的：ASR 结果常带语气词/标点（如「那好吧，再见啊」）。
func (a *ChatSession) checkExitWords(text string) bool {
	exitWords := []string{"再见", "退下吧", "退出", "退出对话"}
	for _, word := range exitWords {
		if strings.Contains(text, word) {
			return true
		}
	}
	return false
}

// normalizeOpenClawKeywordText 归一化关键词文本：转小写、去首尾空白、去标点。
// 关键词匹配必须归一化，否则「进入OpenClaw模式」与「进入 OpenClaw 模式！」会匹配失败。
func normalizeOpenClawKeywordText(text string) string {
	return removePunctuation(strings.ToLower(strings.TrimSpace(text)))
}

// containsOpenClawKeyword 判断文本是否包含任一关键词（双方都先归一化）。
// 空关键词被跳过，避免配置里留了空串导致「什么都匹配」。
func containsOpenClawKeyword(text string, keywords []string) bool {
	normalizedText := normalizeOpenClawKeywordText(text)
	if normalizedText == "" {
		return false
	}
	for _, keyword := range keywords {
		normalizedKeyword := normalizeOpenClawKeywordText(keyword)
		if normalizedKeyword == "" {
			continue
		}
		if strings.Contains(normalizedText, normalizedKeyword) {
			return true
		}
	}
	return false
}

// isOpenClawEnterKeyword 判断是否为「进入 OpenClaw 模式」的触发词（来自设备配置）。
func (s *ChatSession) isOpenClawEnterKeyword(text string) bool {
	return containsOpenClawKeyword(text, s.clientState.DeviceConfig.OpenClaw.EnterKeywords)
}

// isOpenClawExitKeyword 判断是否为「退出 OpenClaw 模式」的触发词（仅在该模式已进入时才有意义）。
func (s *ChatSession) isOpenClawExitKeyword(text string) bool {
	return containsOpenClawKeyword(text, s.clientState.DeviceConfig.OpenClaw.ExitKeywords)
}

// openClawLogSnippet 截取文本前 maxRunes 个字符用于日志。
// 为什么要截断并按「符文」而不是字节：日志里只关心触发词上下文，
// 打全量文本既占空间又可能泄露用户内容；按 rune 截断可避免中文被切成乱码。
func openClawLogSnippet(text string, maxRunes int) string {
	if maxRunes <= 0 {
		return ""
	}
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return ""
	}
	runes := []rune(trimmed)
	if len(runes) <= maxRunes {
		return string(runes)
	}
	return string(runes[:maxRunes]) + "..."
}

// GetRandomGreeting 从配置 greeting_list 里随机取一条欢迎语；未配置则用内置兜底文案。
func (s *ChatSession) GetRandomGreeting() string {
	greetingList := viper.GetStringSlice("greeting_list")
	if len(greetingList) == 0 {
		return "你好，有啥好玩的."
	}
	rand.Seed(time.Now().UnixNano())
	return greetingList[rand.Intn(len(greetingList))]
}

// AddTextToTTSQueue 把文本直接交给 TTS 播报（跳过 LLM）。用于系统提示类播报。
func (s *ChatSession) AddTextToTTSQueue(text string) error {
	return s.llmManager.AddTextToTTSQueue(text)
}

// AddTextToTTSQueueWithOptions 同 AddTextToTTSQueue，但可携带额外选项
// （开播回调、TTS 轮次结束策略），供「主动播报」等场景定制收尾行为。
func (s *ChatSession) AddTextToTTSQueueWithOptions(text string, options llmResponseChannelOptions) error {
	return s.llmManager.AddTextToTTSQueueWithOptions(text, options)
}

// IsTTSActive 查询 TTS 是否正在输出音频（由 TTSManager 的原子标记表示）。
// ChatManager.isConversationActive 用它判断「当前是否处于对话中」。
func (s *ChatSession) IsTTSActive() bool {
	if s == nil || s.ttsManager == nil {
		return false
	}
	return s.ttsManager.ttsActive.Load()
}

// getOrCreateOpenClawStream 取（或首次创建）某个 correlation_id 对应的 OpenClaw 回复流通道。
//
// 背景：OpenClaw 的回复是「按 correlation_id 分片流式」推送的（见 InjectOpenClawResponse），
// 每次分片都要落到同一个 TTS 播报序列里。因此这里以 correlation_id 为单位维护通道 +
// 对应的 LLM 响应处理协程（HandleLLMResponseChannelAsyncWithOptions）。
// 返回的 created 表示本次是否新建：只有新建时才需要做「暖场接管」相关的收尾绑定，
// 重复分片直接往已有通道里写即可。
func (s *ChatSession) getOrCreateOpenClawStream(correlationID string) (chan llm_common.LLMResponseStruct, bool, error) {
	correlationID = strings.TrimSpace(correlationID)
	if correlationID == "" {
		return nil, false, fmt.Errorf("missing correlation_id")
	}

	s.openClawStreamMu.Lock()
	if existing, ok := s.openClawStreams[correlationID]; ok {
		s.openClawStreamMu.Unlock()
		return existing, false, nil
	}
	streamChan := make(chan llm_common.LLMResponseStruct, 16)
	s.openClawStreams[correlationID] = streamChan
	s.openClawStreamMu.Unlock()

	sessionCtx := s.clientState.SessionCtx.Get(s.clientState.Ctx)
	ctx := s.clientState.AfterAsrSessionCtx.Get(sessionCtx)
	options := llmResponseChannelOptions{}
	hasWarmup := s.getOpenClawWarmupTask(correlationID) != nil
	if hasWarmup {
		options.disableTTSCommands = true
		options.onEndFunc = func(err error, args ...any) {
			// 暖场接管了 start，正式 OpenClaw 回复收尾时需要在这里补回 stop；
			// 不能放在暖场切换点发送，否则会把主回复中途截断。
			if !s.clientState.IsRealTime() {
				s.ttsManager.EnqueueTtsStopWithReason(ctx, fmt.Sprintf("OpenClaw stream end correlation_id=%s", correlationID))
			}
			s.ttsManager.RequestTurnEnd(ctx, err)
			s.finishOpenClawWarmup(correlationID, false)
		}
	}
	log.Infof("OpenClaw stream created: device=%s correlation_id=%s warmup_attached=%v", s.clientState.DeviceID, correlationID, hasWarmup)
	if err := s.llmManager.HandleLLMResponseChannelAsyncWithOptions(ctx, nil, streamChan, options); err != nil {
		if hasWarmup && !s.clientState.IsRealTime() {
			s.ttsManager.EnqueueTtsStopWithReason(ctx, fmt.Sprintf("OpenClaw stream setup failed correlation_id=%s", correlationID))
		}
		if hasWarmup {
			s.ttsManager.RequestTurnEnd(ctx, err)
		}
		s.openClawStreamMu.Lock()
		delete(s.openClawStreams, correlationID)
		s.openClawStreamMu.Unlock()
		close(streamChan)
		return nil, false, err
	}

	return streamChan, true, nil
}

// closeOpenClawStream 结束并移除某个 correlation_id 的回复流（正常收尾时调用）。
func (s *ChatSession) closeOpenClawStream(correlationID string) {
	correlationID = strings.TrimSpace(correlationID)
	if correlationID == "" {
		return
	}
	s.openClawStreamMu.Lock()
	delete(s.openClawStreams, correlationID)
	s.openClawStreamMu.Unlock()
}

// clearOpenClawStreams 丢弃所有 OpenClaw 回复流（打断/退出模式/会话清理时调用）。
// 直接整体替换 map 而不是逐个删除：这些流已经作废，不需要保留任何引用。
func (s *ChatSession) clearOpenClawStreams() {
	s.openClawStreamMu.Lock()
	s.openClawStreams = make(map[string]chan llm_common.LLMResponseStruct)
	s.openClawStreamMu.Unlock()
}

// clearPendingSpeakerResult 清空暂存的声纹结果与就绪信号。
// 会话重置/退出时调用，避免把上一轮的说话人身份带到新一轮（会导致音色或权限误判）。
// 就绪通道用循环 select+default 排空，是因为它是缓冲为 1 的信号量语义，排空即「归零」。
func (s *ChatSession) clearPendingSpeakerResult() {
	if s == nil {
		return
	}

	s.speakerResultMu.Lock()
	s.pendingSpeakerResult = nil
	s.speakerResultMu.Unlock()

	for {
		select {
		case <-s.speakerResultReady:
		default:
			return
		}
	}
}

// InjectOpenClawResponse 处理来自 OpenClaw 的一条回复分片（可能带 correlation_id 的流式分片）。
//
// 三种输入形态的差异（都体现在代码分支里）：
//   - 没有 correlation_id：当作一次性单句注入（非流式兜底）；
//   - 空文本且不是结束分片：无意义的中间分片，直接跳过；
//   - 其他：写入对应 correlation_id 的流通道。
//
// 与暖场的配合是关键：如果这条流当初挂了暖场话术（openClawWarmup），
// 那么「第一段真正可播的正文」到达时停掉暖场并把 TTS 交给正式回复——
// 停得太早会被过短的前导分片抢占，停得太晚则暖场和正文会重叠播报。
func (s *ChatSession) InjectOpenClawResponse(event openclaw.ResponseDelivery) error {
	correlationID := strings.TrimSpace(event.CorrelationID)
	text := strings.TrimSpace(event.Text)

	// 非流式兜底：没有 correlation_id 时直接按单句注入。
	if correlationID == "" {
		if text == "" {
			return nil
		}
		return s.AddTextToTTSQueue(text)
	}

	// 中间空分片没有意义，直接跳过；结束空分片保留用于收尾。
	if text == "" && !event.IsEnd {
		return nil
	}

	streamChan, created, err := s.getOrCreateOpenClawStream(correlationID)
	if err != nil {
		return err
	}

	isStart := event.IsStart
	if created && !isStart {
		// 若首个到达分片没有标 start，兜底拉起首段。
		isStart = true
	}
	if isStart {
		if task := s.getOpenClawWarmupTask(correlationID); task != nil {
			if text != "" {
				// 仅在第一段真正可播正文到达时才停掉暖场，避免被过短前导分片过早抢占。
				// 暖场自己的首段标记只用于暖场 TTS，不能吞掉正式回复首段的 IsStart，
				// 否则正式回复会降级成单句 TTS，后续 snapshot 也会被当成第二句再次播报。
				s.cancelOpenClawWarmup(correlationID, false)
				s.beginOpenClawSpeech(task)
			} else {
				isStart = false
			}
		}
	} else if event.IsEnd {
		s.cancelOpenClawWarmup(correlationID, false)
	}

	resp := llm_common.LLMResponseStruct{
		Text:    text,
		IsStart: isStart,
		IsEnd:   event.IsEnd,
	}

	select {
	case <-s.ctx.Done():
		return fmt.Errorf("chat session closed")
	case streamChan <- resp:
	}

	if event.IsEnd {
		s.closeOpenClawStream(correlationID)
	}

	return nil
}

// InterruptAndClearTTSQueue 触发 TTS 打断并清空发送队列（供 realtime 模式 VAD 打断等场景调用）
func (s *ChatSession) InterruptAndClearTTSQueue() {
	s.InterruptAndClearTTSQueueWithReason("ChatSession.InterruptAndClearTTSQueue")
}

// InterruptAndClearTTSQueueWithReason 打断当前 TTS 并清空待发送队列（带原因以便排查）。
//
// 与 StopSpeaking 的区别：这里不取消会话上下文、不清 LLM 队列，只是「让音箱立刻安静」，
// 因此适合 realtime 模式下「用户开口就先别说了」这种轻量打断。
// 先挂起媒体播放再清 TTS 队列，是为了避免媒体通道与 TTS 通道互相穿插。
func (s *ChatSession) InterruptAndClearTTSQueueWithReason(reason string) {
	log.Infof("interrupt and clear tts queue requested: device=%s reason=%s state={%s}", s.clientState.DeviceID, normalizeTTSReason(reason), s.ttsManager.debugState())
	if s.mediaPlayer != nil {
		if err := s.mediaPlayer.Suspend(); err != nil && !errors.Is(err, context.Canceled) {
			log.Warnf("挂起媒体播放失败: %v", err)
		}
	}
	s.ttsManager.ClearTTSQueue()
	s.ttsManager.InterruptAndStopWithReason(s.clientState.Ctx, true, context.Canceled, reason)
}

// handleAbortMessage 处理中止消息
// 即设备发来的 abort（用户主动打断）。
//
// 先取消 detect 去抖任务：abort 意味着本轮不该再有任何延迟产生的输出。
// 然后按模式选择打断强度：
//   - realtime：只停 assistant 输出（不挂起媒体），这样正在播的音乐能继续；
//   - auto/manual：走完整的 StopSpeaking（取消会话上下文、清三条队列），
//     因为这两种模式下打断通常代表「这一轮不要了」。
func (s *ChatSession) HandleAbortMessage(msg *ClientMessage) error {
	s.cancelPendingDetectLLM()

	// 设置打断状态
	s.clientState.Abort = true

	if s.clientState.IsRealTime() {
		s.StopAssistantOutputAfterAsrWithReason(true, "HandleAbortMessage realtime")
	} else {
		s.StopSpeakingWithReason(true, "HandleAbortMessage auto")
	}

	// 记录日志
	log.Infof("设备 %s abort 会话", msg.DeviceID)
	return nil
}

// CheckDeviceActivated 校验设备是否已激活；未激活时顺带触发激活提示播报。
//
// 「短时间节流」是这里的关键设计：未激活设备会高频重发 listen，
// 每次都去查一次激活接口会造成无谓压力（而且结果短时间内不会变），
// 因此在确认未激活后 1 秒内直接复用该结论，跳过实时校验。
// 设备一旦激活成功，会清除节流时间戳并更新 IsActivated，后续不再走这条路径。
func (s *ChatSession) CheckDeviceActivated() (bool, error) {
	if viper.GetBool("auth.enable") {
		if !s.clientState.IsActivated {
			const falseCheckThrottle = time.Second
			s.activationCheckMu.Lock()
			lastFalseAt := s.lastActivationFalseAt
			s.activationCheckMu.Unlock()
			if !lastFalseAt.IsZero() && time.Since(lastFalseAt) < falseCheckThrottle {
				log.Debugf("设备 %s 激活状态仍为未激活，跳过重复实时校验", s.clientState.DeviceID)
				return false, nil
			}

			configProvider, err := user_config.GetProvider(viper.GetString("config_provider.type"))
			if err != nil {
				log.Errorf("获取配置提供者失败: %v", err)
				return false, err
			}
			//调用接口再次确认激活状态
			isActivated, err := configProvider.IsDeviceActivated(s.clientState.Ctx, s.clientState.DeviceID, "client_id")
			if err != nil {
				log.Errorf("获取激活状态失败: %v", err)
				return false, err
			}
			if isActivated {
				s.clientState.IsActivated = true
				s.activationCheckMu.Lock()
				s.lastActivationFalseAt = time.Time{}
				s.activationCheckMu.Unlock()
			} else {
				s.activationCheckMu.Lock()
				s.lastActivationFalseAt = time.Now()
				s.activationCheckMu.Unlock()
				s.HandleNotActivated()
				return false, nil
			}
		}
	}
	return true, nil
}

// [0.5] 开始拾音：listen start 到达，准备进入「上行音频 → ASR」阶段。
//
// HandleListenStart 处理 listen start：开启一轮新的拾音。
//
// 整体思路与顺序：
//  1. 先做激活校验（未激活设备不该进入拾音链路）；
//  2. auto/manual 下若欢迎语正在播，直接忽略本条包（不抢占欢迎语）；
//  3. 按 mode 分流（这是本函数最重要的分野）：
//     · realtime：长驻监听语义。若已有 realtime 监听会话在进行，重复包静默忽略；
//     否则停止当前输出后异步启动监听。
//     · auto/manual：开启一轮新的正式拾音。若上一次启动还在进行中则忽略重复包；
//     否则停止旧输出、启动新监听。
//
// 两个分支最后都通过 beginListenStart() 取得代际号，然后把实际启动放到 goroutine 里，
// 由 OnListenStart 沿途校验代际，保证「启动期间来的新指令」能生效（旧启动会自行作废）。
//
// realtime 与 auto/manual 的本质差异：前者要「尽量不停」，后者是「每轮重新开始」，
// 所以 realtime 跳过 Destroy（保持 ASR 运行），auto/manual 会重建 ASR 与相关状态。
func (s *ChatSession) HandleListenStart(msg *ClientMessage) error {
	s.cancelPendingDetectLLM()

	// 先检查激活状态
	isActivated, err := s.CheckDeviceActivated()
	if err != nil {
		log.Errorf("检查设备激活状态失败: %v", err)
		return err
	}
	if !isActivated {
		return nil
	}

	now := time.Now()
	prevHistory := s.clientState.GetCommandHistorySnapshot()

	// auto/manual 模式下，欢迎语播放期间设备可能会自动补发 listen start；
	// 这类包不应抢占欢迎语，因此欢迎语仍在播放时直接忽略。
	if shouldIgnoreListenStartDuringWelcome(msg.Mode, s.clientState.IsWelcomePlaying) {
		log.Infof("设备 %s 欢迎语播放中，忽略 listen start: history={%s}", msg.DeviceID, prevHistory.DebugString(now))
		return nil
	}

	log.Debugf(
		"ListenStart recv: device=%s mode=%s history={%s} welcomeSpeaking=%v welcomePlaying=%v phase=%s",
		msg.DeviceID,
		msg.Mode,
		prevHistory.DebugString(now),
		s.clientState.IsWelcomeSpeaking,
		s.clientState.IsWelcomePlaying,
		s.clientState.GetListenPhase(),
	)

	// realtime 和 auto/manual 的处理目标不同：
	// realtime 更像“长驻监听会话”，尽量不中断当前链路；
	// auto/manual 更像“开启一轮新的正式拾音”，会重置当前输出并重启 ASR。
	if msg.Mode == "realtime" {
		// 当前 realtime listen 会话尚未走到 listen stop / session cancel / close 时，
		// 重复 listen start 包统一静默忽略，避免打断当前链路。
		if s.clientState.IsRealTime() && s.isRealtimeListenSessionActive() {
			return nil
		}

		// realtime 首次进入时，如果欢迎语还在播，等待它自然结束；
		// 只有欢迎语完整播完，才继续进入 realtime listen。
		if shouldWaitRealtimeListenStartDuringWelcome(msg.Mode, s.clientState.IsWelcomePlaying) {
			if !s.waitForWelcomePlaybackCompletion() {
				return nil
			}
		}

		s.clientState.RecordCommandArrival(CommandTypeListenStart, now)
		if shouldInterruptOutputOnListenStart(msg.Mode, s.clientState.IsWelcomePlaying) {
			// 非欢迎语保护场景下，listen start 代表新一轮监听接管，
			// 需要主动停止当前 TTS/LLM，避免说和听同时进行。
			s.StopSpeakingWithReason(true, fmt.Sprintf("HandleListenStart mode=%s", msg.Mode))
		}

		s.clientState.ListenMode = msg.Mode
		log.Infof("设备 %s 拾音模式: %s", msg.DeviceID, msg.Mode)

		shouldStartAudioIdleWindow := s.clientState.GetListenPhase() != ListenPhaseListening
		startSeq := s.beginListenStart()
		go func() {
			if err := s.OnListenStart(startSeq, shouldStartAudioIdleWindow); err != nil {
				log.Errorf("设备 %s listen start 启动失败: %v", msg.DeviceID, err)
			}
		}()
		return nil
	}

	if s.clientState.GetListenPhase() == ListenPhaseStarting {
		log.Infof("设备 %s listen start 正在启动中，忽略重复 listen start", msg.DeviceID)
		return nil
	}

	s.clientState.RecordCommandArrival(CommandTypeListenStart, now)

	// auto/manual 模式进入这里时，视为显式开启一轮新的拾音流程：
	// 更新模式、停止旧输出，然后异步拉起 OnListenStart 做 ASR 初始化。
	s.clientState.ListenMode = msg.Mode
	log.Infof("设备 %s 拾音模式: %s", msg.DeviceID, msg.Mode)
	s.StopSpeakingWithReason(true, fmt.Sprintf("HandleListenStart mode=%s", msg.Mode))

	startSeq := s.beginListenStart()
	go func() {
		if err := s.OnListenStart(startSeq, true); err != nil {
			log.Errorf("设备 %s listen start 启动失败: %v", msg.DeviceID, err)
		}
	}()

	return nil
}

// HandleListenStop 处理 listen stop：设备表示本轮拾音结束。
//
// 两种模式的处理差异：realtime 需要显式作废监听代际（长驻会话结束，还要清理
// realtimeListenSessionActive 标记，否则下次 listen start 会被当成重复包忽略）；
// auto/manual 不在这里取消会话上下文（注释掉的分支说明曾有此意图），
// 而是交给 OnManualStop 走「收口 ASR 拿结果」的正常路径。
func (s *ChatSession) HandleListenStop() error {
	s.cancelPendingDetectLLM()
	s.clientState.RecordCommandArrival(CommandTypeListenStop, time.Now())
	/*if s.clientState.ListenMode == "auto" {
		s.clientState.CancelSessionCtx()
	}*/

	//调用
	if s.clientState.IsRealTime() {
		s.invalidateListenStart()
	}
	s.clientState.OnManualStop()

	return nil
}

// OnListenStart 真正执行一轮 listen start 的启动：准备上下文/状态，重启 ASR，并跑识别结果循环。
//
// 为什么要带 startSeq（代际号）：本函数在 goroutine 中执行，执行期间可能又来了新的
// listen start、listen stop 或打断。因此每个「可能耗时」的步骤前后都要用
// isCurrentListenStart 校验一次：一旦代际变了就立刻放弃，避免把过期的启动结果写进新状态。
//
// realtime 与 auto/manual 在「上下文/状态准备」上的差异（本函数的第一个大分支）：
//   - realtime：不 Destroy（保持 ASR/VAD 资源常驻），只清空音频缓冲；
//   - auto/manual：Destroy 重置所有会话级状态（ASR 缓冲、VAD、上下文、统计），
//     因为每轮拾音都是全新的，旧状态必须清干净才不会污染识别。
//     这一步还要持 stopSpeakingMu，与打断流程互斥。
//
// 启动成功的判定顺序也有讲究：先 RestartAsrRecognition（拿到 ASR 结果通道），
// 再把相位置为 listening、启动空闲窗口，然后才进入识别结果循环。
// onMessageSave / onError 两个回调分别负责「把用户语音+文本存历史」与「识别循环出错处理」，
// 都带着代际号，因此过期回调会自我作废而不是打扰新的一轮。
func (s *ChatSession) OnListenStart(startSeq uint64, shouldStartAudioIdleWindow bool) error {
	log.Debugf("OnListenStart start")
	defer log.Debugf("OnListenStart end")

	if !s.isCurrentListenStart(startSeq) {
		log.Debugf("OnListenStart stale before init, skip")
		return nil
	}

	select {
	case <-s.clientState.Ctx.Done():
		log.Debugf("OnListenStart Ctx done, return")
		if s.isCurrentListenStart(startSeq) {
			s.clientState.SetListenPhase(ListenPhaseIdle)
		}
		return nil
	default:
	}

	// realtime 模式：跳过 Destroy，保持 ASR 持续运行，但清空 AudioBuffer
	var ctx context.Context
	if s.clientState.IsRealTime() {
		s.clientState.AsrAudioBuffer.ClearAsrAudioData()
	} else {
		s.stopSpeakingMu.Lock()
		if !s.isCurrentListenStart(startSeq) {
			s.stopSpeakingMu.Unlock()
			log.Debugf("OnListenStart stale before destroy, skip")
			return nil
		}
		s.clientState.Destroy()
		if !s.isCurrentListenStart(startSeq) {
			s.stopSpeakingMu.Unlock()
			log.Debugf("OnListenStart stale after destroy, skip")
			return nil
		}

		s.clientState.SetListenPhase(ListenPhaseStarting)
		s.clientState.SetStatus(ClientStatusListening)
		ctx = s.clientState.SessionCtx.Get(s.clientState.Ctx)

		// 初始化 ASR 相关状态需要与会话上下文重建保持一致。
		if s.clientState.ListenMode == "manual" {
			s.clientState.VoiceStatus.SetClientHaveVoice(true)
		}
		s.stopSpeakingMu.Unlock()
	}

	if s.clientState.IsRealTime() {
		s.clientState.SetListenPhase(ListenPhaseStarting)
		s.clientState.SetStatus(ClientStatusListening)
		ctx = s.clientState.SessionCtx.Get(s.clientState.Ctx)

		//初始化asr相关
		if s.clientState.ListenMode == "manual" {
			s.clientState.VoiceStatus.SetClientHaveVoice(true)
		}
	}

	// [1.2] 建 ASR 流：启动asr流式识别（取池化 provider + 建 AsrAudioChannel + StreamingRecognize）
	if !s.isCurrentListenStart(startSeq) {
		log.Debugf("OnListenStart stale before ASR restart, skip")
		return nil
	}
	err := s.asrManager.RestartAsrRecognition(ctx)
	if err != nil {
		if s.shouldIgnoreListenStartError(startSeq, ctx, err) {
			log.Infof("OnListenStart interrupted during ASR restart, ignore err: %v", err)
			if s.isCurrentListenStart(startSeq) {
				s.clientState.SetListenPhase(ListenPhaseIdle)
			}
			return nil
		}

		log.Errorf("asr流式识别失败: %v", err)
		if s.isCurrentListenStart(startSeq) {
			s.clientState.SetListenPhase(ListenPhaseIdle)
		}
		s.CloseWithReason(chatSessionCloseReasonFatalError)
		return err
	}

	if !s.isCurrentListenStart(startSeq) {
		log.Debugf("OnListenStart stale after ASR restart, cancel current start")
		s.clientState.Asr.CancelWithReason("ChatSession.OnListenStart: stale listen start after ASR restart")
		return nil
	}

	s.clientState.SetListenPhase(ListenPhaseListening)
	if shouldStartAudioIdleWindow {
		s.clientState.StartAudioIdleWindow(time.Now())
	}

	// 定义消息保存回调
	// 把「用户这轮的文本 + 对应音频」通过事件总线发布出去，由订阅方落库。
	// 用 eventbus 而不是直接写库的原因：写入属于另一个领域（消息持久化），
	// 会话不应该依赖具体存储；同时这一动作不能阻塞识别循环，交给事件总线异步处理。
	// 注意这里是「一次性保存」（文本与音频同时就绪），所以 IsUpdate=false，
	// 区别于 assistant 消息的两阶段保存（先生成文本、播完再补音频）。
	onMessageSave := func(userMsg *schema.Message, messageID string, audioData []float32) {
		// ASR 文本和音频同时获取，一次性保存（不需要两阶段）
		eventbus.Get().Publish(eventbus.TopicAddMessage, &eventbus.AddMessageEvent{
			ClientState: s.clientState,
			Msg:         *userMsg,
			MessageID:   messageID,
			AudioData:   [][]byte{util.Float32SliceToBytes(audioData)}, // 转换为字节数组
			AudioSize:   len(audioData) * 4,                            // float32 = 4 bytes
			SampleRate:  s.clientState.InputAudioFormat.SampleRate,
			Channels:    s.clientState.InputAudioFormat.Channels,
			IsUpdate:    false, // 一次性保存（文本+音频）
			Timestamp:   time.Now(),
		})
	}

	// 定义错误处理回调
	// 识别循环结束时的收尾。
	// 先判断是否只是「本轮被重置/退出」（shouldIgnoreAsrLoopError），是则安静结束；
	// 否则视为致命错误，关闭会话（带 fatal_error 原因，MQTT 下会据此给设备发 goodbye）。
	// 必须用代际判断，否则旧循环的退出错误会误伤新的一轮。
	onError := func(err error) {
		if s.shouldIgnoreAsrLoopError(startSeq, ctx, err) {
			log.Infof("ASR识别循环在重置/退出中结束，忽略 err: %v", err)
			return
		}
		log.Errorf("ASR识别循环错误: %v", err)
		s.CloseWithReason(chatSessionCloseReasonFatalError)
	}

	// [2.0] 读识别结果：启动ASR识别结果处理循环（资源管理在 ASRManager 内部）
	s.asrManager.StartAsrRecognitionLoop(ctx, onMessageSave, onError)

	return nil
}

// startChat 开始对话
// [2.3] 入队交接（简化版）：委托给 AddAsrResultToQueueWithOptions。
//
// AddAsrResultToQueue 把 ASR 识别结果送进文本队列，交给 LLM 处理（无额外选项的简化版）。
func (s *ChatSession) AddAsrResultToQueue(text string, speakerResult *speaker.IdentifyResult) error {
	return s.AddAsrResultToQueueWithOptions(text, speakerResult, llmResponseChannelOptions{})
}

// [2.3] 入队交接：把 ASR 文本（含声纹结果）推进 chatTextQueue，交给 LLM 阶段。
//
// AddAsrResultToQueueWithOptions 把 ASR 结果（含声纹结果）入队等待 LLM 处理。
//
// 两处关键设计：
//   - TryLock 探测 StopSpeaking：如果打断（stopSpeakingWithLock）正持有 stopSpeakingMu，
//     说明「这一轮正在被清空/取消」，这条文本注定会与清空动作打架，于是直接丢弃。
//     用 TryLock 而不是 Lock 是为了不阻塞识别链路——如果选择等待，等锁释放后队列已被清空，
//     消息仍然是无效的，只是白白卡住 ASR 循环。
//   - ctx 随元素入队（AsrResponseChannelItem.ctx）：处理时用当时的上下文，
//     若中途被打断（ctx 被取消）就能自动丢弃，避免「用旧上下文回答新问题」。
func (s *ChatSession) AddAsrResultToQueueWithOptions(text string, speakerResult *speaker.IdentifyResult, options llmResponseChannelOptions) error {
	log.Debugf("AddAsrResultToQueue text: %s", text)
	if speakerResult != nil && speakerResult.Identified {
		log.Debugf("AddAsrResultToQueue speaker: %s (confidence: %.2f)", speakerResult.SpeakerName, speakerResult.Confidence)
	}

	// 检查 session 是否已被停止（通过尝试获取锁来判断）
	// 如果 StopSpeaking 正在执行，这里会等待；如果已执行完成，tryLock 会立即返回
	if !s.stopSpeakingMu.TryLock() {
		log.Debugf("AddAsrResultToQueue 正在执行 StopSpeaking，丢弃消息")
		return nil
	}
	s.stopSpeakingMu.Unlock()

	sessionCtx := s.clientState.SessionCtx.Get(s.clientState.Ctx)
	// 检查 sessionCtx 是否已取消
	if sessionCtx.Err() != nil {
		log.Debugf("AddAsrResultToQueue sessionCtx 已取消，丢弃消息")
		return nil
	}
	ctx := s.clientState.AfterAsrSessionCtx.Get(sessionCtx)
	ctx = withTTSPlaybackStartHook(ctx, options.onTTSPlaybackStart)
	ctx = withTTSTurnEndPolicy(ctx, options.ttsTurnEndPolicy)

	item := AsrResponseChannelItem{
		ctx:           ctx,
		text:          text,
		speakerResult: speakerResult,
	}
	// [2.3] 入队：ASR 文本进入 chatTextQueue，等待 processChatText 消费
	err := s.chatTextQueue.Push(item)
	if err != nil {
		log.Warnf("chatTextQueue 已满或已关闭, 丢弃消息")
	}
	return nil
}

// [3.0] 取队列：常驻消费 chatTextQueue，逐条取出 ASR 文本。
//
// processChatText 是 chatTextQueue 的常驻消费循环，把 ASR 文本逐条交给 actionDoChat 路由。
//
// Pop(ctx, 0) 的 0 表示「不设等待超时」，即阻塞等待直到有元素或 ctx 结束：
// 这样消费循环空闲时完全不消耗 CPU。只有 ctx 结束（ErrQueueCtxDone）才退出，
// 其他错误仅记日志后继续，避免一次异常就终结整条文本链路。
func (s *ChatSession) processChatText(ctx context.Context) {
	log.Debugf("processChatText start")
	defer log.Debugf("processChatText end")

	for {
		// [3.0] 取队列：阻塞等待下一条 ASR 文本（0 = 不设超时，空闲不耗 CPU）
		item, err := s.chatTextQueue.Pop(ctx, 0)
		if err != nil {
			if err == util.ErrQueueCtxDone {
				return
			}
			continue
		}

		err = s.actionDoChat(item.ctx, item.text, item.speakerResult)
		if err != nil {
			log.Errorf("处理对话失败: %v", err)
			continue
		}
	}
}

// ClearChatTextQueue 清空待处理的 ASR 文本（打断/重置时用）。
// 与 StopSpeaking 配合：队列里的文本代表「还没回答的问题」，打断时它们已失去意义。
func (s *ChatSession) ClearChatTextQueue() {
	s.chatTextQueue.Clear()
}

// DoExitChat 执行退出聊天逻辑（发送再见语并关闭会话）
// 即：播一句再见语，然后关闭会话。
//
// 顺序说明：先把再见语存进历史（免得历史里缺一条），再同步等 TTS 播完
// （handleTextResponse 最后一个参数为 true = 同步），最后才 CloseWithReason(explicit_exit)。
// 必须先播完再关：CloseWithReason 会取消上下文并清空 TTS 队列，
// 若先关闭，这句再见语根本发不出去。关闭原因用 explicit_exit，MQTT 下会据此给设备发 goodbye。
func (s *ChatSession) DoExitChat() {
	// 友好的再见语
	goodbyeText := "好的，再见！期待下次与您聊天～"

	// 保存一条 assistant 角色的消息
	goodbyeMsg := schema.AssistantMessage(goodbyeText, nil)
	if err := s.llmManager.AddLlmMessage(s.clientState.Ctx, goodbyeMsg); err != nil {
		log.Errorf("保存再见消息失败: %v", err)
	}

	// 获取 context
	sessionCtx := s.clientState.SessionCtx.Get(s.clientState.Ctx)
	ctx := s.clientState.AfterAsrSessionCtx.Get(sessionCtx)

	// 发送 TTS 再见语
	s.ttsManager.EnqueueTtsStartWithReason(ctx, "ChatSession.processGoodbye")

	err := s.ttsManager.handleTextResponse(ctx, llm_common.LLMResponseStruct{
		Text:    goodbyeText,
		IsStart: true,
		IsEnd:   true,
	}, true) // 同步处理，等待TTS完成

	if err != nil {
		log.Errorf("发送再见语失败: %v", err)
	}

	s.ttsManager.RequestTurnEnd(ctx, err)
	s.ttsManager.EnqueueTtsStopWithReason(ctx, "ChatSession.processGoodbye")
	// 关闭会话
	s.CloseWithReason(chatSessionCloseReasonExplicitExit)
}

// Close 以 manager_shutdown 为原因关闭会话（ChatManager 整体关闭时使用的简写）。
func (s *ChatSession) Close() {
	s.CloseWithReason(chatSessionCloseReasonManagerShutdown)
}

// IsClosing 判断会话是否正在关闭。nil 接收者也返回 true（视为已关闭），
// 这样调用方拿到 nil 会话时不必额外判空就能安全地「当它已关闭」处理。
func (s *ChatSession) IsClosing() bool {
	if s == nil {
		return true
	}
	return s.closing.Load()
}

// CloseWithReason 关闭会话并释放全部资源；用 closeOnce 保证真正的清理只执行一次
// （打断、超时、连接关闭等多条路径都可能触发关闭）。
//
// 清理顺序刻意安排成这样：
//  1. 先关 ASR（停识别、归还资源池）；
//  2. 再从媒体运行时上「解绑但保留状态」（DetachSession(true)），
//     这样设备重连后还能恢复正在播放的内容；
//  3. 取消 detect 去抖与 session 上下文，让所有后台 goroutine 停下；
//  4. 清空文本队列与 OpenClaw 流；
//  5. stopSpeaking（注意 suspendMedia=false：前面已 Detach，不能再挂起，否则会清掉
//     媒体「重连后自动继续」的标记）；
//  6. 关声纹管理器；
//  7. 发布 TopicSessionEnd 事件（让订阅方做会话级收尾，例如统计/落库）；
//  8. 最后回调 closeHandler，通知 ChatManager 做连接层决策（是否发 goodbye 等）。
func (s *ChatSession) CloseWithReason(reason string) {
	s.closing.Store(true)
	s.closeOnce.Do(func() {
		// 清理ASR资源（资源管理在 ASRManager 内部）
		if s.asrManager != nil {
			s.asrManager.Cleanup()
		}
		deviceID := ""
		if s.clientState != nil {
			deviceID = s.clientState.DeviceID
		}
		log.Debugf("ChatSession.Close() 开始清理会话资源, 设备 %s", deviceID)

		if s.mediaPlayer != nil {
			s.mediaPlayer.DetachSession(true)
		}

		s.cancelPendingDetectLLM()

		// 取消会话级别的上下文
		if s.cancel != nil {
			s.cancel()
		}
		s.finishOpenClawWarmup("", false)

		// 清理聊天文本队列
		s.ClearChatTextQueue()
		s.clearOpenClawStreams()

		// 停止说话和清理音频相关资源。Close 路径前面已经 DetachSession(true)，
		// 这里不要再次 Suspend 媒体，否则会把 resumeOnAttach 清掉。
		s.stopSpeakingWithLock(true, true, false, "ChatSession.Close")

		if s.speakerManager != nil {
			s.speakerManager.Close()
		}

		if s.clientState != nil {
			// 通过事件总线广播「会话结束」，让消息持久化/统计等订阅方收尾。
			// 放最后但仍早于 closeHandler：上层回调可能立即复用/重建会话。
			eventbus.Get().Publish(eventbus.TopicSessionEnd, s.clientState)
		}

		log.Debugf("ChatSession.Close() 会话资源清理完成, 设备 %s", deviceID)

		if s.closeHandler != nil {
			s.closeHandler(s, reason)
		}
	})
}

// [3.1] 路由：决定这段文本走本地播放指令、OpenClaw、退出词，还是进 LLM。
//
// actionDoChat 是「一段用户文本该做什么」的路由表，按优先级依次判断。
//
// 为什么顺序如此（前面的分支会直接 return，越靠前优先级越高）：
//  1. deadline/取消检查：已过期的文本不再处理；
//  2. tryHandleRealtimeMcpAudioASR：realtime 正在播 MCP 音频时，先把文本当作
//     「暂停/继续/下一首/退出」等本地播放指令识别，命中就直接控制播放器，不走 LLM。
//     放在最前是因为音乐控制要求零延迟，且此时把话交给 LLM 反而会打断播放；
//  3. OpenClaw 模式：进入/退出关键词与已进入模式下的消息转发（这是一种「旁路」模式，
//     接管后不该再走普通对话）；
//  4. 退出词 → 发布 TopicExitChat 事件，交给上层（ChatManager）关闭会话，
//     注意这里是「发事件」而不是直接关会话，避免会话层反向依赖连接层；
//  5. switchTTSForSpeaker：按声纹结果切换音色（不同家庭成员用不同声音）；
//  6. 组装可用工具（设备 MCP 工具 + 知识库裁剪）后调用 llmManager.DoLLmRequest。
//
// 关于 realtime 媒体门控的位置：必须在 OpenClaw 与退出词之前，因为它代表「设备此刻
// 在放音频，用户说的很可能是播放指令」，这是比模式判断与退出判断更即时的事实。
func (s *ChatSession) actionDoChat(ctx context.Context, text string, speakerResult *speaker.IdentifyResult) error {
	select {
	case <-ctx.Done():
		log.Debugf("actionDoChat ctx done, return")
		return nil
	default:
	}

	agentID := strings.TrimSpace(s.clientState.AgentID)
	deviceID := strings.TrimSpace(s.clientState.DeviceID)
	openclawSessionID := strings.TrimSpace(s.clientState.SessionID)
	trimmedText := strings.TrimSpace(text)

	handledByRealtimeGate, gateErr := s.tryHandleRealtimeMcpAudioASR(ctx, trimmedText)
	if handledByRealtimeGate {
		return gateErr
	}

	openclawManager := openclaw.GetManager()
	if s.clientState.DeviceConfig.OpenClaw.Allowed {
		isOpenClawMode := openclawManager.IsModeEnabled(agentID, deviceID)
		isEnterKeyword := s.isOpenClawEnterKeyword(text)
		isExitKeyword := false
		if isOpenClawMode {
			isExitKeyword = s.isOpenClawExitKeyword(text)
		}
		log.Debugf(
			"OpenClaw路由判定: agent=%s device=%s session=%s allowed=%v mode=%v enter_keyword=%v exit_keyword=%v text_len=%d text_trim_len=%d text_snippet=%q",
			agentID,
			deviceID,
			openclawSessionID,
			s.clientState.DeviceConfig.OpenClaw.Allowed,
			isOpenClawMode,
			isEnterKeyword,
			isExitKeyword,
			len(text),
			len(trimmedText),
			openClawLogSnippet(trimmedText, 64),
		)
		if isOpenClawMode {
			if isExitKeyword {
				s.finishOpenClawWarmup("", true)
				exited := openclawManager.ExitMode(agentID, deviceID)
				_ = s.AddTextToTTSQueue("已退出OpenClaw模式")
				log.Infof("设备 %s 退出OpenClaw模式: agent=%s exited=%v", deviceID, agentID, exited)
				return nil
			}

			log.Infof(
				"OpenClaw发送STT: agent=%s device=%s session=%s text_len=%d text_snippet=%q",
				agentID,
				deviceID,
				openclawSessionID,
				len(trimmedText),
				openClawLogSnippet(trimmedText, 64),
			)
			s.finishOpenClawWarmup("", true)
			messageID, err := openclawManager.SendMessage(
				agentID,
				deviceID,
				text,
				openclawSessionID,
			)
			if err != nil {
				log.Warnf(
					"设备 %s OpenClaw消息发送失败，已回退普通模式: agent=%s session=%s text_snippet=%q err=%v",
					deviceID,
					agentID,
					openclawSessionID,
					openClawLogSnippet(trimmedText, 64),
					err,
				)
				openclawManager.ExitMode(agentID, deviceID)
				_ = s.AddTextToTTSQueue("OpenClaw当前不可用，已退出OpenClaw模式")
			} else {
				s.startOpenClawWarmup(messageID, text)
				log.Infof("OpenClaw发送STT成功: agent=%s device=%s session=%s message_id=%s", agentID, deviceID, openclawSessionID, messageID)
			}
			return nil
		}

		if isEnterKeyword {
			if !openclawManager.EnterMode(agentID, deviceID) {
				_ = s.AddTextToTTSQueue("OpenClaw当前不可用，请稍后再试")
				log.Warnf("设备 %s 进入OpenClaw模式失败: agent=%s agent session not ready", deviceID, agentID)
				return nil
			}
			_ = s.AddTextToTTSQueue("已进入OpenClaw模式，请继续说")
			log.Infof("设备 %s 进入OpenClaw模式: agent=%s trigger=%q", deviceID, agentID, openClawLogSnippet(trimmedText, 32))
			return nil
		}
		log.Debugf(
			"OpenClaw未接管当前STT: agent=%s device=%s mode=%v enter_keyword=%v text_snippet=%q",
			agentID,
			deviceID,
			isOpenClawMode,
			isEnterKeyword,
			openClawLogSnippet(trimmedText, 64),
		)
	} else {
		s.finishOpenClawWarmup("", false)
		if openclawManager.ExitMode(agentID, deviceID) {
			log.Debugf("OpenClaw配置未开启，已强制退出模式: agent=%s device=%s", agentID, deviceID)
		}
	}

	if s.checkExitWords(text) {
		// 发布退出聊天事件
		eventbus.Get().Publish(eventbus.TopicExitChat, &eventbus.ExitChatEvent{
			ClientState: s.clientState,
			Reason:      "用户主动退出",
			TriggerType: "exit_words",
			UserText:    text,
			Timestamp:   time.Now(),
		})
		return nil
	}

	clientState := s.clientState

	sessionID := clientState.SessionID

	// 声纹识别后动态切换TTS（未识别到时恢复默认TTS）
	if err := s.switchTTSForSpeaker(speakerResult); err != nil {
		log.Warnf("切换TTS失败: %v", err)
		// 不中断流程，继续使用当前TTS
	}

	// 直接创建Eino原生消息
	userMessage := &schema.Message{
		Role:    schema.User,
		Content: text,
	}

	// [3.2] 组装工具：获取全局MCP工具列表（设备 MCP 工具 + 知识库裁剪）
	mcpTools, err := mcp.GetToolsByDeviceIdWithTransport(
		clientState.DeviceID,
		clientState.AgentID,
		s.serverTransport.GetTransportType(),
		clientState.DeviceConfig.MCPServiceNames,
	)
	if err != nil {
		log.Errorf("获取设备 %s 的工具失败: %v", clientState.DeviceID, err)
		mcpTools = make(map[string]tool.InvokableTool)
	}
	if !hasAvailableKnowledgeBase(clientState.DeviceConfig.KnowledgeBases) {
		if _, ok := mcpTools["search_knowledge"]; ok {
			delete(mcpTools, "search_knowledge")
			log.Infof("设备 %s 未关联可用知识库，已移除工具 search_knowledge", clientState.DeviceID)
		}
	}

	// 将MCP工具转换为接口格式以便传递给转换函数
	mcpToolsInterface := make(map[string]interface{})
	for name, tool := range mcpTools {
		mcpToolsInterface[name] = tool
	}

	// 转换MCP工具为Eino ToolInfo格式
	einoTools, err := llm.ConvertMCPToolsToEinoTools(ctx, mcpToolsInterface)
	if err != nil {
		log.Errorf("转换MCP工具失败: %v", err)
		einoTools = nil
	}

	toolNameList := make([]string, 0)
	for _, tool := range einoTools {
		toolNameList = append(toolNameList, tool.Name)
	}

	// [3.3] 请求 LLM：发送带工具的LLM请求（内部会组装消息并起响应消费协程）
	log.Infof("使用 %d 个MCP工具发送LLM请求, tools: %+v", len(einoTools), toolNameList)

	err = s.llmManager.DoLLmRequest(ctx, userMessage, einoTools, true, speakerResult)
	if err != nil {
		log.Errorf("发送带工具的 LLM 请求失败, seesionID: %s, error: %v", sessionID, err)
		return fmt.Errorf("发送带工具的 LLM 请求失败: %v", err)
	}
	return nil
}

// hasAvailableKnowledgeBase 判断设备是否关联了「可用」的知识库。
// 没有可用知识库时会从工具列表里移除 search_knowledge，理由很实际：
// 让模型看见一个必然查不到东西的工具，只会诱导它做无用调用并编造答案。
func hasAvailableKnowledgeBase(knowledgeBases []types.KnowledgeBaseRef) bool {
	for _, kb := range knowledgeBases {
		if strings.EqualFold(strings.TrimSpace(kb.Status), "inactive") {
			continue
		}
		if strings.TrimSpace(kb.ExternalKBID) == "" {
			continue
		}
		return true
	}
	return false
}

// MarkTurnSpeakerInterrupted 标记「本轮因声纹相关原因被打断」。
// 被打断本身就隐含了一个事实：说话人可能不是已登记的人（否则不会打断），
// 因此该标记会影响 ShouldAllowSpeakerChat 的判定。
func (s *ChatSession) MarkTurnSpeakerInterrupted() {
	if s == nil {
		return
	}
	s.turnSpeakerInterrupted.Store(true)
}

// ConsumeTurnSpeakerInterrupted 读取并清除该标记（读后即清，保证只影响当前这一轮）。
func (s *ChatSession) ConsumeTurnSpeakerInterrupted() bool {
	if s == nil {
		return false
	}
	return s.turnSpeakerInterrupted.Swap(false)
}

// ResetTurnSpeakerInterrupted 清除该标记（新一轮开始时调用）。
func (s *ChatSession) ResetTurnSpeakerInterrupted() {
	if s == nil {
		return
	}
	s.turnSpeakerInterrupted.Store(false)
}

// ShouldAllowSpeakerChat 判断当前说话人是否被允许参与对话，并给出拒绝原因。
//
// 两条规则（对应两种配置意图）：
//   - 本轮因声纹被打断、且说话人不是已登记的人 → 拒绝（speaker_interrupt_without_identify）：
//     被打断说明这是个「不请自来」的声音，不该响应；
//   - 设备配置为「仅已登记说话人可对话」（identified_only）→ 未匹配则拒绝。
//
// 返回的第二个值只用于日志/排查，不参与控制流。
func (s *ChatSession) ShouldAllowSpeakerChat(speakerResult *speaker.IdentifyResult, speakerInterrupted bool) (bool, string) {
	if s == nil || s.clientState == nil {
		return true, ""
	}

	matchedConfiguredSpeaker := s.clientState.HasMatchedConfiguredSpeaker(speakerResult)
	if speakerInterrupted && !matchedConfiguredSpeaker {
		return false, "speaker_interrupt_without_identify"
	}

	if s.clientState.RequireMatchedSpeakerForChat() && !matchedConfiguredSpeaker {
		return false, "speaker_chat_mode_identified_only_not_matched"
	}

	return true, ""
}

// switchTTSForSpeaker 为识别的说话人切换TTS
//
// 整体思路：先把 SpeakerTTSConfig 置 nil（回到默认音色），只有「识别成功 + 该说话人
// 配置了自定义音色 + 系统里存在对应 tts 配置」三个条件都满足时才设置新音色。
// 这样任何一步缺失都会自然回退到默认音色，不需要额外的兜底分支。
//
// 两个实现细节的原因：
//   - 配置做深拷贝再存：viper 里的 tts 配置是全局共享的，直接改会污染其他设备/其他说话人；
//   - 音色字段名按 provider 区分（cosyvoice 用 spk_id，其他用 voice）：
//     这是各 TTS provider 的参数约定，统一成一个名字会导致其中一方无效。
func (s *ChatSession) switchTTSForSpeaker(speakerResult *speaker.IdentifyResult) error {
	s.clientState.SpeakerTTSConfig = nil

	// 1. 检查 speakerResult 是否为 nil
	if speakerResult == nil {
		log.Debug("speakerResult 为 nil，清空声纹TTS配置")
		return nil
	}

	// 2. 查找声纹组配置
	speakerGroupInfo, found := s.clientState.DeviceConfig.VoiceIdentify[speakerResult.SpeakerName]
	if !found {
		// 未找到配置，清空声纹TTS配置
		log.Debugf("未找到声纹组 %s 的配置，清空声纹TTS配置", speakerResult.SpeakerName)
		return nil
	}

	// 3. 检查是否配置了自定义音色
	if speakerGroupInfo.TTSConfigID == nil || *speakerGroupInfo.TTSConfigID == "" {
		// 未配置自定义音色，清空声纹TTS配置
		log.Debugf("声纹组 %s 未配置自定义TTS，清空声纹TTS配置", speakerResult.SpeakerName)
		return nil
	}

	// 4. 从系统配置（viper）中查找对应的TTS配置
	var targetTTSConfig *types.TtsConfigItem
	ttsConfigsRaw := viper.Get("tts")
	if ttsConfigsRaw == nil {
		return fmt.Errorf("系统配置中未找到 tts")
	}

	// 解析 tts 配置（现在是一个 map，key 是 config_id）
	if ttsConfigsMap, ok := ttsConfigsRaw.(map[string]interface{}); ok {
		// 查找匹配的 config_id
		if configItem, exists := ttsConfigsMap[*speakerGroupInfo.TTSConfigID]; exists {
			if configMap, ok := configItem.(map[string]interface{}); ok {
				// 解析配置项
				ttsItem := &types.TtsConfigItem{
					ConfigID: *speakerGroupInfo.TTSConfigID,
				}
				if name, ok := configMap["name"].(string); ok {
					ttsItem.Name = name
				}
				if provider, ok := configMap["provider"].(string); ok {
					ttsItem.Provider = provider
				}
				if isDefault, ok := configMap["is_default"].(bool); ok {
					ttsItem.IsDefault = isDefault
				}
				// 配置项的其他字段直接作为 config
				ttsItem.Config = make(map[string]interface{})
				for k, v := range configMap {
					if k != "name" && k != "provider" && k != "is_default" && k != "config_id" {
						ttsItem.Config[k] = v
					}
				}
				targetTTSConfig = ttsItem
			}
		}
	}

	if targetTTSConfig == nil {
		return fmt.Errorf("未找到TTS配置 %s", *speakerGroupInfo.TTSConfigID)
	}

	// 5. 复制TTS配置以避免修改原始配置
	ttsConfig := make(map[string]interface{})
	for k, v := range targetTTSConfig.Config {
		ttsConfig[k] = v
	}

	// 6. 如果配置了音色值，覆盖到TTS配置中
	if speakerGroupInfo.Voice != nil && *speakerGroupInfo.Voice != "" {
		// 根据provider设置对应的音色字段
		if targetTTSConfig.Provider == "cosyvoice" {
			ttsConfig["spk_id"] = *speakerGroupInfo.Voice
		} else {
			ttsConfig["voice"] = *speakerGroupInfo.Voice
		}
		log.Debugf("为说话人 %s 设置音色: %s", speakerResult.SpeakerName, *speakerGroupInfo.Voice)
	}
	if targetTTSConfig.Provider == "aliyun_qwen" &&
		speakerGroupInfo.VoiceModelOverride != nil &&
		strings.TrimSpace(*speakerGroupInfo.VoiceModelOverride) != "" {
		overrideModel := strings.TrimSpace(*speakerGroupInfo.VoiceModelOverride)
		ttsConfig["model"] = overrideModel
		log.Debugf("为说话人 %s 覆盖千问模型: %s", speakerResult.SpeakerName, overrideModel)
	}

	// 7. 保存完整的 TTS 配置（深拷贝）
	s.clientState.SpeakerTTSConfig = make(map[string]interface{})
	for k, v := range ttsConfig {
		s.clientState.SpeakerTTSConfig[k] = v
	}
	// 确保 provider 在 config 中
	s.clientState.SpeakerTTSConfig["provider"] = targetTTSConfig.Provider

	log.Infof("✅ 为说话人 %s 切换TTS配置成功 - Provider: %s, ConfigID: %s, Voice: %v",
		speakerResult.SpeakerName,
		targetTTSConfig.Provider,
		targetTTSConfig.ConfigID,
		speakerGroupInfo.Voice)

	return nil
}

// hookContext 组装钩子调用所需的最小上下文（ctx + 会话 ID + 设备 ID）。
// 注意它同时做了 nil 保护：钩子可能在任何清理阶段被调用，此时会话可能已残缺。
func (s *ChatSession) hookContext(ctx context.Context) chathooks.Context {
	sessionID := ""
	deviceID := ""
	if s != nil && s.clientState != nil {
		sessionID = s.clientState.SessionID
		deviceID = s.clientState.DeviceID
	}

	return chathooks.Context{
		Ctx:       ctx,
		SessionID: sessionID,
		DeviceID:  deviceID,
	}
}

// emitMetricStage 把一次耗时指标阶段上报给钩子中心。
// 所有 Trace* 方法都汇聚到这里，好处是「指标是否上报、失败如何处理」只有一处逻辑：
// 钩子执行失败只记 warn，绝不影响主流程（指标是旁路能力，不该拖垮对话）。
func (s *ChatSession) emitMetricStage(ctx context.Context, stage chathooks.MetricStage, ts int64, err error) {
	if s == nil {
		return
	}

	hookErr := s.hookHub.EmitMetric(s.hookContext(ctx), chathooks.MetricData{Stage: stage, Ts: ts, Err: err})
	if hookErr != nil {
		log.Warnf("METRIC hook 执行失败: stage=%s err=%v", stage, hookErr)
	}
}

// TraceTurnStart 标记「本轮对话开始」，作为整轮耗时的起点。
func (s *ChatSession) TraceTurnStart(ctx context.Context, ts int64) {
	s.emitMetricStage(ctx, chathooks.MetricTurnStart, ts, nil)
}

// TraceTurnEnd 标记「本轮对话结束」，并携带本轮是否出错（用于统计成功率与端到端耗时）。
func (s *ChatSession) TraceTurnEnd(ctx context.Context, ts int64, err error) {
	s.emitMetricStage(ctx, chathooks.MetricTurnEnd, ts, err)
}

// TraceVoiceSilence 标记 VAD 判定「用户说完话了」的时刻，是「说话结束→首字」延迟的分母起点。
func (s *ChatSession) TraceVoiceSilence(ctx context.Context, ts int64) {
	s.emitMetricStage(ctx, chathooks.MetricVoiceSilence, ts, nil)
}

// TraceAsrFirstText 标记 ASR 首次返回文本的时刻，用于衡量 ASR 首字延迟。
func (s *ChatSession) TraceAsrFirstText(ctx context.Context, ts int64) {
	s.emitMetricStage(ctx, chathooks.MetricAsrFirstText, ts, nil)
}

// TraceAsrFinalText 标记 ASR 给出最终文本的时刻，用于衡量 ASR 收口（尾延迟）。
func (s *ChatSession) TraceAsrFinalText(ctx context.Context, ts int64) {
	s.emitMetricStage(ctx, chathooks.MetricAsrFinalText, ts, nil)
}

// TraceLlmStart 标记 LLM 请求发出的时刻。
func (s *ChatSession) TraceLlmStart(ctx context.Context, ts int64) {
	s.emitMetricStage(ctx, chathooks.MetricLlmStart, ts, nil)
}

// TraceLlmFirstToken 标记 LLM 首个 token 到达的时刻（首 token 延迟是体感快慢的关键指标）。
func (s *ChatSession) TraceLlmFirstToken(ctx context.Context, ts int64) {
	s.emitMetricStage(ctx, chathooks.MetricLlmFirstToken, ts, nil)
}

// TraceLlmFirstSentence 标记 LLM 产出第一个完整句子的时刻。
// 它比首 token 更贴近体感：因为 TTS 需要拿到整句才能开播（分句由 streamtransform 插件完成）。
func (s *ChatSession) TraceLlmFirstSentence(ctx context.Context, ts int64) {
	s.emitMetricStage(ctx, chathooks.MetricLlmFirstSentence, ts, nil)
}

// TraceLlmEnd 标记 LLM 一轮输出结束（携带错误信息，便于统计失败率）。
func (s *ChatSession) TraceLlmEnd(ctx context.Context, ts int64, err error) {
	s.emitMetricStage(ctx, chathooks.MetricLlmEnd, ts, err)
}

// TraceTtsStart 标记 TTS 开始合成/发送的时刻。
func (s *ChatSession) TraceTtsStart(ctx context.Context, ts int64) {
	s.emitMetricStage(ctx, chathooks.MetricTtsStart, ts, nil)
}

// TraceTtsFirstFrame 标记 TTS 首个音频帧就绪的时刻（决定「多久听到声音」，最关键的体感指标）。
func (s *ChatSession) TraceTtsFirstFrame(ctx context.Context, ts int64) {
	s.emitMetricStage(ctx, chathooks.MetricTtsFirstFrame, ts, nil)
}

// TraceTtsStop 标记 TTS 一轮播报停止的时刻（携带错误信息，并作为整轮结束耗时的收口点）。
func (s *ChatSession) TraceTtsStop(ctx context.Context, ts int64, err error) {
	s.emitMetricStage(ctx, chathooks.MetricTtsStop, ts, err)
}
