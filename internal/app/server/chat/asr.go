// asr.go —— 上行音频识别链路：VAD（语音活动检测）+ ASR（语音转文字）
//
// 文件职责（把设备送上来的 Opus 音频变成一句文本）：
//  1. 逐帧解码：从 ClientState.OpusAudioBuffer 取 Opus 帧，用 audio.GetAudioProcesser 解码为 PCM float32；
//  2. VAD 判停：判断“用户开始说话 / 还在说 / 说完了”，充当 ASR 的闸门（只有判定有声的帧才会进 ASR）；
//  3. ASR 识别：把 PCM 写入 Asr.AsrAudioChannel，由 ASR provider 流式识别出文本；
//  4. 结果处理：文本 → 声纹补充 → 内存上下文/消息落库 → 推入 chatTextQueue 交给 LLM 阶段。
//
// 核心类型：
//
//	ASRManager —— 挂在 ChatSession 上的“经理”对象，本身不持有 goroutine，方法内部各自起协程：
//	  · ProcessVadAudio           常驻循环：消费 OpusAudioBuffer，做 VAD 并喂音频给 ASR；
//	  · StartAsrRecognitionLoop   常驻循环：消费 ASR 结果，处理文本/重试/声纹/落库/入队；
//	  · RestartAsrRecognition     重建一轮 ASR 流（借池化资源 + 建 channel + 启动 StreamingRecognize）；
//	  · runAudioIdleTimeoutWatchdog 独立看门狗：长时间无语音输入就收口或关闭会话。
//
// 在整体流程中的位置：
//
//	设备连接 → hello 握手 → 上行 Opus 音频 → 【本文件: VAD → ASR】 → chatTextQueue → LLM → TTS → 下行音频
//
// 与其它文件的配合：
//   - session.go：ChatSession 创建本 Manager，并提供 speakerManager（声纹）、hookHub（ASR_OUTPUT 钩子）、
//     CloseWithReason / AddAsrResultToQueue 等能力；
//   - llm.go：不直接调用本文件；ASR 只负责把文本推进聊天文本队列，由 session.go 的 processChatText 消费；
//   - internal/data/client：ClientState 保存音频格式、VAD/ASR 状态机（VoiceStatus、Asr）、指标时间戳；
//     这些字段是 VAD 循环与 ASR 循环之间唯一的共享状态（同一连接内多协程并发读写）；
//   - internal/domain/asr（provider 实现）与 internal/pool（资源池）：识别实例是昂贵的（TCP 长连接/鉴权），
//     所以统一通过 pool.Acquire / pool.Release 借还，而不是每次 new 一个。
//
// 并发要点：两条主循环跑在不同 goroutine 上，通过 ClientState 的状态位（如 ClientHaveVoice、
// ClientVoiceStop、AudioIdleTimeoutPending）以及 AsrAudioChannel / AsrResultChannel 两个 channel 协作。
package chat

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
	. "xiaozhi-esp32-server-golang/internal/data/client"
	"xiaozhi-esp32-server-golang/internal/domain/asr"
	asr_types "xiaozhi-esp32-server-golang/internal/domain/asr/types"
	"xiaozhi-esp32-server-golang/internal/domain/audio"
	chathooks "xiaozhi-esp32-server-golang/internal/domain/chat/hooks"
	"xiaozhi-esp32-server-golang/internal/domain/speaker"
	"xiaozhi-esp32-server-golang/internal/domain/vad/inter"
	"xiaozhi-esp32-server-golang/internal/pool"
	log "xiaozhi-esp32-server-golang/logger"

	"github.com/cloudwego/eino/schema"
	"github.com/spf13/viper"
)

// ASRManagerOption 是“函数式选项”模式：给 NewASRManager 传额外的配置回调。
// 这里目前没有定义具体选项，保留是为了后续扩展（例如注入自定义的 provider）而不改构造函数签名。
type ASRManagerOption func(*ASRManager)

// 首次检测到语音时，最多保留 200 毫秒的前导音频。
// 为什么是 200ms：VAD 判定“有声”之前，声音其实已经开始了几十~上百毫秒（字头/气息）。
// 全部丢掉会截断字头；全部保留又会把大段静音喂给 ASR（浪费算力、甚至让识别提前截断）。
// 200ms 是“保住字头、又不夹带太多静音”的折中值，实际裁剪逻辑见 trimFirstSpeechAudio。
const maxFirstSpeechPreAudioMs = 200

// AsrMessageSaveCallback 消息保存回调函数类型
// 参数：userMsg 用户消息、messageID 服务端生成的消息 ID、audioData 本轮识别用到的音频（用于历史回放）。
// 由 ChatSession 传入，真正落库动作在回调里做，ASRManager 因此不需要知道存储细节（解耦）。
type AsrMessageSaveCallback func(userMsg *schema.Message, messageID string, audioData []float32)

// ASRManager 管理单个会话的上行识别：VAD、ASR 资源、识别结果循环。
//
// 生命周期：由 ChatSession 创建，与 ChatSession 同生命周期；
// 资源（池化的 ASR provider）在 Close/Cleanup 时归还。
//
// 并发：asrResource 会被“识别循环 goroutine”和“外部清理”同时读写，因此用 resourceMu 保护；
// 其余字段在创建后不再变化，属于只读引用。
type ASRManager struct {
	clientState     *ClientState     // 客户端状态
	serverTransport *ServerTransport // 向客户端发消息
	session         *ChatSession     // 所属会话

	asrResource *pool.ResourceWrapper[asr.AsrProvider] // ASR 资源（从池借的）
	resourceMu  sync.RWMutex                           // 保护资源
}

// NewASRManager 创建 ASRManager。此时 session 还未设置（由 ChatSession 在构造后回填），
// 因为两边存在互相引用：ChatSession 需要 ASRManager，ASRManager 也需要 ChatSession 上的
// speakerManager / hookHub 等能力，只能先建对象后补引用（避免构造上的循环依赖）。
func NewASRManager(clientState *ClientState, serverTransport *ServerTransport, opts ...ASRManagerOption) *ASRManager {
	asr := &ASRManager{
		clientState:     clientState,
		serverTransport: serverTransport,
		session:         nil, // 稍后通过 SetSession 设置
	}
	for _, opt := range opts {
		opt(asr)
	}
	return asr
}

// 音频空闲看门狗
//
// 为什么需要它（而不是把超时判断塞进 ProcessVadAudio）：
//   - ProcessVadAudio 只在“有音频帧到达”时才跑，设备长时间不说话时它一直阻塞在 channel 上，
//     根本没有机会检查超时；而且 auto 模式下 ASR 循环会在识别结束后退出、realtime 模式下会重启，
//     任何一个单次循环都不适合承担“整条会话级别的超时”职责；
//   - 所以这里用一个独立的、每秒 tick 的常驻协程，只依赖 ClientState 的状态，不依赖任何单轮流程，
//     从而保证“设备挂机但连接不断”这种情况一定会被处理掉（要么让 ASR 收口，要么直接关会话释放资源）。
//
// 参数 ctx 生命周期：通常等于会话上下文；ctx 取消（会话结束）时本协程退出。
func (a *ASRManager) runAudioIdleTimeoutWatchdog(ctx context.Context) {
	// 客户端的状态数据
	state := a.clientState
	// 没有就没必要检测了
	if state == nil {
		return
	}
	// 周期性定时器
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 以下四组判断都是“前置条件过滤”，任何一个不满足就说明当前不该计时/不该收口：
			// 1) 该设备是否启用空闲时钟、时钟是否已启动、是否被临时暂停（例如正在播报或播放媒体时暂停计数）；
			if !state.UsesAudioIdleClock() || !state.AudioIdleStarted() || state.AudioIdlePaused() {
				continue
			}
			// 2) 是否允许计入超时（realtime 模式等场景可能不允许），以及本轮是否已经收到过 ASR 文本
			//    （收到过文本说明用户在正常交互，不算“空闲”）；
			if !state.ShouldCountAudioIdleTimeout() || state.Asr.HasReceivedText() {
				continue
			}
			// 3) 客户端已主动声明停止说话，或已经有人（VAD 循环 / 上一 tick）标记了超时待处理 —— 避免重复收口；
			if state.GetClientVoiceStop() || state.AudioIdleTimeoutPending() {
				continue
			}

			// 4) 真正比较“空闲了多久”和“阈值多久”。
			elapsed := state.GetAudioIdleElapsed(time.Now())
			threshold := time.Duration(state.GetMaxIdleDuration()) * time.Millisecond
			if elapsed < threshold {
				continue
			}
			// MarkAudioIdleTimeoutPending 内部是原子 CAS：只有一个 tick 能拿到“处理权”，
			// 其余并发路径（VAD 循环、ASR 循环）看到 pending 后不会重复触发收口。
			if !state.MarkAudioIdleTimeoutPending() {
				continue
			}

			// 分两种情况：
			// A. 当前没有活跃的 ASR 输入流（例如 auto 模式下 ASR 循环已退出）→ 没有任何人能“收口”，
			//    只能直接关闭会话（会被上层按“音频空闲超时”原因清理并释放所有资源）。
			if !state.Asr.HasOpenAudioInput() {
				log.Infof(
					"音频空闲超时，当前无活动ASR流，直接关闭会话: device=%s, mode=%s, elapsed=%dms, threshold=%dms",
					state.DeviceID,
					state.ListenMode,
					elapsed.Milliseconds(),
					state.GetMaxIdleDuration(),
				)
				if a.session != nil {
					a.session.CloseWithReason(chatSessionCloseReasonAudioIdleTimeout)
				} else {
					state.ClearAudioIdleTimeoutPending()
				}
				continue
			}

			// B. 有活跃 ASR 流（realtime 长驻）→ 走正常收口：OnVoiceSilence 会让 ASR 产出最终结果，
			//    这样用户“说完就忘”的极端情况也能拿到一次最终文本，而不是静默卡住。
			log.Infof(
				"音频空闲超时，触发ASR收口: device=%s, mode=%s, elapsed=%dms, threshold=%dms",
				state.DeviceID,
				state.ListenMode,
				elapsed.Milliseconds(),
				state.GetMaxIdleDuration(),
			)
			state.OnVoiceSilence()
		}
	}
}

// [1.1] VAD 识别：把 OpusAudioBuffer 里的帧解码成 PCM 并做 VAD，判定「有人说话 / 说完了」。
// ProcessVadAudio 启动VAD音频处理
//
// 这是上行链路的“第一站”，负责把 OpusAudioBuffer 里的音频帧变成 ASR 能用的 PCM，
// 并在此期间完成 VAD 判定。注意它是异步的（内部 go func），调用方（ChatSession）不会阻塞。
//
// 一轮的完整生命周期（以 auto 模式为例）：
//
//	音频帧 → 解码 PCM → VAD 判定有声 → 写 AsrAudioChannel（ASR provider 消费）
//	                → 持续静音超过阈值 → OnVoiceSilence()（通知 ASR “说完了，出最终结果”）
//
// 参数 ctx：会话上下文，取消后本 goroutine 退出（并归还 VAD 资源）。
func (a *ASRManager) ProcessVadAudio(ctx context.Context) {
	state := a.clientState
	// 单独的 goroutine：VAD 是“常驻监听”，不能阻塞调用方（listen start 的处理流程）。
	go func() {
		// hasTriggeredCancel：realtime_mode=1 的 VAD 打断只允许触发一次（打断后要等本轮结束、
		// 检测到静音时才复位），否则一次说话会反复打断多次。
		hasTriggeredCancel := true // 标志位，记录是否已触发过取消操作（当 voiceDuration > 120 时）
		// hasLoggedFirstTextExtendedWait：realtime 下“静音已超时但还没收到 ASR 首个文本”时的日志去重标记，
		// 避免每帧刷屏。
		hasLoggedFirstTextExtendedWait := false
		// 声纹 peek（realtime_mode=3 的打断依据）相关的状态：
		//   speakerInterruptTriggered —— 本轮是否已经因声纹命中而打断过（同样只允许一次）；
		//   speakerPeekInFlight        —— 是否已有一次 peek 请求在执行（防止并发堆积）；
		//   lastSpeakerPeekDoneAt      —— 上次 peek 完成时间（做节流，最小间隔 speakerPeekInterval）；
		//   speakerPeekAudioMs         —— 本轮累计送给声纹服务的音频时长（达到阈值才值得 peek）；
		//   speakerPeekRequestSeq      —— 递增序号，用于生成唯一的 requestID。
		speakerInterruptTriggered := atomic.Bool{}
		speakerPeekInFlight := atomic.Bool{}
		lastSpeakerPeekDoneAt := atomic.Int64{}
		var speakerPeekAudioMs int64
		var speakerPeekRequestSeq uint64
		const speakerPeekInterval = 200 * time.Millisecond
		const firstSpeakerPeekAudioThresholdMs int64 = 400
		// 输入音频格式来自 hello 握手（采样率/声道数），VAD 和声纹都依赖它。
		audioFormat := state.InputAudioFormat
		// 使用一个足够大的缓冲区用于解码（假设最大帧时长为120ms）
		// 先按“最大可能帧”开缓冲，实际帧长由第一帧解码结果 n 推导（设备帧长并不固定）。
		maxFrameSize := audioFormat.SampleRate * audioFormat.Channels * 120 / 1000
		// 音频处理器器对象
		audioProcesser, err := audio.GetAudioProcesser(audioFormat.SampleRate, audioFormat.Channels, 20) // 传入一个默认值用于创建解码器
		if err != nil {
			log.Errorf("获取解码器失败: %v", err)
			return
		}

		// 从第一帧实际数据中获取帧大小和帧时长
		// 这三个值不写死：不同固件/配置的帧长不同（常见 20ms/60ms），
		// 所以第一帧解码后按实际样本数反推，后续帧以此为准（不一致时再修正）。
		var frameSize int
		var frameDurationMs int
		var vadNeedGetCount int // VAD需要的帧数，会在第一帧后计算

		// VAD 资源改为懒加载 + 空闲释放，避免长期独占资源池实例。
		// 原因：VAD 实例（尤其是 silero_vad 这类带模型的）在资源池里是有限且昂贵的，
		// 如果会话一建立就一直占着，多个设备并发时会互相抢不到实例；而 VAD 只在“真有音频”时才需要，
		// 所以这里改成 ensureVad() 借、空闲 2 秒 releaseVad() 还。
		var vadWrapper *pool.ResourceWrapper[inter.VAD]
		var vadProvider inter.VAD
		var vadLastUseAt time.Time
		const vadIdleReleaseTimeout = 2 * time.Second
		vadIdleTicker := time.NewTicker(time.Second)
		defer vadIdleTicker.Stop()
		// needVad 的由来：
		//   · manual 模式：设备端由按键/手动控制说话起止，服务端不需要猜“说完了没有”，跳过 VAD 直接当有声；
		//   · Asr.AutoEnd=true：结束判定由 ASR provider 自己做（如某些提供商的自动断句），也跳过 VAD；
		//   · 其余（auto 模式）= 需要 VAD。
		needVad := !(state.Asr.AutoEnd || state.ListenMode == "manual")
		vadProviderName := state.DeviceConfig.Vad.Provider
		vadProviderConfig := state.DeviceConfig.Vad.Config
		// 兼容两种配置写法：provider 既可能写在 Vad.Provider，也可能写在 Vad.Config["provider"] 里。
		effectiveVadProviderName := vadProviderName
		if configProvider, ok := vadProviderConfig["provider"].(string); ok && configProvider != "" {
			effectiveVadProviderName = configProvider
		}
		// silero_vad 自己维护内部状态（不需要/不支持每帧 Reset），需要区别对待（见下面的分支）。
		isSileroVAD := effectiveVadProviderName == "silero_vad"
		// releaseVad 归还 VAD 资源并把局部变量置空，保证可以安全重复调用（幂等）。
		releaseVad := func(reason string) {
			if vadWrapper == nil {
				return
			}
			pool.Release(vadWrapper)
			vadWrapper = nil
			vadProvider = nil
			vadLastUseAt = time.Time{}
			log.Debugf("释放VAD资源: device=%s, reason=%s", state.DeviceID, reason)
		}
		defer releaseVad("process_exit")
		// ensureVad：懒加载入口。返回 false 表示“当前拿不到 VAD”，调用方直接跳过这一帧（不喂 ASR），
		// 而不是让整条循环退出——VAD 资源只是暂时不可用，不应该打断整段会话。
		ensureVad := func() bool {
			if !needVad {
				return false
			}
			if vadProvider != nil {
				return true
			}

			// 检查 provider 是否为空，如果为空则记录警告
			if vadProviderName == "" {
				log.Warnf("VAD provider 为空，尝试从 config 中获取")
			} else {
				log.Debugf("获取VAD资源: provider=%s", vadProviderName)
			}

			wrapper, err := pool.Acquire[inter.VAD](
				"vad",
				vadProviderName,
				vadProviderConfig,
			)
			if err != nil {
				log.Errorf("获取VAD资源失败: provider=%s, config=%+v, error=%v", vadProviderName, vadProviderConfig, err)
				return false
			}
			vadWrapper = wrapper
			vadProvider = wrapper.GetProvider()
			vadLastUseAt = time.Now()
			return true
		}
		for {
			// 使用最大帧大小作为缓冲区，解码后会得到实际帧大小
			pcmFrame := make([]float32, maxFrameSize)

			select {
			// 空闲释放分支：即使在“没有音频”的静默期，这个 tick 也在跑，
			// 保证 2 秒内没有再用 VAD 就主动归还，不会一直占着池化实例。
			case <-vadIdleTicker.C:
				if vadWrapper != nil && !vadLastUseAt.IsZero() && time.Since(vadLastUseAt) >= vadIdleReleaseTimeout {
					releaseVad("idle_timeout")
				}
				continue
			case opusFrame, ok := <-state.OpusAudioBuffer:
				//log.Debugf("processAsrAudio 收到音频数据, len: %d", len(opusFrame))
				if !ok {
					log.Debugf("processAsrAudio 音频通道已关闭")
					return
				}

				// 这里有两个“有没有声音”的变量，含义不同，是理解本函数状态机的关键：
				//   clientHaveVoice —— “之前（历史）是否已经进入说话状态”，来自 ClientState，跨帧保持；
				//   haveVoice       —— “当前这一帧是否被判为有声”，每帧重新计算。
				// 组合起来就是状态迁移：静音→有声（首次命中，要做前导裁剪）、有声→有声（直通）、
				// 有声→静音（累积静音时长，判断是否说完了）。
				var skipVad bool
				var haveVoice bool
				clientHaveVoice := state.GetClientHaveVoice()
				// manual 模式：设备端按键控制，服务端把每一帧都当成有声（不需要 VAD 结果），
				// 同时把 clientHaveVoice 也置为 true，这样会立刻进入“直通”分支，音频不做任何裁剪地进 ASR。
				if state.ListenMode == "manual" {
					skipVad = true         //跳过vad
					clientHaveVoice = true //之前有声音
					haveVoice = true       //本次有声音
				} else if state.Asr.AutoEnd {
					// AutoEnd：结束判定交给 ASR provider，但仍由服务端把音频全部推进去，
					// 因此这里只把 skipVad 置真（不走 VAD 检测），不伪造 clientHaveVoice，
					// 以免破坏“空闲时钟”等依赖 clientHaveVoice 的语义。
					skipVad = true   // 仍由 provider 控制 stop，但不改变 idle 语义
					haveVoice = true // 本次音频直接进入 ASR
				}

				// 已经判定“本轮说话结束”（OnVoiceSilence 会置位）之后，后续到达的音频属于下一轮，
				// 直接丢弃，避免把两轮音频混进同一次识别。
				if state.GetClientVoiceStop() { //已停止 说话 则不接收音频数据
					//log.Infof("客户端停止说话, 跳过音频数据")
					continue
				}

				//log.Debugf("clientVoiceStop: %+v, asrDataSize: %d, listenMode: %s, isSkipVad: %v\n", state.GetClientVoiceStop(), state.AsrAudioBuffer.GetAsrDataSize(), state.ListenMode, skipVad)

				// Opus → PCM float32。失败只丢这一帧：实时音频流里单帧解码失败是可以容忍的，
				// 不能因此把整段会话断掉。
				n, err := audioProcesser.DecoderFloat32(opusFrame, pcmFrame)
				if err != nil {
					log.Errorf("解码失败: %v", err)
					continue
				}

				// 从实际解码后的数据动态计算帧大小和帧时长
				// 为什么不在初始化时算：握手只告诉采样率/声道数，没告诉帧长；
				// 而 VAD 判定和“说话时长统计”都需要精确的帧时长，所以用第一帧的真实解码样本数反推。
				if frameSize == 0 {
					// 第一帧：从实际解码的数据计算帧信息
					frameSize = n
					samplesPerChannel := n / audioFormat.Channels
					frameDurationMs = samplesPerChannel * 1000 / audioFormat.SampleRate
					audioFormat.FrameDuration = frameDurationMs

					// 计算 VAD 需要的帧数
					// 注意这里恒为 1，而不是按“VAD 窗口 / 帧长”算：编码器每帧解码出的 PCM
					// 已经足够 VAD 使用（20ms 帧是 VAD 的标准输入粒度）。
					vadNeedGetCount = 1
					log.Debugf("从实际音频数据计算帧信息: frameSize=%d, frameDurationMs=%d, vadNeedGetCount=%d", frameSize, frameDurationMs, vadNeedGetCount)
				}

				// pcmData 与 speakerPcmData 指向同一块解码结果（同一帧）：
				// 前者喂 ASR，后者喂声纹。分开命名是为了后续可能只对其中一个做裁剪/跳帧。
				var vadPcmData []float32
				pcmData := pcmFrame[:n]
				speakerPcmData := pcmFrame[:n]

				// 检查帧大小是否一致（正常情况下应该一致，但不一致时使用实际值）
				// 设备偶发会送出稍短/稍长的帧，此时以实际值为准，保证后续时长统计不失真。
				if n != frameSize {
					log.Debugf("帧大小不一致: 期望=%d, 实际=%d，使用实际值", frameSize, n)
					// 重新计算这一帧的时长
					samplesPerChannel := n / audioFormat.Channels
					currentFrameDurationMs := samplesPerChannel * 1000 / audioFormat.SampleRate
					frameSize = n
					frameDurationMs = currentFrameDurationMs
					audioFormat.FrameDuration = frameDurationMs
				}

				if !skipVad && needVad {
					if !ensureVad() {
						continue
					}
					//decode opus to pcm
					// 先把当前帧存进“待判定缓冲”。因为 VAD 需要一定长度的上下文才准，
					// 而且首帧语音时的前导静音也在这里暂存，等判定完再决定丢弃多少（见 trimFirstSpeechAudio）。
					state.AsrAudioBuffer.AddAsrAudioData(pcmData)

					// 计算 VAD 需要的最小数据量
					vadNeedMinSize := frameSize

					if state.AsrAudioBuffer.GetAsrDataSize() >= vadNeedMinSize {
						// 取哪段数据做 VAD 检测，两类 provider 不同：
						//   · silero_vad：自己在内部维护状态窗口，只吃“当前帧”；
						//   · 其它：需要按 vadNeedGetCount 帧从缓冲里取（这里为 1 帧）。
						if isSileroVAD {
							vadPcmData = pcmData
						} else {
							vadPcmData = state.AsrAudioBuffer.GetAsrData(vadNeedGetCount, frameSize)
						}

						//如果已经检测到语音, 则不进行vad检测, 直接将pcmData传给asr
						// 使用循环外获取的VAD资源进行检测
						if !isSileroVAD {
							// 非 silero 的 provider（如基于能量/webrtc 的实现）是“无状态”的，
							// 每次检测前必须 Reset，否则上一帧的判定结果会污染这一次。
							vadLastUseAt = time.Now()
							if err := vadProvider.Reset(); err != nil {
								log.Errorf("重置vad失败: %v", err)
								continue
							}
						}

						// 进行VAD检测
						vadLastUseAt = time.Now()
						haveVoice, err = vadProvider.IsVADExt(vadPcmData, audioFormat.SampleRate, frameSize)
						if err != nil {
							log.Errorf("processAsrAudio VAD检测失败: %v", err)
							continue
						}

						//首次触发识别到语音时,为了语音数据完整性 将vadPcmData赋值给pcmData, 之后的音频数据全部进入asr
						// 状态迁移：静音 → 有声。此时缓冲里存着“开头那段被判为静音的音频”，
						// 不要全部丢掉：其中的字头/气息是有用的；但也不能全给（可能有一大段静音）。
						if haveVoice && !clientHaveVoice {
							//首次检测到语音时，最多只保留200ms的前静音数据
							currentFrameSamples := len(pcmData)
							allData := state.AsrAudioBuffer.GetAndClearAllData()
							pcmData = trimFirstSpeechAudio(allData, currentFrameSamples, audioFormat.SampleRate, audioFormat.Channels)
						}
					}
					//log.Debugf("isVad, pcmData len: %d, vadPcmData len: %d, haveVoice: %v", len(pcmData), len(vadPcmData), haveVoice)
				}

				if haveVoice {
					// 有新语音了，复位“尚未收到首文本”的日志去重标记，方便下一轮重新打印。
					hasLoggedFirstTextExtendedWait = false
					//log.Infof("检测到语音, len: %d", len(pcmData))
					// 进入/保持在“说话中”状态：这三个写入共同构成状态机的一部分——
					// ClientHaveVoice 标记“本轮已开始说话”，ClientHaveVoiceLastTime 记录最后一次有声时刻，
					// ResetIdleDuration 清零“连续静音时长”（说话过程中静音计时必须归零）。
					state.SetClientHaveVoice(true)
					state.SetClientHaveVoiceLastTime(time.Now().UnixMilli())
					state.Vad.ResetIdleDuration()
					// 累积检测到声音的时长（同时更新一次过程中的时长）
					state.Vad.AddVoiceDuration(int64(frameDurationMs))

					// realtime_mode=1 的打断策略：连续说话超过 360ms 就认为“用户真的在说话”，
					// 此时如果还在播 LLM/TTS 的回复就把它掐掉（barge-in）。
					// 为什么用 360ms 而不是一检测到有声就打断：避免咳嗽、环境噪声、设备自身回声
					// 造成“刚开口就被打断”的抖动。
					continuousVoiceDuration := state.Vad.GetVoiceContinuousDuration()
					if state.IsRealTime() && viper.GetInt("chat.realtime_mode") == 1 && continuousVoiceDuration > 360 {
						// 只有在未触发过的情况下才执行，确保只执行一次
						if !hasTriggeredCancel {
							// 例外：如果此时正在通过 MCP 播放媒体音频，则不要打断（否则音乐会被“自己人”的
							// 说话声切断）。门控激活时，后面 ASR 出来的文本会先走本地媒体控制指令识别，
							// 所以这里只是把标记置真、放弃本次打断。
							if a.session != nil && a.session.isRealtimeMcpAudioGateActive() {
								log.Debugf("设备 %s realtime媒体播放门控激活，跳过VAD打断", state.DeviceID)
								hasTriggeredCancel = true
							} else {
								//realtime模式下, 如果此时有正在进行的llm和tts则取消掉
								log.Debugf("realtime模式vad打断下 && 语音时长超过%d ms 如果此时有正在进行的llm和tts则取消掉", continuousVoiceDuration)
								// 两个分支的区别：有 session 时走统一入口（会同时清 LLM/TTS/文本队列、
								// 递增音频代际丢弃在途音频，但保留媒体播放）；没有 session（理论上的边界情况）
								// 时只能退化为取消会话上下文。
								if a.session != nil {
									a.session.StopAssistantOutputAfterAsrWithReason(true, "ASRManager.ProcessVadAudio realtime_mode=1 VAD interrupt")
								} else {
									state.AfterAsrSessionCtx.CancelWithReason("ASRManager.ProcessVadAudio: realtime_mode=1 VAD interrupt")
								}
								hasTriggeredCancel = true // 标记为已触发
							}
						}
					}
				} else {
					// 本帧无声：累积静音时长，并重置“连续有声时长”（连续说话被打断了）。
					state.Vad.AddIdleDuration(int64(frameDurationMs))
					state.Vad.ResetVoiceContinuousDuration()

					// 没有声音时，如果之前也没有语音，则重置累积的声音时长
					// 如果之前有语音但本次没有，保留时长值，让后续逻辑判断是否应该重置
					if !clientHaveVoice {
						// 状态一直是“静音”：这一帧彻底无关紧要，顺手复位声纹 peek 的一次性状态，
						// 保证下一轮说话能重新触发 peek 打断。
						speakerInterruptTriggered.Store(false)
						lastSpeakerPeekDoneAt.Store(0)
						speakerPeekAudioMs = 0
						//保留近10帧
						/*
							if state.AsrAudioBuffer.GetFrameCount(frameSize) > vadNeedGetCount*3 {
								state.AsrAudioBuffer.RemoveAsrAudioData(1, frameSize)
							}*/
						continue
					}
				}

				// 条件 (clientHaveVoice || haveVoice) 的含义：只要“本轮已经开始说话”或“本帧有声”，
				// 就把音频送进 ASR。这样设计的目的是让 ASR 拿到连续的一段音频：
				// 说话过程中即使个别帧 VAD 判成静音（字间停顿），音频也不会断流（否则识别会被切碎）。
				if clientHaveVoice || haveVoice {
					// 首次命中语音时也要立刻转发当前缓存帧，避免极短语音整段未送入 ASR。

					//vad识别成功, 往asr音频通道里发送数据
					//log.Infof("vad识别成功, 往asr音频通道里发送数据, len: %d", len(pcmData))
					// [1.2] 送 ASR：AddAudioData 实际是写进 Asr.AsrAudioChannel，由 ASR provider 的
					// StreamingRecognize 协程消费——VAD 在这里扮演“闸门”角色。
					state.Asr.AddAudioData(pcmData)

					// 声纹只接收当前判定为有声的帧，避免将首段前导静音和尾静音送入识别流。
					if haveVoice &&
						state.IsSpeakerEnabled() && state.HasSpeakerGroups() &&
						a.session != nil && a.session.speakerManager != nil {
						// 首次检测到语音时，启动流式识别
						// （声纹服务同样需要“一段连续音频”，所以按“本轮首次有声”作为开始时机，
						//   整轮结束后由 OnVoiceSilence 触发的收尾流程去 finish）。
						if !a.session.speakerManager.IsActive() {
							sampleRate := audioFormat.SampleRate
							agentId := a.session.clientState.AgentID
							if err := a.session.speakerManager.StartStreaming(ctx, sampleRate, agentId); err != nil {
								log.Warnf("启动声纹识别流失败: %v", err)
							} else {
								// 新一轮开始，复位 peek 打断相关状态。
								speakerInterruptTriggered.Store(false)
								lastSpeakerPeekDoneAt.Store(0)
								speakerPeekAudioMs = 0
							}
						}

						// 发送音频块
						if err := a.session.speakerManager.SendAudioChunk(ctx, speakerPcmData); err != nil {
							log.Warnf("发送音频块到声纹识别服务失败: %v", err)
						} else if a.session.speakerManager.IsActive() {
							// 累计已送入声纹服务的音频时长（毫秒），用于判断“素材是否够多、是否值得 peek”。
							if audioFormat.Channels > 0 && audioFormat.SampleRate > 0 {
								speakerPeekAudioMs += int64(len(speakerPcmData)/audioFormat.Channels) * 1000 / int64(audioFormat.SampleRate)
							}

							// realtime_mode=3 的打断策略：不等 ASR 出结果，而是“peek（探一下）声纹”，
							// 一旦识别出说话人（命中已登记声纹）就立刻打断当前播报。
							// 好处：打断时机比等 ASR 快；代价：peek 是额外请求，所以必须做三重节流——
							//   1) 本轮已打断过就不再 peek（speakerInterruptTriggered）；
							//   2) 音频素材累计够 400ms 才 peek（太短的音频识别不准）；
							//   3) 两次 peek 之间至少间隔 speakerPeekInterval，且同一时刻只允许一个在飞
							//      （speakerPeekInFlight CAS），避免把声纹服务打爆。
							if state.IsRealTime() &&
								viper.GetInt("chat.realtime_mode") == 3 &&
								!speakerInterruptTriggered.Load() &&
								speakerPeekAudioMs >= firstSpeakerPeekAudioThresholdMs {
								now := time.Now()
								lastDoneAt := lastSpeakerPeekDoneAt.Load()
								if (lastDoneAt <= 0 || now.Sub(time.Unix(0, lastDoneAt)) >= speakerPeekInterval) &&
									speakerPeekInFlight.CompareAndSwap(false, true) {
									reqSeq := atomic.AddUint64(&speakerPeekRequestSeq, 1)
									// requestID 带上毫秒时间戳与自增序号，便于服务端排查/去重。
									requestID := fmt.Sprintf("peek_%d_%d", now.UnixMilli(), reqSeq)

									// peek 放到独立 goroutine：识别是网络调用（可能几十~几百毫秒），
									// 不能阻塞 VAD 主循环，否则音频会积压、判定会失真。
									go func(reqID string) {
										// 无论成功失败，都要记录完成时间并释放 in-flight 标记。
										defer func() {
											lastSpeakerPeekDoneAt.Store(time.Now().UnixNano())
											speakerPeekInFlight.Store(false)
										}()

										if a.session == nil || a.session.speakerManager == nil || !a.session.speakerManager.IsActive() {
											return
										}

										// peek 自身带 2 秒超时：即使声纹服务挂住也不会泄漏 goroutine。
										peekCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
										defer cancel()

										peekResult, throttled, err := a.session.speakerManager.PeekAndIdentify(peekCtx, reqID)
										if err != nil {
											// ctx 已取消时的报错是正常退出（会话结束），不刷日志。
											if ctx.Err() == nil {
												log.Debugf("声纹peek失败: device=%s, request_id=%s, err=%v", state.DeviceID, reqID, err)
											}
											return
										}
										// throttled：声纹服务端限流（例如 peek 太频繁），本帧放弃，等下一次机会。
										if throttled {
											return
										}
										// 没识别出人（不说话、噪音、未登记声纹）都不打断——只处理“确定是某个说话人”的情况。
										if peekResult == nil || !peekResult.Identified {
											return
										}
										// CAS 保证“本轮只打断一次”：并发的多个 peek 结果里只有第一个生效。
										if !speakerInterruptTriggered.CompareAndSwap(false, true) {
											return
										}

										log.Infof(
											"realtime模式声纹peek命中，立即打断: device=%s, speaker=%s, confidence=%.4f, threshold=%.4f",
											state.DeviceID,
											peekResult.SpeakerName,
											peekResult.Confidence,
											peekResult.Threshold,
										)
										// 同 mode=1：正在播媒体音频时不打断（门控激活）。
										if a.session != nil && a.session.isRealtimeMcpAudioGateActive() {
											log.Debugf("设备 %s realtime媒体播放门控激活，跳过speaker peek打断", state.DeviceID)
											return
										}
										// 告诉会话“本轮是被声纹打断的”，后续 ShouldAllowSpeakerChat 会用它做判断：
										// 如果打断者不是已登记说话人，这条 ASR 结果会被丢弃（避免串音/噪音触发对话）。
										a.session.MarkTurnSpeakerInterrupted()
										if a.session != nil {
											a.session.StopAssistantOutputAfterAsrWithReason(true, "ASRManager.ProcessVadAudio realtime_mode=3 speaker peek interrupt")
										} else {
											state.AfterAsrSessionCtx.CancelWithReason("ASRManager.ProcessVadAudio: realtime_mode=3 speaker peek interrupt")
										}
									}(requestID)
								}
							}
						}
					}
				}

				//已经有语音了, 但本次没有检测到语音, 则需要判断是否已经停止说话
				// 这一整块就是状态机的第三个迁移：有声 → 静音（可能意味着“说完了”）。
				lastHaveVoiceTime := state.GetClientHaveVoiceLastTime()

				if clientHaveVoice && lastHaveVoiceTime > 0 && !haveVoice {
					// 判断有音频的语音时长，如果小于300ms则重置clientHaveVoice，避免短时间语音造成的误判
					// 实际阈值是 100ms（比注释里的 300ms 更严格）：咳嗽/碰桌子/尾音碎片常常只持续几十毫秒，
					// 若把它们当成一次“说话”，就会出现无意义的空识别轮次，因此直接回退到静音状态。
					voiceDurationInSession := state.Vad.GetVoiceDurationInSession()
					if voiceDurationInSession < 100 {
						log.Debugf("语音时长过短 (%dms < 300ms)，重置clientHaveVoice", voiceDurationInSession)
						state.SetClientHaveVoice(false)
						state.Vad.ResetVoiceDuration()
						speakerInterruptTriggered.Store(false)
						lastSpeakerPeekDoneAt.Store(0)
						speakerPeekAudioMs = 0
						continue
					}

					idleDuration := state.Vad.GetIdleDuration()
					// realtime 特例：还没收到 ASR 的首个文本前，不能急着按静音阈值收口。
					// 因为 realtime 下 ASR 是长驻的，网络/服务端延迟可能让首文本晚于静音判定到达；
					// 一旦提前收口，这句话就丢了。所以这里用一个更长的等待阈值
					// （pre_asr_text_silence_duration，默认是静音阈值的 4 倍、至少 1s）继续等。
					if state.IsRealTime() && !state.Asr.HasReceivedText() {
						preTextSilenceDuration := state.GetPreAsrTextSilenceDuration()
						if idleDuration <= preTextSilenceDuration {
							log.Debugf(
								"realtime模式尚未收到ASR首文本，延迟按静音阈值收口: status=%s, idle=%dms, pre_text_timeout=%dms, voice_duration=%dms, voice_duration_in_session=%dms, history_audio_samples=%d",
								state.Status,
								idleDuration,
								preTextSilenceDuration,
								state.Vad.GetVoiceDuration(),
								voiceDurationInSession,
								state.Asr.GetHistoryAudioLen(),
							)
							continue
						}

						// 超过延长阈值仍无首文本：仍然不立刻收口（继续转发音频），只打一条日志说明情况。
						// 真正的收口交给 StartAsrRecognitionLoop 的空结果保护或音频空闲看门狗兜底。
						if !hasLoggedFirstTextExtendedWait {
							log.Debugf(
								"realtime模式静音超时且仍未收到ASR文本，继续保持当前ASR流并转发音频: status=%s, idle=%dms, pre_text_timeout=%dms, voice_duration=%dms, voice_duration_in_session=%dms, history_audio_samples=%d",
								state.Status,
								idleDuration,
								preTextSilenceDuration,
								state.Vad.GetVoiceDuration(),
								voiceDurationInSession,
								state.Asr.GetHistoryAudioLen(),
							)
							hasLoggedFirstTextExtendedWait = true
						}
						continue
					}

					if state.IsSilence(idleDuration) { //从有声音到 静默的判断
						log.Debugf(
							"判定语音结束，准备停止ASR: status=%s, idle=%dms, voice_duration=%dms, voice_duration_in_session=%dms, history_audio_samples=%d, pending_restart=%v",
							state.Status,
							idleDuration,
							state.Vad.GetVoiceDuration(),
							state.Vad.GetVoiceDurationInSession(),
							state.Asr.GetHistoryAudioLen(),
							state.AudioIdleTimeoutPending(),
						)
						// 在 OnVoiceSilence 之前重置标志位，以便下次可以再次触发
						// （OnVoiceSilence 之后 clientHaveVoice 会被清掉，下一轮说话是全新的状态迁移）。
						hasTriggeredCancel = false
						speakerInterruptTriggered.Store(false)
						lastSpeakerPeekDoneAt.Store(0)
						speakerPeekAudioMs = 0
						// [1.1→2.1] 判定说话结束，触发 [2.1] 收口
						// OnVoiceSilence 是关键动作：置 VoiceStop、停 ASR 让它吐最终结果、
						// 释放 VAD 状态、通知声纹收尾。ASR 的最终文本会在 StartAsrRecognitionLoop 里被取走。
						state.OnVoiceSilence()
						//state.VoiceStatus.Reset()
						continue
					}
				}

			case <-ctx.Done():
				return
			}
		}
	}()
}

// releaseResource 释放ASR资源（内部方法）
//
// 归还池化资源并清空引用。加锁是因为它既可能被识别循环的 defer 调用，
// 也可能被外部的 Cleanup 调用，两者存在并发窗口（重复 Release 会破坏资源池计数）。
// 置 nil 保证“只有第一次 Release 生效”，后续调用直接跳过。
func (a *ASRManager) releaseResource() {
	a.resourceMu.Lock()
	defer a.resourceMu.Unlock()
	if a.asrResource != nil {
		pool.Release(a.asrResource)
		a.asrResource = nil
		log.Debugf("ASR资源已归还")
	}
}

// Cleanup 清理ASR资源（供外部调用）
// 由 ChatSession 在会话结束/重置时调用，保证即使识别循环因异常提前退出，资源也不会泄漏。
func (a *ASRManager) Cleanup() {
	a.releaseResource()
}

// [1.2] 建 ASR 流：重启 ASR 识别（取池化 provider + 建通道 + 调 StreamingRecognize）。
//
// restartAsrRecognition 重启ASR识别
// （方法名首字母小写是历史遗留的注释写法，实际导出名为 RestartAsrRecognition。）
//
// 为什么必须先“取消旧上下文”：
//   - 上一轮的 Asr.Ctx 还活着时，旧的 ASR provider 协程仍在消费旧的 AsrAudioChannel，
//     如果不取消，新旧两路识别会同时往 AsrResultChannel 里塞结果（文本串台、结果顺序错乱）；
//   - CancelWithReason 会连带停掉旧的 StreamingRecognize 内部协程，是安全复用的前提。
//
// 资源归属（realtime 与非 realtime 的差异）：
//   - realtime：本方法会把上一轮的 provider “复用/换新”并继续留下引用（a.asrResource 不置 nil），
//     因为识别是长驻的，每轮都重新 acquire 会造成频繁建连；
//   - 非 realtime：调用方（StartAsrRecognitionLoop）在拿到文本后会 return，
//     defer 里的 releaseResource 归还资源——即“一轮一借一还”。
//
// 参数 ctx：会话上下文，作为新 ASR 上下文的父节点（会话取消时 ASR 一并取消）。
func (a *ASRManager) RestartAsrRecognition(ctx context.Context) error {
	state := a.clientState
	log.Debugf("重启ASR识别开始")
	// 新一轮开始，清掉“上一轮被声纹打断”的标记，否则新一轮的 ASR 结果会被误判为“打断者的语音”。
	if a.session != nil {
		a.session.ResetTurnSpeakerInterrupted()
	}

	// 取消当前ASR上下文
	state.Asr.CancelWithReason("ASRManager.RestartAsrRecognition: cancel previous ASR context before restart")

	// 一轮全新的开始：清掉上一轮的“是否收到过文本”、VAD 状态、待判定音频缓冲和历史音频。
	state.Asr.ResetReceivedText()
	state.VoiceStatus.Reset()
	state.AsrAudioBuffer.ClearAsrAudioData()
	state.Asr.ClearHistoryAudio() // 清空历史音频缓存

	// 检查是否已有资源，如果没有则获取
	a.resourceMu.Lock()
	var asrProvider asr.AsrProvider
	if a.asrResource == nil {
		// 需要获取新资源
		// 注意这里先 Unlock 再 Acquire：Acquire 可能阻塞/较慢，不能持锁（否则 Cleanup 会卡住）。
		a.resourceMu.Unlock()

		asrWrapper, err := pool.Acquire[asr.AsrProvider](
			"asr",
			state.DeviceConfig.Asr.Provider,
			state.DeviceConfig.Asr.Config,
		)
		if err != nil {
			log.Errorf("获取ASR资源失败: %v", err)
			return fmt.Errorf("获取ASR资源失败: %w", err)
		}

		// 保存资源引用到私有字段
		a.resourceMu.Lock()
		a.asrResource = asrWrapper
		asrProvider = asrWrapper.GetProvider()
		a.resourceMu.Unlock()
		log.Debugf("获取新的ASR资源")
	} else {
		// 复用现有资源
		// realtime 下的常态：provider 实例保留在 a.asrResource 上，这里只是再次取出使用。
		asrProvider = a.asrResource.GetProvider()
		a.resourceMu.Unlock()
		log.Debugf("复用现有ASR资源")
	}

	// 重新创建ASR上下文和通道
	// AsrAudioChannel 容量 100 帧：给 provider 侧的消费留一点缓冲，避免 VAD 循环被写阻塞
	// （写入是 VAD 循环做的，一旦通道满，VAD 循环会卡住，进而导致 OpusAudioBuffer 堆积丢帧）。
	state.Asr.Ctx, state.Asr.Cancel = context.WithCancel(ctx)
	state.Asr.AsrAudioChannel = make(chan []float32, 100)

	// [1.2] 送 ASR 的另一半：StreamingRecognize 消费 AsrAudioChannel，返回识别结果通道
	asrResultChannel, err := asrProvider.StreamingRecognize(state.Asr.Ctx, state.Asr.AsrAudioChannel)
	if err != nil {
		// 识别失败，归还资源（因为资源可能已损坏）
		a.releaseResource()
		log.Errorf("重启ASR流式识别失败: %v", err)
		return fmt.Errorf("重启ASR流式识别失败: %w", err)
	}

	state.AsrResultChannel = asrResultChannel
	// 重置统计时间，用于计算本轮对话的整体耗时
	state.MarkTurnStart()
	if a.session != nil {
		a.session.TraceTurnStart(state.Asr.Ctx, state.Statistic.TurnStartTs)
	}
	log.Debugf("重启ASR识别成功")
	return nil
}

// [2.0] 读识别结果：启动 ASR 识别结果处理循环（消费 AsrResultChannel，拿到最终文本）。
//
// StartAsrRecognitionLoop 启动ASR识别结果处理循环
// onMessageSave: 消息保存回调函数
// onError: 错误处理回调函数（如关闭会话）
//
// 本方法是“下行”方向（把 ASR 结果变成聊天文本）的第二段：前面的 RestartAsrRecognition 负责
// 建立流，这里负责持续取结果直到该结束为止。它内部起 goroutine，调用方（ChatSession）不阻塞。
//
// 结束条件（任一满足即退出并归还 ASR 资源）：
//   - 拿到一次非空最终文本且处于非 realtime 模式（auto 模式：一轮一借一还）；
//   - ASR 返回不可恢复错误；
//   - 可恢复错误 / 空结果在短时间窗口内超过上限（保护下游，避免死循环或反复重连）；
//   - ctx 被取消、状态长时间不允许重启、空闲超过 maxIdleTime。
//
// 参数：
//
//	ctx            —— 会话上下文（取消后循环退出）；
//	onMessageSave  —— 消息保存回调（把用户消息与音频落到存储，由 ChatSession 提供）；
//	onError        —— 错误出口（多数情况下会关闭会话/断开连接）。
func (a *ASRManager) StartAsrRecognitionLoop(
	ctx context.Context,
	onMessageSave AsrMessageSaveCallback,
	onError func(error),
) {
	state := a.clientState

	// 启动一个goroutine处理asr结果
	go func() {
		// 使用 defer 确保 goroutine 退出时释放 ASR 资源
		// 非 realtime 模式下这正是“一轮一还”的落点；panic 也算退出路径，所以要 recover。
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("asr结果处理goroutine panic: %v, stack: %s", r, string(debug.Stack()))
			}
			// 无论正常退出还是 panic，都释放资源
			a.releaseResource()
		}()

		//最大空闲 60s
		// 语义：如果 60 秒内 ASR 既没给出文本也没给出错误，就认为上游已经不健康，退出循环避免一直挂着。
		var startIdleTime, maxIdleTime int64
		startIdleTime = time.Now().Unix()
		maxIdleTime = 60

		// 状态不允许重启时的等待计数（避免无限循环）
		// 例如 LLM/TTS 正在跑（状态是 LLMStart/TTSStart）时，非 realtime 下不允许重启 ASR；
		// 此时只能短暂等待状态恢复（200ms × 10 次 ≈ 1 秒），超时就退出，不能原地空转。
		var invalidStatusWaitCount int64
		maxInvalidStatusWaitCount := int64(10) // 最多等待10次（约1秒）

		// 空结果短时保护：避免 ASR 服务异常持续返回空字符串导致主流程死循环
		const emptyResultProtectWindow = 3 * time.Second
		const maxEmptyResultInWindow = 3
		emptyResultWindowStart := time.Now()
		emptyResultCount := 0

		// 可恢复错误短时保护：避免上游持续返回实例失效时无限重连
		const recoverableErrorProtectWindow = 10 * time.Second
		const maxRecoverableErrorInWindow = 3
		recoverableErrorWindowStart := time.Now()
		recoverableErrorCount := 0

		// isAllowedToRestart：判断“现在能不能重建 ASR 流”。
		//   - 非 realtime（auto）：只有 Listening/ListenStop（即处于拾音阶段）才允许；
		//     如果在 LLM/TTS 阶段就重启，会白白占着资源。
		//   - realtime：只要不是初始状态都允许——因为 realtime 下 ASR 必须长驻，
		//     否则用户插话时没人听得见。
		isAllowedToRestart := func() bool {
			allowed := state.Status == ClientStatusListening || state.Status == ClientStatusListenStop
			if state.IsRealTime() {
				allowed = state.Status != ClientStatusInit
			}
			return allowed
		}
		// 下面三个闭包是“音频空闲时钟”的操作封装：
		//   resumeAudioIdle —— 重启/等待期间暂停空闲计时（因为此时正是系统在处理，不算用户空闲）；
		//   startAudioIdle  —— 开启空闲计时（例如刚丢弃了一条结果，重新开始等下一次输入）；
		//   closeAudioIdleTimeout —— 空闲超时后收口：关会话或上报错误。
		resumeAudioIdle := func() {
			state.ResumeAudioIdleWindow(time.Now())
		}
		startAudioIdle := func() {
			state.StartAudioIdleWindow(time.Now())
		}
		closeAudioIdleTimeout := func(reason string) {
			// 只有确实处于“待处理超时”状态时才收口，避免误关活着的会话。
			if !state.AudioIdleTimeoutPending() {
				return
			}

			state.ClearAudioIdleTimeoutPending()
			log.Infof("音频空闲超时收口完成: device=%s, reason=%s", state.DeviceID, reason)
			if a.session != nil {
				a.session.CloseWithReason(chatSessionCloseReasonAudioIdleTimeout)
				return
			}
			if onError != nil {
				onError(fmt.Errorf("audio idle timeout: %s", reason))
			}
		}

		for {
			// 每轮先检查取消，避免 ctx 已取消还去取结果（RetireAsrResult 内部也会检查，这里是双保险）。
			select {
			case <-ctx.Done():
				log.Debugf("asr ctx done")
				return
			default:
			}

			// [2.0] 读识别结果：RetireAsrResult 会阻塞等待，返回三个值：
			//   result  ：识别结果（Text/EmptyReason/RetryReason 等）；
			//   isRetry ：名字容易误解，实际语义是“是否应当继续循环”（源码里叫 shouldContinue）——
			//             true 覆盖了三种情况：拿到最终结果、provider 报告可恢复错误、结果通道被关闭
			//             （关闭时返回空结果）；false 只有两种：ctx 被取消，或不可恢复错误（此时 err 非空）；
			//   err     ：不可恢复错误。
			// 为什么用它而不是直接读 AsrResultChannel：它内部处理了“非 final 片段继续等”“
			// funasr 2pass-online 的伪 final”“首字回调”等细节（见 internal/data/client/asr.go）。
			result, isRetry, err := state.RetireAsrResult(ctx)
			if err != nil {
				if ctx.Err() != nil || errors.Is(err, context.Canceled) {
					log.Debugf("处理asr结果失败，ASR已取消: %v", err)
				} else {
					log.Errorf("处理asr结果失败: %v", err)
				}
				if onError != nil {
					onError(err)
				}
				return
			}
			if !isRetry {
				log.Debugf("asrResult is not retry, return")
				return
			}
			text := result.Text

			// RetryReason 非空 = provider 报告了“可恢复错误”（实例失效、会话超时等），
			// 这里做三件事：判断是否已超时收口、做时间窗限流、按原因选择恢复动作。
			if result.RetryReason != "" {
				// 如果此时正卡在音频空闲超时收口流程里，就不用再重连了，直接结束。
				if state.AudioIdleTimeoutPending() {
					closeAudioIdleTimeout(result.RetryReason)
					return
				}

				// 限流窗口：窗口外的计数过期清零，窗口内累计。这样“偶发一次”可以重连，
				// 而“10 秒内连续 3 次”说明上游是真的坏了，继续重连只会刷爆日志/占用资源。
				now := time.Now()
				if now.Sub(recoverableErrorWindowStart) > recoverableErrorProtectWindow {
					recoverableErrorWindowStart = now
					recoverableErrorCount = 0
				}
				recoverableErrorCount++
				log.Warnf(
					"ASR可恢复错误: reason=%s, count=%d/%d, status=%s",
					result.RetryReason,
					recoverableErrorCount,
					maxRecoverableErrorInWindow,
					state.Status,
				)

				if recoverableErrorCount >= maxRecoverableErrorInWindow {
					err := fmt.Errorf("ASR短时间内连续触发可恢复错误(%d次/%s)，停止重试并断开连接", recoverableErrorCount, recoverableErrorProtectWindow)
					log.Errorf("%v", err)
					if onError != nil {
						onError(err)
					}
					return
				}

				switch result.RetryReason {
				case asr_types.RetryReasonDoubaoResponseCode45000081, asr_types.RetryReasonXunfeiServiceInstanceInvalid, asr_types.RetryReasonAliyunQwen3ConnectionClosed:
					// 这三类都是“provider 实例已不可用”，必须丢掉旧资源重新获取（复用只会继续报错）。
					a.releaseResource()
					if isAllowedToRestart() {
						invalidStatusWaitCount = 0
						if restartErr := a.RestartAsrRecognition(ctx); restartErr != nil {
							log.Errorf("ASR可恢复错误后重启识别失败: reason=%s, err=%v", result.RetryReason, restartErr)
							if onError != nil {
								onError(restartErr)
							}
							return
						}
						resumeAudioIdle()
						continue
					}

					// 当前状态不允许立刻重启（例如还在播报）：先只取消当前流，
					// 等状态回到可拾音时，由后续的空结果分支或下一次 listen 触发重建。
					log.Warnf("ASR可恢复错误发生时当前状态不允许立即重启: reason=%s, status=%s, realtime=%v", result.RetryReason, state.Status, state.IsRealTime())
					state.Asr.CancelWithReason("ASRManager.StartAsrRecognitionLoop: recoverable error but restart not allowed yet")
					resumeAudioIdle()
					continue
				case asr_types.RetryReasonDoubaoWaitingNextPacketTimeout:
					// 这是“用户长时间没说话，云端会话被回收”的正常情况，不是故障：
					// 挂起当前流，等下一次有语音时再重建即可（保持资源，不 release）。
					log.Warnf("doubao ASR 会话空闲超时，挂起当前流并等待下一次语音时重建")
					state.Asr.CancelWithReason("ASRManager.StartAsrRecognitionLoop: doubao waiting next packet timeout")
					resumeAudioIdle()
					continue
				}
			}

			// text != ""：拿到了非空最终文本，这一轮“用户说了什么”已经确定，进入后处理。
			if text != "" {
				// 记录“最终文本到达”的时刻，用于计算 ASR 耗时等指标。
				asrFinalTs := time.Now().UnixMilli()
				state.MarkAsrFinalTextAt(asrFinalTs)
				if a.session != nil {
					a.session.TraceAsrFinalText(ctx, asrFinalTs)
				}
				log.Debugf("处理asr结果: %s, 耗时: %d ms", text, state.GetAsrDuration())

				// 成功了一轮：清掉“待收口”标记并把两种保护计数都归零
				// （连续失败计数必须是“连续”的语义——中间成功一次就重新开始计）。
				state.ClearAudioIdleTimeoutPending()
				// 识别成功后重置空结果计数
				emptyResultWindowStart = time.Now()
				emptyResultCount = 0
				recoverableErrorWindowStart = time.Now()
				recoverableErrorCount = 0

				//如果是realtime模式下，需要停止 当前的llm和tts
				// realtime_mode=2 的打断策略：ASR 一拿到结果就打断（比 mode=1 的 VAD 打断更晚，
				// 但更准确——既然已经确认用户说了内容，就立刻停掉正在播的回复）。
				if state.IsRealTime() && viper.GetInt("chat.realtime_mode") == 2 {
					shouldInterrupt := true
					// 例外：正在播媒体音频时，把打断决定“延后”给 realtime 门控（因为这条 ASR 文本
					// 可能就是“暂停/下一首”这类控制指令，需要先识别完再决定）。
					if a.session != nil && a.session.isRealtimeMcpAudioGateActive() {
						shouldInterrupt = false
						log.Debugf("设备 %s realtime媒体播放门控激活，延后到ASR final门控判定，跳过ASR结果打断", state.DeviceID)
					}
					if shouldInterrupt {
						log.Debugf("OnListenStart realtime模式下, 停止当前的llm和tts")
						if a.session != nil {
							a.session.StopAssistantOutputAfterAsrWithReason(true, "ASRManager.StartAsrRecognitionLoop realtime_mode=2 ASR result interrupt")
						} else {
							state.AfterAsrSessionCtx.CancelWithReason("ASRManager.StartAsrRecognitionLoop: realtime_mode=2 ASR result interrupt")
						}
					}
				}

				// 重置重试计数器
				// 这里的“计数器”是给上面 maxIdleTime 判断用的起点：有结果就刷新起点，
				// 保证 60 秒空闲的判定永远是从“最后一次成功识别”开始算。
				startIdleTime = time.Now().Unix()

				// [2.1] 收口 + 声纹：当获取到asr结果时, 结束语音输入（OnVoiceSilence 中会异步获取声纹结果）
				// 注意这里再调一次 OnVoiceSilence：正常路径下 VAD 循环已经调过（幂等），
				// 但 auto 模式下 ASR 可能先于 VAD 的静音判定拿到最终结果，这里做兜底收口。
				state.OnVoiceSilence()

				// 获取暂存的声纹结果（带超时）
				// 声纹识别是异步的（OnVoiceSilence 触发 finish 后由回调写入），这里最多等 200ms；
				// 等不到就用当前值（可能为 nil），不能为了声纹阻塞整轮对话。
				speakerResult := a.getSpeakerResult()
				// 本轮是否发生过“声纹 peek 打断”（mode=3）：如果是，说明用户是插话者，
				// 后续 ShouldAllowSpeakerChat 会要求打断者必须是已登记说话人。
				speakerInterrupted := false
				if a.session != nil {
					speakerInterrupted = a.session.ConsumeTurnSpeakerInterrupted()
				}

				// ASR_OUTPUT 钩子：给上层一个“在文本进入 LLM 前改写/拦截”的机会
				// （例如上层业务做敏感词替换、指令拦截、或者把文本转成别的意图）。
				// 钩子可以改写 Text / SpeakerResult，也可以要求 stop 中止整个流程。
				if a.session != nil {
					payload, stop, hookErr := a.session.hookHub.EmitASROutput(a.session.hookContext(ctx), chathooks.ASROutputData{Text: text, SpeakerResult: speakerResult})
					if hookErr != nil {
						log.Warnf("ASR_OUTPUT hook 执行失败: %v", hookErr)
					}
					text = payload.Text
					speakerResult = payload.SpeakerResult
					if stop {
						log.Infof("ASR_OUTPUT hook 请求停止当前流程")
						state.Asr.ClearHistoryAudio()
						// 挂在“停止”时也不能把空闲时钟落下不管：
						// 启用空闲时钟的设备重新开始计时，未启用的则复位窗口。
						if state.UsesAudioIdleClock() {
							startAudioIdle()
						} else {
							state.ResetAudioIdleWindow()
						}
						continue
					}
				}

				// 说话人准入判断：声纹模式可能要求“只有已登记的说话人才能和助手对话”。
				// 返回 false 时本条 ASR 结果被丢弃（不进 LLM），但连接保留。
				if a.session != nil {
					allowChat, denyReason := a.session.ShouldAllowSpeakerChat(speakerResult, speakerInterrupted)
					if !allowChat {
						log.Infof(
							"丢弃ASR结果并跳过STT/LLM: device=%s, reason=%s, speaker_interrupted=%v, speaker_result=%+v, text=%q",
							state.DeviceID,
							denyReason,
							speakerInterrupted,
							speakerResult,
							text,
						)
						state.Asr.ClearHistoryAudio()

						// 两条路径：
						//   · 非 realtime：丢弃后就结束本轮循环（auto 模式等下一次 listen start）；
						//   · realtime：识别流要保持可用，所以立即重启 ASR 继续听下一句。
						if !state.IsRealTime() {
							startAudioIdle()
							return
						}
						if restartErr := a.RestartAsrRecognition(ctx); restartErr != nil {
							log.Errorf("丢弃ASR结果后重启识别失败: %v", restartErr)
							if onError != nil {
								onError(restartErr)
							}
							return
						}
						startAudioIdle()
						continue
					}
				}

				// 创建用户消息，使用 hook 改写后的文本进入后续副作用链
				userMsg := &schema.Message{
					Role:    schema.User,
					Content: text,
				}

				// 生成 MessageID（使用 MD5 哈希缩短长度，避免超过数据库 varchar(64) 限制）
				// 原始格式：{SessionID}-{Role}-{Timestamp}
				// 为什么还要 MD5：SessionID 是较长的 UUID，再拼上角色和时间戳很容易超过数据库
				// varchar(64)，一旦超长写入会直接失败/被截断。MD5 十六进制固定 32 字符，既够长足够唯一，
				// 又不会超列宽（LLMManager.AddMessage 用同样的算法保证两侧一致）。
				rawMessageID := fmt.Sprintf("%s-%s-%d",
					state.SessionID,
					userMsg.Role,
					time.Now().UnixMilli())
				// 使用 MD5 哈希生成固定32字符的十六进制字符串
				hash := md5.Sum([]byte(rawMessageID))
				messageID := hex.EncodeToString(hash[:])

				// 同步添加到内存中（用于 LLM 上下文）
				state.AddMessage(userMsg)

				// 获取音频数据（ASR 历史音频）
				// 取走并清空：这份音频要跟着用户消息一起保存，用于历史回放；
				// 清空则保证同一条消息不会被保存两次音频。
				audioData := state.Asr.GetHistoryAudio()
				state.Asr.ClearHistoryAudio()

				// 通过回调保存消息
				// 交给 ChatSession 去落库（ASRManager 不关心存储实现）。
				if onMessageSave != nil {
					onMessageSave(userMsg, messageID, audioData)
				}

				// [2.2] 发 stt：发送给客户端的 ASR 结果也使用 hook 改写后的文本
				err = a.serverTransport.SendAsrResult(text)
				if err != nil {
					log.Errorf("发送asr消息失败: %v", err)
					if onError != nil {
						onError(err)
					}
					return
				}

				// realtime 媒体门控：正在播 MCP 音频时，把“暂停/继续/下一首/退出”等指令
				// 在本地直接识别并执行，不走 LLM —— 目的是让音乐控制零延迟。
				// handledByRealtimeGate=true 表示这条文本已被本地消费，不需要再送 LLM。
				if a.session != nil {
					handledByRealtimeGate, gateErr := a.session.tryHandleRealtimeMcpAudioASR(ctx, text)
					if gateErr != nil {
						log.Warnf("realtime媒体播放快速控制失败: device=%s text=%q err=%v", state.DeviceID, text, gateErr)
					}
					if handledByRealtimeGate {
						if !state.IsRealTime() {
							return
						}
						if restartErr := a.RestartAsrRecognition(ctx); restartErr != nil {
							log.Errorf("realtime媒体控制后重启ASR识别失败: %v", restartErr)
							if onError != nil {
								onError(restartErr)
							}
							return
						}
						startAudioIdle()
						continue
					}
				}

				// [2.3] 入队交接：添加到队列（迁移到 ASRManager 中处理）
				// 这一步完成 VAD/ASR 阶段到 LLM 阶段的“交接”：文本进入 chatTextQueue，
				// 由 ChatSession 的 processChatText → actionDoChat → DoLLmRequest 消费。
				if err := a.addAsrResultToQueue(text, speakerResult); err != nil {
					log.Errorf("开始对话失败: %v", err)
					if onError != nil {
						onError(err)
					}
					return
				}

				// 非 realtime 模式下，ASR 识别完成，归还资源
				// realtime 模式下，资源会在 RestartAsrRecognition 中自动管理（先归还旧资源再获取新资源）
				// 这里 return 后由 defer 里的 a.releaseResource() 真正归还，
				// 下一次 listen start 再重新 acquire —— 这就是 auto 模式“一轮一借一还”的落点。
				if !state.IsRealTime() {
					return
				}

				// realtime 模式下，重启 ASR 识别（RestartAsrRecognition 会先归还旧资源再获取新资源）
				// 之所以要立刻重启：realtime 要求“随时能听见用户”，不能等下一次 listen start。
				if restartErr := a.RestartAsrRecognition(ctx); restartErr != nil {
					log.Errorf("重启ASR识别失败: %v", restartErr)
					if onError != nil {
						onError(restartErr)
					}
					return
				}
				// realtime模式下, 继续循环处理下一个 ASR 结果
				continue
			} else {
				// text == ""：ASR 返回了空结果。空结果本身不一定是故障（用户只是没说话、
				// 或者 VAD 误触发），但如果“短时间 + 极快 + 连续多次”出现，就说明上游异常，
				// 会导致本循环疯狂重启 → CPU 忙轮询，所以需要保护。
				log.Debugf(
					"ASR空结果详情: status=%s, emptyReason=%s, client_voice_stop=%v, history_audio_samples=%d, voice_duration=%dms, voice_duration_in_session=%dms, idle_duration=%dms, realtime=%v",
					state.Status,
					result.EmptyReason,
					state.GetClientVoiceStop(),
					state.Asr.GetHistoryAudioLen(),
					state.Vad.GetVoiceDuration(),
					state.Vad.GetVoiceDurationInSession(),
					state.Vad.GetIdleDuration(),
					state.IsRealTime(),
				)
				// 如果看门狗已经判定空闲超时，就按超时收口，不再重启 ASR。
				if state.AudioIdleTimeoutPending() {
					closeAudioIdleTimeout(result.EmptyReason)
					return
				}
				// EmptyReason 是 provider 给出的“为什么空”的分类：
				if result.EmptyReason != "" {
					log.Debugf("ASR空结果已分类: reason=%s, status=%s", result.EmptyReason, state.Status)
					// 已分类说明这是可控状态（不是故障），清掉空结果保护计数。
					emptyResultWindowStart = time.Now()
					emptyResultCount = 0

					// 但这两种情况例外：provider 明确表示“服务端无响应 / 最终结果就是空”，
					// 说明当前流已经不可用，直接取消并重建，不继续等待。
					if result.EmptyReason == asr_types.EmptyReasonNoServerResponse ||
						result.EmptyReason == asr_types.EmptyReasonProviderEmptyFinal {
						state.Asr.CancelWithReason("ASRManager.StartAsrRecognitionLoop: empty final result from provider")
						resumeAudioIdle()
						continue
					}
				}

				now := time.Now()
				if now.Sub(emptyResultWindowStart) > emptyResultProtectWindow {
					emptyResultWindowStart = now
					emptyResultCount = 0
				}

				// 仅在单次 ASR 轮次耗时极短（如小于500ms）时才计入空结果计数，防止 ASR 故障导致的 CPU 忙轮询死循环；
				// 正常的 VAD 触发+静音检测流程耗时通常大于500ms，不应触发断连保护。
				// 为什么用“耗时”区分：故障时上游会瞬间返回空 → 循环立刻重启 → 又瞬间返回空，
				// 每轮只要几毫秒；而正常流程里必须等用户说话+静音判定，天然超过 500ms。
				// 所以“快”才是异常信号，只看次数会把正常的静音轮次误判成故障。
				turnDuration := now.UnixMilli() - state.Statistic.TurnStartTs
				if turnDuration < 500 {
					emptyResultCount++
					if emptyResultCount >= maxEmptyResultInWindow {
						err := fmt.Errorf("ASR短时间内连续返回空结果(%d次/%s)，触发保护并断开连接", emptyResultCount, emptyResultProtectWindow)
						log.Errorf("%v", err)
						if onError != nil {
							onError(err)
						}
						return
					}
				} else {
					// 耗时正常，说明是正常的无声识别，重置保护计数
					emptyResultCount = 0
					emptyResultWindowStart = now
				}

				// text 为空的情况
				select {
				case <-ctx.Done():
					log.Debugf("asr ctx done")
					return
				default:
				}

				log.Debugf("ready Restart Asr, state.Status: %s", state.Status)
				// realtime 模式下，即使状态是 LLMStart 或 TTSStart，也应该继续监听（允许重启ASR）
				// 非 realtime 模式下，只有 Listening 或 ListenStop 状态才允许重启ASR
				if isAllowedToRestart() {
					// 状态允许重启，重置等待计数
					invalidStatusWaitCount = 0
					// text 为空，检查是否需要重新启动ASR
					// diffTs 是“距离上次成功识别/启动以来过了多久”：超过 maxIdleTime(60s) 则不再重启，
					// 直接上报错误让上层关掉这条连接（避免一条永远识别不出东西的连接挂着不放）。
					diffTs := time.Now().Unix() - startIdleTime
					if startIdleTime > 0 && diffTs <= maxIdleTime {
						log.Warnf("ASR识别结果为空，尝试重启ASR识别, diff ts: %d", diffTs)
						if restartErr := a.RestartAsrRecognition(ctx); restartErr != nil {
							log.Errorf("重启ASR识别失败: %v", restartErr)
							if onError != nil {
								onError(restartErr)
							}
							return
						}
						resumeAudioIdle()
						continue
					} else {
						log.Warnf("ASR识别结果为空，已达到最大空闲时间: %d", maxIdleTime)
						if onError != nil {
							onError(fmt.Errorf("ASR识别结果为空，已达到最大空闲时间: %d", maxIdleTime))
						}
						return
					}
				} else {
					// 状态不允许重启的情况，短暂等待后继续循环，给状态恢复的机会
					// invalidStatusWaitCount 存在的意义：状态可能因为 LLM/TTS 正在跑而暂时不允许重启，
					// 等它结束就好；但如果一直不恢复，就不能无限等下去（否则本协程会永久活着占资源）。
					invalidStatusWaitCount++
					if invalidStatusWaitCount >= maxInvalidStatusWaitCount {
						// 等待超时，退出循环
						log.Debugf("状态为 %s，realtime: %v，等待%d次后仍无变化，退出ASR识别循环", state.Status, state.IsRealTime(), maxInvalidStatusWaitCount)
						return
					}
					// 短暂等待后继续循环，等待状态恢复
					log.Debugf("状态为 %s，realtime: %v，不允许重启，等待状态恢复 (等待次数: %d/%d)", state.Status, state.IsRealTime(), invalidStatusWaitCount, maxInvalidStatusWaitCount)
					// 短暂休眠让出 CPU，并等待 LLM/TTS 结束让状态恢复。
					// 行尾注释里的“100ms”是历史遗留，实际值是 200ms。
					time.Sleep(200 * time.Millisecond) // 等待100ms
					continue
				}
			}
		}
	}()
}

// trimFirstSpeechAudio 裁剪“首次检测到语音”时的前导音频。
//
// 背景：VAD 判定“这帧有声”之前，音频缓冲里已经攒了一段被判为静音的帧（用户开口前的环境音）。
// 这些数据不能全丢（字头/气息可能在最后 200ms 内），也不能全留（前面可能是几秒静音）。
//
// 规则：保留“当前帧 + 最多 200ms（maxFirstSpeechPreAudioMs）的历史数据”。
// 因为语音的字头往往紧贴判定点，往前多留 200ms 足够覆盖，同时不会把大段静音喂给 ASR
// （静音会让部分 ASR 提前触发断句，导致识别被截断或返回空）。
//
// 参数：
//
//	allData             —— ASR 缓冲里的全部数据（含前导静音 + 当前帧）；
//	currentFrameSamples —— 当前帧的样本数（必须完整保留）；
//	sampleRate/channels —— 用于把 200ms 换算成样本数。
//
// 返回：裁剪后的切片（新分配，不改动入参）；数据不足或参数异常时原样返回。
func trimFirstSpeechAudio(allData []float32, currentFrameSamples, sampleRate, channels int) []float32 {
	if len(allData) == 0 {
		return nil
	}
	// 参数异常/越界时保守返回全部数据：宁可多给一点，也不要因为算错长度丢掉字头。
	if currentFrameSamples <= 0 || currentFrameSamples > len(allData) || sampleRate <= 0 || channels <= 0 {
		return allData
	}

	maxPreSpeechSamples := sampleRate * channels * maxFirstSpeechPreAudioMs / 1000
	keepSamples := currentFrameSamples + maxPreSpeechSamples
	// 数据本来就比“允许保留的长度”短（例如刚开始说话），全部保留。
	if keepSamples >= len(allData) {
		return allData
	}

	// 从尾部开始取 keepSamples 个样本：尾部是“最近的数据”，也就是紧邻当前帧的内容。
	audio := make([]float32, keepSamples)
	copy(audio, allData[len(allData)-keepSamples:])
	return audio
}

// getSpeakerResult 获取暂存的声纹结果（带超时）
//
// 声纹识别与 ASR 是并行的两条流：OnVoiceSilence 触发声纹收尾后，provider 的回调会把结果写进
// session.pendingSpeakerResult 并关闭/发送 speakerResultReady 信号。这里以 200ms 为上限等待：
//   - 等到就用精确结果；
//   - 超时就用“当前已有的值”（可能为 nil），绝不为了声纹把整轮对话拖慢。
//
// 返回 nil 的常见情形：声纹未启用、本轮没有说话、或结果还没回来。
func (a *ASRManager) getSpeakerResult() *speaker.IdentifyResult {
	if a.session == nil || a.session.speakerManager == nil {
		return nil
	}

	// 本轮声纹流式识别未激活（整轮无语音/启动失败/上一轮已 finish），
	// 异步回调不会发送就绪信号，直接返回避免无谓的 200ms 等待。
	if !a.session.speakerManager.IsActive() {
		return nil
	}

	log.Debugf("speakerManager: %+v, IsActive: %+v", a.session.speakerManager, a.session.speakerManager.IsActive())

	// timeout 同时兜住“服务端一直不回调”的情况：200ms 后无论有没有结果都继续往下走。
	timeout := time.NewTimer(200 * time.Millisecond)
	defer timeout.Stop()

	var speakerResult *speaker.IdentifyResult
	select {
	case <-a.session.speakerResultReady:
		// 正常路径：收尾回调已把结果写好并通知。
		a.session.speakerResultMu.RLock()
		speakerResult = a.session.pendingSpeakerResult
		a.session.speakerResultMu.RUnlock()
	case <-timeout.C:
		// 超时后读取当前结果（可能为 nil）
		// 仍然读一次 pending：可能结果已写入但信号比预期晚（或已被上一轮消费）。
		a.session.speakerResultMu.RLock()
		speakerResult = a.session.pendingSpeakerResult
		a.session.speakerResultMu.RUnlock()
		log.Debugf("获取声纹识别结果超时，使用当前结果")
	}
	log.Debugf("获取声纹识别结果: %+v", speakerResult)
	return speakerResult
}

// [2.3] 入队交接：添加ASR结果到队列（迁移到 ASRManager 中处理）
//
// 本方法的职责很薄：把会话当作依赖注入进来的对象使用，实际入队逻辑在 ChatSession 上。
// 这样拆分是为了让 ASRManager 只依赖 ChatSession 的少数方法，而不是直接操作 ChatSession 的队列字段
// （队列是 ChatSession 的所有物，谁拥有谁负责，避免多处并发写同一队列）。
func (a *ASRManager) addAsrResultToQueue(text string, speakerResult *speaker.IdentifyResult) error {
	if a.session == nil {
		return fmt.Errorf("session is nil")
	}
	return a.session.AddAsrResultToQueue(text, speakerResult)
}
