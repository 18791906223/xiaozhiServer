# chat 包执行流程说明（新手向）

本文档解释 `internal/app/server/chat` 目录下每个文件在做什么、为什么这么写，以及一次完整对话
（以 `auto` 模式为例）中音频是怎么从设备上传、经过 VAD → ASR → LLM → TTS，再回到设备播放的。

> 说明：本文只覆盖 `chat` 目录本身。`plugins/`（LLM 输出流变换插件）与 `simplified/`（另一套精简实现）
> 是两个独立子目录，不在本文范围内。

> 代码里已经按执行顺序插入了 `[阶段.步骤]` 标号（例如 `// [1.1] VAD 识别：…`）。
> 在编辑器里搜索 `// [0.` ~ `// [6.`，或直接搜 `]`  前的方括号，就能按执行顺序通读整条链路。
> 标号与「文件 + 函数」的完整对照表在第 10 节。

---

## 1. 先记住三个概念


| 概念   | 类型                                      | 生命周期                         | 作用                                   |
| ---- | --------------------------------------- | ---------------------------- | ------------------------------------ |
| 设备连接 | `ChatManager`                           | 一次连接（websocket / mqtt+udp）一条 | 收发协议消息、管理握手、惰性创建/销毁会话                |
| 对话会话 | `ChatSession`                           | 一轮“监听 → 回答 → 再说”的可复用单位       | 串联 ASR / LLM / TTS / 声纹 / 媒体播放       |
| 设备状态 | `ClientState`（在 `internal/data/client`） | 与 `ChatManager` 同生命周期        | 保存配置、音频格式、音频缓冲、VAD 状态、指标时间戳等**共享数据** |


三个“经理”对象都是挂在 `ChatSession` 上的，各自拥有独立的 goroutine 循环：

- `ASRManager`：音频 → 文本
- `LLMManager`：文本 → 文本 / 工具调用
- `TTSManager`：文本 → 音频帧 → 协议消息

---



## 2. 一次连接的启动与握手

```
NewChatManager(deviceID, transport)
  ├─ GenClientState()                // 读设备配置，校验激活状态，建音频缓冲/时钟
  ├─ NewServerTransport()            // 协议发送封装（hello/tts/stt/iot/mcp…）
  ├─ McpTransport{...}               // IoT over MCP 桥接
  ├─ newChatHookHub()                // 事件钩子（ASR/LLM/TTS/指标）
  └─ streamtransform.NewRegistry()   // LLM 输出流变换插件

ChatManager.Start()
  ├─ go cmdMessageLoop()    // 读 JSON 控制消息（hello/listen/abort/iot/mcp/goodbye）
  └─ go audioMessageLoop()  // 读二进制音频（Opus）
```

握手是**一切的前置条件**，因为：

1. `hello` 消息里带 `audio_params`，即设备的**输入音频格式**（采样率/通道数）；
  ASR 与 VAD 都依赖它，所以 `ChatSession.Start()` 会先校验它是否已初始化。
2. 服务端要回一个 `hello` 响应，里面带**输出音频格式**（TTS 播放格式）和 UDP 参数，
  设备据此决定怎么解码下行音频。

`HandleHelloMessage` 的关键动作（顺序很重要）：

1. 校验传输类型（`websocket` / `mqtt_udp`）。
2. 首次 hello：`auth.A().EnsureSession(deviceID, preferredSessionID)` 拿到 `SessionID`，
  并置 `helloInited = true`；重复 hello 只刷新设备配置。
3. `applyOutputAudioFormatForTTS()`：**决定下行音频帧格式**。若 TTS 是小智自带类型，
  用 24kHz / 20ms，否则用默认 `types_audio` 常量。
4. `ensureSessionForHello()`：创建（或复用）`ChatSession`。
5. `sendHelloResponse()`：发出带输出格式的 hello。
6. `scheduleMcpInitOnHelloLocked()`：若设备声明了 `features.mcp`，异步初始化 IoT MCP 运行时
  （放异步是为了不阻塞握手）。



### 为什么 ChatSession 是“惰性创建 + 保留态”

`ChatSession` 不随连接创建，而是等到 `listen` 或 `hello` 时才建（`ensureSession*`）。
设备说完 `goodbye` 之后，会话不会立刻销毁，而是被“重置为静默态”并保留一段时间
（`scheduleRetainedSessionCleanup`，默认 10 分钟），这样设备再次唤醒时能复用会话
（省掉重复初始化 ASR/LLM/TTS 资源的开销）。超时才彻底 `CloseWithReason`。

---



## 3. 上行链路：音频 → VAD → ASR



### 3.1 音频进入缓冲区

```
audioMessageLoop()                 // chat.go
  └─ session.HandleAudioMessage()  // session.go
       └─ clientState.OpusAudioBuffer <- data   // 容量 100，满了直接丢弃并告警
```

缓冲满了丢弃而不是阻塞，是为了**保护收包协程**：音频是可丢的实时数据，
积压的旧音频比丢一帧更糟。

### 3.2 VAD：判断“有人说话 / 说完了”

`ChatSession.Start()` 里启动 `ASRManager.ProcessVadAudio()`，这是一个常驻 goroutine：

1. 从 `OpusAudioBuffer` 取一帧 Opus，用 `audio.GetAudioProcesser` 解码成 PCM float32。
2. 决定是否需要 VAD（`needVad`）：
  - `manual` 模式：**跳过 VAD**（设备端按键/手动控制，直接当有声）。
  - `Asr.AutoEnd == true`：跳过 VAD，由 ASR provider 自己判断结束。
  - 其余（auto 模式）：使用 VAD。
3. VAD 资源**懒加载 + 空闲 2 秒释放**（`pool.Acquire/Release`），
  避免长时间独占资源池里的实例。
4. 检测到首帧语音时，只保留最多 200ms 的前导静音（`trimFirstSpeechAudio`），
  既保证字头不被截断，又不把大段静音塞给 ASR。
5. 判定说话结束：`state.IsSilence(idleDuration)` 为真 → 调用 `state.OnVoiceSilence()`，
  通知 ASR “这一段说完了，请给最终结果”。

同时还有一个独立看门狗 `runAudioIdleTimeoutWatchdog`（每秒 tick）：
如果长时间（`GetMaxIdleDuration`）没有任何语音输入也没有 ASR 文本，
就关闭会话（`chatSessionCloseReasonAudioIdleTimeout`）或触发 ASR 收口，
避免连接一直挂着不放资源。

### 3.3 ASR：流式识别

`listen` 消息（`state=start`）到达 → `ChatSession.HandleListenStart` → 异步 `OnListenStart`：

```
RestartAsrRecognition(ctx)                       // asr.go
  ├─ 取消上一轮 ASR 上下文、清空音频缓冲
  ├─ pool.Acquire("asr", provider, config)       // 池化复用
  ├─ state.Asr.AsrAudioChannel = make(chan []float32, 100)
  ├─ asrProvider.StreamingRecognize(ctx, AsrAudioChannel) -> AsrResultChannel
  └─ state.MarkTurnStart()                       // 本轮耗时统计起点

StartAsrRecognitionLoop(ctx, onMessageSave, onError)
  └─ 循环 state.RetireAsrResult(ctx) 拿识别结果
```

注意：VAD 检测到“有声”后会 `state.Asr.AddAudioData(pcm)`，数据实际是写进
`Asr.AsrAudioChannel`，由 ASR provider 消费——**VAD 是 ASR 的“闸门”**。

拿到**非空最终文本**后，`StartAsrRecognitionLoop` 依次做：

1. 记录耗时指标（`TraceAsrFinalText`）。
2. realtime 模式下按 `realtime_mode` 决定是否打断当前 LLM/TTS。
3. `state.OnVoiceSilence()`（收口），并读取声纹识别结果（`getSpeakerResult`，最多等 200ms）。
4. 触发 `ASR_OUTPUT` 钩子（可改写文本/声纹结果，可要求停止流程）。
5. `ShouldAllowSpeakerChat` 判断是否允许该说话人参与对话（声纹模式可能要求“必须是已登记的说话人”）。
6. 写入内存上下文 `state.AddMessage(userMsg)`，生成 `MessageID`，通过 `eventbus` 发布
  “新增消息”事件（含本轮音频，用于历史回放）。
7. `serverTransport.SendAsrResult(text)` → 给设备发 `stt` 消息（端上屏幕显示识别文本）。
8. `AddAsrResultToQueue(text, speakerResult)` → 推入 `chatTextQueue`，**交接给 LLM 阶段**。

异常处理（都很重要，是稳定性保障）：

- **可恢复错误**（如豆包/讯飞/阿里实例失效）：在保护窗口内最多重试 3 次，超了就断连。
- **空结果保护**：只在“本轮耗时极短（<500ms）”时才计入空结果计数，避免上游异常导致
CPU 忙轮询死循环；正常静音导致的空结果不会触发断连。

> auto 模式：识别完一轮后 `StartAsrRecognitionLoop` **直接返回并释放 ASR 资源**，
> 等下一次 `listen start` 再重建。
> realtime 模式：识别完立即 `RestartAsrRecognition` 继续监听，保持长驻。

---



## 4. 中段链路：文本 → LLM → 工具



### 4.1 队列到 ChatSession

```
chatTextQueue (util.Queue, 容量 10)
  └─ processChatText()  // 常驻 goroutine
       └─ actionDoChat(ctx, text, speakerResult)
```

`actionDoChat` 是一张**路由表**，按优先级依次判断：

1. `tryHandleRealtimeMcpAudioASR`：realtime 下正在播 MCP 音频时，
  本地快速识别“暂停/继续/下一首/退出”等指令，直接控制播放器，不走 LLM。
2. OpenClaw 模式：进入/退出关键词与已进入模式下的消息转发。
3. 退出词（“再见”“退出对话”…）→ 发布 `TopicExitChat` 事件，交给上层关闭会话。
4. `switchTTSForSpeaker`：根据声纹结果切换音色（如不同家庭成员不同声音）。
5. 组装工具：`mcp.GetToolsByDeviceIdWithTransport` 取设备可用 MCP 工具，
  转成 Eino 的 `ToolInfo`；若没有可用知识库则移除 `search_knowledge`。
6. `llmManager.DoLLmRequest(...)`。



### 4.2 LLM 请求

```
DoLLmRequest
  ├─ GetMessages()                        // 组装对话：system prompt + 历史 + 当前输入
  │    ├─ system prompt = 设备 prompt + 全局 prompt + 当前时间 + 记忆 + 声纹 prompt + 知识库路由策略
  │    └─ 历史来自 ClientState（内存），第 1 节启动时异步从 Redis / Manager 加载
  ├─ EmitLLMInput()                       // LLM_INPUT 钩子（可改写消息/工具/停止）
  ├─ handleLLMWithContextAndTools()
  │    ├─ pool.Acquire("llm", ...)         // 池化 LLM provider
  │    ├─ llmProvider.ResponseWithContext(...)   // 返回 msgChan
  │    └─ goroutine: msgChan → streamtransform.Pipeline → responseChannel
  └─ HandleLLMResponseChannelSync/Async()  // 把 responseChannel 包成队列项
```

`streamtransform.Pipeline` 是“LLM 输出流变换”插件的挂载点（例如 `output_segmenter`
把连续文本切成适合播报的句子）。**为什么要这一层**：分句/过滤这类逻辑与具体 LLM 无关，
独立成插件后可以按配置开关，不改主流程。

### 4.3 消费 LLM 响应

```
llmResponseQueue (util.Queue, 容量 10)
  └─ processLLMResponseQueue()   // 常驻 goroutine
       └─ handleLLMResponse(ctx, userMessage, llmResponseChannel)
            ├─ 文本 -> ttsManager.handleTextResponseWithHooks()   // 喂给 TTS
            ├─ 累积 fullText                                       // 用于保存历史
            ├─ ToolCalls -> toolExecutor.Submit()                  // 并发执行工具
            └─ IsEnd:
                 ├─ 无工具 -> 保存 assistant 消息（两阶段保存的第 1 阶段：只有文本）
                 └─ 有工具 -> handleToolCallResponse()
                      ├─ 等所有工具执行完（Wait）
                      ├─ 保存 tool 结果消息
                      ├─ 若有音频/资源产物（hasMediaOutput）则交由媒体播放器播放
                      └─ 工具成功且未被要求停止 -> 带上工具结果再次请求 LLM（nest+1）
```

**“nest”是什么**：工具调用后要继续问 LLM，就会产生“嵌套的一轮”。
`context` 里的 `nest` 值用来保证 `tts_start` / `tts_stop` 这种协议消息只在**最外层一轮**
发一次，避免同一轮对话里 TTS 状态被反复重置。

**两阶段消息保存**（`AddMessage` + `onEndFunc`）：

1. 第 1 阶段：立刻保存文本（让历史记录先有内容），并记住 `MessageID`。
2. 第 2 阶段：TTS 播完后，把累积的音频帧通过 `eventbus` 更新到同一条消息上。

这样即使播报中途被打断，文本也不会丢。

### 4.4 工具调用的三种“结果形态”

`tool.go` 的 `executeToolCall` 会把工具返回解析成不同形态：


| 形态                       | 来源                        | 处理                                                        |
| ------------------------ | ------------------------- | --------------------------------------------------------- |
| 本地 MCP 响应（`MCPResponse`） | `local_mcp_tool.go` 注册的工具 | `handleLocalToolResult` 解析，可能是 action/audio/content/error |
| 标准 MCP `CallToolResult`  | 远程 MCP 工具                 | `handleToolResult` 解析文本内容                                 |
| 音频内容 / 资源链接              | 远程 MCP 工具                 | 交给 `mediaPlayer.PlaySourceWithHandle` 播放                  |


本地工具通过 `ctx.Value("chat_session_operator")` 拿回 `ChatSessionOperator` 接口
（定义在 `types.go`），从而回调会话能力（退出、清历史、切角色、检索知识库、控制音乐）。
**为什么用接口**：`LLMManager`/工具层不应该直接依赖 `ChatSession` 的具体实现，
否则会形成循环依赖。

---



## 5. 下行链路：文本 → TTS → 音频帧

`TTSManager` 里有 4 条关键循环，理解它们是理解下行链路的关键：

```
Start(ctx)
  ├─ go runDelayedSentenceLoop()   // 句子控制消息的延迟发送
  ├─ go runSenderLoop()            // 唯一发送协程
  └─ processTTSQueue()             // 消费句子队列，生成音频
```



### 5.1 `processTTSQueue`：文本 → 音频帧

对每个 `TTSQueueItem`：

- 非双流式：调 `handleTts` → `generateTtsOnly`（取池化 TTS provider，
`TextToSpeechStream` 返回音频帧 channel）→ 依次向 `sessionAudioQueue` 推
`SentenceStart → Frame × N → SentenceEnd`。
- 双流式（配置 `double_stream` 且 provider 支持）：`handleDualStreamTts`
把 LLM 的文本流**边收边喂**给 TTS provider，实现更低的首帧延迟。



### 5.2 `runSenderLoop`：唯一的“发送出口”

所有下发给设备的音频与控制消息都从这里出：

- 维护 `playbackTail`（预计播完时刻）做**流控**：客户端本地缓存约 120ms 音频，
发送时按帧时长推进 `playbackTail`，即“不超过客户端缓冲水位地提前发送”。
- 处理 `AudioQueueKindTtsStart / TtsStop`：发协议消息 `tts state=start/stop`。
发 `tts_stop` 前会等到 `playbackTail` 走完 + 一小段保护时间（`ttsPlaybackCompletionGrace`），
避免客户端尾音还没播完就切状态。
- 用 `audioGeneration`（代际号）做**打断失效**：打断时代际 +1，
队列里旧代际的元素会被直接丢弃并回调 `OnEnd(context.Canceled)`。



### 5.3 `runDelayedSentenceLoop`：为什么句子消息要延迟

`sentence_start/sentence_end` 这些文本控制消息是给端上 UI/字幕用的，
它们需要和音频**对齐**（音频先缓冲约 120ms 才播）。所以这里把句子消息
按 `sentenceControlDelay()` 延迟排队，再交给 `runSenderLoop` 发送。

### 5.4 打断（barge-in）


| 函数                          | 用途                                            |
| --------------------------- | --------------------------------------------- |
| `InterruptAndClearQueue*`   | 清空待发送队列并继续运行（不结束 TTS）                         |
| `InterruptAndStop*`         | 登记“待发 `tts_stop`”，清空队列后由 `runSenderLoop` 统一收口 |
| `ChatSession.StopSpeaking*` | 更高层：取消会话上下文、清空 LLM/TTS/文本队列、挂起媒体播放            |


realtime 模式下有多种打断时机，由 `chat.realtime_mode` 配置决定：

- `1`：VAD 连续语音超过 360ms 就打断；
- `2`：ASR 拿到结果就打断；
- `3`：声纹 peek 命中（识别出说话人）就打断；
- `4`：ASR 首字就打断。

打断统一走 `StopAssistantOutputAfterAsrWithReason`（只停 assistant 输出，不挂起媒体），
这样 realtime 里音乐可以继续播、只有 TTS 被掐掉。

---



## 6. 媒体播放（`media_player.go`）

```
SessionMediaPlayer         // 门面：ChatSession 用它播音乐/音频
  └─ deviceMediaRuntime    // 每设备运行时：状态、歌单、播放活动、会话绑定
       └─ mediaPlaybackCoordinator  // 全局注册表：agent 歌单、设备运行时
```

关键点：

- **两种模式**：`standalone`（播一次就结束）与 `agent_playlist`（同 agent 的设备共享歌单，
可跨设备接续播放）。
- **输出复用 TTS 通道**：播放器通过 `mediaOutputBridge` 向 `ttsManager` 申请**独占**
（`BeginExclusiveMediaPlayback`），保证媒体音频和 TTS 语音不会互相穿插。
- **暂停原因**（`mediaPauseReason`）：区分“用户暂停”和“被打断暂停”，
决定恢复时要不要自动继续播。
- **断线保留**：`detachSession(preserve=true)` 保留播放状态，
重连后 `attachSession` 可恢复（`resumeOnAttach`）。

`realtime_media_gate.go` 是配套的门控：realtime 下正在播 MCP 音频时，
ASR 文本会被本地规则先拦截成“暂停/继续/切歌/退出”指令，
识别不了再交给 LLM——目的是让音乐控制**零延迟**。

---



## 7. 其它文件速查


| 文件                                | 职责                                                        |
| --------------------------------- | --------------------------------------------------------- |
| `chat.go`                         | `ChatManager`：连接生命周期、握手、消息分发、会话创建/保留、speak_request 主动播报链路 |
| `session.go`                      | `ChatSession`：listen 处理、VAD/ASR 启动、文本队列、OpenClaw 路由、结束清理  |
| `asr.go`                          | `ASRManager`：VAD 循环、ASR 识别循环、资源池管理、空闲看门狗                  |
| `llm.go`                          | `LLMManager`：LLM 请求、输出流变换、工具轮次、消息两阶段保存、TTS 交接             |
| `tts.go`                          | `TTSManager`：句子队列、TTS 生成、统一发送循环、打断与指标收口                   |
| `tool.go`                         | 工具调用执行器：并发执行、结果解析、音频/资源播放                                 |
| `local_mcp_tool.go`               | 注册本地 MCP 工具（退出对话、清历史、切角色、检索知识库…）                          |
| `session_mcp_tool.go`             | 上述工具在 `ChatManager`/`ChatSession` 上的具体实现                  |
| `local_mcp_media_control_tool.go` | 本地“媒体控制”工具（暂停/继续/切歌/歌单）                                   |
| `mcp_response_types.go`           | 本地工具的统一响应协议（action/audio/content/error）                   |
| `media_player.go`                 | 会话级媒体播放器与全局协调器                                            |
| `realtime_media_gate.go`          | realtime 媒体播放期间的 ASR 门控与本地控制指令识别                          |
| `openclaw_warmup.go`              | OpenClaw 等待期的暖场话术（生成 + 定时播报 + 被正式回复抢占）                    |
| `server_transport.go`             | 协议消息发送封装（hello/tts/stt/iot/mcp/goodbye…）                  |
| `speaker.go`                      | 声纹识别 provider 的薄封装                                        |
| `vllm.go`                         | 视觉识别（图片理解）的入口                                             |
| `mcp.go`                          | `McpTransport`：IoT over MCP 的收发桥接                         |
| `common.go`                       | 会话级公共动作：`stopSpeaking` 系列、`ResetToSilentState`            |
| `types.go`                        | `ChatSessionOperator` 接口（工具层 ↔ 会话层的解耦契约）                  |
| `util.go`                         | 小工具：去标点、唤醒词判断                                             |


---



## 8. auto 模式一次完整对话（时序）

```
设备                    服务端                                                           说明
 │  hello                 │                                                            握手，协商音频格式
 │ ─────────────────────► │ [0.2] HandleHelloMessage → [0.4] EnsureSession → [0.3] 回 hello
 │  listen(start, auto)   │                                                            开始拾音
 │ ─────────────────────► │ [0.5] HandleListenStart → OnListenStart → [1.2] RestartAsrRecognition
 │  Opus 音频帧 × N       │                                                            上行音频
 │ ─────────────────────► │ [1.0] OpusAudioBuffer → [1.1] ProcessVadAudio
 │                        │   [1.1] 解码 PCM → VAD 检测
 │                        │   [1.2] 有声帧 → AsrAudioChannel → ASR provider（StreamingRecognize）
 │  （用户停顿）           │   [1.1] VAD 判定静音 → OnVoiceSilence → 要求最终结果
 │                        │ [2.0] StartAsrRecognitionLoop 收到文本
 │  stt(文本)             │ ◄────── [2.2] SendAsrResult                                端上显示识别文本
 │                        │ [2.3] AddAsrResultToQueue → chatTextQueue
 │                        │        │
 │                        │ [3.0] processChatText → [3.1] actionDoChat → [3.3] DoLLmRequest
 │                        │        │ [3.2] 组装 system prompt + 历史 + 工具
 │                        │        │ [3.3] LLM 流式输出 → 分句插件 → responseChannel
 │                        │ [3.4] handleLLMResponse
 │                        │        │ [5.0] 文本 → TTSManager.handleTextResponse
 │                        │        │ [5.1] processTTSQueue → [5.2] generateTtsOnly → 音频帧
 │                        │        │ sessionAudioQueue
 │                        │        │ [5.3] runSenderLoop（唯一出口，按 playbackTail 流控）
 │ tts(start)             │ ◄────── [5.3] SendTtsStart
 │ sentence_start         │ ◄────── [5.3] SendSentenceStart                             UI 字幕
 │ 音频帧 × M             │ ◄────── [5.4] SendAudio                                    设备播放
 │ sentence_end           │ ◄────── [5.3] SendSentenceEnd
 │ tts(stop)              │ ◄────── [5.5] SendTtsStop（等播放尾指针 + 保护时间）
 │                        │ 保存 assistant 消息第 2 阶段（带音频）
 │  （设备接着说下一轮 / 或 goodby）                                                  auto 模式会等下一次 listen start
```

（若这一轮里 LLM 调用了工具，则 `[3.4]` 与 `[5.x]` 之间会插入 `[4.0]→[4.1]→[4.2]→[4.3]`：
提交工具、执行工具、等结果并落库、带着结果再问一轮 LLM。）

打断发生在任意时刻（`abort` 消息 / 检测到新语音 / realtime 特定时机）：
`StopSpeaking*` 会取消会话上下文、清空三条队列、递增音频代际（丢弃在途音频），
再挂起媒体播放，使设备立即安静下来。

---



## 9. 阅读代码的建议顺序

1. `chat.go` —— 先搞清“连接怎么起来、消息怎么分发”。
2. `session.go` —— 再看“会话怎么处理 listen、怎么把音频接进 ASR”。
3. `asr.go` —— VAD 与 ASR 循环（最核心的音频逻辑）。
4. `llm.go` + `tool.go` —— 文本进 LLM、工具轮次。
5. `tts.go` —— 音频怎么被生成和发送（流控与打断在这里）。
6. `server_transport.go` —— 对照协议文档看每条消息长什么样。
7. `media_player.go` / `realtime_media_gate.go` —— 媒体播放与 realtime 门控。
8. `openclaw_warmup.go`、`local_mcp_*`、`session_mcp_tool.go`、`mcp_response_types.go` —— 扩展能力。

---



## 10. 执行顺序标号索引

代码里按执行顺序插入了 `[阶段.步骤]` 标号，格式统一为 `// [1.1] 名称：说明`。
标号写在对应函数/调用点的注释最前面，因此直接按标号排序就是执行顺序。
下面是「标号 → 位置 → 含义」的完整对照表（同一标号出现在多处，表示这几处是实现同一步骤的不同环节）。


| 标号      | 位置（文件: 函数 / 调用点）                                                                                        | 含义                             |
| ------- | ------------------------------------------------------------------------------------------------------- | ------------------------------ |
| `[0.0]` | `chat.go` `NewChatManager`                                                                              | 连接建立：一条设备连接一个 ChatManager      |
| `[0.1]` | `chat.go` `Start`                                                                                       | 启动收包：控制消息循环 + 音频循环             |
| `[0.2]` | `chat.go` `HandleHelloMessage`                                                                          | hello 握手，协商输入/输出音频格式           |
| `[0.3]` | `chat.go` `sendHelloResponse`；`server_transport.go` `SendHello`                                         | 回 hello（带下行音频格式 / UDP 参数）      |
| `[0.4]` | `chat.go` `ensureSessionForHello`                                                                       | 惰性创建 / 复用 ChatSession          |
| `[0.5]` | `session.go` `HandleListenStart`                                                                        | listen start：开始一轮拾音            |
| `[1.0]` | `chat.go` `audioMessageLoop`（`RecvAudio` / `HandleAudioMessage`）；`session.go` `HandleAudioMessage`      | 读取音频帧，写入 `OpusAudioBuffer`     |
| `[1.1]` | `asr.go` `ProcessVadAudio`（含 `OnVoiceSilence` 触发点）                                                      | VAD 判定「有人说话 / 说完了」             |
| `[1.2]` | `asr.go` `RestartAsrRecognition`（`StreamingRecognize`）、`AddAudioData` 调用点；`session.go` `OnListenStart`  | 建 ASR 流 / 把音频送进 ASR            |
| `[2.0]` | `asr.go` `StartAsrRecognitionLoop`（`RetireAsrResult`）；`session.go` `OnListenStart`                      | 读 ASR 结果                       |
| `[2.1]` | `asr.go` ASR 结果分支（`OnVoiceSilence` + 声纹 + `ASR_OUTPUT` 钩子）                                              | 说话结束收口、声纹与钩子                   |
| `[2.2]` | `asr.go` `SendAsrResult` 调用点；`server_transport.go` `SendAsrResult`                                      | 发 `stt` 给设备                    |
| `[2.3]` | `asr.go` `addAsrResultToQueue`；`session.go` `AddAsrResultToQueue*`（`chatTextQueue.Push`）                | 入队交接：文本进 `chatTextQueue`       |
| `[3.0]` | `session.go` `processChatText`（`chatTextQueue.Pop`）                                                     | 取队列：消费 ASR 文本                  |
| `[3.1]` | `session.go` `actionDoChat`                                                                             | 路由：本地指令 / OpenClaw / 退出词 / LLM |
| `[3.2]` | `llm.go` `GetMessages`（调用点 + 定义）                                                                        | 组装消息：system prompt + 历史 + 当前输入 |
| `[3.3]` | `llm.go` `DoLLmRequest`、`ResponseWithContext`、`llmResponseQueue.Push`；`session.go` 调用点                  | 请求 LLM，并把响应流包成队列项入队            |
| `[3.4]` | `llm.go` `processLLMResponseQueue`（`Pop`）、`handleLLMResponse`                                           | 消费 LLM 响应                      |
| `[4.0]` | `tool.go` `Submit`                                                                                      | 提交工具：并发执行（去重、非阻塞）              |
| `[4.1]` | `tool.go` `executeToolCall`                                                                             | 执行单个工具，整理返回值                   |
| `[4.2]` | `tool.go` `Wait`、`handleToolCallResponse`（含 `WaitMedia`）                                                | 等工具完成 / 等媒体播完，落历史与上下文          |
| `[4.3]` | `tool.go` `handleToolCallResponse` 尾部 `DoLLmRequest`                                                    | 带工具结果再问一轮 LLM（nest+1）          |
| `[5.0]` | `llm.go` `handleTextResponseWithHooks` 调用点；`tts.go` `handleTextResponseWithHooks`、`pushTTSQueueItem`    | 文本入 TTS 队列（切句 / 双流式投流）         |
| `[5.1]` | `tts.go` `Start`、`processTTSQueue`（`ttsQueue.Pop`）；`session.go` 启动点                                     | 生成音频帧                          |
| `[5.2]` | `tts.go` `generateTtsOnly`、`handleStreamTts`                                                            | 调 TTS provider 合成              |
| `[5.3]` | `tts.go` `runSenderLoop`；`server_transport.go` `SendTtsStart` / `SendSentenceStart` / `SendSentenceEnd` | 唯一发送出口（流控、协议消息、字幕）             |
| `[5.4]` | `tts.go` `sendAudioStream`；`server_transport.go` `SendAudio`                                            | 发音频帧给设备                        |
| `[5.5]` | `tts.go` `RequestTurnEnd`；`server_transport.go` `SendTtsStop`                                           | 轮次收口（收完音频再发 `tts_stop`）        |
| `[5.6]` | `tts.go` `InterruptAndClearQueue` 系列；`common.go` `StopSpeaking`                                         | 打断（清队列、作废代际、挂起媒体）              |
| `[6.0]` | `tts.go` / `media_player.go` `BeginExclusiveMediaPlayback`                                              | 媒体向 TTS 申请独占下行输出               |
| `[6.1]` | `media_player.go` `PlaySourceWithHandle`                                                                | 播放媒体源                          |
| `[6.2]` | `realtime_media_gate.go` `tryHandleRealtimeMcpAudioASR`                                                 | realtime 媒体门控（本地识别播放指令）        |


标号只加注释、不改任何代码语义：新增行全部是 `//` 注释，原有代码行与字段对齐均未改动。