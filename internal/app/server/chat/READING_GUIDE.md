# chat 包源码阅读指南（按执行顺序 + 依赖顺序）

> 行号基准：本文所有 `文件:行号` 都对应你**当前工作区**的文件内容（`chat` 包此刻处于未提交修改状态）。
> 任何代码改动都会让行号漂移，建议用第 9 节的检索命令重新定位。
>
> 配套文档：`ARCHITECTURE.md` 讲的是「每个文件在做什么 + 一次对话的时序」，还有 `// [x.y]` 执行顺序标号索引。
> 本文讲的是「**按什么顺序读、每一步在哪里、读到这里该往哪跳**」，两者互补。建议先扫一眼 `ARCHITECTURE.md` 的第 1、2 节建立名词印象，然后回到本文按批次读代码。

---

## 0. 先回答你的问题：从 `asr.go:92` 往上追，这个读法对不对？

方向对，但只够走一两跳，而且你选的起点不是逻辑起点。

### 0.1 「往上追调用者」在同步调用链上是成立的

同步调用可以一直往上追。你举的 `asr.go:92`（`NewASRManager`）完整往上追是这样：

```
asr.go:92              NewASRManager
 ← session.go:223      NewChatSession 里 s.asrManager = NewASRManager(...)
 ← chat.go:1078        ensureSessionInternal 里 NewChatSession(...)
 ← chat.go:1028        ensureSessionForHello → ensureSessionInternal
 ← chat.go:721         HandleHelloMessage 里 c.ensureSessionForHello()
 ← chat.go:617         handleTextMessage 的 switch 分发（hello 分支在 chat.go:629）
 ← chat.go:547         cmdMessageLoop 里 c.handleTextMessage(message)
 ← chat.go:483         ChatManager.Start 里 go c.cmdMessageLoop(c.ctx)（chat.go:484）
 ← app.go:365          OnNewConnection 里 chatManager.Start()（goroutine 起于 app.go:353）
 ← app.go:334          chat.NewChatManager(deviceID, transport)
 ← app.go:320          App.OnNewConnection
 ← websocket_server.go:197  s.onNewConnection(wsConn)（或 mqtt_udp_adapter.go:536）
 ← websocket_server.go:159  WebSocketServer.internalHandleChat（HTTP handler）
 → app.go:64           App.Run → main.go:138 appInstance.Run() → main.go:20 main()
```

这条路是真实存在的，本文第 3 节把它整条列了出来。

### 0.2 但这个包里有 4 种「合法地打断调用栈」的写法，追到那里就断了


| 断点类型                   | 例子（位置）                                                                                                                                                              | 追到这里的正确动作                                     |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------- |
| `go func()` 起协程        | `chat.go:484`、`chat.go:485`、`app.go:353`、`session.go:396`、`session.go:1539`/`1561`                                                                                  | 别追了。启动点只是「按了开关」，真正的逻辑在这个 goroutine 的函数体里，往下读它 |
| 队列（跨协程交接）              | `chatTextQueue`(session.go:1791 入 → 1805 出)、`llmResponseQueue`(llm.go:1014 入 → 761 出)、`ttsQueue`(tts.go:1055 入 → 1737 出)、`sessionAudioQueue`(tts.go:1366 入 → 357 出) | 顺着「入队」跳到「出队的那个循环」，从出队处继续往下读                   |
| channel（生产者/消费者互不知道对方） | `OpusAudioBuffer`(session.go:614 入 → asr.go 消费)、`Asr.AsrAudioChannel`(asr.go:533 入 → 830 消费)、`AsrResultChannel`(asr.go:830 产 → asr.go:967 取)                        | 同上：找到消费端，从消费端往下读                              |
| 接口 / 回调节点              | `ctx.Value("chat_session_operator")`(chat.go:319 注入)、`hookHub`(chathooks)、`OnVoiceSilenceMetricCallback`(session.go:231)、`OnClose` 回调(chat.go:337)                  | 用「接口方法名 / 回调字段名」全局搜索，跳到实现处或触发处                |


**这就是为什么你会觉得「往上追不到头」**：链路在 `go`、队列、channel 处变成了「生产者 → 消费者」的关系，而不是「被调用 → 调用者」的关系。继续往上追只会得到一堆无意义的「是谁 new 了我」。

### 0.3 `asr.go:92` 本身是个坏起点

```go
// asr.go:92-105
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
```

它只是「造结构体 + 应用选项」，没有业务逻辑，而且有个特殊约定：**构造时** `session` **是 nil，由** `session.go:224` **回填**（因为 `ChatSession` 和 `ASRManager` 互相需要对方，只能先建对象后补引用）。从它往上追，你只能知道「谁 new 了我」，永远不知道「我什么时候被跑起来、以什么节奏跑」。

**真正该读的入口是** `asr.go:202` ****`ProcessVadAudio` —— 那是常驻循环，业务逻辑都在里面。

### 0.4 所以推荐的读法：三步法

1. **先立骨架（自上而下）**：把「进程 → 连接 → 握手 → 会话 → 三条循环」这条主链一次性走完（本文第 3 节），只求知道「有哪些角色、谁在什么时候起谁」，不求看懂细节。
2. **再走一遍完整对话（按执行顺序）**：顺着 `[1.x] → [2.x] → [3.x] → [4.x] → [5.x]` 标号，把「一句话怎么从音频变成回答」读通（本文第 4 节批次 3~5）。
3. **最后按主题横向深挖**：打断、媒体播放、OpenClaw、声纹、工具（批次 6~7）。这些是「主链之外的旁路」，主链不清时读它们只会更乱。

**遇到任何函数，先问一句：「它是被同步调用的，还是被** `go`**/队列/channel 驱动的？」** 前者往上追，后者往下读并去消费端。这一条判据能解决你 90% 的「我该往哪看」的困惑。

---



## 1. 三个必须先分清的概念


| 概念     | 类型            | 粒度                                        | 载体                     |
| ------ | ------------- | ----------------------------------------- | ---------------------- |
| 设备连接   | `ChatManager` | 一条 websocket / mqtt+udp 连接一个              | `chat.go` 全文           |
| 对话会话   | `ChatSession` | 一次「听 → 答」到一个静默保留期一个（可跨多轮复用）               | `session.go` 全文        |
| 设备共享状态 | `ClientState` | 与 `ChatManager` 同生命周期，被 ASR/LLM/TTS 三方共读写 | `internal/data/client` |


挂在 `ChatSession` 上的三个「经理」（`session.go:223-228` 一次性创建完）：

- `ASRManager`（`asr.go`）：音频 → 文本
- `LLMManager`（`llm.go`）：文本 → 文本 / 工具调用
- `TTSManager`（`tts.go`）：文本 → 音频帧 → 协议消息

**关键认知**：这三个 Manager 之间**不互相调用**，它们靠 `ChatSession` 编排 + 队列 + `ClientState` 里的状态位协作。所以「读一个 Manager 时要顺手读谁」的答案通常是「`session.go` 里那个开始/收口它的地方 + `ClientState`」，而不是另一个 Manager。

---



## 2. 全局骨架：四层

```
第 0 层  进程与接入        cmd/server/main.go → internal/app/server/app.go
                            → websocket/websocket_server.go 或 mqtt_udp/mqtt_udp_adapter.go
第 1 层  连接管理层        chat.go      : ChatManager（握手 / 消息分发 / 会话创建与保留）
第 2 层  会话编排层        session.go   : ChatSession（listen 处理 / 起三条循环 / 路由 / 打断 / 关闭）
第 3 层  链路实现层        asr.go / llm.go / tts.go / tool.go / media_player.go / openclaw_warmup.go
第 4 层  协议与外部依赖    server_transport.go（收发）/ mcp.go / speaker.go / vllm.go
                          + internal/data/client（ClientState）、internal/domain/*（provider）、internal/pool（资源池）
```

**同层辅助**：`common.go`（打断/静默态）、`types.go`（工具层契约）、`util.go`（小工具）、`local_mcp_`* / `session_mcp_tool.go`（工具注册与实现）、`realtime_media_gate.go`（realtime 媒体门控）。

---



## 3. 从进程入口到 ASR 循环：完整链路（每步带行号）



### 3.1 进程与接入层


| #   | 位置                                | 函数                             | 做了什么                                                                              | 下一跳                       |
| --- | --------------------------------- | ------------------------------ | --------------------------------------------------------------------------------- | ------------------------- |
| 1   | `cmd/server/main.go:20`           | `main`                         | 解析参数、按需拉起 manager / asr_server、`Init(config)`                                     | 69                        |
| 2   | `cmd/server/main.go:69`           | `server.NewApp()`              | 创建 App                                                                            | `app.go:39`               |
| 3   | `cmd/server/main.go:138`          | `appInstance.Run()`            | 阻塞住主线程                                                                            | `app.go:64`               |
| 4   | `app.go:39`                       | `NewApp`                       | 构造 App                                                                            | —                         |
| 5   | `app.go:64`                       | `App.Run`                      | 起 wsServer（65）、mqtt server、udp adapter；`registerChatMCPTools`；`select{}` 阻塞       | 182 / 135                 |
| 6   | `app.go:182`                      | `newWebSocketServer`           | 组装 wsServer，其中 `app.go:187` 注册 `WithOnNewConnection(app.OnNewConnection)`         | `websocket_server.go:100` |
| 7   | `app.go:135`                      | `newMqttUdpAdapter`            | 组装 adapter，其中 `app.go:149` 注册同一个回调                                                | `mqtt_udp_adapter.go:505` |
| 8a  | `websocket_server.go:100` → `159` | `Start` → `internalHandleChat` | HTTP 升级为 WS，`websocket_server.go:196-197` 调 `onNewConnection(wsConn)`             | → `app.go:320`            |
| 8b  | `mqtt_udp_adapter.go:505`         | `EnsureDeviceTransport`        | 建立 mqtt+udp 设备会话，`mqtt_udp_adapter.go:535-536` 调 `onNewConnection(deviceSession)` | → `app.go:320`            |


**读这一层的目标**：确认「两条接入路径最终汇聚到同一个回调」，以及 `IConn` 这个接口是接入层与 chat 包的唯一耦合点。看到 `IConn` 就去找 `internal/app/server/types/conn.go`。

### 3.2 连接管理层（chat.go）


| #   | 位置            | 函数                  | 做了什么                                                                                                                 | 下一跳              |
| --- | ------------- | ------------------- | -------------------------------------------------------------------------------------------------------------------- | ---------------- |
| 9   | `app.go:320`  | `OnNewConnection`   | 关掉同设备旧 Manager、`app.go:334` 创建新 Manager、存进 map、`app.go:353` 起 goroutine 调 `Start`                                    | 334 / 365        |
| 10  | `chat.go:307` | `NewChatManager`    | 造 Manager：331 建 `ServerTransport`、365 `GenClientState`、337 注册 `OnClose`、373 建 hookHub + 输出流插件注册表                     | `Start`          |
| 11  | `chat.go:483` | `ChatManager.Start` | 起两个收包循环：484 `cmdMessageLoop`、485 `audioMessageLoop`；然后 `<-c.ctx.Done()` 阻塞                                           | 518 / 564        |
| 12  | `chat.go:518` | `cmdMessageLoop`    | 循环 `RecvCmd`（120s 超时，连续超时 4 次判僵尸连接）；`chat.go:547` 交给 `handleTextMessage`                                             | 617              |
| 13  | `chat.go:564` | `audioMessageLoop`  | 循环 `RecvAudio`（600ms 超时）；四道丢弃闸（无会话 / 待完成 speak_request / 未激活 / 客户端已停说）；`chat.go:606` 交给 `session.HandleAudioMessage` | `session.go:612` |
| 14  | `chat.go:617` | `handleTextMessage` | 解 JSON，`switch` 分发：hello 629 / speak_ready 631 / listen 633 / abort 635 / iot 637 / mcp 639 / goodbye 641            | 各项 Handler       |


**读这一层的目标**：把「一条连接上有两个独立收包循环」这件事记牢 —— 控制消息和音频走两条路，之后在 `ASRManager` 里 VAD 循环能直接读 `OpusAudioBuffer` 而不经过控制路径，原因就在这里。

### 3.3 握手 → 会话创建（这是 `asr.go:92` 的上游）


| #   | 位置             | 函数                             | 做了什么                                                                                                                                           | 下一跳              |
| --- | -------------- | ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- | ---------------- |
| 15  | `chat.go:665`  | `HandleHelloMessage`           | 校验传输类型；把 `msg.AudioParams` 写进 `clientState.InputAudioFormat`（`chat.go:684`，VAD/ASR 全靠它）；首轮 `auth.A().EnsureSession` 拿 SessionID（`chat.go:709`） | 721              |
| 16  | `chat.go:721`  | `c.ensureSessionForHello()`    | 走 `ensureSessionForHello`(1028) → `ensureSessionInternal`(1045)                                                                                | 1045             |
| 17  | `chat.go:841`  | `sendHelloResponse`            | 回 hello（含输出音频格式；MQTT 下还带 UDP 参数）                                                                                                               | —                |
| 18  | `chat.go:751`  | `scheduleMcpInitOnHelloLocked` | 异步初始化 IoT MCP（不阻塞握手）                                                                                                                           | —                |
| 19  | `chat.go:1045` | `ensureSessionInternal`        | 三段准入：已有会话直接返回；正在启动则等 `startingSessionDone`；`helloInited` 未置位则报错。然后 `chat.go:1078` 建会话、`chat.go:1089` 启动                                        | 1078             |
| 20  | `chat.go:1078` | `NewChatSession(...)`          | **进入 session.go**                                                                                                                              | `session.go:210` |


`ensureSessionInternal` 是「惰性创建 + 并发去重」的典范，`chat.go:1060-1093` 这段值得逐行读：

```go
// chat.go:1078-1093（节选）
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
```

**为什么先在锁内占位、再到锁外** `Start`：`Start` 会初始化 ASR/LLM/TTS 并起多个 goroutine，耗时且会回调，持锁执行会阻塞 `GetSession` 等读路径。这个「先占位再解锁做耗时操作」的模式在本包反复出现，认出它就能读懂一大堆并发代码。

### 3.4 会话构造 → 你要找的 `asr.go:92`

```go
// session.go:210-232（节选）
func NewChatSession(clientState *ClientState, serverTransport *ServerTransport, hookHub *chathooks.Hub, transformRegistry *streamtransform.Registry, opts ...ChatSessionOption) *ChatSession {
	s := &ChatSession{
		clientState:        clientState,
		serverTransport:    serverTransport,
		chatTextQueue:      util.NewQueue[AsrResponseChannelItem](10),
		speakerResultReady: make(chan struct{}, 1), // 缓冲为1，避免阻塞
		openClawStreams:    make(map[string]chan llm_common.LLMResponseStream),
		hookHub:            hookHub,
	}
	for _, opt := range opts {
		opt(s)
	}

	s.asrManager = NewASRManager(clientState, serverTransport)   // ← asr.go:92
	s.asrManager.session = s // 设置 session 引用（回填双向引用）
	s.ttsManager = NewTTSManager(clientState, serverTransport, s)
	s.mediaPlayer = NewSessionMediaPlayer(s)
	s.llmManager = NewLLMManager(clientState, serverTransport, s.ttsManager, s, transformRegistry)
```

对照位置：


| 位置               | 创建了谁                 | 定义处                    |
| ---------------- | -------------------- | ---------------------- |
| `session.go:223` | `ASRManager`         | `asr.go:92`            |
| `session.go:226` | `TTSManager`         | `tts.go:284`           |
| `session.go:227` | `SessionMediaPlayer` | `media_player.go:1008` |
| `session.go:228` | `LLMManager`         | `llm.go:426`           |


**注意** `session.go:224` **的** `s.asrManager.session = s`：这一行是「构造函数里 session 为 nil」的答案。读任何一个 Manager 的方法时，看到 `a.session.XXX` / `l.session.XXX`，都知道引用是在这里回填的。

### 3.5 会话启动 → 三条循环

```go
// session.go:382-424（节选）
func (s *ChatSession) Start(pctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(pctx)

	if s.clientState.InputAudioFormat.SampleRate <= 0 || s.clientState.InputAudioFormat.Channels <= 0 {
		return fmt.Errorf("输入音频格式未初始化，请先完成 hello 握手")
	}

	err := s.InitAsrLlmTts()      // session.go:389
	...
	go func() { err := s.initHistoryMessages() }()   // session.go:396 异步加载历史

	if !s.vadLoopStarted {
		go s.asrManager.runAudioIdleTimeoutWatchdog(s.ctx)   // session.go:406 → asr.go:114
		s.asrManager.ProcessVadAudio(s.ctx)                  // session.go:408 → asr.go:202
		s.vadLoopStarted = true
	}

	go s.processChatText(s.ctx)   // session.go:416 → session.go:1805
	go s.llmManager.Start(s.ctx)  // session.go:417 → llm.go:748
	go s.ttsManager.Start(s.ctx)  // session.go:418 → tts.go:338
	if s.mediaPlayer != nil {
		s.mediaPlayer.AttachSession()   // session.go:420 → media_player.go:1062
	}
	return nil
}
```

**这就是「三条循环」的出生地，也是本包最重要的一个阅读锚点。** 记住 `session.go:406-418` 这 13 行，你就知道：

- 上行音频循环：`asr.go:202` `ProcessVadAudio`（内部自己 `go func()`，所以 `Start` 不会被它阻塞）
- 会话空闲看门狗：`asr.go:114`
- 文本 → LLM：`session.go:1805` `processChatText`
- LLM 响应消费：`llm.go:761` `processLLMResponseQueue`（由 `llm.go:748` `Start` 起）
- TTS 消费与发送：`tts.go:338` `Start` → `1737` `processTTSQueue`（前台）+ `357` `runSenderLoop` + `1248` `runDelayedSentenceLoop`（两个 goroutine）

**⚠️ 顺序陷阱**：`session.go:417` 的 `llmManager.Start` 和 `418` 的 `ttsManager.Start` 都是 `go` 出去的，但 `llm.go:748` 的 `LLMManager.Start` 内部只起 `processLLMResponseQueue`；而 `tts.go:338` 的 `TTSManager.Start` **自己会阻塞**（`processTTSQueue` 在前台跑）。所以 `session.go:418` 这行必须带 `go`，否则会话启动就卡死在这里。这类「谁阻塞谁不阻塞」是读并发代码时最容易读错的地方，建议每读一个 `Start` 都在注释里记一句。

### 3.6 一条 listen start 进来的完整路径（第二轮迭代）

握手之后，设备发 `listen` 才开始真正的对话流程。这条链是 `asr.go:202` 的**触发者**：


| #   | 位置                                                   | 函数                                                                  | 说明                                                                                    |
| --- | ---------------------------------------------------- | ------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| 1   | `chat.go:633`                                        | `handleTextMessage` 的 listen 分支                                     | → `HandleListenMessage`(899)                                                          |
| 2   | `chat.go:899`                                        | `ChatManager.HandleListenMessage`                                   | 先查 `requiresHelloBootstrapForSession`；`chat.go:912` `ensureSession()`（可能复用保留态会话）      |
| 3   | `session.go:627`                                     | `ChatSession.HandleListenMessage`                                   | 按 `msg.State` 分发：start 631 / stop 633 / detect 635                                    |
| 4   | `session.go:1476`                                    | `HandleListenStart`                                                 | 激活校验、欢迎语保护、`StopSpeakingWithReason`（1531 / 1558）、`beginListenStart` 取代际号（1538 / 1560） |
| 5   | `session.go:1539`（realtime）/ `session.go:1561`（auto） | `go func(){ s.OnListenStart(startSeq, ...) }()`                     | **异步边界**：listen start 是异步处理的                                                          |
| 6   | `session.go:1608`                                    | `OnListenStart`                                                     | auto/manual 下 `clientState.Destroy()` 重置全部会话级状态；realtime 下只清音频缓冲                      |
| 7   | `session.go:1672`                                    | `s.asrManager.RestartAsrRecognition(ctx)`                           | 借池化 ASR + 建 `AsrAudioChannel` + `StreamingRecognize` → `asr.go:774`                   |
| 8   | `session.go:1737`                                    | `s.asrManager.StartAsrRecognitionLoop(ctx, onMessageSave, onError)` | 结果消费循环 → `asr.go:868`                                                                 |


**关键认知**：`ProcessVadAudio`（`asr.go:202`）在**会话启动**时就被拉起了（`session.go:408`），它一直在消费 `OpusAudioBuffer`；而 `RestartAsrRecognition`（`asr.go:774`）在**每次 listen start** 时才重建 ASR 流。所以：

- VAD 循环的生命周期 = 会话
- ASR 流的生命周期 = 一轮拾音（auto 模式）

两者靠 `ClientState` 里的状态位耦合：VAD 判定「有声」就 `state.Asr.AddAudioData(pcm)`（`asr.go:533`），数据实际写进 `Asr.AsrAudioChannel`；ASR 流不存在时这个写入会怎样，就是读 `asr.go:531-540` 时要弄清楚的第一个细节。

---



## 4. 推荐的阅读批次（7 批，按顺序读）

每批给出：**目标 → 读哪些行 → 读完你应该能回答的问题**。

### 批次 0：先读「作者写好的地图」（约 30 分钟）

不要跳过。这几个文件的头部注释就是作者写的架构说明，且互相点名了依赖关系：


| 位置                        | 内容                                |
| ------------------------- | --------------------------------- |
| `ARCHITECTURE.md` 第 1、2 节 | 三个概念 + 一次连接的启动与握手                 |
| `chat.go:1-22`            | ChatManager 职责 + 并发模型声明           |
| `session.go:1-22`         | ChatSession 职责 + 与哪些文件配合          |
| `asr.go:1-33`             | ASR 文件职责 + 与其它文件的配合 + 并发要点        |
| `llm.go:1-32`             | LLM 文件职责 + 为什么用 context 传临时状态     |
| `tts.go:1-30`             | TTS 的 4 条循环 + 3 个队列               |
| `types.go:1-19`           | 为什么需要 `ChatSessionOperator`（循环依赖） |


读完自检：能否说出「三个 Manager 谁持有谁、谁不直接调用谁」？

### 批次 1：连接管理层（chat.go）

- 结构体与字段注释：`chat.go:50-170`（重点看每把锁上方的注释，它告诉你哪块状态被哪些 goroutine 共享）
- `NewChatManager`：`chat.go:307-360`
- `Start` / 两个收包循环：`chat.go:483-616`（`handleLoopExit` 的 panic recover 在 497）
- 消息分发：`chat.go:617-665`
- 握手：`chat.go:665-760` + `sendHelloResponse` `chat.go:841-863`
- 懒创建与保留态：`chat.go:1020-1105`、`chat.go:1303-1390`、`chat.go:1391-1463`
- 关闭：`chat.go:1485-1560`

读完自检：

1. 一条连接上为什么要有两个收包循环？各自超时是多少、超时后怎么办？
2. `duplicate hello` 与首次 hello 的处理差别？为什么 MQTT 下的重复 hello 需要「重协商」？
3. 设备说 goodbye 之后会话为什么不销毁？保留多久？（`chat.go:1265` `getRetainedSessionIdleTimeout`）



### 批次 2：会话编排层（session.go）

- `NewChatSession`：`session.go:203-232`
- `Start`：`session.go:382-434` ← **本包最重要的一段**
- `InitAsrLlmTts`：`session.go:572-611`
- 音频入缓冲：`session.go:612-620`
- listen 分发与代际：`session.go:627-745`（`beginListenStart` 649、`invalidateListenStart` 660、`isCurrentListenStart` 667）
- `HandleListenStart` / `HandleListenStop` / `OnListenStart`：`session.go:1476-1745`
- 文本入队与出队：`session.go:1746-1830`
- 路由表 `actionDoChat`：`session.go:1952-2153`
- 关闭：`session.go:1874-1970`（`CloseWithReason` 在 1901）

读完自检：

1. 「代际号 `listenStartSeq`」解决什么竞态？在 `OnListenStart` 里它被检查了几次、分别防什么？
2. 为什么 `OnListenStart` 要先 `RestartAsrRecognition` 再做 `SetListenPhase(Listening)`？
3. `actionDoChat` 的 6 步优先级顺序是什么？为什么 realtime 媒体门控必须排在 OpenClaw 和退出词之前？



### 批次 3：上行链路（asr.go + session 的桥接）

建议顺序（**不要**从 `NewASRManager` 开始）：

1. 看门狗：`asr.go:106-200`（含 `asr.go:185` 的 `state.OnVoiceSilence()`）—— 短、独立、能帮你先理解「为什么要一个独立循环」
2. **主角**：`asr.go:202-740` `ProcessVadAudio`。分段读：
  - `asr.go:204-250`：解码器初始化、帧长反推
  - `asr.go:250-330`：`needVad` 判定（manual 模式 / `Asr.AutoEnd` / 正常 VAD）
  - VAD 资源懒加载与 2 秒空闲释放（`grep -n vadIdleReleaseTimeout asr.go`）
  - 首帧语音前导静音裁剪：`asr.go:1408` `trimFirstSpeechAudio`
  - 判停与收口：`asr.go:723` `state.OnVoiceSilence()`
  - realtime 打断：`asr.go:493`（mode=1，VAD 连续语音）、`asr.go:637`（mode=3，声纹 peek）
  - 送音频进 ASR：`asr.go:531-533` `state.Asr.AddAudioData(pcmData)`
3. 重建 ASR 流：`asr.go:774-867`（`AsrAudioChannel` 827、`StreamingRecognize` 830）
4. 结果循环：`asr.go:868-1400`。重点锚点：
  - `asr.go:959-967` `state.RetireAsrResult(ctx)` 取结果
  - `asr.go:1085-1087` realtime_mode=2 打断（拿到 ASR 结果就打断）
  - `asr.go:1242` `a.addAsrResultToQueue(text, speakerResult)` → `asr.go:1479` → `session.go:1746`
  - `asr.go:1025 / 1159 / 1227 / 1260 / 1354`：可恢复错误的重试与 realtime 重启（都调 `RestartAsrRecognition`）
  - `asr.go:1300` 空最终结果保护
5. 声纹取值：`asr.go:1438` `getSpeakerResult`（最多等 200ms，配合 `session.go:267` 注册的回调）

读完自检：

1. VAD 判定「有声」之后，音频是「直接给 ASR」还是「先进队列」？队列不存在时会怎样？
2. auto 模式一轮识别完之后，ASR 资源是被释放还是留着？在哪里释放（`asr.go:741 releaseResource` / `753 Cleanup`）？
3. 为什么空识别结果不会立刻断连？（提示：`asr.go:1300` 附近 + 「本轮耗时 < 500ms 才计数」）
4. 声纹结果是「同步算出来」还是「寄存器里取」？`speakerResultReady` 这个 channel 的作用是什么？



### 批次 4：中段（llm.go + tool.go）

- 入口：`session.go:1970-2153` `actionDoChat`（先读，它是调用方，能告诉你 `DoLLmRequest` 的参数从哪来）
- 请求组装：`llm.go:1554-1682` `GetMessages`（重点看 system prompt 由哪几段拼成，1682 是知识库路由策略）
- 发起请求：`llm.go:1377-1462` `DoLLmRequest`
  - `llm.go:1385` 组装消息
  - `llm.go:1391-1411` `LLM_INPUT` 钩子（可改写消息/工具/直接 stop）
  - `llm.go:1417` `handleLLMWithContextAndTools`
  - `llm.go:1432` 同步路径 / `llm.go:1440` 异步路径
- 拿响应流：`llm.go:491-747` `handleLLMWithContextAndTools`
  - 关键内部函数都在 `llm.go:560-720`：`emitResponse`、`pushToPipeline`、`pushRawText`、`pushRawToolCalls`
  - `llm.go:748` `LLMManager.Start` 只起 `processLLMResponseQueue`
- 响应入队 / 出队：`llm.go:899-1029` `handleLLMResponseChannelAsync`（入队在 `llm.go:1014`）→ `llm.go:761-794` `processLLMResponseQueue`（出队，调 `llm.go:780 handleLLMResponse`）
- 消费响应：`llm.go:1149-1375` `handleLLMResponse`
  - `llm.go:1267` `toolExecutor.Submit(llmResponse.ToolCalls)`
  - `llm.go:1277` 文本 → `ttsManager.handleTextResponseWithHooks`
  - `llm.go:1330` `handleToolCallResponse`（有工具时）
  - `llm.go:1341` 工具轮里的文本 → 同样进 TTS
- 消息落库：`llm.go:1464-1550` `AddMessage`（两阶段保存的第一阶段）
- 工具执行器：`tool.go:242` `newToolCallExecutor`、`260` `Submit`、`338` `Wait`、`354` `WaitMedia`、`363` `executeToolCall`
- 工具结果解析：`tool.go:68-240` `handleToolCallResponse`；`665` `handleLocalToolResult`（本地 MCP 协议）；`681` `handleToolResult`（远程 MCP）；`495/530` 资源链接与音频内容 → 交给媒体播放器

读完自检：

1. `nest` 是什么？它为什么导致「只有最外层才发 tts_start/tts_stop」？
2. `llmResponseQueue` 解决什么问题？为什么不用 `go handleLLMResponse(...)` 直接并发跑？（提示：TTS 发送必须有序）
3. 工具调用后「再问一轮 LLM」是在哪里发生的？
4. 「两阶段保存」的两阶段分别在哪两行？



### 批次 5：下行链路（tts.go）

- 头部注释：`tts.go:1-30`（4 条循环 + 3 个队列的关系图）
- `NewTTSManager`：`tts.go:284-305`
- `Start`：`tts.go:338-344` —— **注意它自己会阻塞**
- 句子入队：`tts.go:1037-1090`（`pushTTSQueueItem` 1053、`shouldDropTTSQueueItem` 1067、`dismissTTSQueueItem` 1076）
- 文本 → 队列：`tts.go:2037-2238` `handleTextResponseWithHooks`（三个 `pushTTSQueueItem` 调用点：2084 / 2129 / 2194，对应「何时必须新建一个 TTS 请求」的三种情况）
- 队列 → 音频帧：`tts.go:1737-1800` `processTTSQueue`
  - `tts.go:1824-1934` `handleTts`（非流式）
  - `tts.go:2545-2676` `handleStreamTts`（流式）
  - `tts.go:2399-2544` `handleDualStreamTts`（双流式，首帧延迟最低）
  - provider 获取与释放：`tts.go:2279` `getTTSProviderInstance`、`tts.go:2367` `generateTtsOnly`
- 音频帧 → 发送队列：`tts.go:1363-1380` `enqueueSessionElem`（生产端）；帧推进的调用点在 1860 / 1895 / 1912（非流式）和 2477 / 2498 / 2516 / 2528（流式）
- **唯一发送出口**：`tts.go:357-632` `runSenderLoop`
  - `playbackTail` 流控与 `cacheFrameCount`（357-400 附近是初始化，后面是循环）
  - 代际失效：`tts.go:669-715`（`currentAudioGeneration` / `nextAudioGeneration` / `rotateAudioInterruptCh`）
  - `finishTtsStop`：`tts.go:1557-1640`（等播放尾指针 + 保护时间再发 `tts_stop`）
- 延迟句子对齐：`tts.go:1173` `sentenceControlDelay`、`1217` `enqueueDelayedSentenceTask`、`1248` `runDelayedSentenceLoop`
- 轮次收口与打断：`tts.go:1663` `RequestTurnEnd`、`1668` `EnqueueTtsStop`、`1381/1394` `InterruptAndClearQueue*`、`1409/1417` `InterruptAndStop*`、`1427/1433` `InterruptAndStopSync*`
- 媒体独占：`tts.go:1121` `BeginExclusiveMediaPlayback`、`1149` `EndExclusiveMediaPlayback`、`1088` `waitForMediaPlaybackRelease`

读完自检：

1. 为什么发送必须只有一条协程？（`runSenderLoop` 头部注释已给三个理由）
2. `playbackTail` 是「发送时刻」还是「播放时刻」？哪些行为基于它？
3. 「打断」为什么需要 `audioGeneration` 和 `droppedTTSSeq` 两套失效机制？（一套管已合成待发，一套管未合成）



### 批次 6：打断、关闭、静默态（横切）

- `common.go:40-136`：`StopSpeaking` / `StopSpeakingWithReason` / `StopAssistantOutputAfterAsrWithReason` / `stopSpeakingWithLock` / `stopSpeaking`
- `common.go:147` `ResetToSilentState`
- `session.go:1367-1394` `InterruptAndClearTTSQueue*`
- `session.go:1395-1417` `HandleAbortMessage`
- `session.go:1840-1873` `DoExitChat`
- `session.go:1874-1969` `Close` / `CloseWithReason`
- `chat.go:951-977` `HandleGoodByeMessage`、`chat.go:1235-1264` 各种 `resetSpeakPathAfter*`
- `chat.go:1303-1390` 保留态清理、`chat.go:1391-1463` `handleSessionClosed`
- `chat.go:1485-1560` `shutdown` / `Close` / `OnClose`

读完自检：

1. `StopSpeaking` 与 `StopAssistantOutputAfterAsrWithReason` 的区别是什么？realtime 下为什么要用后者（只掐 TTS、不挂起媒体）？
2. 打断时会清空哪几条队列？分别由谁负责清？



### 批次 7：扩展能力（主链读通后再看）


| 主题              | 入口                                                                                                                                                                                 |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 媒体播放            | `media_player.go:1008` `NewSessionMediaPlayer` → `1062` `AttachSession` → `1099` `PlaySourceWithHandle` → 运行时 `2145` `runPlayback` / `2344` `playItem` / `2376` `streamMediaAudio` |
| realtime 媒体门控   | `realtime_media_gate.go:229` `tryHandleRealtimeMcpAudioASR`（在 `session.go:1983` 被调用）                                                                                               |
| 本地 MCP 工具       | `local_mcp_tool.go`（注册）、`session_mcp_tool.go`（接口实现）、`mcp_response_types.go`（响应协议）、`types.go:29`（接口定义）                                                                              |
| 媒体控制工具          | `local_mcp_media_control_tool.go`                                                                                                                                                  |
| OpenClaw        | `openclaw_warmup.go`（暖场话术）、`session.go:1205-1366`（流式回复的通道管理）、`session.go:1988-2073`（路由）                                                                                            |
| 协议发送            | `server_transport.go:104` 起（`SendHello` 216、`SendAsrResult` 261、`SendSentenceStart` 282、`SendAudio` 331、`RecvCmd` 488、`RecvAudio` 483）                                             |
| 声纹 provider     | `speaker.go`                                                                                                                                                                       |
| 视觉（图片理解）        | `vllm.go`                                                                                                                                                                          |
| IoT over MCP 桥接 | `mcp.go`                                                                                                                                                                           |


---



## 5. 逆向索引：函数 → 谁调用它（带行号）

这一节是「往上追」的速查表，省掉你每次都要全局搜索。

### 5.1 asr.go


| 函数（定义行）                             | 被谁调用（行号）                                                                                     |
| ----------------------------------- | -------------------------------------------------------------------------------------------- |
| `NewASRManager` (92)                | `session.go:223`                                                                             |
| `runAudioIdleTimeoutWatchdog` (114) | `session.go:406`                                                                             |
| `ProcessVadAudio` (202)             | `session.go:408`                                                                             |
| `releaseResource` (741)             | `RestartAsrRecognition` / `Cleanup` / 结果循环内部                                                 |
| `Cleanup` (753)                     | `session.go` 关闭路径 + `RestartAsrRecognition`                                                  |
| `RestartAsrRecognition` (774)       | `session.go:1672`（OnListenStart）、`asr.go:1025 / 1159 / 1227 / 1260 / 1354`（自身重试/realtime 重启） |
| `StartAsrRecognitionLoop` (868)     | `session.go:1737`（OnListenStart，唯一外部调用方）                                                     |
| `trimFirstSpeechAudio` (1408)       | `ProcessVadAudio` 内部                                                                         |
| `getSpeakerResult` (1438)           | `StartAsrRecognitionLoop` 内部                                                                 |
| `addAsrResultToQueue` (1479)        | `asr.go:1242` → 转调 `session.go:1746`                                                         |




### 5.2 session.go


| 函数（定义行）                                 | 被谁调用                                                                                               |
| --------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `NewChatSession` (210)                  | `chat.go:1078`                                                                                     |
| `Start` (382)                           | `chat.go:1089`                                                                                     |
| `initHistoryMessages` (434)             | `session.go:396`（goroutine）                                                                        |
| `InitAsrLlmTts` (572)                   | `session.go:389`                                                                                   |
| `HandleAudioMessage` (612)              | `chat.go:606`                                                                                      |
| `HandleListenMessage` (627)             | `chat.go:913`                                                                                      |
| `HandleListenStart` (1476)              | `session.go:631`                                                                                   |
| `HandleListenStop` (1576)               | `session.go:633`                                                                                   |
| `HandleListenDetect` (959)              | `session.go:635`                                                                                   |
| `OnListenStart` (1608)                  | `session.go:1539` / `1561`（goroutine）                                                              |
| `AddAsrResultToQueue` (1746)            | `asr.go:1483`、`session.go:943`                                                                     |
| `AddAsrResultToQueueWithOptions` (1761) | `session.go:1747`、`chat.go:1636`                                                                   |
| `processChatText` (1805)                | `session.go:416`（goroutine）                                                                        |
| `actionDoChat` (1970)                   | `session.go:1819`（processChatText 内）                                                               |
| `HandleAbortMessage` (1395)             | `chat.go:924`                                                                                      |
| `HandleWelcome` (1068)                  | 会话启动/欢迎语路径                                                                                         |
| `InjectOpenClawResponse` (1304)         | `chat.go:2077`（ChatManager 转发）、`app.go:381`（App 入口）                                                |
| `switchTTSForSpeaker` (2228)            | `session.go:2093`                                                                                  |
| `CloseWithReason` (1901)                | `chat.go:1353` / `1503` / `2070`、`asr.go:169` / `942`、`session.go:1686` / `1733` / `1870` / `1875` |
| `DoExitChat` (1840)                     | 退出事件处理                                                                                             |
| Trace* 系列 (2366-2422)                   | `asr.go` / `llm.go` / `tts.go` 打点处                                                                 |




### 5.3 llm.go


| 函数（定义行）                               | 被谁调用                                                                |
| ------------------------------------- | ------------------------------------------------------------------- |
| `NewLLMManager` (426)                 | `session.go:228`                                                    |
| `Start` (748)                         | `session.go:417`（goroutine）                                         |
| `processLLMResponseQueue` (761)       | `llm.go:748` 内部 goroutine                                           |
| `openOutputPipeline` (445)            | `handleLLMWithContextAndTools` 内部                                   |
| `handleLLMWithContextAndTools` (491)  | `llm.go:1417`                                                       |
| `HandleLLMResponseChannelAsync` (881) | `llm.go:1440`                                                       |
| `handleLLMResponseChannelAsync` (899) | 881 / 887                                                           |
| `HandleLLMResponseChannelSync` (1030) | `llm.go:1432`                                                       |
| `handleLLMResponse` (1149)            | `llm.go:780`（队列消费端）                                                 |
| `DoLLmRequest` (1377)                 | `session.go:2143`（唯一外部调用方）、`tool.go:handleToolCallResponse` 尾部（工具轮） |
| `AddMessage` (1464)                   | `handleLLMResponse` / 工具路径                                          |
| `GetMessages` (1554)                  | `llm.go:1385`                                                       |
| `waitForTTSTurnDrainIfRoot` (1844)    | `llm.go:783`                                                        |




### 5.4 tts.go


| 函数（定义行）                              | 被谁调用                                                                                              |
| ------------------------------------ | ------------------------------------------------------------------------------------------------- |
| `NewTTSManager` (284)                | `session.go:226`                                                                                  |
| `Start` (338)                        | `session.go:418`（goroutine）                                                                       |
| `processTTSQueue` (1737)             | `tts.go:342`（前台，使 Start 阻塞）                                                                       |
| `runSenderLoop` (357)                | `tts.go:340`（goroutine）                                                                           |
| `runDelayedSentenceLoop` (1248)      | `tts.go:339`（goroutine）                                                                           |
| `handleTextResponse` (2021)          | `llm.go` 少数无钩子路径                                                                                  |
| `handleTextResponseWithHooks` (2037) | `llm.go:1277`、`llm.go:1341`                                                                       |
| `pushTTSQueueItem` (1053)            | `tts.go:2084 / 2129 / 2194`                                                                       |
| `handleTts` (1824)                   | `tts.go:1785`                                                                                     |
| `handleStreamTts` (2545)             | `tts.go:1776`                                                                                     |
| `handleDualStreamTts` (2399)         | `pushTTSQueueItem` 的双流分支                                                                          |
| `generateTtsOnly` (2367)             | `handleTts`                                                                                       |
| `getTTSProviderInstance` (2279)      | `generateTtsOnly` / `handleStreamTts` / `handleDualStreamTts`                                     |
| `enqueueSessionElem` (1363)          | `tts.go:1659 / 1679 / 1685 / 1860 / 1895 / 1912 / 2477 / 2498 / 2516 / 2528 / 2576 / 2641 / 2652` |
| `sendAudioStream` (2692)             | `SendTTSAudio`(2777) / `SendMediaAudio`(2785)                                                     |
| `RequestTurnEnd` (1663)              | `llm.go` 收口路径                                                                                     |
| `InterruptAnd*` (1381-1556)          | `common.go:stopSpeaking`、`session.go:1367-1393`                                                   |
| `BeginExclusiveMediaPlayback` (1121) | `media_player.go` 独占播放路径                                                                          |
| `ClearTTSQueue` (1801)               | 打断路径                                                                                              |




### 5.5 tool.go / common.go / server_transport.go


| 函数（定义行）                                             | 被谁调用                                                              |
| --------------------------------------------------- | ----------------------------------------------------------------- |
| `newToolCallExecutor` (242)                         | `handleLLMResponse` 内部                                            |
| `Submit` (260)                                      | `llm.go:1267`                                                     |
| `Wait` (338) / `WaitMedia` (354)                    | `handleToolCallResponse`                                          |
| `executeToolCall` (363)                             | `Submit` 起的 goroutine                                             |
| `handleToolCallResponse` (68)                       | `llm.go:1330`                                                     |
| `StopSpeaking` (40) / `StopSpeakingWithReason` (46) | `session.go:1531` / `1558`、`session.go:1009` / `1404`、`asr.go` 多处 |
| `StopAssistantOutputAfterAsrWithReason` (53)        | `asr.go:493 / 637 / 1085`                                         |
| `ResetToSilentState` (147)                          | `chat.go:955`（goodbye）、`chat.go:1194`                             |
| `NewServerTransport` (104)                          | `chat.go:331`                                                     |
| `SendTtsStart` (115) / `SendTtsStop` (142)          | `runSenderLoop` 内                                                 |
| `SendHello` (216)                                   | `chat.go:841` `sendHelloResponse`                                 |
| `SendAsrResult` (261)                               | `asr.go` 结果循环（`[2.2]` 标号处）                                        |
| `SendAudio` (331)                                   | `runSenderLoop` → `sendAudioStream`                               |
| `RecvCmd` (488) / `RecvAudio` (483)                 | `chat.go:535` / `chat.go:575`                                     |


---



## 6. 异步断点速查表（队列 / channel）

**用法**：读者读到「入队/写 channel」时，别往上追，直接跳到「出队/读 channel」那一行继续往下读。


| 交接载体                            | 生产端（写）                                                   | 消费端（读）                                               | 容量 / 语义            |
| ------------------------------- | -------------------------------------------------------- | ---------------------------------------------------- | ------------------ |
| `OpusAudioBuffer`               | `session.go:614`（`HandleAudioMessage`，调用点 `chat.go:606`） | `asr.go:202` `ProcessVadAudio` 内部                    | 100 帧，满了丢帧（保护收包协程） |
| `Asr.AsrAudioChannel`           | `asr.go:533` `AddAudioData`（VAD 判定有声后）                   | `asr.go:830` provider 的 `StreamingRecognize`         | 100 帧              |
| `AsrResultChannel`              | `asr.go:830`（provider 内部产出）                              | `asr.go:967` `RetireAsrResult`                       | 流式识别结果             |
| `speakerResultReady`            | 声纹回调（注册于 `session.go:267`，异步 goroutine 起于 `271`）         | `asr.go:1438` `getSpeakerResult`                     | 缓冲 1，就绪信号          |
| `chatTextQueue`                 | `session.go:1791` `chatTextQueue.Push`                   | `session.go:1805` `processChatText`（`Pop` 在 1811 附近） | 10，ASR → LLM       |
| `llmResponseQueue`              | `llm.go:1014` `Push`                                     | `llm.go:761` `processLLMResponseQueue`（`Pop` 764 附近） | 10，保证串行处理          |
| `ttsQueue`                      | `tts.go:1055` `Push`                                     | `tts.go:1737` `processTTSQueue`（`Pop` 1740）          | 句子队列               |
| `sessionAudioQueue`             | `tts.go:1366 / 1375` `enqueueSessionElem`                | `tts.go:357` `runSenderLoop`                         | 音频帧与协议消息           |
| `delayedSentenceReadyQueue`     | `tts.go:1305`                                            | `tts.go:1248` `runDelayedSentenceLoop`               | 延迟对齐后的句子消息         |
| `dualStreamChan` / `StreamChan` | `llm.go` 文本流（`sendLLMResponseToDualStream` 1959）         | `tts.go:2399` `handleDualStreamTts`                  | 双流式边收边合成           |
| `openClawStreams`(map)          | `session.go:1217` 创建 / `InjectOpenClawResponse` 写入       | `session.go:1321-1360`                               | correlation_id → 流 |


---



## 7. 并发与状态：为什么要和 `ClientState` 一起读

`ClientState`（`internal/data/client`）是三个 Manager 之间**唯一共享的可变状态载体**。读任何一个 Manager 时，凡是看到 `state.XXX` / `clientState.XXX` 的判断，都要回到 `ClientState` 定义处确认「谁改它、什么时候改」。关键状态：


| 状态                                              | 作用                    | 主要在哪些行被读写                                                                  |
| ----------------------------------------------- | --------------------- | -------------------------------------------------------------------------- |
| `InputAudioFormat`                              | 输入采样率/声道，VAD 与 ASR 依赖 | 写：`chat.go:695`；读：`asr.go` 解码初始化                                           |
| `OutputAudioFormat`                             | 输出帧长/格式，流控依赖          | `chat.go:430` `applyOutputAudioFormatForTTS`                               |
| `OpusAudioBuffer`                               | 上行音频缓冲                | 写 `session.go:614`；读 `asr.go:202` 内                                        |
| `Asr`（含 `AsrAudioChannel` / `AsrResultChannel`） | ASR 流状态               | `session.go:574` `InitAsr`；`asr.go:774-867`                                |
| `VoiceStatus` / `ClientVoiceStop`               | VAD 判停、音频丢弃闸          | `asr.go` VAD 循环；`chat.go:600`                                              |
| `ListenPhase` / `ListenMode`                    | listen 状态机            | `session.go:649-672`、`chat.go` 握手                                          |
| `SessionCtx` / `AfterAsrSessionCtx`             | 可取消的会话/轮次上下文          | `session.go:1647` / `1659` 重建；`asr.go:495 / 639 / 1087`、`common.go:100` 取消 |
| 指标时间戳（`StartLlmTs` 等）                           | 打点                    | `llm.go:1411`、`tts.go` 各 Trace 调用点                                         |
| `MemoryProvider` / `MemoryContext`              | 长记忆                   | `session.go:572-608`                                                       |


**三套「代际/失效号」**（读打断逻辑必须分清）：


| 失效号                                 | 定义/维护                               | 作废谁                              |
| ----------------------------------- | ----------------------------------- | -------------------------------- |
| `listenStartSeq`                    | `session.go:649` `beginListenStart` | 作废上一轮还没跑完的 `OnListenStart` 流程    |
| `audioGeneration`                   | `tts.go:669-715`                    | 作废 `sessionAudioQueue` 里旧代际的音频元素 |
| `droppedTTSSeq` / `dualStreamEpoch` | `tts.go:1801` `ClearTTSQueue`       | 作废 `ttsQueue` 里还没合成的句子           |


---



## 8. 常见卡点 FAQ

**Q1：**`ProcessVadAudio` **里** `go func()` **了，那** `session.go:408` **调用它会不会阻塞？**
不阻塞，它立刻返回。所以 `session.go:408` 后面 `vadLoopStarted = true` 是在循环已经开始跑之后才执行的 —— 这是「用一次调用启动一个常驻循环」的写法，不要误以为它是同步等待。

**Q2：**`asr.go:967` **的** `RetireAsrResult` **阻塞多久？会不会永远等？**
它阻塞等待识别结果，但受 `ctx` 与 provider 侧超时约束（如 doubao 的 `waiting next packet timeout`，见 `asr.go:1046`）。读这段时重点看「哪些错误算可恢复、哪些直接断连」。

**Q3：为什么 auto 模式识别完一轮后 ASR 会「消失」？**
结果循环在拿到最终文本并 `addAsrResultToQueue` 后会 return 并释放资源（`asr.go:1251` 附近的注释说明了这一点），要等下一次 `listen start` 再 `RestartAsrRecognition`。realtime 模式则会在循环内直接重启（`asr.go:1260`）。

**Q4：**`llm.go` **为什么把** `fullText`**、**`nest`**、tracker 放进** `context` **而不是结构体字段？**
因为工具调用会产生「嵌套的多轮请求」，每轮的临时状态必须互相隔离。`llm.go:1-32` 头部的「并发模型」段落明确说明了这一点。

**Q5：为什么「发 tts_stop」要等** `playbackTail`**？**
客户端本地有约 120ms 音频缓冲，`playbackTail` 表示「预计播完时刻」。提前发 stop 会让尾音被切。见 `tts.go:357` 头部注释与 `tts.go:1557` `finishTtsStop`。

**Q6：**`session.go:224` **的** `s.asrManager.session = s` **为什么不能放在** `NewASRManager` **里？**
因为 `ChatSession` 需要 `ASRManager`、`ASRManager` 也需要 `ChatSession`，Go 里只能先造对象再补引用；`asr.go:89-91` 的注释写明了这个约束。

**Q7：工具层怎么回调会话能力，为什么不直接调用** `ChatSession`**？**
见 `types.go:1-19`：直接引用会形成 `ChatSession ↔ 工具层` 循环依赖。解决方式是接口 + `ctx.Value("chat_session_operator")`（注入点在 `chat.go:319`）。读工具代码时，先把 `types.go:29` 的接口方法列表看完再进去。

**Q8：**`hookHub` **到处出现，它是干什么的？**
它是「事件钩子」：ASR 输出、LLM 输入、LLM 原始输出、TTS 输出、指标。运行时可以注册插件改写数据甚至中断流程。读主链时把它当「可选的一层透明变换」跳过即可，等主链通了再回来看 `chathooks` 与 `chat.go:275` `chatHookBuiltinOverrides`。

---



## 9. 检索命令速查（Windows PowerShell）

```powershell
cd e:\workspace\go\xiaozhiServer\internal\app\server\chat

# 1. 某个函数定义在哪（^func 只匹配行首的定义）
Select-String -Path *.go -Pattern '^func .*ProcessVadAudio' | ForEach-Object { "{0}:{1}: {2}" -f $_.Filename,$_.LineNumber,$_.Line.Trim() }

# 2. 谁调用了它（全仓，排除测试）
Get-ChildItem -Recurse -Filter *.go ..\..\..\.. | Select-String -Pattern 'RestartAsrRecognition' | Where-Object { $_.Path -notmatch '_test\.go$' } | ForEach-Object { "{0}:{1}: {2}" -f $_.Filename,$_.LineNumber,$_.Line.Trim() }

# 3. 找队列/channel 的生产端与消费端
Select-String -Path *.go -Pattern 'chatTextQueue\.(Push|Pop)|llmResponseQueue\.(Push|Pop)|ttsQueue\.(Push|Pop)|sessionAudioQueue <-'

# 4. 按执行顺序标号读（ARCHITECTURE.md 第 10 节的索引）
Select-String -Path *.go -Pattern '^\s*// \[[0-9]\.[0-9]' | ForEach-Object { "{0}:{1}: {2}" -f $_.Filename,$_.LineNumber,$_.Line.Trim() } | Sort-Object

# 5. 找某个接口/回调的实现（无参搜索）
Select-String -Path *.go -Pattern 'OnVoiceSilenceSpeakerCallback|OnVoiceSilenceMetricCallback|chat_session_operator'
```

如果装了 ripgrep，把 `Select-String` 换成 `rg -n`，速度会快很多（例如 `rg -n "RestartAsrRecognition" -g "!*_test.go"`）。

---



## 10. 一页速记：读这份代码的 8 条原则

1. **先骨架后细节**：`main.go:138 → app.go:320 → chat.go:307 → chat.go:483 → chat.go:617 → chat.go:1045 → session.go:382 → session.go:406-418`。这条链读不懂，下面全都读不懂。
2. **遇到** `go` **/ 队列 / channel 就换方向**：不要往上追，跳到消费端继续往下读。
3. **每个 Manager 的入口是循环，不是构造函数**：`ProcessVadAudio`(asr)、`processLLMResponseQueue`(llm)、`processTTSQueue`+`runSenderLoop`(tts)。
4. **谁阻塞谁不阻塞要单独确认**：`TTSManager.Start` 阻塞、`LLMManager.Start` 不阻塞、`ProcessVadAudio` 不阻塞。
5. **状态在** `ClientState`**，流程在 Manager**：任何判不清的因果关系，回去看状态位是谁写的。
6. **三套失效号（listenStartSeq / audioGeneration / droppedTTSSeq）是打断逻辑的钥匙**。
7. `ctx` **里带的是「本轮临时状态」**（nest、fullText、tracker、policy），不要找结构体字段。
8. **主链不通不读旁路**：媒体播放、OpenClaw、声纹、工具全部留到最后。

---



## 11. 优先读的 15 个函数（如果只有两小时）

按顺序，每个都值得逐行读：

1. `chat.go:307` `NewChatManager`
2. `chat.go:483` `ChatManager.Start`
3. `chat.go:617` `handleTextMessage`
4. `chat.go:665` `HandleHelloMessage`
5. `chat.go:1045` `ensureSessionInternal`
6. `session.go:210` `NewChatSession`
7. `session.go:382` `ChatSession.Start`
8. `session.go:1476` `HandleListenStart`
9. `session.go:1608` `OnListenStart`
10. `asr.go:202` `ProcessVadAudio`
11. `asr.go:774` `RestartAsrRecognition`
12. `asr.go:868` `StartAsrRecognitionLoop`
13. `session.go:1970` `actionDoChat`
14. `llm.go:1377` `DoLLmRequest` + `llm.go:1149` `handleLLMResponse`
15. `tts.go:2037` `handleTextResponseWithHooks` + `tts.go:357` `runSenderLoop`

