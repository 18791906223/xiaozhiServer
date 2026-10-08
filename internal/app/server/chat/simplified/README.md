# 简化版小智服务端：实时语音对话主链路

这个目录是 `internal/app/server/chat`（约 15000 行）的**教学用简化实现**。

目的只有一个：让你能用一条直线看懂「设备说一句话 → 音箱答一句话」到底发生了什么。

真实实现里塞满了数据上报、MCP 工具、音乐播放、记忆(mem0)、知识库(RAG)、
OpenClaw、声纹识别、MQTT/UDP 双通道、配置热更新……这些东西各自合理，
但堆在一起之后，主链路就被淹没了。这里把它们全部拿掉，只留主干。

代码量：产品代码约 1500 行 + 测试约 700 行，对比真实实现的约 15000 行。

---

## 1. 主链路长什么样

```
 设备                                                   服务端
  │                                                      │
  │  ① WebSocket 连接 ws://host:8989/xiaozhi/v1/         │
  ├─────────────────────────────────────────────────────►│
  │                                                      │
  │  ② {"type":"hello", audio_params:{...}}              │  握手：协商音频格式
  ├─────────────────────────────────────────────────────►│
  │◄─────────────────────────────────────────────────────┤
  │     {"type":"hello", session_id, audio_params}       │
  │                                                      │
  │  ③ {"type":"listen","state":"start"}                 │  开始收音
  ├─────────────────────────────────────────────────────►│
  │                                                      │
  │  ④ 二进制帧：Opus 音频（60ms 一帧，16kHz 单声道）      │
  ├─────────────────────────────────────────────────────►│
  │                                                      │    Opus 解码
  │                                                      │    VAD 判断有没有人说话
  │                                                      │    静音超过 400ms → 说完了
  │                                                      │         │
  │                                                      │         ▼
  │                                                      │      ASR 语音 → 文本
  │◄─────────────────────────────────────────────────────┤
  │     {"type":"stt","text":"今天天气怎么样"}             │
  │                                                      │         │
  │                                                      │         ▼
  │                                                      │      LLM 流式生成回复
  │                                                      │         │
  │                                                      │         ▼
  │                                                      │      按标点切句
  │                                                      │         │
  │                                                      │         ▼
  │                                                      │      TTS 文本 → Opus
  │◄─ {"type":"tts","state":"start"}                     │
  │◄─ {"type":"tts","state":"sentence_start","text":...} │
  │◄─ 二进制帧：Opus 音频                                 │
  │◄─ {"type":"tts","state":"sentence_end","text":...}   │
  │◄─ ...（下一句重复）                                   │
  │◄─ {"type":"tts","state":"stop"}                      │
  │                                                      │
```

一句话总结：**收 Opus → VAD 断句 → ASR → LLM → 分句 → TTS → 发 Opus**。

打断（barge-in）叠在这条主链路上：播报期间仍然接收音频，一旦检测到用户
持续说话，就把当前这一轮作废、立刻停止下发音频并补一个 `tts/stop`，
然后把用户正在说的这句接成新一轮。

---

## 2. 简化版文件 ↔ 真实实现对照

| 简化版文件 | 内容 | 真实实现里对应什么 |
|---|---|---|
| `protocol.go` | 设备协议消息结构 | `internal/data/msg/message_types.go` + `internal/data/client/client.go` |
| `transport.go` | WebSocket 收发封装 | `internal/app/server/websocket/websocket_conn.go`（`types.IConn` 的实现） |
| `server.go` | HTTP 路由与连接升级 | `websocket_server.go` 的 `handleChat` |
| `session.go` | 会话状态机、主流程编排 | `chat.go` 的 `ChatManager` + `session.go` 的 `ChatSession` |
| `audio.go` | Opus 编解码、PCM 转换 | `internal/domain/audio/audio_handler.go` |
| `vad.go` | 能量阈值 VAD | `internal/domain/vad/`（silero_vad / ten_vad 神经网络） |
| `asr.go` | ASR 接口 + 两种实现 | `internal/domain/asr/base.go` 的 `AsrProvider` |
| `llm.go` | LLM 接口 + 两种实现 | `internal/domain/llm/base.go` 的 `LLMProvider` |
| `sentence.go` | 流式文本分句 | `internal/util/sentence.go` + `plugins/output_segmenter.go` |
| `tts.go` | TTS 接口 + 两种实现 + 重采样 | `internal/domain/tts/base.go` 的 `TTSProvider` |
| `providers.go` | 配置与「选哪个实现」 | `internal/pool` 资源池 + 各 provider 工厂 |

真实实现里每一环之间都靠一个带容量限制的 channel 队列解耦：

```
OpusAudioBuffer → AsrAudioChannel → AsrResultChannel → chatTextQueue
   → llmResponseQueue → ttsQueue → sessionAudioQueue → socket
```

简化版把队列数量降到最低，但**保留了「每环独立 goroutine + channel 传递」
这个关键结构**，因为它正是真实实现里打断、背压、超时能工作的原因：

```
conn.Audio() → opusQueue → [processAudioLoop] 解码 + VAD
                            ↓
                          [ASR 识别]  ← 每轮一个 goroutine
                            ↓
                          [LLM 流式] → [分句] → [TTS 合成] → conn.SendAudio()
```

---

## 3. 协议速查

### 设备 → 服务端

| type | 关键字段 | 含义 |
|---|---|---|
| `hello` | `audio_params` | 握手，上报上行音频格式 |
| `listen` | `state=start/stop/detect` | 开始收音 / 结束收音 / 唤醒词检测 |
| `abort` | — | 打断，立刻停止当前播报 |
| `goodbye` | — | 主动断开 |
| `iot` | `payload` | 设备侧 IoT 状态，简化版忽略 |

### 服务端 → 设备

| type | state | 关键字段 | 含义 |
|---|---|---|---|
| `hello` | — | `session_id`,`audio_params` | 握手应答 |
| `stt` | — | `text` | 语音识别结果 |
| `tts` | `start` | — | 即将开始播报 |
| `tts` | `sentence_start` | `text` | 一句开始，紧跟其后是这句的音频帧 |
| `tts` | `sentence_end` | `text` | 一句结束 |
| `tts` | `stop` | — | 本轮播报结束，设备可以重新收音 |

音频永远是**二进制帧**，夹在 `sentence_start` 和 `sentence_end` 之间。

---

## 4. 分阶段实现（每个阶段一个 git commit）

用 `git log --oneline -- internal/app/server/chat/simplified` 看这一串提交，
再用 `git show <hash>` 看每一步加了什么。每个阶段都配了会通过的测试，
所以你可以 checkout 到任意一步并直接验证。

| 提交信息 | 你会看到什么 | 对应测试 |
|---|---|---|
| `docs(simplified): ...路线图` | 本文件 | — |
| `feat(simplified): 阶段1 握手与文本指令` | WebSocket 服务、`hello` 握手、`listen` 分发。此时音频帧被直接丢弃 | `TestStage1_Handshake` |
| `feat(simplified): 阶段2 Opus 解码、能量 VAD 与断句` | Opus 解码、能量 VAD、静音 400ms 判定「说完了」 | `TestStage2_*` |
| `feat(simplified): 阶段3 抽出 ASR 接口` | ASR 接口 + 两种实现，把 PCM 变成文本，回 `stt` | `TestStage3_*` |
| `feat(simplified): 阶段4 抽出 LLM 接口与流式分句` | LLM 接口 + 流式 token 按标点切成句子 | `TestStage4_*` |
| `feat(simplified): 阶段5 抽出 TTS 接口` | TTS 接口 + 句子变成 Opus 帧发回设备，全链路打通 | `TestStage5_*` |
| `feat(simplified): 阶段6 打断（barge-in）` | 说话打断播报，轮次编号作废旧输出 | `TestStage6_*` |
| `docs(simplified): ...完整消息序列测试与文档补全` | 完整消息序列测试、本文件补全 | `TestFullPipelineMessageSequence` |

建议的阅读方式：

```bash
git log --oneline -- internal/app/server/chat/simplified
git show <某个阶段的hash>       # 看这一阶段改了什么
```

---

## 5. 一条完整的消息序列（实录）

`TestFullPipelineMessageSequence` 会把整条链路的消息序列打印出来。
下面是它的真实输出（去掉日志前缀），这是理解整个流程最快的方式：

```
① 握手  session_id=demo-device-1791470958128903100
        audio_params={Format:opus SampleRate:16000 Channels:1 FrameDuration:60}

③ 识别  [模拟识别] 我听到了约 1020 毫秒的语音

④ 播报  tts/start
        sentence_start  "你说的是「[模拟识别] 我听到了约 1020 毫秒的语音」。"
        sentence_end    "你说的是「[模拟识别] 我听到了约 1020 毫秒的语音」。"（其间 60 帧音频）
        sentence_start  "这是模拟回复，用来演示流式输出和分句。"
        sentence_end    "这是模拟回复，用来演示流式输出和分句。"（其间 38 帧音频）
        sentence_start  "今天天气不错，要不要出去走走？"
        sentence_end    "今天天气不错，要不要出去走走？"（其间 30 帧音频）
        tts/stop —— 本轮结束，设备可以重新收音
```

几个值得注意的点：

- **`tts/start` 在识别结果出来之后立刻发出**，而不是等整段回复写完 ——
  这是首字延迟低的关键。
- **音频帧夹在 `sentence_start` 和 `sentence_end` 之间**。设备收到
  `sentence_end` 就知道这句的音频到齐了。
- **每句的帧数不等**（60 / 38 / 30），因为它按句子长度合成：
  内置模拟 TTS 每字 120ms，60ms 一帧。
- 回复被切成了 3 句，因为 LLM 是流式吐字的，服务端一见句末标点就切。

同样的序列，在 `abort` 或语音插话时会在中途插入 `tts/stop`，
然后旧轮次剩下的音频帧一帧都不会再发出来。

---

## 6. 怎么跑

```bash
# 默认全用内置的假实现（不需要任何 API Key，能听到 660Hz 提示音）
go run ./internal/app/server/chat/simplified

# 接真实服务（任何 OpenAI 兼容接口都行）
go run ./internal/app/server/chat/simplified \
  -asr-url   https://api.siliconflow.cn/v1 -asr-key $KEY -asr-model FunAudioLLM/SenseVoiceSmall \
  -llm-url   https://api.siliconflow.cn/v1 -llm-key $KEY -llm-model Qwen/Qwen2.5-7B-Instruct \
  -tts-url   https://api.siliconflow.cn/v1 -tts-key $KEY -tts-model FunAudioLLM/CosyVoice2-0.5B -tts-voice alex

# 如果你的 TTS 支持直接指定采样率，就不用重采样了
go run ./internal/app/server/chat/simplified -tts-url ... -tts-sample-rate 16000
```

默认监听 `:8989`，路由 `/xiaozhi/v1/`，和真实服务一致。
设备端把 OTA 地址指到 `ws://<你的内网IP>:8989/xiaozhi/v1/` 即可。

**没给 `-asr-key` / `-llm-key` / `-tts-key` 时，对应环节自动使用内置假实现**，
所以哪怕三个都没配，也能完整跑一遍流程、看到全部日志、并在设备上听到提示音。

内置假实现分别做了什么：

| 环节 | 假实现 | 你能观察到什么 |
|---|---|---|
| ASR | `mockASR` | 回一句「我听到了约 N 毫秒的语音」，N 就是 VAD 切出来的净语音时长 |
| LLM | `mockLLM` | 逐字（每 10ms 一个）吐出一段带标点的固定回复，用来验证分句 |
| TTS | `mockTTS` | 按字数生成长度不同的 660Hz 提示音，用 Opus 编码后按帧下发 |

---

## 7. 怎么跑测试

```bash
go test ./internal/app/server/chat/simplified/ -v

# 看完整消息序列（最直观）
go test ./internal/app/server/chat/simplified/ -run TestFullPipeline -v

# 竞态检查（这些都涉及多个 goroutine + 共享状态，值得跑）
go test -race ./internal/app/server/chat/simplified/
```

测试用一个「进程内的假设备」驱动：起一个真实的 WebSocket 服务端，
用 `gorilla/websocket` 连上去，然后用自己生成的 Opus 帧模拟人声
（440Hz 正弦波）和静音，最后断言设备收到了什么。

| 测试 | 验证什么 |
|---|---|
| `TestStage1_Handshake` | `hello` 应答里有 `session_id` 和下行音频格式 |
| `TestStage2_SpeechEndTriggersRecognition` | 1 秒人声 + 600ms 静音 → 被切成一句 |
| `TestStage2_ShortNoiseDoesNotTrigger` | 100ms 短促噪声 → 不触发识别 |
| `TestStage3_WAVHeader` | 送给 ASR 的 WAV 头字节正确 |
| `TestStage3_OpenAIASRRequestShape` | multipart 请求字段与 `Authorization` 正确 |
| `TestStage3_MockASRErrorPropagation` | 上游 500 时错误能抛出来，不是静默返回空文本 |
| `TestStage4_SentenceSplitter*` | 分句：基本切分、连续标点合并、最长长度兜底 |
| `TestStage4_OpenAILLMStream` | SSE 流式片段解析与拼接 |
| `TestStage4_EndToEndText` | 文本播报的完整序列与句子配对 |
| `TestStage5_Resampler` | 24k→16k 重采样，且分块喂入与整块喂入结果完全一致 |
| `TestStage5_OpenAITTSStreamAndResample` | TTS 流式返回 + 重采样，总时长和能量都对 |
| `TestStage5_EndToEndAudio` | 每一句都夹带音频，首帧不是静音 |
| `TestStage6_BargeInByVoice` | 播报中说话 → 收到 `tts/stop` 且之后不再有音频 |
| `TestStage6_BargeInSpeechBecomesNewTurn` | 插话的内容被接成新一轮，不用重说 |
| `TestStage6_ShortBlipDoesNotInterrupt` | 60ms 的响声不会打断播报 |
| `TestStage6_AbortMessage` | 设备发 `abort` 能打断 |
| `TestStage6_VADMarkSpeakingThenSilence` | 插话后用户直接住口也能收尾 |
| `TestFullPipelineMessageSequence` | 完整序列，见上一节 |

---

## 8. 刻意省略了什么

真实实现里有、简化版里没有的东西（不是不重要，是和学习主链路无关）：

- **数据上报 / 埋点统计**：`eventbus` 事件、`internal/pool/reporter.go`、
  hooks 里的 `statistic_plugin.go`
- **MCP 工具调用**：`tool.go`、`mcp.go`、`session_mcp_tool.go`、
  `local_mcp_tool.go`（约 1500 行）
- **音乐播放器**：`media_player.go`（约 1950 行）、`realtime_media_gate.go`
- **记忆与知识库**：`internal/domain/memory`、`internal/domain/rag`
- **OpenClaw**：`openclaw_warmup.go` + `internal/domain/openclaw`
- **声纹识别**：`speaker.go` + `internal/domain/speaker`
- **视觉 / vLLM**：`vllm.go`
- **MQTT/UDP 旁路通道**：`internal/app/mqtt_server`、`mqtt_udp`
  （走这条通道时音频帧会多 16 字节私有头）
- **资源池与配置热更新**：`internal/pool`、`internal/domain/config`
- **真实 VAD 模型**：silero/ten-vad 换成能量阈值
- **多轮上下文管理**：只保留最近 10 条消息，不做工具调用对齐、超长裁剪、
  长期记忆注入
- **发送侧缓冲**：真实实现有 `sessionAudioQueue`（容量 150）按播放节奏预发；
  这里直接同步写下去，顺序一样但没有「提前缓冲」

---

## 9. 已知的简化与假设

写代码过程中踩到的几个真实问题，特意留在注释里，值得单独说一遍。

### 9.1 `listen start` 和音频帧之间存在时序空窗

控制指令（文本帧）和音频（二进制帧）由两个独立的消费循环处理。设备发出
`listen start` 之后，服务端需要几微秒才会把「正在收音」置为 true，
这期间到达的音频帧会被丢弃。

真实实现是同样的结构，靠「设备不会在发起收音的同一瞬间就开始灌音频」
以及首帧预滚缓冲（`trimFirstSpeechAudio`）来规避。

所以在测试里，假设备会先发 `listen start`、稍等 80ms 再推音频 ——
真机本来就是这个节奏。见 `device.startListening()`。

### 9.2 播报期间为什么要单独一个 VAD

打断检测用的是独立的 `bargeVad`，不能和收音用的 `vad` 共用：
收音那个 VAD 有自己的静音计时，两边混用会互相把状态清掉。

更隐蔽的一个坑：用户插话打断之后，我们转入收音并把插话开头那几个字
收进缓冲，但如果此时直接 `vad.Reset()`，而用户接下来正好说完了
（后面全是静音），`vad.Push` 会一直返回「静音」——因为它不知道
「已经在说话中间」。结果是这一轮永远收不了尾。

解法是 `vad.MarkSpeaking(bargeInSpeechMs)`：显式告诉 VAD
「已经在说，而且至少已经说了这么久」。

### 9.3 打断必须能穿透「整段播报」

最初 `endTurn` 是同步调用 `recognize` 的，结果整个播报过程把
`handleCmdLoop` 堵死了 —— 连 `abort` 都收不到，用户根本没法打断。
改成丢进 goroutine 之后才通。

同理，`interrupt` 里用的是「轮次编号」（`gen`）而不是直接删除队列元素：
播报 goroutine 每发一帧前都确认自己还是不是当前轮，不是就把剩下的
全丢掉。这正是真实实现里 `audioGeneration` 计数器的做法。

### 9.4 采样率不匹配

OpenAI 兼容的 `/audio/speech` 里的 `pcm` 格式固定是 24kHz，而设备要
16kHz。所以 `tts.go` 里有一个线性插值重采样器，并且它必须支持
**分块喂入**（每读到一块就重采样一块、够一帧就发一帧），否则
「边说边播」就退化成了「整句说完再播」。

`TestStage5_Resampler` 专门验证了「分块喂入与整块喂入结果完全一致」。
