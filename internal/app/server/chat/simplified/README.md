# 简化版小智服务端：实时语音对话主链路

这个目录是 `internal/app/server/chat`（约 15000 行）的**教学用简化实现**。

目的只有一个：让你能用一条直线看懂「设备说一句话 → 音箱答一句话」到底发生了什么。

真实实现里塞满了数据上报、MCP 工具、音乐播放、记忆(mem0)、知识库(RAG)、
OpenClaw、声纹识别、MQTT/UDP 双通道、配置热更新……这些东西各自合理，
但堆在一起之后，主链路就被淹没了。这里把它们全部拿掉，只留主干。

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

---

## 2. 简化版文件 ↔ 真实实现对照

| 简化版文件 | 真实实现里对应什么 |
|---|---|
| `protocol.go` | `internal/data/msg/message_types.go` + `internal/data/client/client.go` 的消息结构体 |
| `transport.go` | `internal/app/server/websocket/websocket_conn.go`（`types.IConn` 的 WebSocket 实现） |
| `server.go` | `internal/app/server/websocket/websocket_server.go` 的 `handleChat` |
| `session.go` | `internal/app/server/chat/chat.go` 的 `ChatManager` + `session.go` 的 `ChatSession` |
| `audio.go` | `internal/domain/audio/audio_handler.go`（Opus 编解码） |
| `vad.go` | `internal/domain/vad/`（真实实现用 silero_vad / ten_vad 神经网络模型，这里用能量阈值） |
| `asr.go` | `internal/domain/asr/base.go` 的 `AsrProvider` 接口 |
| `llm.go` | `internal/domain/llm/base.go` 的 `LLMProvider` 接口 |
| `tts.go` | `internal/domain/tts/base.go` 的 `TTSProvider` 接口 |
| `providers.go` | 真实实现散落在 `internal/pool` 资源池 + 各 provider 工厂里的「选哪个实现」逻辑 |

真实实现里，每一环之间都靠一个带容量限制的 channel 队列解耦：

```
OpusAudioBuffer → AsrAudioChannel → AsrResultChannel → chatTextQueue
   → llmResponseQueue → ttsQueue → sessionAudioQueue → socket
```

简化版把队列数量降到最低，但**保留了「每环独立 goroutine + channel 传递」这个关键结构**，
因为它正是真实实现里打断(barge-in)、超时、背压这些东西能工作的原因。

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
| `tts` | `sentence_start` | `text` | 一句开始，紧跟其后的是这句的音频帧 |
| `tts` | `sentence_end` | `text` | 一句结束 |
| `tts` | `stop` | — | 本轮播报结束，设备可以重新收音 |

音频永远是**二进制帧**，夹在 `sentence_start` 和 `sentence_end` 之间。

---

## 4. 分阶段实现（每个阶段一个 git commit）

用 `git log --oneline` 看这一串提交，再用 `git show <hash>` 看每一步加了什么。

| # | 提交 | 你会看到什么 |
|---|---|---|
| 1 | 文档：路线图 | 本文件 |
| 2 | 阶段 1：握手与文本指令 | WebSocket 服务、`hello` 握手、`listen` 分发。此时音频帧被直接丢弃 |
| 3 | 阶段 2：音频接入与 VAD | Opus 解码、能量 VAD、静音 400ms 判定「说完了」，先打印一个假的识别结果 |
| 4 | 阶段 3：ASR | 抽出 `ASR` 接口，把 PCM 变成真正的文本，回 `stt` |
| 5 | 阶段 4：LLM 与分句 | 抽出 `LLM` 接口，流式 token 按标点切成句子 |
| 6 | 阶段 5：TTS 与音频下发 | 抽出 `TTS` 接口，句子变成 Opus 帧发回设备。全链路打通 |
| 7 | 阶段 6：打断 | 说话打断播报，generation 计数 + 队列清空 |
| 8 | 端到端测试 | 一个进程内跑通「连接 → 握手 → 说话 → 收到回复音频」 |

建议的阅读方式：

```bash
git log --oneline -- internal/app/server/chat/simplified
git show <阶段N的hash>          # 看这一阶段改了什么
```

---

## 5. 怎么跑

```bash
# 默认全用内置的假实现（不需要任何 API Key，能听到「哔——」的方波提示音）
go run ./internal/app/server/chat/simplified

# 接真实服务（任何 OpenAI 兼容接口都行）
go run ./internal/app/server/chat/simplified \
  -asr-url   https://api.siliconflow.cn/v1 -asr-key $KEY -asr-model FunAudioLLM/SenseVoiceSmall \
  -llm-url   https://api.siliconflow.cn/v1 -llm-key $KEY -llm-model Qwen/Qwen2.5-7B-Instruct \
  -tts-url   https://api.siliconflow.cn/v1 -tts-key $KEY -tts-model FunAudioLLM/CosyVoice2-0.5B -tts-voice alex
```

默认监听 `:8989`，路由 `/xiaozhi/v1/`，和真实服务一致。
设备端把 OTA 地址指到 `ws://<你的内网IP>:8989/xiaozhi/v1/` 即可。

**没给 `-asr-key` / `-llm-key` / `-tts-key` 时，对应环节自动使用内置假实现**，
所以哪怕三个都没配，也能完整跑一遍流程、看到全部日志。

---

## 6. 刻意省略了什么

真实实现里有、简化版里没有的东西（不是不重要，是和学习主链路无关）：

- **数据上报 / 埋点统计**：`eventbus` 事件、`internal/pool/reporter.go`、hooks 里的 `statistic_plugin.go`
- **MCP 工具调用**：`tool.go`、`mcp.go`、`session_mcp_tool.go`、`local_mcp_tool.go`（约 1500 行）
- **音乐播放器**：`media_player.go`（约 1950 行）、`realtime_media_gate.go`
- **记忆与知识库**：`internal/domain/memory`、`internal/domain/rag`
- **OpenClaw**：`openclaw_warmup.go` + `internal/domain/openclaw`
- **声纹识别**：`speaker.go` + `internal/domain/speaker`
- **视觉 / vLLM**：`vllm.go`
- **MQTT/UDP 旁路通道**：`internal/app/mqtt_server`、`mqtt_udp`（设备端走这条时，音频帧会多 16 字节私有头）
- **资源池与配置热更新**：`internal/pool`、`internal/domain/config`
- **真实 VAD 模型**：silero/ten-vad 换成能量阈值
- **多轮上下文管理**：只保留最近 N 条消息，不做工具调用对齐、超长裁剪、记忆注入
