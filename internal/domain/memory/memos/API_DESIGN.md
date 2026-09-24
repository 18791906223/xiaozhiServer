# MemOS 独立 Provider API 对接文档（基于官方文档，可按部署覆盖 endpoint）

> 官方文档：`https://memos-docs.openmem.net/cn/api_docs/start/overview`
>
> 示例 base_url：`https://memos.memtensor.cn/api/openmem/v1`
>
> 目标：将 MemOS 作为**独立 provider**接入，不再与 `mem0` 复用。

---

## 1. 原则

- 不再猜测 API 路径。
- 不再将 `memos` 路由到 `mem0` 客户端。
- 仅以你提供的官方文档为准做字段与 endpoint 映射。

---

## 2. 当前仓库改造约束

系统接口 `MemoryProvider` 要求实现：

- `AddMessage`
- `GetMessages`
- `GetContext`
- `Search`
- `Flush`
- `ResetMemory`

因此 MemOS 对接时，必须在官方文档里逐项找到对应 API（或组合 API）并完成映射。

---

## 3. 对接要点（按官方文档）

以下字段按官方固定路径对接，不在控制台暴露 endpoint 配置：

1. 鉴权方式
   - Header 名称：
   - Token 前缀（如 `Bearer `）：

2. 写入记忆 API
   - Method + Path：
   - 请求体示例：
   - 响应体关键字段：

3. 查询记忆 API
   - Method + Path：
   - 过滤参数（agent_id / user_id / session_id 等）：
   - 响应体示例：

4. 检索/召回 API
   - Method + Path：
   - 参数（query/top_k/threshold/time_range）：
   - 响应字段（文本、分数、时间戳）：

5. 清空/删除 API
   - Method + Path：
   - 删除维度（用户级/会话级/agent级）：

6. 是否存在 flush/index refresh API
   - 若无，`Flush` 如何语义降级：

---

## 4. 代码落地计划（确认后执行）

```text
internal/domain/memory/memos/
  memos_client.go        # 真实 HTTP 调用
  types.go               # request/response DTO
  mapper.go              # API -> schema.Message
  memos_test.go          # httptest mock
```

并修改：

- `internal/domain/memory/base.go`
  - `MemoryTypeMemOS -> memos.GetWithConfig(config)`
- 管理端配置保留 `memos`（已支持）
- 示例配置保留 `memory.memos`（已支持）

---

## 5. 环境说明

当前执行环境对该官方文档站点请求返回 403，无法在本地自动抓取文档内容。

当前已按固定路径实现，控制台不提供 endpoint 路径编辑。


## 6. 当前实现说明

- 实际请求 URL = `base_url + endpoint_path`（例如 `http://host/api/v1` + `/add/message`）。
- 已实现 `memos_client.go`，默认使用以下接口：
  - `/add/message`
  - `/get/messages`
  - `/search/memory`
  - `/flush`
  - `/reset/memory`
- 路径采用固定官方语义：`/add/message`、`/get/messages`、`/search/memory`、`/flush`、`/reset/memory`。


## 7. Add Message 字段约束（已按文档调整）

- `user_id` / `conversation_id` 为必填。
- `agent_id` 为可选，仅在有值时传递。
- 当前实现中使用 `agentID` 同时映射到 `user_id` 与 `conversation_id`；当 `agentID` 为空时直接报错，不再使用默认占位值。


## 8. Search Memory 字段映射（已按文档调整）

- Path: `/search/memory`
- `user_id`: 使用 `agentID` 映射
- `conversation_id`: 使用 `agentID` 映射
- `query`: 透传用户输入
- `memory_limit_number`: 由 `topK` 映射
- `relativity`: 由 `search_threshold` 映射
