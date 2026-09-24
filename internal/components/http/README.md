# HTTP 组件

统一的 HTTP 客户端组件，用于管理所有对 Manager 后端的 HTTP 调用。

## 目录结构

```
internal/components/http/
├── client.go          # 通用 HTTP 客户端（支持重试、认证等）
├── manager_client.go  # Manager 后端专用客户端
├── types.go           # 类型定义
└── README.md          # 本文档
```

## 设计说明

### Client（通用 HTTP 客户端）

提供基础的 HTTP 请求功能：
- 支持重试机制（使用 exponential backoff）
- 支持认证 Token（Bearer Token）
- 支持自定义超时时间
- 统一错误处理
- 自动 JSON 序列化/反序列化

### ManagerClient（Manager 后端专用客户端）

基于通用客户端封装，专门用于调用 Manager 后端 API。

## 使用示例

### 创建 Manager 客户端

```go
import "xiaozhi-esp32-server-golang/internal/components/http"

client := http.NewManagerClient(http.ManagerClientConfig{
    BaseURL:    "http://localhost:8080",
    AuthToken:  "your-token",  // 可选
    Timeout:    10 * time.Second,
    MaxRetries: 3,
})
```

### 发送 GET 请求

```go
var response MyResponse
err := client.DoRequest(ctx, http.RequestOptions{
    Method: "GET",
    Path:   "/api/configs",
    QueryParams: map[string]string{
        "device_id": "device123",
    },
    Response: &response,
})
```

### 发送 POST 请求

```go
request := MyRequest{
    Field1: "value1",
    Field2: "value2",
}

err := client.DoRequest(ctx, http.RequestOptions{
    Method: "POST",
    Path:   "/api/internal/history/messages",
    Body:   request,
})
```

### 获取原始响应

```go
body, err := client.DoRequestRaw(ctx, http.RequestOptions{
    Method: "GET",
    Path:   "/api/system/configs",
})
```

## 重构说明

### 重构前

- `HistoryClient` 和 `ConfigManager` 各自实现 HTTP 调用逻辑
- 代码重复，维护成本高
- 重试、认证等逻辑分散

### 重构后

- 统一的 HTTP 组件，集中管理
- 代码复用，易于维护
- 统一的错误处理和重试机制

## 已重构的模块

1. **internal/data/history/client.go** - 聊天历史客户端
2. **internal/domain/config/manager/manager.go** - 配置管理器
3. **internal/domain/config/manager/auth.go** - 认证相关 API

## 注意事项

- 所有对 Manager 后端的 HTTP 调用都应使用 `ManagerClient`
- 如需调用其他后端服务，可以基于 `Client` 创建新的专用客户端
- 重试机制默认最多 3 次，可通过配置调整

