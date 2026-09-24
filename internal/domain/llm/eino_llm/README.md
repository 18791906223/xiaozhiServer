# Eino LLM Provider - 统一多提供者实现

## 概述

EinoLLMProvider 是基于 CloudWeGo Eino 框架的统一 LLM 提供者实现，支持多种大语言模型提供者，包括 OpenAI 和 Ollama。该实现完全使用 Eino 原生类型和接口，提供了一致的 API 体验。

## 核心特性

### ✅ 多提供者支持
- **OpenAI**: 支持 GPT-3.5、GPT-4 等模型
- **Ollama**: 支持本地部署的开源模型
- **统一接口**: 所有提供者使用相同的 API

### ✅ Eino 原生实现
- 直接使用 `*schema.Message` 和 `*schema.ToolInfo` 类型
- 调用 `chatModel.Generate()` 和 `chatModel.Stream()` 方法
- 支持 `chatModel.BindTools()` 进行工具绑定

### ✅ 完整功能支持
- 流式和非流式响应
- 工具调用和函数绑定
- 上下文控制和取消
- 链式配置调用

### ✅ 高度兼容
- 实现标准 `LLMProvider` 接口
- 支持现有代码无缝迁移
- 提供向后兼容的类型转换

## 架构设计

```
┌─────────────────────────────────────────────────────────────┐
│                    LLMProvider Interface                    │
│  Response() / ResponseWithFunctions() / ResponseWithContext() │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   EinoLLMProvider                          │
│  • 统一配置管理                                              │
│  • 多提供者支持                                              │
│  • 链式调用                                                 │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                 Eino ChatModel Interface                   │
│  Generate() / Stream() / BindTools()                       │
└─────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
┌─────────────────────────┐  ┌─────────────────────────┐
│   OpenAI ChatModel      │  │   Ollama ChatModel      │
│   (eino-ext/openai)     │  │   (eino-ext/ollama)     │
└─────────────────────────┘  └─────────────────────────┘
```

## 快速开始

### 1. 基本配置

```go
// OpenAI 配置
openaiConfig := map[string]interface{}{
    "type":       "openai",
    "model_name": "gpt-3.5-turbo",
    "api_key":    "your-openai-api-key",
    "base_url":   "https://api.openai.com/v1",
    "max_tokens": 500,
    "streamable": true,
}

// Ollama 配置
ollamaConfig := map[string]interface{}{
    "type":       "ollama",
    "model_name": "llama2",
    "base_url":   "http://localhost:11434",
    "max_tokens": 500,
    "streamable": true,
}
```

### 2. 创建提供者

```go
// 创建 OpenAI 提供者
openaiProvider, err := NewEinoLLMProvider(openaiConfig)
if err != nil {
    log.Fatalf("创建 OpenAI 提供者失败: %v", err)
}

// 创建 Ollama 提供者
ollamaProvider, err := NewEinoLLMProvider(ollamaConfig)
if err != nil {
    log.Fatalf("创建 Ollama 提供者失败: %v", err)
}
```

### 3. 使用 Eino 原生消息类型

```go
messages := []*schema.Message{
    {
        Role:    schema.System,
        Content: "你是一个有用的助手",
    },
    {
        Role:    schema.User,
        Content: "请介绍一下 Eino 框架",
    },
}
```

### 4. 基本对话

```go
// 流式响应
responseChan := provider.Response("session_id", messages)
for content := range responseChan {
    fmt.Print(content)
}
```

### 5. 工具调用

```go
tools := []*schema.ToolInfo{
    {
        Name: "get_weather",
        ParamsOneOf: &schema.ParamsOneOf{
            // 工具参数定义
        },
    },
}

toolResponseChan := provider.ResponseWithFunctions("session_id", messages, tools)
for response := range toolResponseChan {
    switch resp := response.(type) {
    case map[string]string:
        if resp["type"] == "content" {
            fmt.Print(resp["content"])
        }
    case map[string]interface{}:
        if resp["type"] == "tool_calls" {
            fmt.Printf("工具调用: %+v\n", resp["tool_calls"])
        }
    }
}
```

### 6. 链式调用

```go
enhancedProvider := provider.
    WithMaxTokens(1000).
    WithStreamable(false)

fmt.Printf("提供者类型: %s\n", enhancedProvider.GetProviderType())
fmt.Printf("模型信息: %+v\n", enhancedProvider.GetModelInfo())
```

## API 文档

### 核心接口

#### `NewEinoLLMProvider(config map[string]interface{}) (*EinoLLMProvider, error)`
创建新的 Eino LLM 提供者实例。

**参数:**
- `config`: 配置映射，必须包含 `type` 字段

**返回:**
- `*EinoLLMProvider`: 提供者实例
- `error`: 错误信息

#### `Response(sessionID string, dialogue []*schema.Message) chan string`
生成基本文本响应。

#### `ResponseWithFunctions(sessionID string, dialogue []*schema.Message, functions []*schema.ToolInfo) chan interface{}`
生成带工具调用的响应。

#### `ResponseWithContext(ctx context.Context, sessionID string, dialogue []*schema.Message) chan string`
带上下文控制的响应生成。

### 配置选项

| 字段 | 类型 | 必需 | 描述 |
|------|------|------|------|
| `type` | string | ✅ | 提供者类型: "openai", "ollama" |
| `model_name` | string | ✅ | 模型名称 |
| `api_key` | string | ⚠️ | API 密钥 (OpenAI 必需) |
| `base_url` | string | ❌ | 基础 URL |
| `max_tokens` | int | ❌ | 最大令牌数 (默认: 500) |
| `streamable` | bool | ❌ | 是否支持流式 (默认: true) |

### 链式方法

#### `WithMaxTokens(maxTokens int) *EinoLLMProvider`
设置最大令牌数，返回新的提供者实例。

#### `WithStreamable(streamable bool) *EinoLLMProvider`
设置流式支持，返回新的提供者实例。

#### `GetChatModel() model.ChatModel`
获取底层的 Eino ChatModel 实例。

#### `GetProviderType() string`
获取提供者类型。

#### `GetModelInfo() map[string]interface{}`
获取模型信息和元数据。

## 高级用法

### 直接使用 Eino ChatModel

```go
chatModel := provider.GetChatModel()

// 直接调用生成
result, err := chatModel.Generate(ctx, messages)
if err != nil {
    log.Printf("生成失败: %v", err)
    return
}
fmt.Printf("结果: %s\n", result.Content)

// 直接调用流式
streamReader, err := chatModel.Stream(ctx, messages)
if err != nil {
    log.Printf("流式调用失败: %v", err)
    return
}
defer streamReader.Close()

for {
    message, err := streamReader.Recv()
    if err == io.EOF {
        break
    }
    if err != nil {
        log.Printf("接收失败: %v", err)
        break
    }
    fmt.Print(message.Content)
}
```

### 多提供者管理

```go
providers := make(map[string]*EinoLLMProvider)

configs := map[string]map[string]interface{}{
    "openai": {
        "type":       "openai",
        "model_name": "gpt-3.5-turbo",
        "api_key":    "your-openai-key",
    },
    "ollama": {
        "type":       "ollama",
        "model_name": "llama2",
        "base_url":   "http://localhost:11434",
    },
}

for name, config := range configs {
    provider, err := NewEinoLLMProvider(config)
    if err != nil {
        log.Printf("创建 %s 提供者失败: %v", name, err)
        continue
    }
    providers[name] = provider
}

// 使用不同提供者处理相同请求
for name, provider := range providers {
    fmt.Printf("=== %s 提供者响应 ===\n", name)
    responseChan := provider.Response("session", messages)
    for content := range responseChan {
        fmt.Print(content)
    }
    fmt.Println()
}
```

## 测试

运行完整测试套件：

```bash
go test ./internal/domain/llm/eino_llm/... -v
```

### 测试覆盖

- ✅ 提供者创建和配置
- ✅ 多种提供者类型支持
- ✅ 基本对话功能
- ✅ 工具调用功能
- ✅ 链式调用
- ✅ 错误处理
- ✅ 性能基准测试

## 依赖

- `github.com/cloudwego/eino` v0.3.40+
- `github.com/cloudwego/eino-ext` v0.0.1-alpha+

## 最佳实践

### 1. 错误处理
```go
provider, err := NewEinoLLMProvider(config)
if err != nil {
    log.Errorf("创建提供者失败: %v", err)
    return
}
```

### 2. 上下文控制
```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

responseChan := provider.ResponseWithContext(ctx, sessionID, messages)
```

### 3. 资源管理
```go
// 对于流式响应，确保消费完所有数据
for content := range responseChan {
    // 处理内容
}
```

### 4. 配置管理
```go
// 使用环境变量管理敏感信息
config := map[string]interface{}{
    "type":       "openai",
    "model_name": "gpt-3.5-turbo",
    "api_key":    os.Getenv("OPENAI_API_KEY"),
}
```

## 扩展说明

### 添加新提供者

要添加新的提供者支持，需要：

1. 在 `createXXXChatModel` 函数中添加新的实现
2. 在 `NewEinoLLMProvider` 的 switch 语句中添加新的 case
3. 确保新提供者实现 `model.ChatModel` 接口

### 自定义配置

可以通过扩展配置映射来支持提供者特定的选项：

```go
config := map[string]interface{}{
    "type":        "openai",
    "model_name":  "gpt-4",
    "api_key":     "your-key",
    "temperature": 0.7,  // 自定义参数
    "top_p":       0.9,  // 自定义参数
}
```

## 版本历史

### v3.0.0 (当前版本)
- ✅ 完全基于 Eino 框架重写
- ✅ 支持多提供者 (OpenAI, Ollama)
- ✅ 使用 Eino 原生类型
- ✅ 直接调用 Eino ChatModel 方法
- ✅ 移除适配器层，提高性能
- ✅ 完整的测试覆盖

### v2.x.x (已废弃)
- 混合实现，使用适配器模式
- 部分 Eino 集成

### v1.x.x (已废弃)
- 基于传统 OpenAI 实现
- 无 Eino 集成

## 贡献

欢迎提交 Issue 和 Pull Request 来改进这个实现。

## 许可证

本项目遵循项目根目录的许可证。 