package eino_llm

import (
	"context"
	"fmt"
	"time"

	"github.com/cloudwego/eino/schema"

	log "xiaozhi-esp32-server-golang/logger"
)

// ExampleConfig 示例配置
var ExampleConfig = map[string]interface{}{
	"type":       "eino_llm",
	"model_name": "gpt-3.5-turbo",
	"api_key":    "your-api-key-here",
	"base_url":   "https://api.openai.com/v1",
	"max_tokens": 500,
	"streamable": true,
}

// ExampleUsage 展示如何使用EinoLLMProvider
func ExampleUsage() {
	// 1. OpenAI配置示例
	openaiConfig := map[string]interface{}{
		"type":       "openai",
		"model_name": "gpt-3.5-turbo",
		"api_key":    "your-openai-api-key",
		"base_url":   "https://api.openai.com/v1",
		"max_tokens": 500,
		"streamable": true,
	}

	// 2. Ollama配置示例
	ollamaConfig := map[string]interface{}{
		"type":       "ollama",
		"model_name": "llama2",
		"base_url":   "http://localhost:11434",
		"max_tokens": 500,
		"streamable": true,
	}

	// 3. 创建提供者
	openaiProvider, err := NewEinoLLMProvider(openaiConfig)
	if err != nil {
		log.Errorf("创建OpenAI提供者失败: %v", err)
		return
	}

	ollamaProvider, err := NewEinoLLMProvider(ollamaConfig)
	if err != nil {
		log.Errorf("创建Ollama提供者失败: %v", err)
		return
	}

	// 4. 使用Eino原生消息类型
	messages := []*schema.Message{
		{
			Role:    schema.System,
			Content: "你是一个有用的助手",
		},
		{
			Role:    schema.User,
			Content: "请介绍一下Eino框架",
		},
	}

	// 5. 基本对话
	fmt.Println("=== OpenAI 基本对话 ===")
	responseChan := openaiProvider.ResponseWithContext(context.Background(), "example_session", messages, nil)
	for resp := range responseChan {
		if resp.Content != "" {
			fmt.Print(resp.Content)
		}
		if len(resp.ToolCalls) > 0 {
			fmt.Printf("工具调用: %+v\n", resp.ToolCalls)
		}
	}
	fmt.Println()

	fmt.Println("=== Ollama 基本对话 ===")
	responseChan = ollamaProvider.ResponseWithContext(context.Background(), "example_session", messages, nil)
	for resp := range responseChan {
		if resp.Content != "" {
			fmt.Print(resp.Content)
		}
		if len(resp.ToolCalls) > 0 {
			fmt.Printf("工具调用: %+v\n", resp.ToolCalls)
		}
	}
	fmt.Println()

	// 6. 工具调用示例
	tools := []*schema.ToolInfo{
		{
			Name:        "get_weather",
			ParamsOneOf: &schema.ParamsOneOf{
				// 工具参数定义
			},
		},
	}

	fmt.Println("=== 带工具调用的对话 ===")
	toolResponseChan := openaiProvider.ResponseWithContext(context.Background(), "example_session", messages, tools)
	for resp := range toolResponseChan {
		if resp.Content != "" {
			fmt.Print(resp.Content)
		}
		if len(resp.ToolCalls) > 0 {
			fmt.Printf("工具调用: %+v\n", resp.ToolCalls)
		}
	}
	fmt.Println()

	// 7. 链式调用示例
	fmt.Println("=== 链式调用示例 ===")
	enhancedProvider := openaiProvider.
		WithMaxTokens(1000).
		WithStreamable(false)

	fmt.Printf("提供者类型: %s\n", enhancedProvider.GetProviderType())
	fmt.Printf("模型信息: %+v\n", enhancedProvider.GetModelInfo())
}

// ExampleAdvancedUsage 高级用法示例
func ExampleAdvancedUsage() {
	config := map[string]interface{}{
		"type":       "openai",
		"model_name": "gpt-4",
		"api_key":    "your-api-key",
		"max_tokens": 1000,
		"streamable": true,
	}

	provider, err := NewEinoLLMProvider(config)
	if err != nil {
		log.Errorf("创建提供者失败: %v", err)
		return
	}

	// 使用上下文控制
	ctx := context.Background()
	messages := []*schema.Message{
		{
			Role:    schema.User,
			Content: "请写一个关于AI的长篇文章",
		},
	}

	fmt.Println("=== 带上下文控制的对话 ===")
	responseChan := provider.ResponseWithContext(ctx, "advanced_session", messages, nil)
	for resp := range responseChan {
		if resp.Content != "" {
			fmt.Print(resp.Content)
		}
		if len(resp.ToolCalls) > 0 {
			fmt.Printf("工具调用: %+v\n", resp.ToolCalls)
		}
	}
	fmt.Println()

	// 直接使用Eino ChatModel
	chatModel := provider.GetChatModel()
	result, err := chatModel.Generate(ctx, messages)
	if err != nil {
		log.Errorf("直接调用ChatModel失败: %v", err)
		return
	}

	fmt.Printf("直接调用结果: %s\n", result.Content)
}

// ExampleMultiProvider 多提供者示例
func ExampleMultiProvider() {
	providers := make(map[string]*EinoLLMProvider)

	// 创建多个提供者
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
			log.Errorf("创建%s提供者失败: %v", name, err)
			continue
		}
		providers[name] = provider
	}

	// 使用不同提供者处理相同请求
	messages := []*schema.Message{
		{
			Role:    schema.User,
			Content: "你好，请介绍一下你自己",
		},
	}

	for name, provider := range providers {
		fmt.Printf("=== %s 提供者响应 ===\n", name)
		responseChan := provider.ResponseWithContext(context.Background(), "multi_session", messages, nil)
		for resp := range responseChan {
			if resp.Content != "" {
				fmt.Print(resp.Content)
			}
			if len(resp.ToolCalls) > 0 {
				fmt.Printf("工具调用: %+v\n", resp.ToolCalls)
			}
		}
		fmt.Println()
	}
}

// ExampleWithTools 工具调用示例
func ExampleWithTools() {
	provider, err := NewEinoLLMProvider(ExampleConfig)
	if err != nil {
		log.Errorf("创建提供者失败: %v", err)
		return
	}

	// 使用Eino原生消息类型
	messages := []*schema.Message{
		{
			Role:    schema.User,
			Content: "今天北京的天气如何？请帮我查询一下。",
		},
	}

	// 使用Eino原生工具类型
	tools := []*schema.ToolInfo{
		{
			Name:        "get_weather",
			ParamsOneOf: &schema.ParamsOneOf{
				// 简化的工具参数定义
				// 在实际使用中，这里需要正确定义参数结构
			},
		},
	}

	fmt.Println("=== 工具调用示例 ===")

	// 使用Eino原生工具调用接口
	fmt.Println("--- Eino原生工具调用 ---")
	responseChan := provider.ResponseWithContext(context.Background(), "tool_session", messages, tools)
	for resp := range responseChan {
		fmt.Printf("响应: %+v\n", resp)
	}
}

// MultiProviderExample 多提供者示例
func MultiProviderExample() {
	// OpenAI提供者示例
	fmt.Println("=== OpenAI 提供者示例 ===")
	openaiConfig := map[string]interface{}{
		"type":       "openai",
		"model_name": "gpt-3.5-turbo",
		"api_key":    "your-openai-api-key",
		"base_url":   "https://api.openai.com/v1",
		"max_tokens": 500,
	}

	openaiProvider, err := NewEinoLLMProvider(openaiConfig)
	if err != nil {
		log.Errorf("创建OpenAI提供者失败: %v", err)
		return
	}

	fmt.Printf("提供者类型: %s\n", openaiProvider.GetProviderType())

	// Ollama提供者示例
	fmt.Println("\n=== Ollama 提供者示例 ===")
	ollamaConfig := map[string]interface{}{
		"type":       "ollama",
		"model_name": "llama2",
		"base_url":   "http://localhost:11434",
		"max_tokens": 500,
	}

	ollamaProvider, err := NewEinoLLMProvider(ollamaConfig)
	if err != nil {
		log.Errorf("创建Ollama提供者失败: %v", err)
		return
	}

	fmt.Printf("提供者类型: %s\n", ollamaProvider.GetProviderType())

	// 使用Eino原生消息类型
	messages := []*schema.Message{
		{
			Role:    schema.User,
			Content: "请介绍一下你自己。",
		},
	}

	// 分别测试两个提供者
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fmt.Println("\n--- OpenAI 响应 ---")
	openaiResponse := openaiProvider.ResponseWithContext(ctx, "openai_session", messages, nil)
	for resp := range openaiResponse {
		if resp.Content != "" {
			fmt.Print(resp.Content)
		}
		if len(resp.ToolCalls) > 0 {
			fmt.Printf("工具调用: %+v\n", resp.ToolCalls)
		}
	}

	fmt.Println("\n--- Ollama 响应 ---")
	ollamaResponse := ollamaProvider.ResponseWithContext(ctx, "ollama_session", messages, nil)
	for resp := range ollamaResponse {
		if resp.Content != "" {
			fmt.Print(resp.Content)
		}
		if len(resp.ToolCalls) > 0 {
			fmt.Printf("工具调用: %+v\n", resp.ToolCalls)
		}
	}
	fmt.Println()
}

// EinoFrameworkAdvantages Eino框架的优势说明
func EinoFrameworkAdvantages() string {
	return `
Eino框架的主要优势：

1. **组件化设计**
   - 丰富的组件抽象（ChatModel, Tool, ChatTemplate, Retriever等）
   - 每个组件都有统一的输入输出接口
   - 支持组件嵌套和复杂业务逻辑封装

2. **强大的编排能力**
   - 基于图的数据流编排
   - 自动处理类型检查、流处理、并发管理
   - 支持分支执行、状态管理、字段映射

3. **完整的流处理**
   - 自动串联流式数据块
   - 自动装箱非流数据为流
   - 自动合并多个流
   - 自动复制流到多个下游节点

4. **高扩展性**
   - 支持自定义回调处理器
   - 五种切面支持（OnStart, OnEnd, OnError等）
   - 可注入日志、追踪、监控等横切关注点

5. **生产就绪**
   - 完整的错误处理机制
   - 支持超时和取消操作
   - 连接池和性能优化
   - 详细的日志和监控

本实现特点：

**多提供者支持**：
- 统一的Eino接口支持OpenAI和Ollama
- 通过type配置灵活切换提供者
- 每个提供者都使用相同的Eino ChatModel接口

**Eino原生实现**：
- 直接使用*schema.Message类型进行对话
- 直接使用*schema.ToolInfo类型进行工具调用
- 完全基于Eino框架构建，无需类型转换

**增强功能**：
- 链式调用支持 (WithMaxTokens, WithStreamable)
- 统一的错误处理和日志记录
- 支持流式和非流式调用模式
- 完全兼容原有LLMProvider接口

**最佳实践**：
- 支持上下文取消和超时控制
- 结构化日志和监控集成
- 类型安全的配置管理
- 资源自动管理和清理

这种实现方式真正使用了Eino框架的核心能力，同时支持多种LLM提供者。
`
}

// BasicUsageExample 基础用法示例
func BasicUsageExample() {
	provider, err := NewEinoLLMProvider(ExampleConfig)
	if err != nil {
		log.Errorf("创建提供者失败: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 演示链式配置
	enhancedProvider := provider.
		WithMaxTokens(2000).
		WithStreamable(true)

	// 获取底层的Eino ChatModel
	chatModel := enhancedProvider.GetChatModel()
	fmt.Printf("底层ChatModel: %+v\n", chatModel)

	// 获取提供者类型
	providerType := enhancedProvider.GetProviderType()
	fmt.Printf("提供者类型: %s\n", providerType)

	// 获取增强后的模型信息
	modelInfo := enhancedProvider.GetModelInfo()
	fmt.Printf("增强模型信息: %+v\n", modelInfo)

	// 复杂对话示例 - 使用Eino原生消息类型
	messages := []*schema.Message{
		{
			Role:    schema.System,
			Content: "你是一个专业的软件架构师，精通Go语言和AI应用开发。",
		},
		{
			Role:    schema.User,
			Content: "请设计一个基于Eino框架的聊天机器人系统架构。",
		},
	}

	// 使用增强配置进行调用
	responseChan := enhancedProvider.ResponseWithContext(ctx, "basic_example", messages, nil)
	fmt.Printf("架构设计响应:\n")
	for resp := range responseChan {
		if resp.Content != "" {
			fmt.Print(resp.Content)
		}
		if len(resp.ToolCalls) > 0 {
			fmt.Printf("工具调用: %+v\n", resp.ToolCalls)
		}
	}
	fmt.Println()
}

// EinoNativeExample Eino原生API示例
func EinoNativeExample() {
	provider, err := NewEinoLLMProvider(ExampleConfig)
	if err != nil {
		log.Errorf("创建提供者失败: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 使用Eino原生消息类型
	messages := []*schema.Message{
		{
			Role:    schema.System,
			Content: "你是一个有用的AI助手。",
		},
		{
			Role:    schema.User,
			Content: "请简单介绍一下Eino框架。",
		},
	}

	fmt.Println("=== Eino原生API示例 ===")

	// 1. 使用EinoResponse
	fmt.Println("--- EinoResponse ---")
	responseChan := provider.ResponseWithContext(ctx, "eino_session", messages, nil)
	for resp := range responseChan {
		if resp.Content != "" {
			fmt.Print(resp.Content)
		}
		if len(resp.ToolCalls) > 0 {
			fmt.Printf("工具调用: %+v\n", resp.ToolCalls)
		}
	}
	fmt.Println()

	// 2. 使用EinoResponseWithTools
	fmt.Println("\n--- EinoResponseWithTools ---")
	tools := []*schema.ToolInfo{
		{
			Name:        "search_docs",
			ParamsOneOf: &schema.ParamsOneOf{
				// 工具参数定义
			},
		},
	}

	toolResponseChan := provider.ResponseWithContext(ctx, "eino_tools_session", messages, tools)
	for resp := range toolResponseChan {
		if resp.Content != "" {
			fmt.Printf("内容: %s\n", resp.Content)
		}
		if len(resp.ToolCalls) > 0 {
			fmt.Printf("工具调用: %+v\n", resp.ToolCalls)
		}
	}
}

func main() {
	BasicUsageExample()
}
