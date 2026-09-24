package eino_llm

import (
	"fmt"
	"testing"

	"github.com/cloudwego/eino/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEinoLLMProvider(t *testing.T) {
	tests := []struct {
		name      string
		config    map[string]interface{}
		expectErr bool
	}{
		{
			name: "valid openai config",
			config: map[string]interface{}{
				"type":       "openai",
				"model_name": "gpt-3.5-turbo",
				"api_key":    "test-key",
				"base_url":   "https://api.openai.com/v1",
				"max_tokens": 500,
			},
			expectErr: false,
		},
		{
			name: "valid ollama config",
			config: map[string]interface{}{
				"type":       "ollama",
				"model_name": "llama2",
				"base_url":   "http://localhost:11434",
				"max_tokens": 500,
			},
			expectErr: false,
		},
		{
			name: "missing type",
			config: map[string]interface{}{
				"model_name": "gpt-3.5-turbo",
				"api_key":    "test-key",
			},
			expectErr: true,
		},
		{
			name: "missing model_name",
			config: map[string]interface{}{
				"type":    "openai",
				"api_key": "test-key",
			},
			expectErr: true,
		},
		{
			name: "with streamable config",
			config: map[string]interface{}{
				"type":       "openai",
				"model_name": "gpt-4",
				"api_key":    "test-key",
				"streamable": false,
			},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider, err := NewEinoLLMProvider(tt.config)

			if tt.expectErr {
				assert.Error(t, err)
				assert.Nil(t, provider)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, provider)
				assert.Equal(t, tt.config["model_name"], provider.modelName)
				assert.NotNil(t, provider.chatModel)
				if tt.config["type"] != nil {
					assert.Equal(t, tt.config["type"], provider.providerType)
				}
			}
		})
	}
}

func TestEinoLLMProvider_GetModelInfo(t *testing.T) {
	config := map[string]interface{}{
		"type":       "openai",
		"model_name": "gpt-3.5-turbo",
		"api_key":    "test-key",
		"max_tokens": 1000,
	}

	provider, err := NewEinoLLMProvider(config)
	require.NoError(t, err)

	info := provider.GetModelInfo()

	assert.Equal(t, "eino", info["framework"])
	assert.Equal(t, "eino", info["type"])
	assert.Equal(t, "openai", info["provider_type"])
	assert.Equal(t, "3.0.0", info["adapter_version"])
	assert.Equal(t, true, info["streamable"])
	assert.Contains(t, info, "model_name")
}

func TestEinoLLMProvider_WithMaxTokens(t *testing.T) {
	config := map[string]interface{}{
		"type":       "openai",
		"model_name": "gpt-3.5-turbo",
		"api_key":    "test-key",
		"max_tokens": 500,
	}

	provider, err := NewEinoLLMProvider(config)
	require.NoError(t, err)

	// 测试链式调用
	newProvider := provider.WithMaxTokens(1000)

	assert.NotEqual(t, provider, newProvider)    // 应该是不同的实例
	assert.Equal(t, 500, provider.maxTokens)     // 原实例不变
	assert.Equal(t, 1000, newProvider.maxTokens) // 新实例已更新
}

func TestEinoLLMProvider_WithStreamable(t *testing.T) {
	config := map[string]interface{}{
		"type":       "openai",
		"model_name": "gpt-3.5-turbo",
		"api_key":    "test-key",
		"streamable": true,
	}

	provider, err := NewEinoLLMProvider(config)
	require.NoError(t, err)

	// 测试链式调用
	newProvider := provider.WithStreamable(false)

	assert.NotEqual(t, provider, newProvider)      // 应该是不同的实例
	assert.Equal(t, true, provider.streamable)     // 原实例不变
	assert.Equal(t, false, newProvider.streamable) // 新实例已更新
}

func TestEinoLLMProvider_GetChatModel(t *testing.T) {
	config := map[string]interface{}{
		"type":       "openai",
		"model_name": "gpt-3.5-turbo",
		"api_key":    "test-key",
	}

	provider, err := NewEinoLLMProvider(config)
	require.NoError(t, err)

	chatModel := provider.GetChatModel()
	assert.NotNil(t, chatModel)
	assert.Equal(t, provider.chatModel, chatModel)
}

func TestEinoLLMProvider_GetProviderType(t *testing.T) {
	config := map[string]interface{}{
		"type":       "ollama",
		"model_name": "llama2",
		"base_url":   "http://localhost:11434",
	}

	provider, err := NewEinoLLMProvider(config)
	require.NoError(t, err)

	providerType := provider.GetProviderType()
	assert.Equal(t, "ollama", providerType)
}

func TestEinoLLMProvider_ResponseWithEinoMessages(t *testing.T) {
	config := map[string]interface{}{
		"type":       "openai", // 使用openai类型
		"model_name": "gpt-3.5-turbo",
		"api_key":    "test-key",
		"streamable": false, // 使用非流式以便测试
	}

	provider, err := NewEinoLLMProvider(config)
	require.NoError(t, err)

	// 使用Eino原生消息类型
	messages := []*schema.Message{
		{
			Role:    schema.System,
			Content: "你是一个助手",
		},
		{
			Role:    schema.User,
			Content: "你好",
		},
	}

	// 测试Response方法 - 注意：这将尝试真实API调用
	// 在没有真实API密钥的情况下，这会失败，但我们主要测试结构
	responseChan := provider.Response("test_session", messages)
	var responses []string
	for content := range responseChan {
		responses = append(responses, content)
		break // 只获取第一个响应以避免长时间等待
	}

	// 对于真实API调用，我们主要验证不会panic
	// assert.Len(t, responses, 1)
}

func TestEinoLLMProvider_ResponseWithFunctionsEinoTypes(t *testing.T) {
	config := map[string]interface{}{
		"type":       "openai", // 使用openai类型
		"model_name": "gpt-3.5-turbo",
		"api_key":    "test-key",
		"streamable": false,
	}

	provider, err := NewEinoLLMProvider(config)
	require.NoError(t, err)

	// 使用Eino原生消息类型
	messages := []*schema.Message{
		{
			Role:    schema.User,
			Content: "今天北京的天气如何？",
		},
	}

	// 使用Eino原生工具类型
	tools := []*schema.ToolInfo{
		{
			Name:        "get_weather",
			ParamsOneOf: &schema.ParamsOneOf{
				// 简化的工具参数定义
			},
		},
	}

	// 测试ResponseWithFunctions方法 - 仅验证结构
	responseChan := provider.ResponseWithFunctions("test_session", messages, tools)
	go func() {
		for range responseChan {
			// 消费响应但不验证内容
		}
	}()
}

func TestEinoConfig_Structure(t *testing.T) {
	// 测试配置结构体
	config := EinoConfig{
		Type:       "openai",
		ModelName:  "gpt-4",
		APIKey:     "test-key",
		BaseURL:    "https://api.openai.com/v1",
		MaxTokens:  1000,
		Streamable: true,
		Parameters: map[string]interface{}{
			"temperature": 0.7,
		},
	}

	assert.Equal(t, "openai", config.Type)
	assert.Equal(t, "gpt-4", config.ModelName)
	assert.Equal(t, "test-key", config.APIKey)
	assert.Equal(t, true, config.Streamable)
	assert.Contains(t, config.Parameters, "temperature")
}

// BenchmarkEinoLLMProvider_Response 性能基准测试
func BenchmarkEinoLLMProvider_Response(b *testing.B) {
	config := map[string]interface{}{
		"type":       "openai", // 使用openai类型
		"model_name": "gpt-3.5-turbo",
		"api_key":    "test-key",
	}

	provider, _ := NewEinoLLMProvider(config)
	messages := []*schema.Message{
		{
			Role:    schema.User,
			Content: "这是一个性能测试的内容",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		responseChan := provider.Response("bench_session", messages)
		// 消费响应以完成调用
		go func() {
			for range responseChan {
				// 消费响应
			}
		}()
	}
}

// BenchmarkEinoLLMProvider_WithMaxTokens 链式调用性能测试
func BenchmarkEinoLLMProvider_WithMaxTokens(b *testing.B) {
	config := map[string]interface{}{
		"type":       "openai", // 使用openai类型
		"model_name": "gpt-3.5-turbo",
		"api_key":    "test-key",
	}

	provider, _ := NewEinoLLMProvider(config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		provider.WithMaxTokens(1000 + i)
	}
}

// TestExampleConfig 测试示例配置
func TestExampleConfig(t *testing.T) {
	assert.Equal(t, "eino_llm", ExampleConfig["type"])
	assert.Equal(t, "gpt-3.5-turbo", ExampleConfig["model_name"])
	assert.Equal(t, 500, ExampleConfig["max_tokens"])
	assert.Equal(t, true, ExampleConfig["streamable"])
}

// TestEinoLLMProvider_FullWorkflow 完整工作流测试（仅结构验证，不涉及真实API）
func TestEinoLLMProvider_FullWorkflow(t *testing.T) {
	config := map[string]interface{}{
		"type":       "openai", // 使用openai类型
		"model_name": "gpt-3.5-turbo",
		"api_key":    "test-key",
		"max_tokens": 500,
		"streamable": true,
	}

	// 1. 创建提供者
	provider, err := NewEinoLLMProvider(config)
	require.NoError(t, err)
	assert.NotNil(t, provider)

	// 2. 测试配置链式调用
	enhancedProvider := provider.WithMaxTokens(1000).WithStreamable(false)
	assert.Equal(t, 1000, enhancedProvider.maxTokens)
	assert.Equal(t, false, enhancedProvider.streamable)

	// 3. 测试模型信息获取
	info := enhancedProvider.GetModelInfo()
	assert.Equal(t, "eino", info["framework"])
	assert.Equal(t, "eino", info["type"])
	assert.Equal(t, "openai", info["provider_type"])

	// 4. 测试底层ChatModel访问
	chatModel := enhancedProvider.GetChatModel()
	assert.NotNil(t, chatModel)

	// 5. 测试提供者类型
	providerType := enhancedProvider.GetProviderType()
	assert.Equal(t, "openai", providerType)

	// 6. 测试结构验证（不调用真实API）
	messages := []*schema.Message{
		{
			Role:    schema.User,
			Content: "测试消息",
		},
	}

	// 仅验证函数调用不会panic，不验证响应内容
	responseChan := provider.Response("full_workflow_test", messages)
	go func() {
		for range responseChan {
			// 消费响应但不验证内容
		}
	}()
}

// TestMultipleProviderTypes 测试多种提供者类型
func TestMultipleProviderTypes(t *testing.T) {
	testCases := []struct {
		name         string
		providerType string
		modelName    string
	}{
		{
			name:         "OpenAI Provider",
			providerType: "openai",
			modelName:    "gpt-3.5-turbo",
		},
		{
			name:         "Ollama Provider",
			providerType: "ollama",
			modelName:    "llama2",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := map[string]interface{}{
				"type":       tc.providerType,
				"model_name": tc.modelName,
				"api_key":    "test-key",
			}

			if tc.providerType == "ollama" {
				config["base_url"] = "http://localhost:11434"
			}

			provider, err := NewEinoLLMProvider(config)
			require.NoError(t, err)
			assert.NotNil(t, provider)
			assert.Equal(t, tc.providerType, provider.GetProviderType())
			assert.Equal(t, tc.modelName, provider.modelName)

			// 测试基本结构
			messages := []*schema.Message{
				{
					Role:    schema.User,
					Content: fmt.Sprintf("测试%s提供者", tc.providerType),
				},
			}

			// 仅验证函数调用不会panic
			responseChan := provider.Response("multi_provider_test", messages)
			go func() {
				for range responseChan {
					// 消费响应
				}
			}()
		})
	}
}
