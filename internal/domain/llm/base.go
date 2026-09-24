package llm

import (
	"context"
	"fmt"
	"strings"

	"github.com/cloudwego/eino/schema"

	"xiaozhi-esp32-server-golang/constants"
	"xiaozhi-esp32-server-golang/internal/domain/llm/coze_llm"
	"xiaozhi-esp32-server-golang/internal/domain/llm/dify_llm"
	"xiaozhi-esp32-server-golang/internal/domain/llm/eino_llm"
)

// LLMExtraErrorKey 错误透传约定：ResponseWithContext 失败时在 Message.Extra 中使用的 key
const LLMExtraErrorKey = "error"

// IsLLMErrorMessage 判断是否为 LLM 透传的错误消息（Extra 中含 error）
func IsLLMErrorMessage(msg *schema.Message) bool {
	if msg == nil || msg.Extra == nil {
		return false
	}
	v, ok := msg.Extra[LLMExtraErrorKey]
	if !ok || v == nil {
		return false
	}
	_, ok = v.(string)
	return ok
}

// LLMErrorMessage 从 Message.Extra 中解析出错误文案（若为错误消息）
func LLMErrorMessage(msg *schema.Message) string {
	if msg == nil || msg.Extra == nil {
		return ""
	}
	v, ok := msg.Extra[LLMExtraErrorKey].(string)
	if !ok {
		return ""
	}
	return v
}

// LLMProvider 大语言模型提供者接口
// 所有LLM实现必须遵循此接口，使用Eino原生类型
type LLMProvider interface {
	// ResponseWithContext 带有上下文控制的响应，支持取消操作
	// ctx: 上下文，可用于取消长时间运行的请求
	// sessionID: 会话标识符
	// dialogue: 对话历史，使用Eino原生消息类型
	ResponseWithContext(ctx context.Context, sessionID string, dialogue []*schema.Message, functions []*schema.ToolInfo) chan *schema.Message

	ResponseWithVllm(ctx context.Context, file []byte, text string, mimeType string) (string, error)

	// GetModelInfo 获取模型信息
	// 返回模型名称和其他元数据
	GetModelInfo() map[string]interface{}
	// Close 关闭资源，释放连接等
	Close() error
	// IsValid 检查资源是否有效
	IsValid() bool
}

// LLMFactory 大语言模型工厂接口
// 用于创建不同类型的LLM提供者
type LLMFactory interface {
	// CreateProvider 根据配置创建LLM提供者
	CreateProvider(config map[string]interface{}) (LLMProvider, error)
}

// GetLLMProvider 创建LLM提供者
// 统一使用EinoLLMProvider处理所有类型
func GetLLMProvider(providerName string, config map[string]interface{}) (LLMProvider, error) {
	cfg := cloneConfigMap(config)
	if providerName != "" {
		if _, ok := cfg["provider"]; !ok {
			cfg["provider"] = providerName
		}
	}

	llmType := resolveLLMType(providerName, cfg)
	cfg["type"] = llmType
	providerKey := resolveLLMProviderName(providerName, cfg, llmType)
	if defaultBaseURL := resolveDefaultBaseURL(providerKey); defaultBaseURL != "" {
		cfg["base_url"] = defaultBaseURL
	} else if baseURL, _ := cfg["base_url"].(string); strings.TrimSpace(baseURL) == "" {
		delete(cfg, "base_url")
	}

	switch llmType {
	case constants.LlmTypeOpenai, constants.LlmTypeOllama, constants.LlmTypeEinoLLM, constants.LlmTypeEino:
		// 统一使用 EinoLLMProvider 处理所有类型
		provider, err := eino_llm.NewEinoLLMProvider(cfg)
		if err != nil {
			return nil, fmt.Errorf("创建Eino LLM提供者失败: %v", err)
		}
		return provider, nil
	case constants.LlmTypeDify:
		provider, err := dify_llm.NewDifyLLMProvider(cfg)
		if err != nil {
			return nil, fmt.Errorf("创建Dify LLM提供者失败: %v", err)
		}
		return provider, nil
	case constants.LlmTypeCoze:
		provider, err := coze_llm.NewCozeLLMProvider(cfg)
		if err != nil {
			return nil, fmt.Errorf("创建Coze LLM提供者失败: %v", err)
		}
		return provider, nil
	}
	return nil, fmt.Errorf("不支持的LLM提供者: %s", llmType)
}

func resolveLLMProviderName(providerName string, config map[string]interface{}, llmType string) string {
	provider := strings.ToLower(strings.TrimSpace(providerName))
	if provider == "" {
		if rawProvider, ok := config["provider"].(string); ok {
			provider = strings.ToLower(strings.TrimSpace(rawProvider))
		}
	}
	if provider == "openai" {
		switch llmType {
		case constants.LlmTypeOllama:
			return "ollama"
		case constants.LlmTypeDify:
			return "dify"
		case constants.LlmTypeCoze:
			return "coze"
		}
	}
	return provider
}

func resolveDefaultBaseURL(provider string) string {
	switch provider {
	case "anthropic":
		return "https://api.anthropic.com/v1/"
	case "zhipu":
		return "https://open.bigmodel.cn/api/paas/v4"
	case "aliyun":
		return "https://dashscope.aliyuncs.com/compatible-mode/v1"
	case "doubao":
		return "https://ark.cn-beijing.volces.com/api/v3"
	case "siliconflow":
		return "https://api.siliconflow.cn/v1"
	case "deepseek":
		return "https://api.deepseek.com/v1"
	default:
		return ""
	}
}

func resolveLLMType(providerName string, config map[string]interface{}) string {
	provider := strings.ToLower(strings.TrimSpace(providerName))
	if provider == "" {
		if rawProvider, ok := config["provider"].(string); ok {
			provider = strings.ToLower(strings.TrimSpace(rawProvider))
		}
	}

	llmType, _ := config["type"].(string)
	llmType = strings.ToLower(strings.TrimSpace(llmType))

	if provider == "openai" {
		switch llmType {
		case constants.LlmTypeOllama:
			return constants.LlmTypeOllama
		case constants.LlmTypeDify:
			return constants.LlmTypeDify
		case constants.LlmTypeCoze:
			return constants.LlmTypeCoze
		}
	}

	switch provider {
	case "ollama":
		return constants.LlmTypeOllama
	case "dify":
		return constants.LlmTypeDify
	case "coze":
		return constants.LlmTypeCoze
	case "openai", "azure", "anthropic", "zhipu", "aliyun", "doubao", "siliconflow", "deepseek":
		return constants.LlmTypeOpenai
	}

	switch llmType {
	case constants.LlmTypeOllama:
		return constants.LlmTypeOllama
	case constants.LlmTypeDify:
		return constants.LlmTypeDify
	case constants.LlmTypeCoze:
		return constants.LlmTypeCoze
	case constants.LlmTypeOpenai, constants.LlmTypeEinoLLM, constants.LlmTypeEino:
		return constants.LlmTypeOpenai
	default:
		return constants.LlmTypeOpenai
	}
}

// Config LLM配置结构
type Config struct {
	ModelName  string                 `json:"model_name"`
	APIKey     string                 `json:"api_key"`
	BaseURL    string                 `json:"base_url"`
	MaxTokens  int                    `json:"max_tokens"`
	Parameters map[string]interface{} `json:"parameters,omitempty"`
}

func cloneConfigMap(src map[string]interface{}) map[string]interface{} {
	if len(src) == 0 {
		return make(map[string]interface{})
	}

	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
