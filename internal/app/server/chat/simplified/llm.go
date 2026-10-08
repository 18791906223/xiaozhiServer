package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// ============================================================================
// LLM 的最小接口
// ============================================================================

// llmProvider 是大模型的最小接口。
//
// 对应真实实现 internal/domain/llm/base.go 的 LLMProvider：
//
//	type LLMProvider interface {
//	    ResponseWithContext(ctx context.Context, sessionID string,
//	        dialogue []*schema.Message, functions []*schema.ToolInfo) chan *schema.Message
//	    ResponseWithVllm(ctx context.Context, file []byte, text string, mimeType string) (string, error)
//	    GetModelInfo() map[string]interface{}
//	    Close() error
//	    IsValid() bool
//	}
//
// 真实实现还要处理工具调用（function calling）、多模态输入、流式转换插件链、
// 历史消息的工具消息对齐……简化版只保留最核心的一件事：
// 给一串消息，流式吐出回复文本。
type llmProvider interface {
	// Chat 发起一次对话，返回逐段吐出的文本。
	// 返回的 channel 在本轮结束时关闭；ctx 取消时也应尽快关闭。
	Chat(ctx context.Context, messages []message) (<-chan string, error)
	// Name 用于日志。
	Name() string
}

// message 是一条对话消息，字段名和 OpenAI 接口对齐。
// 真实实现用的是 eino 的 *schema.Message，字段多得多（工具调用、多模态等）。
type message struct {
	Role    string `json:"role"` // system / user / assistant
	Content string `json:"content"`
}

// systemPrompt 是系统提示词。
// 真实实现会在这里拼上：角色设定、全局提示词、当前日期时间、长期记忆、
// 声纹说话人、知识库路由策略……简化版只留一句。
const systemPrompt = "你是一个语音助手。回答要简短、口语化，一次不要超过三句话。"

// maxHistoryMessages 是带给 LLM 的上下文条数上限。
// 真实实现里这个值是 10，另外还会做超长裁剪和记忆注入。
const maxHistoryMessages = 10

// ============================================================================
// 实现一：内置模拟大模型
// ============================================================================

// mockLLM 不说人话，但它会「流式」吐出带标点的句子，
// 正好用来验证流式解析、分句、逐句合成这一整段逻辑。
type mockLLM struct{}

func (m *mockLLM) Name() string { return "mock" }

func (m *mockLLM) Chat(ctx context.Context, messages []message) (<-chan string, error) {
	userText := ""
	if len(messages) > 0 {
		userText = messages[len(messages)-1].Content
	}
	reply := fmt.Sprintf("你说的是「%s」。这是模拟回复，用来演示流式输出和分句。今天天气不错，要不要出去走走？", userText)

	out := make(chan string)
	go func() {
		defer close(out)
		// 一个字一个字地吐，模拟真实 LLM 的流式输出节奏
		for _, r := range reply {
			select {
			case <-ctx.Done():
				return
			case out <- string(r):
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	return out, nil
}

// ============================================================================
// 实现二：OpenAI 兼容的流式对话接口
// ============================================================================

// openAILLM 调用的接口长这样：
//
//	POST {baseURL}/chat/completions
//	{"model":..., "messages":[...], "stream":true}
//
// 响应是 SSE（Server-Sent Events）流，每行一个 data: 片段：
//
//	data: {"choices":[{"delta":{"content":"你"}}]}
//	data: {"choices":[{"delta":{"content":"好"}}]}
//	data: [DONE]
//
// 几乎所有厂商都实现这个协议，是当前事实标准。
type openAILLM struct {
	baseURL string
	apiKey  string
	model   string
	client  *http.Client
}

func newOpenAILLM(baseURL, apiKey, model string) *openAILLM {
	return &openAILLM{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		model:   model,
		// 流式请求可能很长，不能设太短的超时；连接超时靠 ctx 控制。
		client: &http.Client{Timeout: 0},
	}
}

func (l *openAILLM) Name() string { return "openai-compatible:" + l.model }

func (l *openAILLM) Chat(ctx context.Context, messages []message) (<-chan string, error) {
	payload, err := json.Marshal(map[string]any{
		"model":    l.model,
		"messages": messages,
		"stream":   true,
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		l.baseURL+"/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+l.apiKey)

	resp, err := l.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求大模型失败: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, fmt.Errorf("大模型返回 %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	out := make(chan string, 16)
	go func() {
		defer close(out)
		defer resp.Body.Close()

		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 1<<20) // 单行可能很长

		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			// SSE 里除了 data: 还有 event:/id:/空行，只关心 data
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if data == "[DONE]" {
				return
			}

			var chunk struct {
				Choices []struct {
					Delta struct {
						Content string `json:"content"`
					} `json:"delta"`
				} `json:"choices"`
			}
			if err := json.Unmarshal([]byte(data), &chunk); err != nil {
				log.Printf("解析流式片段失败: %v，原文: %s", err, data)
				continue
			}
			if len(chunk.Choices) == 0 || chunk.Choices[0].Delta.Content == "" {
				continue
			}
			select {
			case out <- chunk.Choices[0].Delta.Content:
			case <-ctx.Done():
				return
			}
		}
		if err := scanner.Err(); err != nil && ctx.Err() == nil {
			log.Printf("读取大模型流失败: %v", err)
		}
	}()
	return out, nil
}

// ============================================================================
// 组装
// ============================================================================

// newLLM 按配置选择 LLM 实现。
// 对应真实实现 internal/domain/llm/base.go 的 GetLLMProvider。
func (c *config) newLLM() llmProvider {
	if c.llmURL == "" || c.llmKey == "" {
		log.Print("LLM：未配置 -llm-url / -llm-key，使用内置模拟大模型")
		return &mockLLM{}
	}
	model := c.llmModel
	if model == "" {
		model = "gpt-4o-mini"
	}
	log.Printf("LLM：使用 OpenAI 兼容接口 %s（模型 %s）", c.llmURL, model)
	return newOpenAILLM(c.llmURL, c.llmKey, model)
}
