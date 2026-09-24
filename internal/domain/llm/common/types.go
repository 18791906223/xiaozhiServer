package common

import (
	"github.com/cloudwego/eino/schema"
)

// 请求与响应结构体
// Message 表示对话消息

// 响应类型常量
const (
	ResponseTypeContent   = "content"
	ResponseTypeToolCalls = "tool_calls"
)

type LLMResponseStruct struct {
	Text      string            `json:"text,omitempty"`
	IsStart   bool              `json:"is_start"`
	IsEnd     bool              `json:"is_end"`
	ToolCalls []schema.ToolCall `json:"tool_calls,omitempty"`
}
