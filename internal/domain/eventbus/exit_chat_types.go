package eventbus

import (
	"time"

	. "xiaozhi-esp32-server-golang/internal/data/client"
)

// ExitChatEvent 退出聊天事件
type ExitChatEvent struct {
	// 客户端状态
	ClientState *ClientState

	// 退出原因
	Reason string // "用户主动退出"、"工具调用退出"、"超时退出" 等

	// 退出触发方式
	TriggerType string // "exit_words"（退出词检测）、"tool_call"（工具调用）、"timeout"（超时）等

	// 用户输入的原始文本（如果有）
	UserText string

	// 时间戳
	Timestamp time.Time
}
