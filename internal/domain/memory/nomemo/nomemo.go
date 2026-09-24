package nomemo

import (
	"context"

	"github.com/cloudwego/eino/schema"
)

// NoMemoProvider 空的记忆提供者实现
// 用于当用户不需要记忆功能时使用，所有方法都是空实现
type NoMemoProvider struct{}

// Get 获取 NoMemoProvider 实例
func Get() *NoMemoProvider {
	return &NoMemoProvider{}
}

// AddMessage 添加一条消息到记忆（空实现）
func (n *NoMemoProvider) AddMessage(ctx context.Context, agentID string, msg schema.Message) error {
	// 空实现，不执行任何操作
	return nil
}

// GetMessages 获取用户的历史消息（空实现）
func (n *NoMemoProvider) GetMessages(ctx context.Context, agentId string, count int) ([]*schema.Message, error) {
	// 返回空的消息列表
	return []*schema.Message{}, nil
}

// GetContext 获取用户的上下文信息（空实现）
func (n *NoMemoProvider) GetContext(ctx context.Context, agentId string, maxToken int) (string, error) {
	// 返回空字符串
	return "", nil
}

// Search 搜索用户的记忆（空实现）
func (n *NoMemoProvider) Search(ctx context.Context, agentId string, query string, topK int, timeRangeDays int64) (string, error) {
	// 返回空字符串
	return "", nil
}

// Flush 刷新用户的记忆（空实现）
func (n *NoMemoProvider) Flush(ctx context.Context, agentId string) error {
	// 空实现，不执行任何操作
	return nil
}

// ResetMemory 重置用户的记忆（空实现）
func (n *NoMemoProvider) ResetMemory(ctx context.Context, agentId string) error {
	// 空实现，不执行任何操作
	return nil
}
