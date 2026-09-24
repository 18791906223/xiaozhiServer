package memory

import (
	"context"
	"fmt"
	"sync"

	"xiaozhi-esp32-server-golang/internal/domain/config/types"
	log "xiaozhi-esp32-server-golang/logger"
)

// MemoryUserConfigProvider 内存用户配置提供者
// 实现UserConfigProvider接口，将配置存储在内存中
// 注意：重启后数据会丢失，适用于测试或临时存储场景
type MemoryUserConfigProvider struct {
	mu         sync.RWMutex
	configs    map[string]types.UConfig
	maxEntries int
}

// MemoryConfig 内存配置结构
type MemoryConfig struct {
	MaxEntries int `json:"max_entries"` // 最大存储条目数
}

// NewMemoryUserConfigProvider 创建内存用户配置提供者
// config: 配置参数map，包含max_entries等
func NewMemoryUserConfigProvider(config map[string]interface{}) (*MemoryUserConfigProvider, error) {
	// 解析配置参数
	memoryConfig := &MemoryConfig{
		MaxEntries: 1000, // 默认最大1000个配置
	}

	if maxEntries, ok := config["max_entries"].(int); ok && maxEntries > 0 {
		memoryConfig.MaxEntries = maxEntries
	} else if maxEntriesFloat, ok := config["max_entries"].(float64); ok && maxEntriesFloat > 0 {
		memoryConfig.MaxEntries = int(maxEntriesFloat)
	}

	provider := &MemoryUserConfigProvider{
		configs:    make(map[string]types.UConfig),
		maxEntries: memoryConfig.MaxEntries,
	}

	log.Log().Infof("内存用户配置提供者初始化成功，最大条目数: %d", memoryConfig.MaxEntries)
	return provider, nil
}

// GetUserConfig 获取用户配置
func (m *MemoryUserConfigProvider) GetUserConfig(ctx context.Context, userID string) (types.UConfig, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	config, exists := m.configs[userID]
	if !exists {
		log.Log().Debugf("用户 %s 配置不存在，返回空配置", userID)
		return types.UConfig{}, nil
	}

	return config, nil
}

// SetUserConfig 设置用户配置
func (m *MemoryUserConfigProvider) SetUserConfig(ctx context.Context, userID string, config types.UConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查是否超过最大条目数
	if len(m.configs) >= m.maxEntries && !m.configExists(userID) {
		return fmt.Errorf("已达到最大存储条目数 %d，无法添加新配置", m.maxEntries)
	}

	m.configs[userID] = config
	log.Log().Infof("用户 %s 配置设置成功 (内存存储)", userID)
	return nil
}

// DeleteUserConfig 删除用户配置
func (m *MemoryUserConfigProvider) DeleteUserConfig(ctx context.Context, userID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.configs[userID]; !exists {
		log.Log().Warnf("用户 %s 配置不存在，无需删除", userID)
		return nil
	}

	delete(m.configs, userID)
	log.Log().Infof("用户 %s 配置删除成功 (内存存储)", userID)
	return nil
}

// Close 关闭提供者（内存提供者无需特殊清理）
func (m *MemoryUserConfigProvider) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 清空所有配置
	m.configs = make(map[string]types.UConfig)
	log.Log().Info("内存用户配置提供者已关闭，所有配置已清空")
	return nil
}

// configExists 检查配置是否存在（内部方法，调用时需要持有锁）
func (m *MemoryUserConfigProvider) configExists(userID string) bool {
	_, exists := m.configs[userID]
	return exists
}

// GetStats 获取存储统计信息（额外的实用方法）
func (m *MemoryUserConfigProvider) GetStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return map[string]interface{}{
		"total_configs": len(m.configs),
		"max_entries":   m.maxEntries,
		"usage_percent": float64(len(m.configs)) / float64(m.maxEntries) * 100,
	}
}

// ListUserIDs 列出所有用户ID（额外的实用方法）
func (m *MemoryUserConfigProvider) ListUserIDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	userIDs := make([]string, 0, len(m.configs))
	for userID := range m.configs {
		userIDs = append(userIDs, userID)
	}
	return userIDs
}

// GetSystemConfig 获取系统配置
func (m *MemoryUserConfigProvider) GetSystemConfig(ctx context.Context) (string, error) {
	// 内存配置提供者不提供系统配置
	return "", nil
}

// Init 初始化Memory配置提供者
func Init(ctx context.Context) error {
	log.Log().Info("Memory config provider initialized successfully")
	return nil
}

// Close 关闭Memory配置提供者，清理资源
func Close() error {
	log.Log().Info("Memory config provider closed")
	return nil
}

// IsConnected 检查Memory配置提供者是否已连接
func IsConnected() bool {
	// 内存配置提供者始终是"连接"状态
	return true
}
