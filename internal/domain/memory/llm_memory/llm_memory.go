package llm_memory

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	i_redis "xiaozhi-esp32-server-golang/internal/db/redis"
	log "xiaozhi-esp32-server-golang/logger"

	"github.com/cloudwego/eino/schema"
	"github.com/spf13/viper"

	"github.com/redis/go-redis/v9"
)

var (
	memoryInstance *Memory
	once           sync.Once
	configOnce     sync.Once
)

// Memory 表示对话记忆体
type Memory struct {
	redisClient *redis.Client
	keyPrefix   string
	sync.RWMutex
}

// Get 获取记忆体实例
func Get() *Memory {
	if memoryInstance == nil {
		once.Do(func() {
			redisInstance := i_redis.GetClient()

			memoryInstance = &Memory{
				redisClient: redisInstance,
				keyPrefix:   viper.GetString("redis.key_prefix"),
			}
		})
	}
	return memoryInstance
}

// GetWithConfig 使用配置获取记忆体实例（单例模式）
func GetWithConfig(config map[string]interface{}) (*Memory, error) {
	var initErr error
	configOnce.Do(func() {
		// 从配置中读取 redis 相关配置
		redisConfig, ok := config["redis"]
		if !ok {
			initErr = fmt.Errorf("redis 配置不存在")
			return
		}

		redisConfigMap, ok := redisConfig.(map[string]interface{})
		if !ok {
			initErr = fmt.Errorf("redis 配置格式错误")
			return
		}

		// 读取 key_prefix 配置
		var keyPrefix string
		if keyPrefixInterface, exists := redisConfigMap["key_prefix"]; exists {
			if kp, ok := keyPrefixInterface.(string); ok {
				keyPrefix = kp
			} else {
				initErr = fmt.Errorf("redis.key_prefix 必须是字符串")
				return
			}
		} else {
			keyPrefix = "xiaozhi:" // 默认值
		}

		// 获取 Redis 客户端（这里仍然使用现有的 Redis 客户端获取方式）
		// 因为 Redis 客户端的初始化比较复杂，暂时保持现有方式
		redisClient := i_redis.GetClient()
		if redisClient == nil {
			initErr = fmt.Errorf("无法获取 Redis 客户端")
			return
		}

		// 创建 LLM 记忆实例
		memoryInstance = &Memory{
			redisClient: redisClient,
			keyPrefix:   keyPrefix,
		}

		log.Log().Infof("LLM 记忆初始化成功, key_prefix: %s", keyPrefix)
	})

	if initErr != nil {
		return nil, initErr
	}
	return memoryInstance, nil
}

// NewWithConfig 使用配置创建新的LLM记忆实例
func NewWithConfig(config map[string]interface{}) (*Memory, error) {
	// 从配置中读取 redis 相关配置
	redisConfig, ok := config["redis"]
	if !ok {
		return nil, fmt.Errorf("redis 配置不存在")
	}

	redisConfigMap, ok := redisConfig.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("redis 配置格式错误")
	}

	// 读取 key_prefix 配置
	var keyPrefix string
	if keyPrefixInterface, exists := redisConfigMap["key_prefix"]; exists {
		if kp, ok := keyPrefixInterface.(string); ok {
			keyPrefix = kp
		} else {
			return nil, fmt.Errorf("redis.key_prefix 必须是字符串")
		}
	} else {
		keyPrefix = "xiaozhi:" // 默认值
	}

	// 获取 Redis 客户端（这里仍然使用现有的 Redis 客户端获取方式）
	// 因为 Redis 客户端的初始化比较复杂，暂时保持现有方式
	redisClient := i_redis.GetClient()
	if redisClient == nil {
		return nil, fmt.Errorf("无法获取 Redis 客户端")
	}

	// 创建 LLM 记忆实例
	llmMemory := &Memory{
		redisClient: redisClient,
		keyPrefix:   keyPrefix,
	}

	log.Log().Infof("LLM 记忆初始化成功, key_prefix: %s", keyPrefix)
	return llmMemory, nil
}

// NewMemory 创建新的记忆体实例（仅用于测试）
func NewMemory(redisClient *redis.Client) *Memory {
	return &Memory{
		redisClient: redisClient,
	}
}

// getMemoryKey 生成设备对应的 Redis key
func (m *Memory) getMemoryKey(deviceID string) string {
	return fmt.Sprintf("%s:llm:%s", m.keyPrefix, deviceID)
}

// getSystemPromptKey 生成设备对应的系统 prompt 的 Redis key
func (m *Memory) getSystemPromptKey(deviceID string) string {
	return fmt.Sprintf("%s:llm:system:%s", m.keyPrefix, deviceID)
}

// AddMessage 添加一条新的对话消息到记忆体
func (m *Memory) AddMessage(ctx context.Context, deviceID string, agentID string, msg schema.Message) error {
	if m.redisClient == nil {
		log.Log().Warn("redis client is nil")
		return nil
	}

	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message failed: %w", err)
	}

	key := m.getMemoryKey(deviceID)
	// 使用纳秒时间戳作为分数
	// ZREVRANGE 会返回分数从大到小的结果
	score := float64(time.Now().UnixNano())

	log.Debugf("添加消息到记忆体: %s, %s", key, string(msgBytes))

	return m.redisClient.ZAdd(ctx, key, redis.Z{
		Score:  score,
		Member: string(msgBytes),
	}).Err()
}

// GetMessages 获取设备的所有对话记忆
func (m *Memory) GetMessages(ctx context.Context, deviceID string, agentID string, count int) ([]*schema.Message, error) {
	if m.redisClient == nil {
		log.Log().Warn("redis client is nil")
		return []*schema.Message{}, nil
	}

	key := m.getMemoryKey(deviceID)

	if count == 0 {
		count = 10
	}

	// 使用 ZREVRANGE 获取最新的 N 条消息
	// 分数（时间戳）大的在前，所以需要反转顺序以保证旧消息在前
	startIndex := int64(-(count))
	results, err := m.redisClient.ZRange(ctx, key, startIndex, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("get messages failed: %w", err)
	}

	// 预分配切片
	messages := make([]*schema.Message, 0)

	for i := 0; i < len(results); i++ {
		msg := schema.Message{}
		if err := json.Unmarshal([]byte(results[i]), &msg); err != nil {
			return nil, fmt.Errorf("unmarshal message failed: %w", err)
		}

		messages = append(messages, &msg)
	}

	return messages, nil
}

// GetMessagesForLLM 获取适用于 LLM 的消息格式
func (m *Memory) GetMessagesForLLM(ctx context.Context, deviceID string, count int) ([]*schema.Message, error) {
	if m.redisClient == nil {
		log.Log().Warn("redis client is nil")
		return []*schema.Message{}, nil
	}

	// 获取历史消息（已经是按时间顺序：旧->新）
	memoryMessages, err := m.GetMessages(ctx, deviceID, "", count)
	if err != nil {
		return nil, err
	}

	return memoryMessages, nil
}

// SetSystemPrompt 设置或更新设备的系统 prompt
func (m *Memory) SetSystemPrompt(ctx context.Context, deviceID string, prompt string) error {
	if m.redisClient == nil {
		log.Log().Warn("redis client is nil")
		return nil
	}

	key := m.getSystemPromptKey(deviceID)
	return m.redisClient.Set(ctx, key, prompt, 0).Err()
}

// GetSystemPrompt 获取设备的系统 prompt
func (m *Memory) GetSystemPrompt(ctx context.Context, deviceID string) (schema.Message, error) {
	if m.redisClient == nil {
		log.Log().Warn("redis client is nil")
		return schema.Message{Role: schema.System, Content: viper.GetString("system_prompt")}, nil
	}

	key := m.getSystemPromptKey(deviceID)

	result, err := m.redisClient.Get(ctx, key).Result()
	if err == redis.Nil {
		return schema.Message{}, nil // 返回空消息结构
	}
	if err != nil {
		return schema.Message{}, fmt.Errorf("get system prompt failed: %w", err)
	}

	return schema.Message{
		Role:    schema.System,
		Content: result,
	}, nil
}

// ResetMemory 重置设备的对话记忆（包括系统 prompt）
func (m *Memory) ResetMemory(ctx context.Context, deviceID string) error {
	if m.redisClient == nil {
		log.Log().Warn("redis client is nil")
		return nil
	}

	// 删除对话历史
	historyKey := m.getMemoryKey(deviceID)
	if err := m.redisClient.Del(ctx, historyKey).Err(); err != nil {
		return fmt.Errorf("delete history failed: %w", err)
	}

	return nil
}

// GetLastNMessages 获取最近的 N 条消息
func (m *Memory) GetLastNMessages(ctx context.Context, deviceID string, n int64) ([]schema.Message, error) {
	if m.redisClient == nil {
		log.Log().Warn("redis client is nil")
		return []schema.Message{}, nil
	}

	key := m.getMemoryKey(deviceID)

	// 获取最后 N 条消息
	results, err := m.redisClient.ZRevRange(ctx, key, 0, n-1).Result()
	if err != nil {
		return nil, fmt.Errorf("get last messages failed: %w", err)
	}

	messages := make([]schema.Message, 0, len(results))
	for i := len(results) - 1; i >= 0; i-- { // 反转顺序以保持时间顺序
		var msg schema.Message
		if err := json.Unmarshal([]byte(results[i]), &msg); err != nil {
			return nil, fmt.Errorf("unmarshal message failed: %w", err)
		}
		messages = append(messages, msg)
	}

	return messages, nil
}

// RemoveOldMessages 删除指定时间之前的消息
func (m *Memory) RemoveOldMessages(ctx context.Context, deviceID string, before time.Time) error {
	if m.redisClient == nil {
		log.Log().Warn("redis client is nil")
		return nil
	}

	key := m.getMemoryKey(deviceID)
	score := float64(before.UnixNano())

	return m.redisClient.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprintf("%f", score)).Err()
}

// Summary 获取对话的摘要
func (m *Memory) GetSummary(ctx context.Context, deviceID string) (string, error) {
	return "", nil
}

// SetSummary 设置对话的摘要
func (m *Memory) SetSummary(ctx context.Context, deviceID string, summary string) error {
	return nil
}

// 进行总结
func (m *Memory) Summary(ctx context.Context, deviceID string, msgList []schema.Message) (string, error) {
	return "", nil
}

func (m *Memory) GetContext(ctx context.Context, deviceID string, agentID string, maxToken int) (string, error) {
	return "", nil
}

func (m *Memory) Search(ctx context.Context, deviceID string, query string, topK int, timeRangeDays int64) (string, error) {
	return "", nil
}
