package user_config

import (
	"context"
	"testing"
)

func TestMemoryProvider(t *testing.T) {
	ctx := context.Background()

	// 创建内存provider
	config := map[string]interface{}{
		"max_entries": 10,
	}

	provider, err := GetUserConfigProvider("memory", config)
	if err != nil {
		t.Fatalf("创建内存provider失败: %v", err)
	}
	// 注意：接口中没有Close方法，所以不需要调用

	userID := "test_user_123"

	// 由于接口中没有SetUserConfig方法，我们只测试GetUserConfig方法
	// 测试获取不存在用户的配置（应该返回空配置）
	retrievedConfig, err := provider.GetUserConfig(ctx, userID)
	if err != nil {
		t.Fatalf("获取用户配置失败: %v", err)
	}

	// 验证返回的是空配置
	if retrievedConfig.Llm.Provider != "" {
		t.Errorf("期望空配置，但得到了 LLM Provider: %s", retrievedConfig.Llm.Provider)
	}

	// 测试系统配置获取
	systemConfig, err := provider.GetSystemConfig(ctx)
	if err != nil {
		t.Fatalf("获取系统配置失败: %v", err)
	}
	_ = systemConfig // 系统配置可能为空，这是正常的
}

func TestProviderAdapter(t *testing.T) {
	ctx := context.Background()

	// 创建内存provider
	provider, err := GetUserConfigProvider("memory", map[string]interface{}{
		"max_entries": 5,
	})
	if err != nil {
		t.Fatalf("创建内存provider失败: %v", err)
	}
	// 注意：接口中没有Close方法，所以不需要调用

	// 测试适配器获取配置
	userID := "adapter_test_user"

	// 使用适配器获取配置（可能为空配置）
	adapter := NewUserConfigAdapter(provider)
	retrievedConfig, err := adapter.GetUserConfig(ctx, userID)
	if err != nil {
		t.Fatalf("通过适配器获取配置失败: %v", err)
	}

	// 验证适配器正常工作（获取到配置结构）
	if retrievedConfig.SystemPrompt == "" {
		t.Logf("适配器获取到空的系统提示，这是正常的")
	} else {
		t.Logf("适配器获取到系统提示: %s", retrievedConfig.SystemPrompt)
	}
}

func TestDefaultConfig(t *testing.T) {
	// 测试Redis默认配置
	redisConfig := DefaultConfig("redis")
	if redisConfig["host"] != "localhost" {
		t.Errorf("Redis默认host配置错误，期望: localhost, 实际: %v", redisConfig["host"])
	}

	// 测试Memory默认配置
	memoryConfig := DefaultConfig("memory")
	if memoryConfig["max_entries"] != 1000 {
		t.Errorf("Memory默认max_entries配置错误，期望: 1000, 实际: %v", memoryConfig["max_entries"])
	}

	// 测试不支持的类型
	unknownConfig := DefaultConfig("unknown")
	if len(unknownConfig) != 0 {
		t.Errorf("未知类型应返回空配置，实际: %v", unknownConfig)
	}
}

func TestValidateConfig(t *testing.T) {
	// 测试有效的Redis配置
	validRedisConfig := map[string]interface{}{
		"host": "localhost",
		"port": 6379,
	}
	err := ValidateConfig("redis", validRedisConfig)
	if err != nil {
		t.Errorf("有效Redis配置验证失败: %v", err)
	}

	// 测试无效的Redis配置（缺少host）
	invalidRedisConfig := map[string]interface{}{
		"port": 6379,
	}
	err = ValidateConfig("redis", invalidRedisConfig)
	if err == nil {
		t.Error("缺少host的Redis配置应该验证失败")
	}

	// 测试Memory配置（无需验证）
	err = ValidateConfig("memory", map[string]interface{}{})
	if err != nil {
		t.Errorf("Memory配置验证失败: %v", err)
	}
}
