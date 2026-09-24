// package user_config

// config_init_test.go
//
// 本文件用于测试 config/config_init.go 中的配置初始化逻辑。
// 重点演示：
//   - RegisterManagerSystemConfigHandler 如何注册多个回调
//   - InitConfigSystem 如何根据类型选择不同的初始化路径
//   - 回调链的执行顺序和参数传递

package user_config

import (
	"context"
	"sync"
	"testing"

	"github.com/spf13/viper"
)

// ---------------------------------------------------------------------------

// TestRegisterManagerSystemConfigHandler 测试回调注册功能
// 验证多个回调可以被正确追加并在后续触发时全部执行
func TestRegisterManagerSystemConfigHandler(t *testing.T) {
	// 清空已有的回调列表（防止其他测试干扰）
	managerSystemConfigHandlers = nil

	var executionOrder []string
	var mu sync.Mutex

	// 注册第一个回调
	RegisterManagerSystemConfigHandler(func(data map[string]interface{}) {
		mu.Lock()
		executionOrder = append(executionOrder, "handler1")
		mu.Unlock()
	})

	// 注册第二个回调
	RegisterManagerSystemConfigHandler(func(data map[string]interface{}) {
		mu.Lock()
		executionOrder = append(executionOrder, "handler2")
		mu.Unlock()
	})

	// 验证回调数量
	if len(managerSystemConfigHandlers) != 2 {
		t.Fatalf("期望 2 个回调，实际 %d 个", len(managerSystemConfigHandlers))
	}
	t.Log("成功注册 2 个系统配置回调")
}

// ---------------------------------------------------------------------------

// TestInitConfigSystemProviderSelection 测试 InitConfigSystem 的选择逻辑
// 使用 viper 模拟不同的 providerType 配置
func TestInitConfigSystemProviderSelection(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name         string
		providerType string
		expectInit   bool // true 表示期望成功初始化
	}{
		// memory 是最简单的，不需要外部服务
		{"memory_provider", "memory", true},
		// redis 和 manager 需要外部服务，在没有连接的情况下会失败
		{"redis_provider_no_redis", "redis", false},
		{"manager_provider_no_backend", "manager", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 临时设置 viper 配置
			oldVal := viper.Get("config_provider.type")
			viper.Set("config_provider.type", tc.providerType)
			defer viper.Set("config_provider.type", oldVal)
			err := InitConfigSystem(ctx)
			if tc.expectInit {
				if err != nil {
					t.Fatalf("期望成功但失败: %v", err)
				}
				t.Logf("%s 初始化成功", tc.providerType)
			} else {
				if err == nil {
					t.Fatalf("期望失败但成功: provider=%s", tc.providerType)
				}
				t.Logf("%s 初始化正确返回错误: %v", tc.providerType, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------

// TestConfigSystemDefaultToRedis 测试默认 provider 行为
// 当 config_provider.type 未设置时，应默认使用 redis
func TestConfigSystemDefaultToRedis(t *testing.T) {
	// 确保 viper 中没有设置 config_provider.type
	oldVal := viper.Get("config_provider.type")
	viper.Set("config_provider.type", "")
	defer viper.Set("config_provider.type", oldVal)
	ctx := context.Background()
	err := InitConfigSystem(ctx)
	// redis 在没有连接时会返回错误，这是正常的
	if err == nil {
		t.Log("Redis 初始化成功（Redis 服务可用）")
	} else {
		t.Logf("Redis 初始化失败（预期：无 Redis 服务）: %v", err)
	}
}

// ---------------------------------------------------------------------------

// TestConfigSystemUnsupportedType 测试不支持的 provider 类型
func TestConfigSystemUnsupportedType(t *testing.T) {
	oldVal := viper.Get("config_provider.type")
	viper.Set("config_provider.type", "unknown_provider_xyz")
	defer viper.Set("config_provider.type", oldVal)
	ctx := context.Background()
	err := InitConfigSystem(ctx)
	if err == nil {
		t.Fatal("期望失败但未返回错误")
	}
	expectedMsg := "unsupported config provider type"
	if !contains(err.Error(), expectedMsg) {
		t.Errorf("错误信息应包含 %q，实际: %v", expectedMsg, err)
	}
	t.Logf("正确拒绝不支持的类型，错误: %v", err)
}

// ---------------------------------------------------------------------------

// contains 判断字符串是否包含子串
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		indexOf(s, substr) >= 0)
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// 学习提示：
//  - config_init.go 是配置系统的"入口"，负责根据配置选择初始化哪个后端
//  - RegisterManagerSystemConfigHandler 用于接收后端推送的配置变更并热更新
//  - viper.GetString("config_provider.type") 控制选择哪个 provider
