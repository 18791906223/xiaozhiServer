// package user_config

// interface_test.go
//
// 本文件用于测试 config/interface.go 中定义的 UserConfigProvider 接口。
// 接口是 Go 中隐式实现的，这里通过"适配层"方式来演示各后端 provider
// 如何实现该接口，并验证接口方法的调用行为。
//
// 学习要点：
//   - Go 接口无需显式声明实现，只要方法签名匹配即可
//   - 每种后端（memory/redis/manager）都实现了相同的接口，便于切换
//   - UserConfigAdapter 是一个适配器，将完整 Provider 接口转换为 minimal UserConfigProvider

package user_config

import (
	"context"
	"fmt"
	"testing"

	"xiaozhi-esp32-server-golang/internal/domain/config/memory"
	"xiaozhi-esp32-server-golang/internal/domain/config/types"
)

// ---------------------------------------------------------------------------

// TestInterfaceImplementation 验证 memory provider 满足 UserConfigProvider 接口
// 这是 Go 接口验证的惯用方式：将具体类型赋给接口变量
func TestInterfaceImplementation(t *testing.T) {
	ctx := context.Background()

	// 创建内存 provider
	provider, err := NewMemoryUserConfigProvider(ctx, map[string]interface{}{
		"max_entries": 100,
	})
	if err != nil {
		t.Fatalf("创建内存 provider 失败: %v", err)
	}

	// 验证 provider 实现了 UserConfigProvider 接口
	// Go 编译器会自动检查：如果 provider 不满足接口，这里会编译报错
	var _ UserConfigProvider = provider
	t.Log("memory provider 成功实现 UserConfigProvider 接口")

	// 调用接口方法
	config, err := provider.GetUserConfig(ctx, "test_user")
	if err != nil {
		t.Fatalf("GetUserConfig 失败: %v", err)
	}
	t.Logf("获取用户配置成功: SystemPrompt=%q, LLM provider=%q",
		config.SystemPrompt, config.Llm.Provider)
}

// ---------------------------------------------------------------------------

// TestInterfaceSwitching 演示如何根据配置选择不同的 provider
// 对应 base.go 中 GetUserConfigProvider 的工厂模式
func TestInterfaceSwitching(t *testing.T) {
	ctx := context.Background()

	// 模拟：根据配置类型选择 provider
	type testCase struct {
		providerType string
		config       map[string]interface{}
	}

	cases := []testCase{
		{"memory", map[string]interface{}{"max_entries": 50}},
		// Redis 和 Manager 需要外部服务，跳过
	}

	for _, tc := range cases {
		provider, err := GetUserConfigProvider(tc.providerType, tc.config)
		if err != nil {
			t.Fatalf("[%s] 创建 provider 失败: %v", tc.providerType, err)
		}

		// 调用接口方法，不关心具体实现
		_, err = provider.GetUserConfig(ctx, "user_1")
		if err != nil {
			t.Errorf("[%s] GetUserConfig 失败: %v", tc.providerType, err)
		}

		systemConfig, err := provider.GetSystemConfig(ctx)
		t.Logf("[%s] GetSystemConfig: %q err=%v", tc.providerType, systemConfig, err)
	}
}

// ---------------------------------------------------------------------------

// TestAdapterWrapper 测试适配器模式：将完整 MemoryProvider 包装为 UserConfigProvider
func TestAdapterWrapper(t *testing.T) {
	ctx := context.Background()

	// 创建完整的内存 provider（有 SetUserConfig 等方法）
	memProvider, err := memory.NewMemoryUserConfigProvider(map[string]interface{}{
		"max_entries": 10,
	})
	if err != nil {
		t.Fatalf("创建 memory provider 失败: %v", err)
	}

	// 通过适配器包装，转成 UserConfigProvider 接口
	adapter := NewUserConfigAdapter(memProvider)

	// 测试 GetUserConfig
	config, err := adapter.GetUserConfig(ctx, "adapter_user")
	if err != nil {
		t.Fatalf("Adapter.GetUserConfig 失败: %v", err)
	}
	if config.SystemPrompt != "" {
		t.Logf("获取到系统提示: %s", config.SystemPrompt)
	}

	// 测试 GetSystemConfig
	systemConfig, err := adapter.GetSystemConfig(ctx)
	if err != nil {
		t.Fatalf("Adapter.GetSystemConfig 失败: %v", err)
	}
	t.Logf("系统配置: %q (空表示由配置中心管理)", systemConfig)
}

// ---------------------------------------------------------------------------

// TestUserConfigProviderMethods 逐一测试 UserConfigProvider 接口的所有方法
func TestUserConfigProviderMethods(t *testing.T) {
	ctx := context.Background()

	provider, err := GetUserConfigProvider("memory", map[string]interface{}{"max_entries": 10})
	if err != nil {
		t.Fatalf("创建 provider 失败: %v", err)
	}

	testCases := []struct {
		name     string
		testFunc func() error
	}{
		{
			name: "GetUserConfig-空用户",
			testFunc: func() error {
				_, err := provider.GetUserConfig(ctx, "nonexistent_user")
				return err
			},
		},
		{
			name: "GetSystemConfig",
			testFunc: func() error {
				_, err := provider.GetSystemConfig(ctx)
				return err
			},
		},
		{
			name: "NotifyDeviceEvent",
			testFunc: func() error {
				provider.NotifyDeviceEvent(ctx, types.EventDeviceOnline, map[string]interface{}{
					"device_id": "test_device",
				})
				return nil
			},
		},
		{
			name: "RegisterMessageEventHandler",
			testFunc: func() error {
				provider.RegisterMessageEventHandler(ctx, "/api/device/inject_msg",
					func(ctx context.Context, eventType string, eventData map[string]interface{}) (string, error) {
						return "handled", nil
					})
				return nil
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.testFunc()
			if err != nil {
				t.Errorf("步骤失败: %v", err)
			}
		})
	}
}

// ---------------------------------------------------------------------------

// TestProviderFactoryDefaultBehavior 测试工厂函数的默认行为
func TestProviderFactoryDefaultBehavior(t *testing.T) {
	testCases := []struct {
		providerType string
		config       map[string]interface{}
		expectError  bool
	}{
		// memory 类型不需要特殊配置
		{"memory", nil, false},
		{"memory", map[string]interface{}{}, false},
		// redis 类型在没有连接时会失败（取决于实际 Redis 是否可用）
		{"redis", nil, true}, // 期望失败：无 Redis 连接
		// unknown 类型始终失败
		{"unknown", nil, true},
	}

	for _, tc := range testCases {
		t.Run(tc.providerType, func(t *testing.T) {
			provider, err := GetUserConfigProvider(tc.providerType, tc.config)
			if tc.expectError {
				if err == nil {
					t.Fatalf("期望失败但成功创建了 provider: %v", provider)
				}
				t.Logf("正确返回错误: %v", err)
				return
			}
			if err != nil {
				t.Fatalf("不应失败的类型返回错误: %v", err)
			}
			if provider == nil {
				t.Fatal("provider 不应为 nil")
			}
			t.Logf("成功创建 %s provider", tc.providerType)
		})
	}
}

// 学习提示：
//  - interface.go 定义了 UserConfigProvider，是整个配置系统的核心抽象
//  - 三种后端（memory/redis/manager）通过工厂函数 GetUserConfigProvider 创建
//  - UserConfigAdapter 将完整 Provider 包装成最小接口，方便使用
