// package memory

// memory_test.go
//
// 本文件用于测试 config/memory/memory.go 中 MemoryUserConfigProvider 的所有方法。
// 重点演示：
//   - 内存存储的基本 CRUD 操作
//   - 最大条目限制行为
//   - 并发安全（RWMutex 保护）
//   - GetStats 和 ListUserIDs 辅助方法
//   - Close 清理行为

package memory

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"xiaozhi-esp32-server-golang/internal/domain/config/types"
)

// ---------------------------------------------------------------------------

// TestNewMemoryProvider 测试内存 provider 的创建
func TestNewMemoryProvider(t *testing.T) {
	provider, err := NewMemoryUserConfigProvider(nil)
	if err != nil {
		t.Fatalf("nil 配置创建失败: %v", err)
	}
	if provider == nil {
		t.Fatal("provider 不应为 nil")
	}
	t.Log("nil 配置创建成功（默认 max_entries=1000）")
	provider2, err := NewMemoryUserConfigProvider(map[string]interface{}{
		"max_entries": 50,
	})
	if err != nil {
		t.Fatalf("自定义配置创建失败: %v", err)
	}
	_ = provider2
	t.Logf("自定义配置创建成功（max_entries=50）")
	provider3, err := NewMemoryUserConfigProvider(map[string]interface{}{
		"max_entries": float64(200),
	})
	if err != nil {
		t.Fatalf("float64 配置创建失败: %v", err)
	}
	_ = provider3
	t.Log("float64 类型 max_entries 兼容测试通过")
}

// ---------------------------------------------------------------------------

// TestGetUserConfig_NonExistent 测试获取不存在的用户配置
func TestGetUserConfig_NonExistent(t *testing.T) {
	ctx := context.Background()
	provider, _ := NewMemoryUserConfigProvider(nil)
	config, err := provider.GetUserConfig(ctx, "nonexistent_user")
	if err != nil {
		t.Fatalf("获取配置不应返回错误: %v", err)
	}
	if config.SystemPrompt != "" {
		t.Errorf("空用户应返回空 SystemPrompt，实际: %q", config.SystemPrompt)
	}
	t.Log("不存在用户的配置返回零值（正确行为）")
}

// ---------------------------------------------------------------------------

// TestSetAndGetUserConfig 测试设置和获取用户配置的基本流程
func TestSetAndGetUserConfig(t *testing.T) {
	ctx := context.Background()
	provider, _ := NewMemoryUserConfigProvider(nil)
	testConfig := types.UConfig{
		SystemPrompt:    "你是一个友好的助手",
		MemoryMode:      "long",
		SpeakerChatMode: "identified_only",
		Asr: types.AsrConfig{
			Provider: "funasr",
			Config:   map[string]interface{}{"model": "paraformer-zh"},
		},
		Tts: types.TtsConfig{
			Provider: "edge",
			Config:   map[string]interface{}{"voice": "zh-CN-Xiaoxiao"},
		},
		Llm: types.LlmConfig{
			Provider: "dify",
			Config:   map[string]interface{}{"api_key": "test_key"},
		},
	}
	err := provider.SetUserConfig(ctx, "user_001", testConfig)
	if err != nil {
		t.Fatalf("设置配置失败: %v", err)
	}
	retrieved, err := provider.GetUserConfig(ctx, "user_001")
	if err != nil {
		t.Fatalf("获取配置失败: %v", err)
	}
	if retrieved.SystemPrompt != "你是一个友好的助手" {
		t.Errorf("SystemPrompt 不匹配: 期望 '你是一个友好的助手', 实际 %q", retrieved.SystemPrompt)
	}
	if retrieved.Asr.Provider != "funasr" {
		t.Errorf("ASR Provider 不匹配: 期望 funasr, 实际 %q", retrieved.Asr.Provider)
	}
	t.Logf("设置并获取配置测试通过（用户: user_001）")
}

// ---------------------------------------------------------------------------

// TestSetUserConfig_Overwrite 测试覆盖已有配置
func TestSetUserConfig_Overwrite(t *testing.T) {
	ctx := context.Background()
	provider, _ := NewMemoryUserConfigProvider(nil)
	cfg1 := types.UConfig{SystemPrompt: "提示1", MemoryMode: "short"}
	provider.SetUserConfig(ctx, "user_A", cfg1)
	cfg2 := types.UConfig{SystemPrompt: "提示2", MemoryMode: "long"}
	provider.SetUserConfig(ctx, "user_A", cfg2)
	retrieved, _ := provider.GetUserConfig(ctx, "user_A")
	if retrieved.SystemPrompt != "提示2" {
		t.Errorf("覆盖未生效: 期望 '提示2', 实际 %q", retrieved.SystemPrompt)
	}
	if retrieved.MemoryMode != "long" {
		t.Errorf("覆盖未生效: 期望 'long', 实际 %q", retrieved.MemoryMode)
	}
	t.Log("配置覆盖测试通过")
}

// ---------------------------------------------------------------------------

// TestDeleteUserConfig 测试删除用户配置
func TestDeleteUserConfig(t *testing.T) {
	ctx := context.Background()
	provider, _ := NewMemoryUserConfigProvider(nil)
	provider.SetUserConfig(ctx, "user_delete_me", types.UConfig{SystemPrompt: "临时配置"})
	exists, _ := provider.GetUserConfig(ctx, "user_delete_me")
	if exists.SystemPrompt == "" {
		t.Fatal("设置后应能获取到配置")
	}
	err := provider.DeleteUserConfig(ctx, "user_delete_me")
	if err != nil {
		t.Fatalf("删除失败: %v", err)
	}
	deleted, err := provider.GetUserConfig(ctx, "user_delete_me")
	if err != nil {
		t.Fatalf("删除后获取不应报错: %v", err)
	}
	if deleted.SystemPrompt != "" {
		t.Errorf("删除后不应有配置: 实际 %q", deleted.SystemPrompt)
	}
	err = provider.DeleteUserConfig(ctx, "nonexistent")
	if err != nil {
		t.Errorf("删除不存在的用户不应报错: %v", err)
	}
	t.Log("删除配置测试通过")
}

// ---------------------------------------------------------------------------

// TestMaxEntriesLimit 测试最大条目数限制
func TestMaxEntriesLimit(t *testing.T) {
	ctx := context.Background()
	provider, _ := NewMemoryUserConfigProvider(map[string]interface{}{
		"max_entries": 3,
	})
	for i := 1; i <= 3; i++ {
		cfg := types.UConfig{SystemPrompt: "prompt_" + string(rune('0'+i))}
		err := provider.SetUserConfig(ctx, "user_"+string(rune('0'+i)), cfg)
		if err != nil {
			t.Fatalf("设置第 %d 个用户失败: %v", i, err)
		}
	}
	err := provider.SetUserConfig(ctx, "user_overflow", types.UConfig{SystemPrompt: "overflow"})
	if err == nil {
		t.Fatal("超过最大条目数时应返回错误")
	}
	t.Logf("超出最大条目数时正确拒绝: %v", err)
	provider.DeleteUserConfig(ctx, "user_1")
	err = provider.SetUserConfig(ctx, "user_overflow", types.UConfig{SystemPrompt: "overflow"})
	if err != nil {
		t.Fatalf("删除后添加应成功: %v", err)
	}
	t.Log("最大条目限制测试通过")
}

// ---------------------------------------------------------------------------

// TestGetStats 测试统计信息
func TestGetStats(t *testing.T) {
	ctx := context.Background()
	provider, _ := NewMemoryUserConfigProvider(map[string]interface{}{
		"max_entries": 10,
	})
	stats := provider.GetStats()
	if stats["total_configs"] != 0 {
		t.Errorf("初始 total_configs 应为 0, 实际: %v", stats["total_configs"])
	}
	for i := 0; i < 5; i++ {
		provider.SetUserConfig(ctx, "stats_user_"+string(rune('0'+i)),
			types.UConfig{SystemPrompt: "prompt"})
	}
	stats = provider.GetStats()
	if stats["total_configs"] != 5 {
		t.Errorf("total_configs 应为 5, 实际: %v", stats["total_configs"])
	}
	if stats["max_entries"] != 10 {
		t.Errorf("max_entries 应为 10, 实际: %v", stats["max_entries"])
	}
	if stats["usage_percent"] != 50.0 {
		t.Errorf("usage_percent 应为 50.0, 实际: %v", stats["usage_percent"])
	}
	t.Logf("统计信息: %+v", stats)
}

// ---------------------------------------------------------------------------

// TestListUserIDs 测试列出所有用户 ID
func TestListUserIDs(t *testing.T) {
	ctx := context.Background()
	provider, _ := NewMemoryUserConfigProvider(nil)
	ids := provider.ListUserIDs()
	if len(ids) != 0 {
		t.Errorf("初始用户列表应为空，实际长度: %d", len(ids))
	}
	provider.SetUserConfig(ctx, "user_alpha", types.UConfig{SystemPrompt: "a"})
	provider.SetUserConfig(ctx, "user_beta", types.UConfig{SystemPrompt: "b"})
	provider.SetUserConfig(ctx, "user_gamma", types.UConfig{SystemPrompt: "c"})
	ids = provider.ListUserIDs()
	if len(ids) != 3 {
		t.Errorf("应有 3 个用户，实际: %d", len(ids))
	}
	found := make(map[string]bool)
	for _, id := range ids {
		found[id] = true
	}
	if !found["user_alpha"] || !found["user_beta"] || !found["user_gamma"] {
		t.Error("用户列表中缺少预期用户")
	}
	t.Logf("用户列表: %v", ids)
}

// ---------------------------------------------------------------------------

// TestClose 测试关闭和清理
func TestClose(t *testing.T) {
	ctx := context.Background()
	provider, _ := NewMemoryUserConfigProvider(nil)
	provider.SetUserConfig(ctx, "close_user_1", types.UConfig{SystemPrompt: "test1"})
	provider.SetUserConfig(ctx, "close_user_2", types.UConfig{SystemPrompt: "test2"})
	err := provider.Close()
	if err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	config, _ := provider.GetUserConfig(ctx, "close_user_1")
	if config.SystemPrompt != "" {
		t.Errorf("Close 后配置应为空，实际: %q", config.SystemPrompt)
	}
	stats := provider.GetStats()
	if stats["total_configs"] != 0 {
		t.Errorf("Close 后 total_configs 应为 0, 实际: %v", stats["total_configs"])
	}
	t.Log("Close 清理测试通过")
}

// ---------------------------------------------------------------------------

// TestInitAndLifecycle 测试包的初始化函数
func TestInitAndLifecycle(t *testing.T) {
	ctx := context.Background()
	err := Init(ctx)
	if err != nil {
		t.Fatalf("Init 失败: %v", err)
	}
	t.Log("Init 调用成功")
	if !IsConnected() {
		t.Error("内存 provider 应始终连接")
	}
	t.Log("IsConnected 返回 true（正确）")
	err = Close()
	if err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	t.Log("Close 调用成功")
}

// ---------------------------------------------------------------------------

// TestConcurrentAccess 测试并发访问的安全性
func TestConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	provider, _ := NewMemoryUserConfigProvider(map[string]interface{}{
		"max_entries": 1000,
	})
	const numUsers = 100
	const numIterations = 10
	var wg sync.WaitGroup
	wg.Add(2)
	// 并发写入
	go func() {
		defer wg.Done()
		for i := 0; i < numUsers; i++ {
			cfg := types.UConfig{SystemPrompt: fmt.Sprintf("prompt_%d", i)}
			provider.SetUserConfig(ctx, fmt.Sprintf("user_%d", i), cfg)
		}
	}()
	// 并发读取
	go func() {
		defer wg.Done()
		for j := 0; j < numIterations; j++ {
			for i := 0; i < numUsers; i++ {
				provider.GetUserConfig(ctx, fmt.Sprintf("user_%d", i))
			}
		}
	}()
	wg.Wait()
	stats := provider.GetStats()
	var total int
	switch v := stats["total_configs"].(type) {
	case int:
		total = v
	case float64:
		total = int(v)
	}
	if total == 0 {
		t.Error("并发写入后应有数据")
	}
	t.Logf("并发测试通过，最终条目数: %d", total)
}

// ---------------------------------------------------------------------------

// TestConcurrentReadWrite 更严格的并发读写测试
func TestConcurrentReadWrite(t *testing.T) {
	ctx := context.Background()
	provider, _ := NewMemoryUserConfigProvider(map[string]interface{}{
		"max_entries": 100,
	})
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				userID := fmt.Sprintf("user_%d_%d", id, j)
				provider.SetUserConfig(ctx, userID, types.UConfig{SystemPrompt: fmt.Sprintf("prompt_%d", j)})
				provider.GetUserConfig(ctx, userID)
			}
		}(i)
	}
	wg.Wait()
	stats := provider.GetStats()
	t.Logf("并发读写测试通过，总条目数: %v", stats["total_configs"])
}

// 学习提示：
//  - MemoryUserConfigProvider 使用 sync.RWMutex 保证并发安全
//  - RLock/RUnlock 用于读操作（GetUserConfig, GetStats, ListUserIDs）
//  - Lock/Unlock 用于写操作（SetUserConfig, DeleteUserConfig, Close）
//  - max_entries 限制仅在添加新条目时检查，已有条目不会被清除
//  - Close 会清空所有数据，适用于测试环境重置
