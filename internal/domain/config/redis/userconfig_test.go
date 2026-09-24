// package redis_config

// userconfig_test.go
//
// 本文件用于测试 config/redis/userconfig.go 中的用户配置逻辑。
// 重点演示：
//   - GetUserConfigKey：Redis key 的生成规则
//   - getVadConfig / getConfigByType：配置合并逻辑
//   - NewRedisUserConfigProvider：初始化过程
//
// 注意：这些测试依赖 Redis 连接，实际运行时需要 Redis 服务可用。
// 在没有 Redis 的环境中，部分测试会 skip。

package redis_config

import (
	"context"
	"testing"

	"github.com/spf13/viper"
)

// ---------------------------------------------------------------------------

// TestGetUserConfigKey 测试 Redis key 的生成规则
// 重点理解 key 的命名规范，便于后续调试 Redis 数据
func TestGetUserConfigKey(t *testing.T) {
	// 保存并恢复原始 prefix
	originalPrefix := viper.GetString("redis.key_prefix")
	defer viper.Set("redis.key_prefix", originalPrefix)

	testCases := []struct {
		name     string
		prefix   string
		userID   string
		expected string
	}{
		{"default_prefix", "xiaozhi", "device_001", "xiaozhi:userconfig:device_001"},
		{"empty_prefix", "", "device_001", ":userconfig:device_001"},
		{"custom_prefix", "custom", "my_device", "custom:userconfig:my_device"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			viper.Set("redis.key_prefix", tc.prefix)
			provider := &UserConfig{prefix: tc.prefix}
			key := provider.GetUserConfigKey(tc.userID)
			if key != tc.expected {
				t.Errorf("key 不匹配: 期望 %q, 实际 %q", tc.expected, key)
			}
			t.Logf("key 生成正确: %s", key)
		})
	}
}

// ---------------------------------------------------------------------------

// TestGetConfigByType 测试配置合并逻辑
// 理解 getConfigByType 如何将 viper 配置与 Redis 配置合并
func TestGetConfigByType(t *testing.T) {
	// 保存并恢复原始配置
	originalLLMProvider := viper.GetString("llm.provider")
	originalASRProvider := viper.GetString("asr.provider")
	defer func() {
		viper.Set("llm.provider", originalLLMProvider)
		viper.Set("asr.provider", originalASRProvider)
	}()

	// 设置 viper 中的 provider 默认值
	viper.Set("llm.provider", "dify")
	viper.Set("asr.provider", "funasr")

	provider := &UserConfig{}

	// 测试 1: Redis 中有配置，合并 viper 默认值
	redisConfig := map[string]interface{}{
		"provider": "dify",
		"api_key":  "test_key",
		"base_url": "http://test:5001",
	}

	gotProvider, mergedConfig, err := provider.getConfigByType(context.Background(), redisConfig, "llm")
	if err != nil {
		t.Fatalf("getConfigByType 失败: %v", err)
	}
	if gotProvider != "dify" {
		t.Errorf("provider 应为 dify, 实际: %s", gotProvider)
	}
	if mergedConfig["api_key"] != "test_key" {
		t.Error("Redis 配置项应被合并")
	}
	t.Logf("LLM 配置合并: provider=%s, keys=%v", gotProvider, mapKeys(mergedConfig))

	// 测试 2: Redis 中没有 provider 字段，使用 viper 默认值
	redisConfig2 := map[string]interface{}{
		"model": "qwen-turbo",
	}
	gotProvider2, _, err := provider.getConfigByType(context.Background(), redisConfig2, "llm")
	if err != nil {
		t.Fatalf("getConfigByType 失败: %v", err)
	}
	// 当 Redis 中无 provider 时，viper 的 llm.provider="dify" 应被使用
	t.Logf("无 provider 字段时使用 viper 默认: provider=%s", gotProvider2)
}

// ---------------------------------------------------------------------------

// TestGetAsrConfig 测试 ASR 配置解析
// 演示 getAsrConfig 如何从 Redis 数据构建 AsrConfig 结构
func TestGetAsrConfig(t *testing.T) {
	// 设置 viper 默认值
	viper.Set("asr.provider", "sherpa_onnx")

	provider := &UserConfig{}

	// 模拟 Redis 中的 ASR 配置
	redisData := map[string]interface{}{
		"provider":    "sherpa_onnx",
		"model_path":  "/models/sherpa-onnx-streaming-zipformer-zh-14M-onnx/model.onnx",
		"tokens":      "/models/sherpa-onnx-streaming-zipformer-zh-14M-onnx/tokens.txt",
		"sample_rate": 16000,
		"feature_dim": 80,
	}

	asrConfig, err := provider.getAsrConfig(context.Background(), redisData)
	if err != nil {
		t.Fatalf("getAsrConfig 失败: %v", err)
	}

	if asrConfig.Provider != "sherpa_onnx" {
		t.Errorf("Provider 不匹配: 期望 sherpa_onnx, 实际 %s", asrConfig.Provider)
	}
	if asrConfig.Config["model_path"] == "" {
		t.Error("model_path 应被保留")
	}
	t.Logf("ASR 配置解析成功: provider=%s, keys=%v",
		asrConfig.Provider, mapKeys(asrConfig.Config))
}

// ---------------------------------------------------------------------------

// TestGetTtsConfig 测试 TTS 配置解析
func TestGetTtsConfig(t *testing.T) {
	viper.Set("tts.provider", "edge")

	provider := &UserConfig{}

	redisData := map[string]interface{}{
		"provider": "edge",
		"voice":    "zh-CN-XiaoxiaoNeural",
		"rate":     "1.0",
	}

	ttsConfig, err := provider.getTtsConfig(context.Background(), redisData)
	if err != nil {
		t.Fatalf("getTtsConfig 失败: %v", err)
	}

	if ttsConfig.Provider != "edge" {
		t.Errorf("Provider 不匹配: 期望 edge, 实际 %s", ttsConfig.Provider)
	}
	t.Logf("TTS 配置解析成功: provider=%s", ttsConfig.Provider)
}

// ---------------------------------------------------------------------------

// TestGetLlmConfig 测试 LLM 配置解析
func TestGetLlmConfig(t *testing.T) {
	viper.Set("llm.provider", "dify")

	provider := &UserConfig{}

	redisData := map[string]interface{}{
		"provider": "dify",
		"api_key":  "test_dify_key",
		"mode":     "chat",
	}

	llmConfig, err := provider.getLlmConfig(context.Background(), redisData)
	if err != nil {
		t.Fatalf("getLlmConfig 失败: %v", err)
	}

	if llmConfig.Provider != "dify" {
		t.Errorf("Provider 不匹配: 期望 dify, 实际 %s", llmConfig.Provider)
	}
	if llmConfig.Config["mode"] != "chat" {
		t.Error("mode 应被保留")
	}
	t.Logf("LLM 配置解析成功: provider=%s", llmConfig.Provider)
}

// ---------------------------------------------------------------------------

// TestGetMemoryConfig 测试 Memory 配置解析
func TestGetMemoryConfig(t *testing.T) {
	viper.Set("memory.provider", "mem0")

	provider := &UserConfig{}

	redisData := map[string]interface{}{
		"provider": "mem0",
		"api_key":  "test_mem0_key",
		"model":    "gpt-4",
	}

	memConfig, err := provider.getMemoryConfig(context.Background(), redisData)
	if err != nil {
		t.Fatalf("getMemoryConfig 失败: %v", err)
	}

	if memConfig.Provider != "mem0" {
		t.Errorf("Provider 不匹配: 期望 mem0, 实际 %s", memConfig.Provider)
	}
	t.Logf("Memory 配置解析成功: provider=%s", memConfig.Provider)
}

// ---------------------------------------------------------------------------

// TestGetVadConfig 测试 VAD 配置获取
// 注意：getVadConfig 直接从 viper 读取，不从 Redis 读取
func TestGetVadConfig(t *testing.T) {
	// 保存并恢复
	originalProvider := viper.GetString("vad.provider")
	defer viper.Set("vad.provider", originalProvider)

	viper.Set("vad.provider", "silero_vad")
	viper.Set("vad.silero_vad.threshold", 0.5)
	viper.Set("vad.silero_vad.sample_rate", 16000)

	provider := &UserConfig{}

	vadConfig := provider.getVadConfig(context.Background())

	if vadConfig.Provider != "silero_vad" {
		t.Errorf("Provider 不匹配: 期望 silero_vad, 实际 %s", vadConfig.Provider)
	}
	if vadConfig.Config["threshold"] != 0.5 {
		t.Error("threshold 应被正确获取")
	}
	t.Logf("VAD 配置获取成功: provider=%s", vadConfig.Provider)
}

// ---------------------------------------------------------------------------

// mapKeys 返回 map 的键列表（辅助函数）
func mapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// 学习提示：
//  - getUserConfigKey 生成的 key 格式：{prefix}:userconfig:{userID}
//  - getConfigByType 优先使用 Redis 中的 provider，fallback 到 viper 默认值
//  - Redis 中的配置项会与 viper 中的同 provider 配置合并（viper 提供默认值）
//  - VAD 配置只从 viper 读取，不从 Redis 读取（与其他子系统不同）
//  - 所有 config.*.provider 字段在 viper 中设置默认值，Redis 中可以覆盖
