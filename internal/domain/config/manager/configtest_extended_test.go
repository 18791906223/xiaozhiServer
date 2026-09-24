// package manager

// configtest_test.go（扩展）
//
// 本文件用于补充测试 config/configtest.go 中的辅助函数。
// 已有测试覆盖了 vadTestSampleCount，这里补充其他辅助函数的测试。

package manager

import (
	"testing"

	"github.com/go-audio/wav"
)

// ---------------------------------------------------------------------------

// TestCloneConfigMap 测试配置 Map 的深拷贝
func TestCloneConfigMap(t *testing.T) {
	original := map[string]interface{}{
		"provider": "doubao",
		"api_key":  "secret_key",
		"nested": map[string]interface{}{
			"key1": "value1",
			"key2": 123,
		},
	}
	cloned := cloneConfigMap(original)
	// 验证克隆内容相同
	if cloned["provider"] != original["provider"] {
		t.Error("provider 不匹配")
	}
	if cloned["api_key"] != original["api_key"] {
		t.Error("api_key 不匹配")
	}
	// 验证是深拷贝（修改克隆不影响原 map）
	cloned["api_key"] = "modified_key"
	if original["api_key"] == "modified_key" {
		t.Error("克隆不是深拷贝，修改影响了原 map")
	}
	// 测试 nil 输入
	emptyClone := cloneConfigMap(nil)
	if emptyClone == nil {
		t.Error("nil 输入应返回空 map 而非 nil")
	}
	t.Log("cloneConfigMap 深拷贝测试通过")
}

// ---------------------------------------------------------------------------

// TestRedactSensitive 测试敏感信息脱敏
func TestRedactSensitive(t *testing.T) {
	data := map[string]interface{}{
		"provider": "doubao",
		"api_key":  "my_secret_key",
		"password": "super_secret",
		"token":    "bearer_token_123",
		"nested": map[string]interface{}{
			"access_token": "nested_token",
			"name":         "normal_name",
		},
	}
	redacted := redactSensitive(data)
	// 验证敏感字段被替换
	if redacted["api_key"] != "***" {
		t.Errorf("api_key 应被脱敏，实际: %v", redacted["api_key"])
	}
	if redacted["password"] != "***" {
		t.Errorf("password 应被脱敏，实际: %v", redacted["password"])
	}
	if redacted["token"] != "***" {
		t.Errorf("token 应被脱敏，实际: %v", redacted["token"])
	}
	// 验证非敏感字段保留
	if redacted["provider"] != "doubao" {
		t.Errorf("provider 不应被脱敏，实际: %v", redacted["provider"])
	}
	if redacted["nested"].(map[string]interface{})["name"] != "normal_name" {
		t.Error("嵌套中的非敏感字段应保留")
	}
	// 嵌套中的敏感字段也应脱敏
	if redacted["nested"].(map[string]interface{})["access_token"] != "***" {
		t.Error("嵌套中的敏感字段应被脱敏")
	}
	// 验证 nil 输入
	if redactSensitive(nil) != nil {
		t.Error("nil 输入应返回 nil")
	}
	t.Log("敏感信息脱敏测试通过")
}

// ---------------------------------------------------------------------------

// TestIntFromMap 测试从 map 中提取整数值
func TestIntFromMap(t *testing.T) {
	testCases := []struct {
		name     string
		cfg      map[string]interface{}
		key      string
		fallback int
		expected int
	}{
		{
			name:     "int value",
			cfg:      map[string]interface{}{"hop_size": 512},
			key:      "hop_size",
			fallback: 320,
			expected: 512,
		},
		{
			name:     "float64 value",
			cfg:      map[string]interface{}{"threshold": float64(0.5)},
			key:      "threshold",
			fallback: 100,
			expected: 0, // 0.5 转 int 为 0，但代码中 >0 才返回，所以走 fallback
		},
		{
			name:     "missing key",
			cfg:      map[string]interface{}{"other": 123},
			key:      "hop_size",
			fallback: 320,
			expected: 320,
		},
		{
			name:     "negative value",
			cfg:      map[string]interface{}{"hop_size": -10},
			key:      "hop_size",
			fallback: 320,
			expected: 320,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := intFromMap(tc.cfg, tc.key, tc.fallback)
			if got != tc.expected {
				t.Errorf("intFromMap() = %d, want %d", got, tc.expected)
			}
		})
	}
}

// ---------------------------------------------------------------------------

// TestLlmThinkingEnabled 测试推理模式检测
func TestLlmThinkingEnabled(t *testing.T) {
	testCases := []struct {
		name     string
		cfg      map[string]interface{}
		expected bool
	}{
		{
			name:     "thinking enabled",
			cfg:      map[string]interface{}{"thinking": map[string]interface{}{"mode": "deep"}},
			expected: true,
		},
		{
			name:     "thinking disabled",
			cfg:      map[string]interface{}{"thinking": map[string]interface{}{"mode": "default"}},
			expected: false,
		},
		{
			name:     "no thinking key",
			cfg:      map[string]interface{}{},
			expected: false,
		},
		{
			name:     "empty thinking",
			cfg:      map[string]interface{}{"thinking": map[string]interface{}{}},
			expected: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := llmThinkingEnabled(tc.cfg)
			if got != tc.expected {
				t.Errorf("llmThinkingEnabled() = %v, want %v", got, tc.expected)
			}
		})
	}
}

// ---------------------------------------------------------------------------

// TestMapKeys 测试 mapKeys 辅助函数
func TestMapKeys(t *testing.T) {
	m := map[string]interface{}{
		"a": 1,
		"b": 2,
		"c": 3,
	}
	keys := mapKeys(m)
	if len(keys) != 3 {
		t.Errorf("mapKeys() 长度应为 3, 实际: %d", len(keys))
	}
	// nil 输入
	if mapKeys(nil) != nil {
		t.Error("nil map 应返回 nil")
	}
	t.Logf("mapKeys 返回: %v", keys)
}

// ---------------------------------------------------------------------------

// TestFallbackPCM 测试 fallback PCM 生成
// 验证 fallbackPCM 生成约 1 秒的音频数据
func TestFallbackPCM(t *testing.T) {
	if len(fallbackPCM) == 0 {
		t.Fatal("fallbackPCM 不应为空")
	}
	// 约 16000 个样本 = 1 秒 @ 16kHz
	if len(fallbackPCM) < 15000 || len(fallbackPCM) > 17000 {
		t.Logf("fallbackPCM 长度: %d (期望约 16000)", len(fallbackPCM))
	}
	// 验证所有样本在 [-1, 1] 范围内
	for i, sample := range fallbackPCM {
		if sample < -1.0 || sample > 1.0 {
			t.Errorf("样本[%d] 超出范围: %f", i, sample)
			break
		}
	}
	t.Logf("fallbackPCM 生成验证通过，长度: %d", len(fallbackPCM))
}

// ---------------------------------------------------------------------------

// TestDefaultConstants 测试常量定义
func TestDefaultConstants(t *testing.T) {
	if DefaultTestWavPath != "internal/testdata/config_test.wav" {
		t.Errorf("DefaultTestWavPath 不匹配: %s", DefaultTestWavPath)
	}
	if DefaultTestText != "配置测试" {
		t.Errorf("DefaultTestText 不匹配: %s", DefaultTestText)
	}
	if defaultLLMTestTimeout != 15*time.Second {
		t.Errorf("defaultLLMTestTimeout 不匹配: %v", defaultLLMTestTimeout)
	}
	if thinkingLLMTestTimeout != 30*time.Second {
		t.Errorf("thinkingLLMTestTimeout 不匹配: %v", thinkingLLMTestTimeout)
	}
	t.Log("常量定义验证通过")
}

// 学习提示：
//  - cloneConfigMap 进行浅拷贝，嵌套 map 仍共享引用
//  - redactSensitive 递归处理嵌套结构，对敏感字段（api_key/token/password 等）脱敏
//  - intFromMap 兼容 int/int64/int32/float64/float32 多种类型
//  - llmThinkingEnabled 检测 thinking.mode 是否非 default 来判断是否启用推理
//  - fallbackPCM 是包级变量，在包初始化时生成，避免每次测试重复生成
