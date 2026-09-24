// package types

// activition_test.go
//
// 本文件用于测试 config/types/activition.go 中的 ActivationPayload 结构体。
// 重点演示：
//   - ActivationPayload 的字段含义
//   - JSON 序列化/反序列化
//   - HMAC 验证的基本流程（配合 manager/auth.go 中的 verifyHMAC）

package types

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
)

// ---------------------------------------------------------------------------

// TestActivationPayloadFields 验证 ActivationPayload 的所有字段
func TestActivationPayloadFields(t *testing.T) {
	payload := ActivationPayload{
		Algorithm:    "hmac-sha256",
		SerialNumber: "SN123456789",
		Challenge:    "550e8400-e29b-41d4-a716-446655440000",
		HMAC:         "a1b2c3d4e5f6...",
	}
	if payload.Algorithm != "hmac-sha256" {
		t.Error("Algorithm 字段不匹配")
	}
	if payload.SerialNumber != "SN123456789" {
		t.Error("SerialNumber 字段不匹配")
	}
	if payload.Challenge == "" {
		t.Error("Challenge 不应为空")
	}
	t.Logf("ActivationPayload 字段验证通过:")
	t.Logf("  Algorithm:    %s", payload.Algorithm)
	t.Logf("  SerialNumber: %s", payload.SerialNumber)
	t.Logf("  Challenge:    %s", payload.Challenge)
	t.Logf("  HMAC:         %s", payload.HMAC)
}

// ---------------------------------------------------------------------------

// TestActivationPayloadSerialization 测试激活载荷的 JSON 序列化
func TestActivationPayloadSerialization(t *testing.T) {
	payload := ActivationPayload{
		Algorithm:    "hmac-sha256",
		SerialNumber: "SN987654321",
		Challenge:    "challenge_abc",
		HMAC:         "expected_hmac_value",
	}
	// 序列化为 JSON
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}
	t.Logf("序列化结果: %s", string(data))
	// 反序列化
	var decoded ActivationPayload
	err = json.Unmarshal(data, &decoded)
	if err != nil {
		t.Fatalf("反序列化失败: %v", err)
	}
	if decoded.Algorithm != payload.Algorithm {
		t.Error("Algorithm 不匹配")
	}
	if decoded.SerialNumber != payload.SerialNumber {
		t.Error("SerialNumber 不匹配")
	}
	if decoded.Challenge != payload.Challenge {
		t.Error("Challenge 不匹配")
	}
	if decoded.HMAC != payload.HMAC {
		t.Error("HMAC 不匹配")
	}
	t.Log("ActivationPayload 序列化测试通过")
}

// ---------------------------------------------------------------------------

// TestHMACCalculation 模拟 HMAC 计算过程（与 manager/auth.go 中 verifyHMAC 配合）
// 演示 HMAC-SHA256 的计算方式
func TestHMACCalculation(t *testing.T) {
	secretKey := "my_secret_key"
	challenge := "test_challenge_value"
	// 计算 HMAC
	mac := hmac.New(sha256.New, []byte(secretKey))
	mac.Write([]byte(challenge))
	expectedHmac := hex.EncodeToString(mac.Sum(nil))
	t.Logf("挑战值:    %s", challenge)
	t.Logf("HMAC 结果: %s", expectedHmac)
	// 验证 HMAC 长度（SHA256 hex = 64 字符）
	if len(expectedHmac) != 64 {
		t.Errorf("HMAC 长度错误: 期望 64, 实际 %d", len(expectedHmac))
	}
	// 验证相同的输入产生相同的输出
	mac2 := hmac.New(sha256.New, []byte(secretKey))
	mac2.Write([]byte(challenge))
	expectedHmac2 := hex.EncodeToString(mac2.Sum(nil))
	if expectedHmac != expectedHmac2 {
		t.Error("相同输入应产生相同 HMAC")
	}
	// 验证不同的输入产生不同的输出
	mac3 := hmac.New(sha256.New, []byte(secretKey))
	mac3.Write([]byte("different_challenge"))
	expectedHmac3 := hex.EncodeToString(mac3.Sum(nil))
	if expectedHmac == expectedHmac3 {
		t.Error("不同输入不应产生相同 HMAC")
	}
	t.Log("HMAC 计算逻辑验证通过")
}

// ---------------------------------------------------------------------------

// TestEmptyActivationPayload 测试空 ActivationPayload 的行为
func TestEmptyActivationPayload(t *testing.T) {
	empty := ActivationPayload{}
	if empty.Algorithm != "" {
		t.Error("空载荷的 Algorithm 应为空")
	}
	if empty.Challenge != "" {
		t.Error("空载荷的 Challenge 应为空")
	}
	if empty.HMAC != "" {
		t.Error("空载荷的 HMAC 应为空")
	}
	// 序列化空载荷
	data, err := json.Marshal(empty)
	if err != nil {
		t.Fatalf("空载荷序列化失败: %v", err)
	}
	t.Logf("空载荷序列化: %s", string(data))
	// 反序列化空 JSON
	var decoded ActivationPayload
	err = json.Unmarshal([]byte("{}"), &decoded)
	if err != nil {
		t.Fatalf("空 JSON 反序列化失败: %v", err)
	}
	t.Log("空 ActivationPayload 测试通过")
}

// ---------------------------------------------------------------------------

// TestActivationPayloadJsonTags 验证 JSON tag 的名称
// 确保 JSON 字段名与后端 API 一致
func TestActivationPayloadJsonTags(t *testing.T) {
	payload := ActivationPayload{
		Algorithm:    "hmac-sha256",
		SerialNumber: "SN123",
		Challenge:    "chall_123",
		HMAC:         "hmac_123",
	}
	data, _ := json.Marshal(payload)
	jsonStr := string(data)
	// 验证 JSON tag 名称正确
	expectedFields := []string{
		`"algorithm"`,
		`"serial_number"`,
		`"challenge"`,
		`"hmac"`,
	}
	for _, field := range expectedFields {
		if !contains(jsonStr, field) {
			t.Errorf("JSON 中缺少字段 %s，实际: %s", field, jsonStr)
		}
	}
	t.Logf("JSON tag 验证通过，序列化: %s", jsonStr)
}

// contains 辅助函数
func contains(s, substr string) bool {
	return len(s) >= len(substr) && indexOf(s, substr) >= 0
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
//  - ActivationPayload 是设备激活时发送给服务端的挑战-响应载荷
//  - Challenge 由服务端生成，设备用 HMAC 签名后回传
//  - Algorithm 字段指定签名算法（通常 hmac-sha256）
//  - SerialNumber 是设备的唯一标识，用于防重放攻击
