// package redis_config

// auth_test.go
//
// 本文件用于测试 config/redis/auth.go 中的设备激活逻辑。
// 重点演示：
//   - IsDeviceActivated：检查设备是否已激活
//   - GetActivationInfo：生成挑战码和验证码
//   - VerifyChallenge：验证挑战响应
//   - 全局变量的状态管理（注意：这些是包级变量，测试间可能相互影响）

package redis_config

import (
	"context"
	"testing"
	"xiaozhi-esp32-server-golang/internal/domain/config/types"
)

// ---------------------------------------------------------------------------

// TestIsDeviceActivated_NonActivated 测试未激活设备的状态
func TestIsDeviceActivated_NonActivated(t *testing.T) {
	ctx := context.Background()
	// 注意：verfiyDeviceId 是包级 map，这里假设它是空的
	// 如果之前有测试注册过，需要先清理
	provider := &UserConfig{}
	activated, err := provider.IsDeviceActivated(ctx, "new_device_001", "client_001")
	if err != nil {
		t.Fatalf("IsDeviceActivated 不应报错: %v", err)
	}
	if activated {
		t.Error("新设备应处于未激活状态")
	}
	t.Log("未激活设备返回 false（正确）")
}

// ---------------------------------------------------------------------------

// TestGetActivationInfo 测试激活信息生成
func TestGetActivationInfo(t *testing.T) {
	ctx := context.Background()
	provider := &UserConfig{}
	deviceID := "test_device_001"
	// 首次请求：应生成新的激活信息
	code, challenge, msg, timeoutMs := provider.GetActivationInfo(ctx, deviceID, "client_001")
	if code == "" {
		t.Error("激活码不应为空")
	}
	if challenge == "" {
		t.Error("Challenge 不应为空")
	}
	if msg == "" {
		t.Error("消息不应为空")
	}
	if timeoutMs != 300 {
		t.Errorf("timeoutMs 应为 300, 实际: %d", timeoutMs)
	}
	// 验证消息格式
	expectedMsgPrefix := "xiaozhi\n"
	if len(msg) < len(expectedMsgPrefix) || msg[:len(expectedMsgPrefix)] != expectedMsgPrefix {
		t.Errorf("消息格式应为 'xiaozhi\\n%q...', 实际: %q", code, msg)
	}
	t.Logf("激活信息: code=%q, challenge=%q, msg=%q, timeout=%dms",
		code, challenge, msg, timeoutMs)
}

// ---------------------------------------------------------------------------

// TestGetActivationInfo_Cached 测试缓存的激活信息（再次请求同一设备）
func TestGetActivationInfo_Cached(t *testing.T) {
	ctx := context.Background()
	provider := &UserConfig{}
	deviceID := "test_device_cached"
	// 第一次请求
	code1, challenge1, msg1, _ := provider.GetActivationInfo(ctx, deviceID, "client_001")
	// 第二次请求同一设备
	code2, challenge2, msg2, _ := provider.GetActivationInfo(ctx, deviceID, "client_001")
	// 缓存的激活信息应相同
	if code1 != code2 {
		t.Errorf("code 应缓存: 第一次 %q, 第二次 %q", code1, code2)
	}
	if challenge1 != challenge2 {
		t.Errorf("challenge 应缓存: 第一次 %q, 第二次 %q", challenge1, challenge2)
	}
	if msg1 != msg2 {
		t.Errorf("msg 应缓存: 第一次 %q, 第二次 %q", msg1, msg2)
	}
	t.Log("缓存激活信息测试通过")
}

// ---------------------------------------------------------------------------

// TestVerifyChallenge_Success 测试正确的挑战验证
func TestVerifyChallenge_Success(t *testing.T) {
	ctx := context.Background()
	provider := &UserConfig{}
	deviceID := "test_device_verify"
	// 获取激活信息
	_, challenge, _, _ := provider.GetActivationInfo(ctx, deviceID, "client_001")
	// 构造正确的 payload
	payload := types.ActivationPayload{
		Algorithm:    "hmac-sha256",
		SerialNumber: "SN_TEST_001",
		Challenge:    challenge,
		HMAC:         "", // 简化测试，不使用 HMAC
	}
	verified, err := provider.VerifyChallenge(ctx, deviceID, "client_001", payload)
	if err != nil {
		t.Fatalf("VerifyChallenge 不应报错: %v", err)
	}
	if !verified {
		t.Error("正确的 challenge 应验证通过")
	}
	// 验证后设备应标记为已激活
	activated, _ := provider.IsDeviceActivated(ctx, deviceID, "client_001")
	if !activated {
		t.Error("验证通过后设备应处于已激活状态")
	}
	t.Log("正确 challenge 验证通过测试通过")
}

// ---------------------------------------------------------------------------

// TestVerifyChallenge_WrongChallenge 测试错误的挑战验证
func TestVerifyChallenge_WrongChallenge(t *testing.T) {
	ctx := context.Background()
	provider := &UserConfig{}
	deviceID := "test_device_wrong"
	// 获取激活信息
	challenge, _, _, _ := provider.GetActivationInfo(ctx, deviceID, "client_001")
	// 构造错误的 payload
	payload := types.ActivationPayload{
		Algorithm:    "hmac-sha256",
		SerialNumber: "SN_WRONG_001",
		Challenge:    challenge,
		HMAC:         "",
	}
	verified, err := provider.VerifyChallenge(ctx, deviceID, "client_001", payload)
	if err != nil {
		t.Fatalf("VerifyChallenge 不应报错: %v", err)
	}
	if verified {
		t.Error("错误的 challenge 应验证失败")
	}
	// 设备应保持未激活状态
	activated, _ := provider.IsDeviceActivated(ctx, deviceID, "client_001")
	if activated {
		t.Error("验证失败后设备不应被标记为已激活")
	}
	t.Log("错误 challenge 验证失败测试通过")
}

// ---------------------------------------------------------------------------

// TestVerifyChallenge_AlreadyActivated 测试已激活设备跳过验证
func TestVerifyChallenge_AlreadyActivated(t *testing.T) {
	ctx := context.Background()
	provider := &UserConfig{}
	deviceID := "test_device_activated"
	// 先手动标记为已激活（模拟 verfiyDeviceId map 中有记录）
	// 由于 verfiyDeviceId 是包级变量，这里直接验证逻辑
	// 先通过正确挑战激活
	_, challenge, _, _ := provider.GetActivationInfo(ctx, deviceID, "client_001")
	payload := types.ActivationPayload{Challenge: challenge}
	provider.VerifyChallenge(ctx, deviceID, "client_001", payload)
	// 已激活设备再次验证应直接通过
	verified, _ := provider.VerifyChallenge(ctx, deviceID, "client_001", types.ActivationPayload{})
	if !verified {
		t.Error("已激活设备应直接通过验证")
	}
	t.Log("已激活设备跳过验证测试通过")
}

// ---------------------------------------------------------------------------

// TestVerificationFlow 测试完整的设备激活流程
func TestVerificationFlow(t *testing.T) {
	ctx := context.Background()
	provider := &UserConfig{}
	deviceID := "flow_test_device"
	// Step 1: 检查激活状态（应未激活）
	activated, _ := provider.IsDeviceActivated(ctx, deviceID, "client_001")
	if activated {
		t.Fatal("新设备应处于未激活状态")
	}
	t.Log("Step 1: 设备未激活")
	// Step 2: 获取激活信息
	code, challenge, _, _ := provider.GetActivationInfo(ctx, deviceID, "client_001")
	if code == "" || challenge == "" {
		t.Fatal("激活信息生成失败")
	}
	t.Logf("Step 2: 获取激活信息成功 code=%q challenge=%q", code, challenge)
	// Step 3: 设备提交正确的 challenge 响应
	payload := types.ActivationPayload{
		Challenge: challenge,
	}
	verified, err := provider.VerifyChallenge(ctx, deviceID, "client_001", payload)
	if err != nil || !verified {
		t.Fatalf("Step 3: 验证失败: err=%v verified=%v", err, verified)
	}
	t.Log("Step 3: 挑战验证通过")
	// Step 4: 再次检查激活状态（应已激活）
	activated, _ = provider.IsDeviceActivated(ctx, deviceID, "client_001")
	if !activated {
		t.Fatal("验证通过后设备应处于已激活状态")
	}
	t.Log("Step 4: 设备已激活")
	t.Log("完整激活流程测试通过")
}

// 学习提示：
//  - verfiyDeviceId 和 preActivationInfo 是包级 map，跨测试共享状态
//  - GetActivationInfo 会缓存已有的激活信息，重复调用返回相同值
//  - VerifyChallenge 成功后会从 preActivationInfo 删除该设备记录
//  - 真实 Redis 版本中这些数据会持久化到 Redis，这里是简化测试版
