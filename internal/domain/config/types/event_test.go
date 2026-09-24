// package types

// event_test.go
//
// 本文件用于测试 config/types/event.go 中定义的事件类型常量。
// 重点演示：
//   - 上行事件（主程序 → 管理后台）和下行事件（管理后台 → 主程序）的区别
//   - 事件路径常量的使用场景
//   - EventHandler 函数签名的理解

package types

import (
	"context"
	"testing"
)

// ---------------------------------------------------------------------------

// TestEventConstants 验证事件常量值正确
func TestEventConstants(t *testing.T) {
	// 上行事件常量
	if EventDeviceOnline != "/api/device/active" {
		t.Errorf("EventDeviceOnline 值错误: 期望 /api/device/active, 实际 %s", EventDeviceOnline)
	}
	if EventDeviceOffline != "/api/device/inactive" {
		t.Errorf("EventDeviceOffline 值错误: 期望 /api/device/inactive, 实际 %s", EventDeviceOffline)
	}
	// 下行事件常量
	if EventHandleMessageInject != "/api/device/inject_msg" {
		t.Errorf("EventHandleMessageInject 值错误: 期望 /api/device/inject_msg, 实际 %s", EventHandleMessageInject)
	}
	t.Logf("事件常量验证通过:")
	t.Logf("  上行 - 设备上线: %s", EventDeviceOnline)
	t.Logf("  上行 - 设备下线: %s", EventDeviceOffline)
	t.Logf("  下行 - 消息注入: %s", EventHandleMessageInject)
}

// ---------------------------------------------------------------------------

// TestEventHandlerSignature 验证 EventHandler 的函数签名
// 测试函数能否按定义的类型调用
func TestEventHandlerSignature(t *testing.T) {
	// 创建一个符合 EventHandler 签名的处理函数
	var handler EventHandler = func(ctx context.Context, eventType string, eventData map[string]interface{}) (string, error) {
		// 模拟处理逻辑
		deviceID, _ := eventData["device_id"].(string)
		return "handled:" + deviceID, nil
	}
	// 调用测试
	ctx := context.Background()
	result, err := handler(ctx, EventDeviceOnline, map[string]interface{}{
		"device_id": "device_001",
	})
	if err != nil {
		t.Fatalf("EventHandler 调用失败: %v", err)
	}
	expected := "handled:device_001"
	if result != expected {
		t.Errorf("返回值不匹配: 期望 %q, 实际 %q", expected, result)
	}
	t.Log("EventHandler 签名验证通过")
}

// ---------------------------------------------------------------------------

// TestEventHandlerErrorReturn 验证 EventHandler 可以返回错误
func TestEventHandlerErrorReturn(t *testing.T) {
	// 创建一个会返回错误的 handler
	errHandler := EventHandler(func(ctx context.Context, eventType string, eventData map[string]interface{}) (string, error) {
		_, cancel := context.WithCancel(context.Background())
		cancel()
		return "", ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消，使 context.Err() 返回错误
	_, err := errHandler(ctx, EventDeviceOffline, nil)
	if err == nil {
		t.Fatal("期望返回错误但未返回")
	}
	t.Logf("EventHandler 错误返回验证通过: %v", err)
}

// ---------------------------------------------------------------------------

// TestEventHandlerWithData 验证 handler 可以访问事件数据
func TestEventHandlerWithData(t *testing.T) {
	var receivedEvent string
	var receivedData map[string]interface{}
	handler := EventHandler(func(ctx context.Context, eventType string, eventData map[string]interface{}) (string, error) {
		receivedEvent = eventType
		receivedData = eventData
		return "ok", nil
	})
	testData := map[string]interface{}{
		"device_id":   "device_abc",
		"timestamp":   1234567890,
		"extra_field": "extra_value",
	}
	_, err := handler(context.Background(), EventDeviceOnline, testData)
	if err != nil {
		t.Fatalf("handler 调用失败: %v", err)
	}
	if receivedEvent != EventDeviceOnline {
		t.Errorf("事件类型不匹配: 期望 %q, 实际 %q", EventDeviceOnline, receivedEvent)
	}
	if receivedData["device_id"] != "device_abc" {
		t.Error("事件数据 device_id 不匹配")
	}
	t.Log("EventHandler 数据传递验证通过")
}

// ---------------------------------------------------------------------------

// TestEventDirection 验证事件的"方向"语义
// 上行：设备 -> 服务端（push）
// 下行：服务端 -> 设备（pull/response）
func TestEventDirection(t *testing.T) {
	// 上行事件列表
	upstreamEvents := []string{EventDeviceOnline, EventDeviceOffline}
	// 下行事件列表
	downstreamEvents := []string{EventHandleMessageInject}
	t.Log("上行事件（设备推送给后台）:")
	for _, e := range upstreamEvents {
		t.Logf("  %s", e)
	}
	t.Log("下行事件（后台推送给设备）:")
	for _, e := range downstreamEvents {
		t.Logf("  %s", e)
	}
	// 验证没有重复的路径
	allEvents := append(upstreamEvents, downstreamEvents...)
	seen := make(map[string]bool)
	for _, e := range allEvents {
		if seen[e] {
			t.Errorf("事件路径重复: %s", e)
		}
		seen[e] = true
	}
	t.Log("事件方向语义验证通过，无重复路径")
}

// 学习提示：
//  - 事件分"上行"和"下行"两个方向，通过常量路径字符串区分
//  - EventHandler 是 Go 函数类型，可以作为回调存储在 map 中
//  - 上行事件由设备主动推送，下行事件由后台主动下发
