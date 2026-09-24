package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestConfigManager_GetSystemConfig(t *testing.T) {
	// 创建配置管理器
	config := map[string]interface{}{
		"backend_url": "http://192.168.208.214:8080", // 根据实际backend地址调整
	}

	manager, err := NewManagerUserConfigProvider(config)
	if err != nil {
		t.Fatalf("创建配置管理器失败: %v", err)
	}

	// 创建上下文
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 获取系统配置
	configJSON, err := manager.GetSystemConfig(ctx)
	if err != nil {
		t.Fatalf("获取系统配置失败: %v", err)
	}

	// 验证返回的JSON格式
	var configMap map[string]interface{}
	if err := json.Unmarshal([]byte(configJSON), &configMap); err != nil {
		t.Fatalf("解析配置JSON失败: %v", err)
	}

	// 检查是否包含预期的配置项
	expectedKeys := []string{"mqtt", "mqtt_server", "udp", "ota"}
	for _, key := range expectedKeys {
		if _, exists := configMap[key]; !exists {
			t.Errorf("配置中缺少预期的键: %s", key)
		}
	}

	fmt.Printf("获取到的系统配置: %s\n", configJSON)
	t.Logf("配置大小: %d 字节", len(configJSON))
}
