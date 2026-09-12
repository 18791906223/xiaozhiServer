package util

import (
	"strings"

	"github.com/spf13/viper"
)

const DefaultManagerAuthToken = "xiaozhi_admin_secret_key"
const DefaultManagerEndpointAuthToken = "xiaozhi_mcp_openclaw_secret_key"

// GetManagerAuthToken 获取主程序与控制台之间通用的内部调用鉴权 Token。
// 优先级：
// 1. manager.auth_token
// 2. 默认值（两端保持一致）
func GetManagerAuthToken() string {
	if token := strings.TrimSpace(viper.GetString("manager.auth_token")); token != "" {
		return token
	}
	return DefaultManagerAuthToken
}

// GetManagerEndpointAuthToken 获取 MCP/OpenClaw 端点 JWT 的签名/校验 Token。
// 优先级：
// 1. manager.endpoint_auth_token
// 2. 默认值（需与控制台保持一致）
func GetManagerEndpointAuthToken() string {
	if token := strings.TrimSpace(viper.GetString("manager.endpoint_auth_token")); token != "" {
		return token
	}
	return DefaultManagerEndpointAuthToken // xiaozhi_mcp_openclaw_secret_key
}
