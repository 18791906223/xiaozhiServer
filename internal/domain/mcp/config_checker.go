package mcp

import (
	"fmt"
	"net/url"
	"strings"

	log "xiaozhi-esp32-server-golang/logger"

	"github.com/spf13/viper"
)

// CheckMCPConfig 检查MCP配置并报告潜在问题
func CheckMCPConfig() {
	log.Info("=== MCP配置检查 ===")

	// 检查全局启用状态
	globalEnabled := viper.GetBool("mcp.global.enabled")
	log.Infof("全局MCP启用状态: %v", globalEnabled)

	if !globalEnabled {
		log.Info("全局MCP已禁用，配置检查完成")
		return
	}

	// 检查重连配置
	reconnectInterval := viper.GetInt("mcp.global.reconnect_interval")
	maxAttempts := viper.GetInt("mcp.global.max_reconnect_attempts")
	log.Infof("重连配置: 间隔=%d秒, 最大尝试次数=%d", reconnectInterval, maxAttempts)

	// 检查服务器配置
	var serverConfigs []MCPServerConfig
	if err := viper.UnmarshalKey("mcp.global.servers", &serverConfigs); err != nil {
		log.Errorf("❌ 解析MCP服务器配置失败: %v", err)
		return
	}

	if len(serverConfigs) == 0 {
		log.Warn("⚠️  未配置任何MCP服务器")
		return
	}

	log.Infof("共配置了 %d 个MCP服务器:", len(serverConfigs))

	enabledCount := 0
	problemCount := 0

	for i, config := range serverConfigs {
		status := "✅"
		issues := []string{}

		// 检查名称
		if config.Name == "" {
			status = "❌"
			issues = append(issues, "名称为空")
			problemCount++
		}

		transportType, endpoint, err := endpointForConfig(config)
		if err != nil {
			status = "❌"
			issues = append(issues, err.Error())
			problemCount++
		} else {
			if _, parseErr := url.ParseRequestURI(endpoint); parseErr != nil {
				status = "❌"
				issues = append(issues, "URL格式不正确")
				problemCount++
			}
			if transportType == "sse" && !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
				status = "⚠️"
				issues = append(issues, "SSE URL格式可能不正确")
			}
		}

		// 检查启用状态
		if config.Enabled {
			enabledCount++
		}

		// 输出检查结果
		issueStr := ""
		if len(issues) > 0 {
			issueStr = fmt.Sprintf(" - 问题: %s", strings.Join(issues, ", "))
		}

		log.Infof("  [%d] %s %s (URL: %s, 启用: %v)%s",
			i+1, status, config.Name, endpointForLog(config), config.Enabled, issueStr)
	}

	// 总结
	log.Infof("配置检查完成: %d个服务器已启用, %d个存在问题", enabledCount, problemCount)

	if problemCount > 0 {
		log.Warn("⚠️  发现配置问题，请检查上述错误并修复")
	}

	log.Info("=== MCP配置检查完成 ===")
}

func endpointForLog(config MCPServerConfig) string {
	_, endpoint, err := endpointForConfig(config)
	if err != nil {
		if strings.TrimSpace(config.Url) != "" {
			return config.Url
		}
		return config.SSEUrl
	}
	return endpoint
}
