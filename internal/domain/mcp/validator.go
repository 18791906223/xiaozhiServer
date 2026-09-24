package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/client"
	mcpgo "github.com/mark3labs/mcp-go/mcp"
)

const validateTimeout = 25 * time.Second

// ValidateMCPConfigMap 对传入的 mcp 配置执行连接级预检（initialize + tools/list）。
func ValidateMCPConfigMap(mcpConfig map[string]interface{}) error {
	if mcpConfig == nil {
		return fmt.Errorf("mcp 配置为空")
	}

	global := asAnyMap(mcpConfig["global"])
	if global == nil {
		return fmt.Errorf("mcp.global 配置缺失")
	}

	enabled := toBool(global["enabled"])
	if !enabled {
		return nil
	}

	servers, err := decodeServerConfigs(global["servers"])
	if err != nil {
		return fmt.Errorf("解析 mcp.global.servers 失败: %w", err)
	}
	if len(servers) == 0 {
		return fmt.Errorf("mcp.global.enabled=true 但 servers 为空")
	}

	return ValidateServerConfigs(servers)
}

// ValidateServerConfigs 校验服务器配置可用性。
func ValidateServerConfigs(serverConfigs []MCPServerConfig) error {
	if len(serverConfigs) == 0 {
		return fmt.Errorf("未提供任何MCP服务器配置")
	}

	errs := make([]string, 0)
	enabledCount := 0
	for _, cfg := range serverConfigs {
		if !cfg.Enabled {
			continue
		}
		enabledCount++
		if err := validateSingleServer(cfg); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", cfg.Name, err))
		}
	}

	if enabledCount == 0 {
		return fmt.Errorf("没有启用的MCP服务器")
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}

func validateSingleServer(config MCPServerConfig) error {
	transportInstance, endpoint, err := buildMCPTransport(config)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), validateTimeout)
	defer cancel()

	mcpClient := client.NewClient(transportInstance)
	defer mcpClient.Close()

	if err := mcpClient.Start(ctx); err != nil {
		return fmt.Errorf("启动失败(%s): %w", endpoint, err)
	}

	initReq := mcpgo.InitializeRequest{
		Params: mcpgo.InitializeParams{
			ProtocolVersion: mcpgo.LATEST_PROTOCOL_VERSION,
			ClientInfo: mcpgo.Implementation{
				Name:    "xiaozhi-mcp-validator",
				Version: "1.0.0",
			},
			Capabilities: mcpgo.ClientCapabilities{
				Experimental: make(map[string]any),
			},
		},
	}

	if _, err := mcpClient.Initialize(ctx, initReq); err != nil {
		return fmt.Errorf("initialize 失败: %w", err)
	}

	if _, err := mcpClient.ListTools(ctx, mcpgo.ListToolsRequest{}); err != nil {
		return fmt.Errorf("tools/list 失败: %w", err)
	}

	return nil
}

func decodeServerConfigs(v interface{}) ([]MCPServerConfig, error) {
	if v == nil {
		return []MCPServerConfig{}, nil
	}
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var ret []MCPServerConfig
	if err := json.Unmarshal(body, &ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func asAnyMap(v interface{}) map[string]interface{} {
	if v == nil {
		return nil
	}
	ret, _ := v.(map[string]interface{})
	return ret
}

func toBool(v interface{}) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		return strings.EqualFold(strings.TrimSpace(t), "true")
	case float64:
		return t != 0
	case int:
		return t != 0
	default:
		return false
	}
}
