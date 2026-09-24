package llm

import (
	"context"

	log "xiaozhi-esp32-server-golang/logger"

	"github.com/cloudwego/eino/schema"
)

// ConvertMCPToolsToEinoTools 将MCP工具转换为Eino ToolInfo格式
func ConvertMCPToolsToEinoTools(ctx context.Context, mcpTools map[string]interface{}) ([]*schema.ToolInfo, error) {
	var einoTools []*schema.ToolInfo

	for toolName, mcpTool := range mcpTools {
		// 尝试获取工具信息
		if invokableTool, ok := mcpTool.(interface {
			Info(context.Context) (*schema.ToolInfo, error)
		}); ok {
			toolInfo, err := invokableTool.Info(ctx)
			if err != nil {
				log.Errorf("获取工具 %s 信息失败: %v", toolName, err)
				continue
			}
			einoTools = append(einoTools, toolInfo)
		} else {
			log.Warnf("工具 %s 不支持Info接口，跳过转换", toolName)
		}
	}

	log.Infof("成功转换了 %d 个MCP工具为Eino工具", len(einoTools))
	return einoTools, nil
}
