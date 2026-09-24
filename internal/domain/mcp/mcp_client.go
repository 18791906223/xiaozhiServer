package mcp

import (
	"context"
	"fmt"
	"strings"

	log "xiaozhi-esp32-server-golang/logger"

	"github.com/cloudwego/eino/components/tool"
	mcp_go "github.com/mark3labs/mcp-go/mcp"
)

func parseSelectedMCPServiceNames(raw string) map[string]struct{} {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}

	selected := make(map[string]struct{})
	for _, part := range strings.Split(raw, ",") {
		name := strings.TrimSpace(part)
		if name == "" {
			continue
		}
		selected[name] = struct{}{}
	}
	if len(selected) == 0 {
		return nil
	}
	return selected
}

func isGlobalToolAllowed(toolKey string, selected map[string]struct{}) bool {
	if len(selected) == 0 {
		return true
	}
	for serviceName := range selected {
		if strings.HasPrefix(toolKey, serviceName+"_") {
			return true
		}
	}
	return false
}

func filterGlobalToolsBySelectedServices(globalTools map[string]tool.InvokableTool, selectedNames string) map[string]tool.InvokableTool {
	selected := parseSelectedMCPServiceNames(selectedNames)
	if len(selected) == 0 {
		result := make(map[string]tool.InvokableTool, len(globalTools))
		for name, invokable := range globalTools {
			result[name] = invokable
		}
		return result
	}

	result := make(map[string]tool.InvokableTool)
	for toolKey, invokable := range globalTools {
		if isGlobalToolAllowed(toolKey, selected) {
			result[toolKey] = invokable
		}
	}
	return result
}

func GetToolByName(deviceId string, agentId string, toolName string, selectedMCPServiceNames string) (tool.InvokableTool, bool) {
	return GetToolByNameWithTransport(deviceId, agentId, "", toolName, selectedMCPServiceNames)
}

func GetToolByNameWithTransport(deviceId string, agentId string, transportType string, toolName string, selectedMCPServiceNames string) (tool.InvokableTool, bool) {
	// 优先从本地管理器获取
	localManager := GetLocalMCPManager()
	tool, ok := localManager.GetToolByName(toolName)
	if ok {
		return tool, ok
	}

	// 其次从全局管理器获取
	selected := parseSelectedMCPServiceNames(selectedMCPServiceNames)
	globalManager := GetGlobalMCPManager()
	if len(selected) == 0 {
		tool, ok = globalManager.GetToolByName(toolName)
		if ok {
			return tool, ok
		}
	} else {
		globalTools := globalManager.GetAllTools()

		// 兼容直接传入 "server_tool" 的场景
		if invokable, exists := globalTools[toolName]; exists && isGlobalToolAllowed(toolName, selected) {
			return invokable, true
		}

		for serviceName := range selected {
			candidate := serviceName + "_" + toolName
			if invokable, exists := globalTools[candidate]; exists {
				return invokable, true
			}
		}
	}

	// 最后从设备MCP客户端池获取，优先当前 transport 上报的工具
	if transportType = strings.TrimSpace(transportType); transportType != "" {
		deviceClient := mcpClientPool.GetMcpClient(deviceId)
		if deviceClient != nil {
			tool, ok = deviceClient.GetIotToolByTransportAndName(transportType, toolName)
			if ok {
				return tool, true
			}
		}
		if agentId != "" && agentId != deviceId {
			return mcpClientPool.GetToolByDeviceId(agentId, toolName)
		}
		return nil, false
	}

	tool, ok = mcpClientPool.GetToolByDeviceId(deviceId, toolName)
	if !ok && agentId != "" && agentId != deviceId {
		tool, ok = mcpClientPool.GetToolByDeviceId(agentId, toolName)
	}
	return tool, ok
}

func GetDeviceMcpClient(deviceId string) *DeviceMcpSession {
	return mcpClientPool.GetMcpClient(deviceId)
}

func GetOrCreateDeviceMcpClient(deviceId string) *DeviceMcpSession {
	return mcpClientPool.GetOrCreateMcpClient(deviceId)
}

func AddDeviceMcpClient(deviceId string, mcpClient *DeviceMcpSession) error {
	mcpClientPool.AddMcpClient(deviceId, mcpClient)
	return nil
}

func RemoveDeviceMcpClient(deviceId string) error {
	mcpClientPool.RemoveMcpClient(deviceId)
	return nil
}

func ShouldScheduleDeviceIotOverMcp(deviceId string, conn ConnInterface) bool {
	if deviceId = strings.TrimSpace(deviceId); deviceId == "" || conn == nil {
		return false
	}
	transportType := strings.TrimSpace(conn.GetMcpTransportType())
	if transportType == "" {
		return false
	}

	session := GetDeviceMcpClient(deviceId)
	if session == nil {
		return true
	}
	return session.ShouldScheduleIotInit(transportType, conn)
}

// EnsureDeviceIotOverMcp 确保设备侧 IotOverMcp 运行时与 transport 绑定。
// 复用已有连接；当 transport 变化时替换旧连接。
func EnsureDeviceIotOverMcp(deviceId string, conn ConnInterface) error {
	if deviceId == "" || conn == nil {
		return fmt.Errorf("deviceId 或 conn 为空")
	}
	transportType := strings.TrimSpace(conn.GetMcpTransportType())
	if transportType == "" {
		return fmt.Errorf("transportType 为空")
	}

	mcpClientSession := GetOrCreateDeviceMcpClient(deviceId)
	if mcpClientSession == nil {
		return fmt.Errorf("获取或创建设备MCP会话失败")
	}

	transportType = normalizeDeviceTransportType(transportType)

	mcpClientSession.iotMux.Lock()
	existing := mcpClientSession.iotOverMcpByTransport[transportType]
	if existing != nil && existing.conn == conn {
		if existing.IsInitializing() || existing.IsReady() {
			mcpClientSession.iotMux.Unlock()
			return nil
		}
	}

	iotOverMcpClient := NewIotOverMcpClient(deviceId, transportType, conn)
	if iotOverMcpClient == nil {
		mcpClientSession.iotMux.Unlock()
		return fmt.Errorf("创建IotOverMcp客户端失败")
	}
	var old *McpClientInstance
	if existing := mcpClientSession.iotOverMcpByTransport[transportType]; existing != nil && existing != iotOverMcpClient {
		old = existing
	}
	mcpClientSession.iotOverMcpByTransport[transportType] = iotOverMcpClient
	iotOverMcpClient.SetOnCloseHandler(mcpClientSession.handleMcpClientClose)
	mcpClientSession.iotMux.Unlock()
	if old != nil {
		old.closeWithReason("iot_transport_replaced")
	}

	if err := iotOverMcpClient.startIotOverMcp(); err != nil {
		iotOverMcpClient.setInitState(mcpClientInitStateIdle)
		CloseDeviceIotOverMcp(deviceId, conn)
		return fmt.Errorf("初始化IotOverMcp客户端失败: %w", err)
	}
	iotOverMcpClient.setInitState(mcpClientInitStateReady)

	return nil
}

func HandleDeviceIotMcpMessage(deviceId string, transportType string, payload []byte) error {
	mcpClientSession := GetDeviceMcpClient(deviceId)
	if mcpClientSession == nil {
		return nil
	}
	transportType = strings.TrimSpace(transportType)
	if transportType == "" {
		return fmt.Errorf("transportType 为空")
	}

	mcpClientSession.iotMux.RLock()
	iotClient := mcpClientSession.iotOverMcpByTransport[normalizeDeviceTransportType(transportType)]
	mcpClientSession.iotMux.RUnlock()
	if iotClient == nil {
		return nil
	}
	if iotClient.iotTransport != nil {
		// 设备侧入站 MCP 消息已经按 device + transportType 路由到了当前 runtime，
		// 直接注入当前 transport，避免在共享 conn 队列上与历史 runtime 竞争消费。
		iotClient.iotTransport.handleMessage(payload)
		return nil
	}
	if iotClient.conn == nil {
		return nil
	}
	return iotClient.conn.HandleMcpMessage(payload)
}

func CloseDeviceIotOverMcp(deviceId string, conn ConnInterface) {
	mcpClientSession := GetDeviceMcpClient(deviceId)
	if mcpClientSession == nil {
		return
	}
	if conn == nil {
		return
	}

	mcpClientSession.iotMux.Lock()
	transportType := normalizeDeviceTransportType(conn.GetMcpTransportType())
	iotClient := mcpClientSession.iotOverMcpByTransport[transportType]
	if iotClient == nil {
		mcpClientSession.iotMux.Unlock()
		return
	}
	if conn != nil && iotClient.conn != conn {
		mcpClientSession.iotMux.Unlock()
		return
	}
	delete(mcpClientSession.iotOverMcpByTransport, transportType)
	mcpClientSession.iotMux.Unlock()

	iotClient.closeWithReason("device_iot_closed")
}

func GetToolsByDeviceId(deviceId string, agentId string, selectedMCPServiceNames string) (map[string]tool.InvokableTool, error) {
	return GetToolsByDeviceIdWithTransport(deviceId, agentId, "", selectedMCPServiceNames)
}

func GetToolsByDeviceIdWithTransport(deviceId string, agentId string, transportType string, selectedMCPServiceNames string) (map[string]tool.InvokableTool, error) {
	retTools := make(map[string]tool.InvokableTool)

	// 优先从本地管理器获取
	localManager := GetLocalMCPManager()
	localTools := localManager.GetAllTools()
	for toolName, tool := range localTools {
		retTools[toolName] = tool
	}
	log.Infof("从本地管理器获取到 %d 个工具", len(localTools))

	// 其次从全局管理器获取
	globalTools := GetGlobalMCPManager().GetAllTools()
	filteredGlobalTools := filterGlobalToolsBySelectedServices(globalTools, selectedMCPServiceNames)
	for toolName, tool := range filteredGlobalTools {
		// 本地工具优先，如果已存在同名工具则不覆盖
		if _, exists := retTools[toolName]; !exists {
			retTools[toolName] = tool
		}
	}
	log.Infof("从全局管理器获取到 %d 个工具（过滤后）", len(filteredGlobalTools))

	if transportType = strings.TrimSpace(transportType); transportType != "" && deviceId != "" {
		deviceClient := mcpClientPool.GetMcpClient(deviceId)
		if deviceClient != nil {
			for toolName, tool := range deviceClient.GetIotToolsByTransport(transportType) {
				if _, exists := retTools[toolName]; !exists {
					retTools[toolName] = tool
				}
			}
		}
	}

	if transportType == "" {
		deviceTools, err := mcpClientPool.GetAllToolsByDeviceIdAndAgentId(deviceId, agentId)
		if err != nil {
			log.Errorf("获取设备 %s 的工具失败: %v", deviceId, err)
			return retTools, nil
		}
		for toolName, tool := range deviceTools {
			if _, exists := retTools[toolName]; !exists {
				retTools[toolName] = tool
			}
		}
		log.Infof("从设备 %s 获取到 %d 个工具", deviceId, len(deviceTools))
	} else if agentId != "" && agentId != deviceId {
		log.Debugf("开始从智能体 %s 获取 ws endpoint MCP 工具, device=%s, transport=%s", agentId, deviceId, transportType)
		agentTools, err := mcpClientPool.GetWsEndpointMcpTools(agentId)
		if err != nil {
			log.Errorf("获取智能体 %s 的工具失败: %v", agentId, err)
			return retTools, nil
		}
		log.Debugf("从智能体 %s 获取到 %d 个 ws endpoint MCP 工具, device=%s", agentId, len(agentTools), deviceId)
		for toolName, tool := range agentTools {
			if _, exists := retTools[toolName]; !exists {
				retTools[toolName] = tool
			}
		}
	}
	log.Infof("设备 %s 总共获取到 %d 个工具", deviceId, len(retTools))

	return retTools, nil
}

func GetWsEndpointMcpTools(agentId string) (map[string]tool.InvokableTool, error) {
	return mcpClientPool.GetWsEndpointMcpTools(agentId)
}

func GetWsEndpointConnectionStatus(agentId string) (bool, int) {
	if strings.TrimSpace(agentId) == "" {
		return false, 0
	}
	client := mcpClientPool.GetMcpClient(agentId)
	if client == nil {
		return false, 0
	}
	return client.GetWsEndpointConnectionStatus()
}

// GetReportedToolsByDeviceID 获取设备通过 Iot over MCP 上报的工具。
// 控制台设备维度仅返回 websocket / mqtt_udp(udp) transport 下的工具，不混入 ws endpoint 等其它类型。
func GetReportedToolsByDeviceID(deviceId string) (map[string]tool.InvokableTool, error) {
	retTools := make(map[string]tool.InvokableTool)
	if deviceId == "" {
		return retTools, nil
	}

	client := mcpClientPool.GetMcpClient(deviceId)
	if client == nil {
		return retTools, nil
	}

	transportType, resolved := ResolveCurrentDeviceTransport(deviceId)
	if !resolved || transportType == "" {
		return retTools, nil
	}

	for toolName, invokable := range client.GetIotToolsByTransport(transportType) {
		retTools[toolName] = invokable
	}

	return retTools, nil
}

// RefreshReportedToolsByDeviceID 强制向当前在线 transport 发起一次 tools/list。
// 刷新失败时返回空列表，同时清空对应 runtime 的内存工具快照。
func RefreshReportedToolsByDeviceID(deviceId string) (map[string]tool.InvokableTool, error) {
	retTools := make(map[string]tool.InvokableTool)
	if deviceId == "" {
		return retTools, nil
	}

	client := mcpClientPool.GetMcpClient(deviceId)
	if client == nil {
		return retTools, nil
	}

	transportType, resolved := ResolveCurrentDeviceTransport(deviceId)
	if !resolved || transportType == "" {
		return retTools, nil
	}

	return client.RefreshIotToolsByTransport(transportType)
}

// GetReportedToolsByAgentID 仅获取智能体(WebSocket端点)上报的MCP工具
func GetReportedToolsByAgentID(agentId string) (map[string]tool.InvokableTool, error) {
	retTools := make(map[string]tool.InvokableTool)
	if agentId == "" {
		return retTools, nil
	}

	return mcpClientPool.GetWsEndpointMcpTools(agentId)
}

// RefreshReportedToolsByAgentID 强制向智能体的 ws endpoint 发起一次 tools/list。
// 刷新失败时返回空列表，同时清空对应 runtime 的内存工具快照。
func RefreshReportedToolsByAgentID(agentId string) (map[string]tool.InvokableTool, error) {
	retTools := make(map[string]tool.InvokableTool)
	if agentId == "" {
		return retTools, nil
	}

	client := mcpClientPool.GetMcpClient(agentId)
	if client == nil {
		return retTools, nil
	}

	return client.RefreshWsEndpointTools()
}

// GetReportedToolByDeviceIDAndName 仅在设备上报工具中查找
func GetReportedToolByDeviceIDAndName(deviceId, toolName string) (tool.InvokableTool, bool) {
	if deviceId == "" {
		return nil, false
	}

	client := mcpClientPool.GetMcpClient(deviceId)
	if client == nil {
		return nil, false
	}

	transportType, resolved := ResolveCurrentDeviceTransport(deviceId)
	if !resolved || transportType == "" {
		return nil, false
	}

	invokable, ok := client.GetIotToolByTransportAndName(transportType, toolName)
	return invokable, ok
}

// GetReportedToolByAgentIDAndName 仅在智能体上报工具中查找
func GetReportedToolByAgentIDAndName(agentId, toolName string) (tool.InvokableTool, bool) {
	reportedTools, err := GetReportedToolsByAgentID(agentId)
	if err != nil {
		log.Errorf("获取智能体上报MCP工具失败: agent=%s err=%v", agentId, err)
		return nil, false
	}

	return findInvokableToolByName(reportedTools, toolName)
}

func RawCallReportedToolByDeviceID(deviceId, toolName string, arguments map[string]interface{}) (string, bool, error) {
	if deviceId == "" {
		return "", false, nil
	}

	client := mcpClientPool.GetMcpClient(deviceId)
	if client == nil {
		return "", false, nil
	}

	transportType, resolved := ResolveCurrentDeviceTransport(deviceId)
	if !resolved || transportType == "" {
		return "", false, nil
	}

	return client.RawCallIotToolByTransport(context.Background(), transportType, toolName, arguments)
}

func RawCallReportedToolByAgentID(agentId, toolName string, arguments map[string]interface{}) (string, bool, error) {
	if agentId == "" {
		return "", false, nil
	}

	client := mcpClientPool.GetMcpClient(agentId)
	if client == nil {
		return "", false, nil
	}

	return client.RawCallWsEndpointTool(context.Background(), toolName, arguments)
}

// GetReportedToolsByDeviceIdAndAgentId 兼容方法：明确分流设备/智能体查询，不再混用
func GetReportedToolsByDeviceIdAndAgentId(deviceId string, agentId string) (map[string]tool.InvokableTool, error) {
	if deviceId != "" {
		return GetReportedToolsByDeviceID(deviceId)
	}
	if agentId != "" {
		return GetReportedToolsByAgentID(agentId)
	}
	return make(map[string]tool.InvokableTool), nil
}

// GetReportedToolByName 兼容方法：按维度分流，不再混用
func GetReportedToolByName(deviceId string, agentId string, toolName string) (tool.InvokableTool, bool) {
	if deviceId != "" {
		return GetReportedToolByDeviceIDAndName(deviceId, toolName)
	}
	if agentId != "" {
		return GetReportedToolByAgentIDAndName(agentId, toolName)
	}
	return nil, false
}

func GetAudioResourceByTool(tool McpTool, resourceLink mcp_go.ResourceLink) (mcp_go.ReadResourceResult, error) {
	/*client := tool.GetClient()
	resourceRequest := mcp_go.ReadResourceRequest{
		Request: mcp_go.Request{
			Params: mcp_go.ReadResourceParams{
				URI: resourceLink.URL,
			},
		},
	}
	client.ReadResource(context.Background(), resourceRequest)*/
	return mcp_go.ReadResourceResult{}, nil
}
