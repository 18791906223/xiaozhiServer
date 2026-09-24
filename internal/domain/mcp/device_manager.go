package mcp

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"xiaozhi-esp32-server-golang/logger"

	"github.com/cloudwego/eino/components/tool"
	"github.com/gorilla/websocket"
	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/mcp"
)

const (
	deviceMCPPingInterval          = 2 * time.Minute
	wsEndpointToolsRefreshInterval = 10 * time.Minute
	heartbeatRefreshFailureLimit   = 5
	mcpConnectionOfflineTimeout    = 3 * deviceMCPPingInterval
)

// DeviceMcpSession 代表一个设备的MCP会话，聚合了多种MCP连接
type DeviceMcpSession struct {
	deviceID              string
	Ctx                   context.Context
	cancel                context.CancelFunc
	wsEndPointMcp         sync.Map
	iotOverMcpByTransport map[string]*McpClientInstance
	iotMux                sync.RWMutex
}

type mcpClientInitState uint32

const (
	mcpClientInitStateIdle mcpClientInitState = iota
	mcpClientInitStateInitializing
	mcpClientInitStateReady
)

func normalizeDeviceTransportType(transportType string) string {
	transportType = strings.TrimSpace(transportType)
	if transportType == "" {
		return "unknown"
	}
	return transportType
}

func buildIotServerName(deviceID, transportType string) string {
	return fmt.Sprintf("iot_over_mcp_%s_%s", deviceID, normalizeDeviceTransportType(transportType))
}

func (dcs *DeviceMcpSession) AddWsEndPointMcp(mcpClient *McpClientInstance) {
	dcs.wsEndPointMcp.Store(mcpClient.serverName, mcpClient)

	// 设置关闭回调
	mcpClient.SetOnCloseHandler(dcs.handleMcpClientClose)

	mcpClient.refreshTools()
}

func (dcs *DeviceMcpSession) SetIotOverMcp(transportType string, mcpClient *McpClientInstance) {
	transportType = normalizeDeviceTransportType(transportType)
	if mcpClient != nil {
		mcpClient.SetOnCloseHandler(dcs.handleMcpClientClose)
	}

	var old *McpClientInstance
	dcs.iotMux.Lock()
	// 同 device + transportType 保持单实例
	if existing := dcs.iotOverMcpByTransport[transportType]; existing != nil && existing != mcpClient {
		old = existing
	}
	dcs.iotOverMcpByTransport[transportType] = mcpClient
	dcs.iotMux.Unlock()

	// 锁外关闭旧实例，避免在会话锁里执行取消逻辑。
	if old != nil {
		old.closeWithReason("iot_transport_replaced")
	}
}

func (dcs *DeviceMcpSession) RemoveWsEndPointMcp(mcpClient *McpClientInstance) {
	dcs.wsEndPointMcp.Delete(mcpClient.serverName)
}

// GetDeviceID 获取设备ID
func (dcs *DeviceMcpSession) GetDeviceID() string {
	return dcs.deviceID
}

// handleMcpClientClose 处理MCP客户端关闭事件
func (dcs *DeviceMcpSession) handleMcpClientClose(instance *McpClientInstance, reason string) {
	logger.Infof("设备 %s 的MCP客户端 %s 已关闭，原因: %s", dcs.deviceID, instance.serverName, reason)

	// 从会话中移除已关闭的客户端
	dcs.RemoveWsEndPointMcp(instance)
	dcs.removeIotOverMcpByInstance(instance)

	if !dcs.hasAnyClient() {
		logger.Infof("设备 %s 的所有MCP连接已关闭，清理会话", dcs.deviceID)
		dcs.cancel()
		mcpClientPool.RemoveMcpClient(dcs.deviceID)
	}
}

func (dcs *DeviceMcpSession) removeIotOverMcpByInstance(instance *McpClientInstance) {
	dcs.iotMux.Lock()
	defer dcs.iotMux.Unlock()
	for transportType, iotClient := range dcs.iotOverMcpByTransport {
		if iotClient == instance {
			delete(dcs.iotOverMcpByTransport, transportType)
		}
	}
}

func (dcs *DeviceMcpSession) hasAnyClient() bool {
	hasWsClient := false
	dcs.wsEndPointMcp.Range(func(_, _ interface{}) bool {
		hasWsClient = true
		return false
	})
	if hasWsClient {
		return true
	}

	dcs.iotMux.RLock()
	defer dcs.iotMux.RUnlock()
	return len(dcs.iotOverMcpByTransport) > 0
}

// McpClientInstance 代表一个具体的MCP客户端连接
type McpClientInstance struct {
	serverName       string
	mcpClient        *client.Client // 是从ws endpoint连上来的mcp server
	tools            map[string]tool.InvokableTool
	toolsState       atomic.Value // map[string]tool.InvokableTool，刷新时整体替换，读路径走快照
	serverInfo       *mcp.InitializeResult
	Ctx              context.Context
	cancel           context.CancelFunc
	conn             ConnInterface
	iotTransport     *IotOverMcpTransport
	initState        uint32
	lastPing         atomic.Int64
	lastToolsRefresh atomic.Int64
	refreshFailures  atomic.Int32
	connected        atomic.Bool

	// 添加关闭回调
	onCloseHandler func(instance *McpClientInstance, reason string)
	closed         atomic.Bool
}

// NewDeviceMCPClient 创建新的MCP客户端
func NewDeviceMCPSession(deviceID string) *DeviceMcpSession {
	ctx, cancel := context.WithCancel(context.Background())

	deviceMcpClient := &DeviceMcpSession{
		deviceID:              deviceID,
		Ctx:                   ctx,
		cancel:                cancel,
		iotOverMcpByTransport: make(map[string]*McpClientInstance),
		iotMux:                sync.RWMutex{},
		// wsEndPointMcp: make(map[string]*McpClientInstance),
	}

	go deviceMcpClient.refreshToolsAndPing()

	return deviceMcpClient
}

func NewWsEndPointMcpClient(ctx context.Context, deviceID string, conn *websocket.Conn) *McpClientInstance {
	ctx, cancel := context.WithCancel(ctx)

	wsTransport, err := NewWebsocketTransport(conn)
	if err != nil {
		logger.Errorf("创建MCP客户端失败: %v", err)
		return nil
	}
	mcpClient := client.NewClient(wsTransport)

	wsEndPointMcp := &McpClientInstance{
		serverName: fmt.Sprintf("ws_endpoint_mcp_%s_%s", deviceID, conn.RemoteAddr().String()),
		mcpClient:  mcpClient,
		Ctx:        ctx,
		cancel:     cancel,
		initState:  uint32(mcpClientInitStateReady),
	}
	wsEndPointMcp.storeToolsSnapshot(make(map[string]tool.InvokableTool))
	wsEndPointMcp.setConnected(true)
	wsEndPointMcp.setLastPing(time.Now())
	mcpClient.OnNotification(wsEndPointMcp.handleJSONRPCNotification)

	// 设置transport的关闭回调
	wsTransport.SetOnCloseHandler(wsEndPointMcp.handleTransportClose)

	wsEndPointMcp.sendInitlize(ctx)
	wsEndPointMcp.mcpClient.Start(ctx)
	return wsEndPointMcp
}

func NewIotOverMcpClient(deviceID string, transportType string, conn ConnInterface) *McpClientInstance {
	ctx, cancel := context.WithCancel(context.Background())

	iotTransport, err := NewIotOverMcpTransport(conn)
	if err != nil {
		logger.Errorf("创建MCP客户端失败: %v", err)
		return nil
	}
	mcpClient := client.NewClient(iotTransport)

	iotOverMcp := &McpClientInstance{
		serverName:   buildIotServerName(deviceID, transportType),
		mcpClient:    mcpClient,
		Ctx:          ctx,
		cancel:       cancel,
		conn:         conn,
		iotTransport: iotTransport,
		initState:    uint32(mcpClientInitStateInitializing),
	}
	iotOverMcp.storeToolsSnapshot(make(map[string]tool.InvokableTool))
	iotOverMcp.setConnected(true)
	iotOverMcp.setLastPing(time.Now())
	iotTransport.SetNotificationHandler(iotOverMcp.handleJSONRPCNotification)
	iotTransport.SetActivityHandler(func() {
		iotOverMcp.setLastPing(time.Now())
	})

	// 设置transport的关闭回调
	iotTransport.SetOnCloseHandler(iotOverMcp.handleTransportClose)

	return iotOverMcp
}

func (dc *McpClientInstance) startIotOverMcp() error {
	if err := dc.sendInitlize(dc.Ctx); err != nil {
		return err
	}
	dc.mcpClient.Start(dc.Ctx)
	return dc.refreshTools()
}

// refreshToolsCommon 通用的工具列表刷新逻辑
func (dc *McpClientInstance) refreshTools() error {
	_, err := dc.refreshToolsWithPolicy(false)
	return err
}

func (dc *McpClientInstance) refreshToolsStrict() (map[string]tool.InvokableTool, error) {
	return dc.refreshToolsWithPolicy(true)
}

func (dc *McpClientInstance) refreshToolsWithPolicy(clearOnFailure bool) (map[string]tool.InvokableTool, error) {
	emptyTools := make(map[string]tool.InvokableTool)
	if dc == nil || dc.mcpClient == nil {
		err := fmt.Errorf("mcp client未初始化")
		if clearOnFailure {
			dc.clearToolsSnapshot()
		}
		return emptyTools, err
	}
	if dc.serverInfo == nil {
		err := fmt.Errorf("client not initialized")
		if clearOnFailure {
			dc.clearToolsSnapshot()
		}
		return emptyTools, err
	}

	tools, err := dc.mcpClient.ListTools(dc.Ctx, mcp.ListToolsRequest{})
	if err != nil {
		logger.Errorf("刷新工具列表失败: %v", err)
		if clearOnFailure {
			dc.clearToolsSnapshot()
			logger.Warnf("刷新工具列表失败，已清空 %s 的内存工具快照", dc.serverName)
		}
		return emptyTools, err
	}

	// 工具转换可能比较重，先在锁外完成，避免阻塞读工具列表的路径。
	convertedTools := ConvertMcpToolListToInvokableToolList(tools.Tools, dc.serverName, dc.mcpClient)

	dc.storeToolsSnapshot(convertedTools)
	dc.setLastPing(time.Now())
	dc.setLastToolsRefresh(time.Now())
	dc.resetRefreshFailures()

	logger.Infof("刷新工具列表成功: %s 获取到 %d 个工具", dc.serverName, len(convertedTools))
	return convertedTools, nil
}

func (dc *McpClientInstance) GetServerName() string {
	return dc.serverName
}

func (dc *McpClientInstance) IsInitialized() bool {
	return dc != nil && dc.serverInfo != nil
}

func (dc *McpClientInstance) storeToolsSnapshot(tools map[string]tool.InvokableTool) {
	if dc == nil {
		return
	}
	if tools == nil {
		tools = make(map[string]tool.InvokableTool)
	}
	dc.tools = tools
	dc.toolsState.Store(tools)
}

func (dc *McpClientInstance) clearToolsSnapshot() {
	if dc == nil {
		return
	}
	dc.storeToolsSnapshot(make(map[string]tool.InvokableTool))
	dc.setLastToolsRefresh(time.Time{})
}

func (dc *McpClientInstance) loadToolsSnapshot() map[string]tool.InvokableTool {
	if dc == nil {
		return nil
	}
	if snapshot := dc.toolsState.Load(); snapshot != nil {
		if tools, ok := snapshot.(map[string]tool.InvokableTool); ok {
			return tools
		}
	}
	return dc.tools
}

func (dc *McpClientInstance) copyToolsInto(dst map[string]tool.InvokableTool) {
	if dc == nil {
		return
	}
	for name, invokable := range dc.loadToolsSnapshot() {
		dst[name] = invokable
	}
}

func (dc *McpClientInstance) toolCount() int {
	return len(dc.loadToolsSnapshot())
}

func (dc *McpClientInstance) getToolByName(toolName string) (tool.InvokableTool, bool) {
	tools := dc.loadToolsSnapshot()
	return findInvokableToolByName(tools, toolName)
}

func (dc *McpClientInstance) setConnected(connected bool) {
	if dc == nil {
		return
	}
	dc.connected.Store(connected)
}

func (dc *McpClientInstance) setLastPing(ts time.Time) {
	if dc == nil {
		return
	}
	if ts.IsZero() {
		dc.lastPing.Store(0)
		return
	}
	dc.lastPing.Store(ts.UnixNano())
}

func (dc *McpClientInstance) setLastToolsRefresh(ts time.Time) {
	if dc == nil {
		return
	}
	if ts.IsZero() {
		dc.lastToolsRefresh.Store(0)
		return
	}
	dc.lastToolsRefresh.Store(ts.UnixNano())
}

func (dc *McpClientInstance) incrementRefreshFailures() int32 {
	if dc == nil {
		return 0
	}
	return dc.refreshFailures.Add(1)
}

func (dc *McpClientInstance) resetRefreshFailures() {
	if dc == nil {
		return
	}
	dc.refreshFailures.Store(0)
}

func (dc *McpClientInstance) RefreshFailureCount() int32 {
	if dc == nil {
		return 0
	}
	return dc.refreshFailures.Load()
}

func (dc *McpClientInstance) LastPing() time.Time {
	if dc == nil {
		return time.Time{}
	}
	unixNano := dc.lastPing.Load()
	if unixNano == 0 {
		return time.Time{}
	}
	return time.Unix(0, unixNano)
}

func (dc *McpClientInstance) LastToolsRefresh() time.Time {
	if dc == nil {
		return time.Time{}
	}
	unixNano := dc.lastToolsRefresh.Load()
	if unixNano == 0 {
		return time.Time{}
	}
	return time.Unix(0, unixNano)
}

func (dc *McpClientInstance) getInitState() mcpClientInitState {
	if dc == nil {
		return mcpClientInitStateIdle
	}
	return mcpClientInitState(atomic.LoadUint32(&dc.initState))
}

func (dc *McpClientInstance) setInitState(state mcpClientInitState) {
	if dc == nil {
		return
	}
	atomic.StoreUint32(&dc.initState, uint32(state))
}

func (dc *McpClientInstance) IsInitializing() bool {
	return dc.getInitState() == mcpClientInitStateInitializing
}

func (dc *McpClientInstance) IsReady() bool {
	return dc.getInitState() == mcpClientInitStateReady
}

func (dc *McpClientInstance) closeWithReason(reason string) {
	if dc == nil {
		return
	}
	if !dc.closed.CompareAndSwap(false, true) {
		return
	}
	logger.Infof("MCP客户端 %s 关闭，原因: %s", dc.serverName, reason)

	dc.setConnected(false)
	dc.setInitState(mcpClientInitStateIdle)
	if dc.cancel != nil {
		dc.cancel()
	}
	if dc.mcpClient != nil && reason != "connection_closed" && reason != "manual_close" {
		if err := dc.mcpClient.Close(); err != nil {
			logger.Warnf("关闭MCP客户端 %s transport 失败: %v", dc.serverName, err)
		}
	}

	if dc.onCloseHandler != nil {
		dc.onCloseHandler(dc, reason)
	}
}

func (dc *DeviceMcpSession) snapshotWsEndpointClients() []*McpClientInstance {
	clients := make([]*McpClientInstance, 0)
	dc.wsEndPointMcp.Range(func(_, value interface{}) bool {
		mcpInstance, ok := value.(*McpClientInstance)
		if ok && mcpInstance != nil {
			clients = append(clients, mcpInstance)
		}
		return true
	})
	return clients
}

func (dc *DeviceMcpSession) GetWsEndpointConnectionStatus() (bool, int) {
	connectedCount := 0
	for _, mcpInstance := range dc.snapshotWsEndpointClients() {
		if mcpInstance != nil && mcpInstance.IsConnected() && mcpInstance.IsInitialized() {
			connectedCount++
		}
	}
	return connectedCount > 0, connectedCount
}

func (dc *DeviceMcpSession) snapshotIotClients() []*McpClientInstance {
	dc.iotMux.RLock()
	defer dc.iotMux.RUnlock()

	clients := make([]*McpClientInstance, 0, len(dc.iotOverMcpByTransport))
	for _, instance := range dc.iotOverMcpByTransport {
		if instance != nil {
			clients = append(clients, instance)
		}
	}
	return clients
}

type iotTransportClientSnapshot struct {
	transportType string
	client        *McpClientInstance
}

func (dc *DeviceMcpSession) snapshotIotTransports() []iotTransportClientSnapshot {
	dc.iotMux.RLock()
	defer dc.iotMux.RUnlock()

	clients := make([]iotTransportClientSnapshot, 0, len(dc.iotOverMcpByTransport))
	for transportType, instance := range dc.iotOverMcpByTransport {
		if instance != nil {
			clients = append(clients, iotTransportClientSnapshot{
				transportType: transportType,
				client:        instance,
			})
		}
	}
	return clients
}

func (dc *DeviceMcpSession) heartbeatMcpInstance(mcpInstance *McpClientInstance) {
	if mcpInstance == nil || !mcpInstance.IsInitialized() {
		return
	}
	if mcpInstance.conn != nil {
		if err := mcpInstance.refreshTools(); err != nil {
			dc.handleHeartbeatRefreshFailure(mcpInstance, err)
			return
		}
		logger.Debugf("设备 %s 通过 tools/list 心跳维持 IoT MCP 存活", mcpInstance.serverName)
		return
	}
	err := mcpInstance.mcpClient.Ping(mcpInstance.Ctx)
	if err == nil {
		mcpInstance.setLastPing(time.Now())
		logger.Debugf("设备 %s ping成功", mcpInstance.serverName)
	} else {
		logger.Warnf("设备 %s ping失败: %v", mcpInstance.serverName, err)
	}
	if lastRefresh := mcpInstance.LastToolsRefresh(); lastRefresh.IsZero() || time.Since(lastRefresh) >= wsEndpointToolsRefreshInterval {
		if err := mcpInstance.refreshTools(); err != nil {
			dc.handleHeartbeatRefreshFailure(mcpInstance, err)
			return
		}
	}
}

func (dc *DeviceMcpSession) handleHeartbeatRefreshFailure(mcpInstance *McpClientInstance, err error) {
	failures := mcpInstance.incrementRefreshFailures()
	if failures < heartbeatRefreshFailureLimit {
		logger.Warnf(
			"设备 %s 心跳刷新工具列表失败(%d/%d)，暂不销毁 runtime: %v",
			mcpInstance.serverName,
			failures,
			heartbeatRefreshFailureLimit,
			err,
		)
		return
	}

	logger.Warnf(
		"设备 %s 心跳刷新工具列表连续失败 %d 次，主动销毁 runtime: %v",
		mcpInstance.serverName,
		failures,
		err,
	)
	mcpInstance.closeWithReason("refresh_tools_failed")
}

func (dc *DeviceMcpSession) refreshToolsAndPing() {
	// 只在初始化时获取一次工具列表
	findTools := func(mcpInstance *McpClientInstance) {
		if mcpInstance == nil || !mcpInstance.IsInitialized() {
			return
		}
		mcpInstance.refreshTools()
	}

	// 初始化时获取工具列表
	for _, instance := range dc.snapshotWsEndpointClients() {
		findTools(instance)
	}

	for _, instance := range dc.snapshotIotClients() {
		findTools(instance)
	}

	// 每2分钟进行一次心跳
	pingTick := time.NewTicker(deviceMCPPingInterval)
	defer pingTick.Stop()

	for {
		select {
		case <-dc.Ctx.Done():
			logger.Infof("设备 %s 会话已取消，停止ping", dc.deviceID)
			return
		case <-pingTick.C:
			for _, instance := range dc.snapshotWsEndpointClients() {
				dc.heartbeatMcpInstance(instance)
			}
			for _, instance := range dc.snapshotIotClients() {
				dc.heartbeatMcpInstance(instance)
			}
		}
	}
}

func (dc *McpClientInstance) sendInitlize(ctx context.Context) error {
	initRequest := mcp.InitializeRequest{
		Params: mcp.InitializeParams{
			ProtocolVersion: mcp.LATEST_PROTOCOL_VERSION,
			ClientInfo: mcp.Implementation{
				Name:    "mcp-go",
				Version: "0.1.0",
			},
			Capabilities: mcp.ClientCapabilities{},
		},
	}

	serverInfo, err := dc.mcpClient.Initialize(ctx, initRequest)
	if err != nil {
		fmt.Printf("Failed to initialize: %v", err)
		return err
	}
	dc.serverInfo = serverInfo
	return nil
}

func (dc *McpClientInstance) findTools() (*mcp.ListToolsResult, error) {
	tools, err := dc.mcpClient.ListTools(dc.Ctx, mcp.ListToolsRequest{})
	if err != nil {
		logger.Errorf("获取工具列表失败: %v", err)
		return nil, err
	}
	return tools, nil
}

// handleJSONRPCNotification 处理JSON-RPC通知
func (dc *McpClientInstance) handleJSONRPCNotification(notification mcp.JSONRPCNotification) {
	switch notification.Method {
	case "notifications/progress":
		//handleProgressNotification(notification)
	case "notifications/message":
		//handleMessageNotification(notification)
	case "notifications/resources/updated":
		//handleResourceUpdateNotification(notification)
	case "notifications/tools/updated":
		// 收到工具更新通知，刷新工具列表
		logger.Infof("收到工具更新通知，刷新工具列表")
		go dc.refreshToolsOnNotification()
	default:
		log.Printf("Unknown notification: %s", notification.Method)
	}
}

// refreshToolsOnNotification 基于通知刷新工具列表
func (dc *McpClientInstance) refreshToolsOnNotification() {
	// 添加短暂延迟避免频繁刷新
	time.Sleep(100 * time.Millisecond)
	dc.refreshTools()
}

// handleJSONRPCError 处理JSON-RPC错误
func (dc *McpClientInstance) handleJSONRPCError(errMsg mcp.JSONRPCError) error {
	logger.Errorf("收到MCP服务器错误: %+v", errMsg.Error)
	return nil
}

// handleTransportClose 处理transport层关闭事件
func (dc *McpClientInstance) handleTransportClose(reason string) {
	dc.closeWithReason(reason)
}

// SetOnCloseHandler 设置关闭回调
func (dc *McpClientInstance) SetOnCloseHandler(handler func(instance *McpClientInstance, reason string)) {
	dc.onCloseHandler = handler
}

// IsConnected 检查连接是否仍然活跃
func (dc *McpClientInstance) IsConnected() bool {
	if dc == nil {
		return false
	}
	return dc.connected.Load()
}

func (dc *DeviceMcpSession) ShouldScheduleIotInit(transportType string, conn ConnInterface) bool {
	transportType = normalizeDeviceTransportType(transportType)
	if transportType == "unknown" || conn == nil {
		return false
	}

	dc.iotMux.RLock()
	existing := dc.iotOverMcpByTransport[transportType]
	dc.iotMux.RUnlock()
	if existing == nil {
		return true
	}
	if existing.conn != conn {
		return true
	}

	switch existing.getInitState() {
	case mcpClientInitStateReady, mcpClientInitStateInitializing:
		return false
	default:
		return true
	}
}

// GetConnectionStatus 获取连接状态信息
func (dc *McpClientInstance) GetConnectionStatus() map[string]interface{} {
	toolsCount := dc.toolCount()

	initState := "idle"
	switch dc.getInitState() {
	case mcpClientInitStateInitializing:
		initState = "initializing"
	case mcpClientInitStateReady:
		initState = "ready"
	}

	return map[string]interface{}{
		"server_name": dc.serverName,
		"connected":   dc.IsConnected(),
		"init_state":  initState,
		"last_ping":   dc.LastPing(),
		"tools_count": toolsCount,
	}
}

func (dc *McpClientInstance) RawCallTool(ctx context.Context, toolName string, arguments map[string]interface{}) (string, error) {
	if dc == nil || dc.mcpClient == nil {
		return "", fmt.Errorf("MCP客户端未初始化")
	}
	if !dc.IsConnected() || !dc.IsInitialized() {
		return "", fmt.Errorf("MCP客户端未就绪")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	result, err := dc.mcpClient.CallTool(ctx, mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Name:      toolName,
			Arguments: arguments,
		},
	})
	if err != nil {
		return "", fmt.Errorf("调用工具失败: %v", err)
	}

	resultBytes, err := result.MarshalJSON()
	if err != nil {
		return "", fmt.Errorf("工具调用返回内容转换失败: %v", err)
	}
	return string(resultBytes), nil
}

// GetTools 获取工具列表
func (dc *DeviceMcpSession) GetTools() map[string]tool.InvokableTool {
	tools := make(map[string]tool.InvokableTool)
	for _, mcpInstance := range dc.snapshotWsEndpointClients() {
		mcpInstance.copyToolsInto(tools)
	}

	for _, iotClient := range dc.snapshotIotClients() {
		iotClient.copyToolsInto(tools)
	}
	return tools
}

func (dc *DeviceMcpSession) GetWsEndpointMcpTools() map[string]tool.InvokableTool {
	tools := make(map[string]tool.InvokableTool)
	for _, mcpInstance := range dc.snapshotWsEndpointClients() {
		mcpInstance.copyToolsInto(tools)
	}
	return tools
}

func (dc *DeviceMcpSession) RefreshWsEndpointTools() (map[string]tool.InvokableTool, error) {
	tools := make(map[string]tool.InvokableTool)
	for _, mcpInstance := range dc.snapshotWsEndpointClients() {
		refreshedTools, err := mcpInstance.refreshToolsStrict()
		if err != nil {
			for _, cleanupTarget := range dc.snapshotWsEndpointClients() {
				cleanupTarget.clearToolsSnapshot()
			}
			return map[string]tool.InvokableTool{}, err
		}
		for name, invokable := range refreshedTools {
			tools[name] = invokable
		}
	}
	return tools, nil
}

// GetPreferredIotTransportType 返回当前设备最适合用于设备维度 MCP 查询/调用的 transport。
// 优先选择仍处于 connected 状态且最近有心跳的 transport；如果都不活跃，则退回最近一次存在的 transport。
func (dc *DeviceMcpSession) GetPreferredIotTransportType() string {
	preferredTransport := ""
	var preferredClient *McpClientInstance
	isSupportedTransport := func(transportType string) bool {
		switch normalizeDeviceTransportType(transportType) {
		case "websocket", "udp", "mqtt_udp":
			return true
		default:
			return false
		}
	}

	selectPreferred := func(connectedOnly bool) string {
		preferredTransport = ""
		preferredClient = nil
		for _, snapshot := range dc.snapshotIotTransports() {
			transportType := snapshot.transportType
			iotClient := snapshot.client
			transportType = normalizeDeviceTransportType(transportType)
			if iotClient == nil {
				continue
			}
			if !isSupportedTransport(transportType) {
				continue
			}
			if connectedOnly && !iotClient.IsConnected() {
				continue
			}
			if preferredClient == nil {
				preferredTransport = transportType
				preferredClient = iotClient
				continue
			}
			currentLastPing := iotClient.LastPing()
			preferredLastPing := preferredClient.LastPing()
			if currentLastPing.After(preferredLastPing) {
				preferredTransport = transportType
				preferredClient = iotClient
				continue
			}
			if currentLastPing.Equal(preferredLastPing) && transportType < preferredTransport {
				preferredTransport = transportType
				preferredClient = iotClient
			}
		}
		return preferredTransport
	}

	if transportType := selectPreferred(true); transportType != "" {
		return transportType
	}
	return selectPreferred(false)
}

func (dc *DeviceMcpSession) GetIotToolsByTransport(transportType string) map[string]tool.InvokableTool {
	transportType = strings.TrimSpace(transportType)
	tools := make(map[string]tool.InvokableTool)
	if transportType == "" {
		return tools
	}

	dc.iotMux.RLock()
	iotClient := dc.iotOverMcpByTransport[transportType]
	dc.iotMux.RUnlock()
	if iotClient == nil {
		return tools
	}

	iotClient.copyToolsInto(tools)

	return tools
}

func (dc *DeviceMcpSession) RefreshIotToolsByTransport(transportType string) (map[string]tool.InvokableTool, error) {
	transportType = normalizeDeviceTransportType(transportType)
	tools := make(map[string]tool.InvokableTool)
	if transportType == "unknown" {
		return tools, nil
	}

	dc.iotMux.RLock()
	iotClient := dc.iotOverMcpByTransport[transportType]
	dc.iotMux.RUnlock()
	if iotClient == nil {
		return tools, nil
	}

	return iotClient.refreshToolsStrict()
}

func (dc *DeviceMcpSession) GetIotToolByTransportAndName(transportType, toolName string) (tool.InvokableTool, bool) {
	transportType = strings.TrimSpace(transportType)
	if transportType == "" {
		return nil, false
	}

	dc.iotMux.RLock()
	iotClient := dc.iotOverMcpByTransport[transportType]
	dc.iotMux.RUnlock()
	if iotClient == nil {
		return nil, false
	}

	return iotClient.getToolByName(toolName)
}

func (dc *DeviceMcpSession) RawCallIotToolByTransport(ctx context.Context, transportType, toolName string, arguments map[string]interface{}) (string, bool, error) {
	transportType = strings.TrimSpace(transportType)
	if transportType == "" {
		return "", false, nil
	}

	dc.iotMux.RLock()
	iotClient := dc.iotOverMcpByTransport[transportType]
	dc.iotMux.RUnlock()
	if iotClient == nil || !iotClient.IsConnected() || !iotClient.IsInitialized() {
		return "", false, nil
	}

	if invokable, ok := iotClient.getToolByName(toolName); ok {
		toolName = remoteCallNameForTool(invokable, toolName)
	}
	result, err := iotClient.RawCallTool(ctx, toolName, arguments)
	return result, true, err
}

func (dc *DeviceMcpSession) RawCallWsEndpointTool(ctx context.Context, toolName string, arguments map[string]interface{}) (string, bool, error) {
	var selected *McpClientInstance
	for _, mcpInstance := range dc.snapshotWsEndpointClients() {
		if mcpInstance == nil || !mcpInstance.IsConnected() || !mcpInstance.IsInitialized() {
			continue
		}
		selected = mcpInstance
		break
	}
	if selected == nil {
		return "", false, nil
	}

	if invokable, ok := selected.getToolByName(toolName); ok {
		toolName = remoteCallNameForTool(invokable, toolName)
	}
	result, err := selected.RawCallTool(ctx, toolName, arguments)
	return result, true, err
}

func (dc *DeviceMcpSession) GetToolByName(toolName string) (tool tool.InvokableTool, ok bool) {
	for _, mcpInstance := range dc.snapshotWsEndpointClients() {
		if tool, ok = mcpInstance.getToolByName(toolName); ok {
			return tool, true
		}
	}
	if ok {
		return tool, true
	}

	for _, iotClient := range dc.snapshotIotClients() {
		if tool, ok = iotClient.getToolByName(toolName); ok {
			return tool, true
		}
	}
	return nil, false
}
