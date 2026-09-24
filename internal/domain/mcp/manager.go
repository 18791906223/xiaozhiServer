package mcp

import (
	"fmt"
	"sync"

	log "xiaozhi-esp32-server-golang/logger"
)

// MCPManager 统一的MCP管理器，负责协调所有子管理器
type MCPManager struct {
	localManager  *LocalMCPManager
	globalManager *GlobalMCPManager
	// deviceManager 将来可以在这里管理设备管理器池

	mu      sync.RWMutex
	started bool
}

var (
	mcpManager *MCPManager
	mcpOnce    sync.Once
)

// GetMCPManager 获取统一MCP管理器单例
func GetMCPManager() *MCPManager {
	mcpOnce.Do(func() {
		mcpManager = &MCPManager{
			localManager:  GetLocalMCPManager(),
			globalManager: GetGlobalMCPManager(),
			started:       false,
		}
	})
	return mcpManager
}

// Start 启动所有MCP管理器
func (m *MCPManager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.started {
		log.Warn("MCP管理器已经启动")
		return nil
	}

	log.Info("=== 启动MCP管理器集群 ===")

	// 1. 首先启动本地管理器
	log.Info("启动本地MCP管理器...")
	if err := m.localManager.Start(); err != nil {
		log.Errorf("启动本地MCP管理器失败: %v", err)
		return fmt.Errorf("启动本地MCP管理器失败: %v", err)
	}

	// 2. 然后启动全局管理器
	log.Info("启动全局MCP管理器...")
	if err := m.globalManager.Start(); err != nil {
		log.Errorf("启动全局MCP管理器失败: %v", err)
		return fmt.Errorf("启动全局MCP管理器失败: %v", err)
	}

	// 3. 设备管理器通过连接时动态创建，这里不需要启动
	log.Info("设备MCP管理器将根据连接动态创建")

	m.started = true
	log.Info("=== MCP管理器集群启动完成 ===")

	// 输出启动状态统计
	m.printStartupStats()

	return nil
}

// Stop 停止所有MCP管理器
func (m *MCPManager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.started {
		log.Info("MCP管理器未启动，无需停止")
		return nil
	}

	log.Info("=== 停止MCP管理器集群 ===")

	// 按相反顺序停止管理器
	// 1. 停止全局管理器
	log.Info("停止全局MCP管理器...")
	if err := m.globalManager.Stop(); err != nil {
		log.Errorf("停止全局MCP管理器失败: %v", err)
	}

	// 2. 停止本地管理器
	log.Info("停止本地MCP管理器...")
	if err := m.localManager.Stop(); err != nil {
		log.Errorf("停止本地MCP管理器失败: %v", err)
	}

	// 3. 设备管理器通过连接断开自动清理
	log.Info("设备MCP连接将自动清理")

	m.started = false
	log.Info("=== MCP管理器集群已停止 ===")
	return nil
}

// IsStarted 检查管理器是否已启动
func (m *MCPManager) IsStarted() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.started
}

// GetLocalManager 获取本地管理器
func (m *MCPManager) GetLocalManager() *LocalMCPManager {
	return m.localManager
}

// GetGlobalManager 获取全局管理器
func (m *MCPManager) GetGlobalManager() *GlobalMCPManager {
	return m.globalManager
}

// printStartupStats 输出启动状态统计
func (m *MCPManager) printStartupStats() {
	localToolCount := m.localManager.GetToolCount()
	globalToolCount := len(m.globalManager.GetAllTools())

	log.Infof("MCP管理器启动统计:")
	log.Infof("  - 本地工具数量: %d", localToolCount)
	log.Infof("  - 全局工具数量: %d", globalToolCount)
	log.Infof("  - 设备管理器: 动态管理")
	log.Infof("  - 总计工具数量: %d", localToolCount+globalToolCount)
}

// GetAllManagersStatus 获取所有管理器的状态信息
func (m *MCPManager) GetAllManagersStatus() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := map[string]interface{}{
		"mcp_manager": map[string]interface{}{
			"started": m.started,
		},
		"local_manager": map[string]interface{}{
			"tool_count": m.localManager.GetToolCount(),
			"tool_names": m.localManager.GetToolNames(),
		},
		"global_manager": map[string]interface{}{
			"tool_count": len(m.globalManager.GetAllTools()),
		},
		"device_manager": map[string]interface{}{
			"active_devices": mcpClientPool.device2McpClient.Count(),
		},
	}

	return status
}

// RestartManager 重启指定的管理器
func (m *MCPManager) RestartManager(managerType string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.started {
		return fmt.Errorf("MCP管理器集群未启动")
	}

	switch managerType {
	case "local":
		log.Info("重启本地MCP管理器...")
		if err := m.localManager.Stop(); err != nil {
			log.Errorf("停止本地管理器失败: %v", err)
		}
		if err := m.localManager.Start(); err != nil {
			return fmt.Errorf("重启本地管理器失败: %v", err)
		}
		log.Info("本地MCP管理器重启完成")

	case "global":
		log.Info("重启全局MCP管理器...")
		if err := m.globalManager.Stop(); err != nil {
			log.Errorf("停止全局管理器失败: %v", err)
		}
		if err := m.globalManager.Start(); err != nil {
			return fmt.Errorf("重启全局管理器失败: %v", err)
		}
		log.Info("全局MCP管理器重启完成")

	default:
		return fmt.Errorf("不支持的管理器类型: %s", managerType)
	}

	return nil
}

// 为了向后兼容，提供便捷函数

// StartMCPManagers 启动所有MCP管理器（便捷函数）
func StartMCPManagers() error {
	return GetMCPManager().Start()
}

// StopMCPManagers 停止所有MCP管理器（便捷函数）
func StopMCPManagers() error {
	return GetMCPManager().Stop()
}

// GetMCPManagerStatus 获取MCP管理器状态（便捷函数）
func GetMCPManagerStatus() map[string]interface{} {
	return GetMCPManager().GetAllManagersStatus()
}
