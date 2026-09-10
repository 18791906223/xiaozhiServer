package hotreload

import (
	"asr_server_learn/config"
	"asr_server_learn/internal/logger"
	"fmt"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

// HotReloadManager 配置热加载管理器
type HotReloadManager struct {
	mu            sync.RWMutex
	callbacks     map[string][]func() // 使用锁为了保护这个切片
	watcher       *fsnotify.Watcher
	debounceTimer *time.Timer
	stopChan      chan struct{}
}

// NewHotReloadManager 创建新的热加载管理器
func NewHotReloadManager() (*HotReloadManager, error) {
	// 创建一个监视器对象
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create file watcher: %w", err)
	}
	// 管理对象
	manager := &HotReloadManager{
		callbacks: make(map[string][]func()),
		watcher:   watcher,
		stopChan:  make(chan struct{}),
	}

	return manager, nil
}

// RegisterCallback 注册配置变更回调
func (m *HotReloadManager) RegisterCallback(configKey string, callback func()) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.callbacks[configKey] == nil {
		m.callbacks[configKey] = make([]func(), 0)
	}
	// 添加回调到切片中
	m.callbacks[configKey] = append(m.callbacks[configKey], callback)
}

// StartWatching 开始监听配置文件
func (m *HotReloadManager) StartWatching(configPath string) error {
	// 添加配置文件到监听列表
	if err := m.watcher.Add(configPath); err != nil {
		return fmt.Errorf("failed to watch config file: %w", err)
	}

	// 启动监听协程
	go m.watchLoop()

	logger.Infof("🔍 Started watching config file: %s", configPath)
	return nil
}

// watchLoop 监听循环
func (m *HotReloadManager) watchLoop() {
	defer m.watcher.Close()

	for {
		select {
		case event := <-m.watcher.Events:
			if event.Op&fsnotify.Write == fsnotify.Write {
				// 有写入，就是通知文件被改动
				m.handleConfigChange()
			}
		case err := <-m.watcher.Errors:
			// 监听出错误
			logger.Errorf("❌ Config file watcher error: %v", err)
		case <-m.stopChan:
			// 监听停止了
			logger.Infof("🛑 Config file watcher stopped")
			return
		}
	}
}

// handleConfigChange 处理配置文件变更
func (m *HotReloadManager) handleConfigChange() {
	// 防抖动处理， 先将之前的定时器停止掉
	if m.debounceTimer != nil {
		m.debounceTimer.Stop()
	}
	// 创建一个定时器，2秒钟去触发重新加载的方法
	m.debounceTimer = time.AfterFunc(2*time.Second, func() {
		m.reloadConfig()
	})
}

// reloadConfig 重新加载配置
func (m *HotReloadManager) reloadConfig() {
	logger.Infof("🔄 Reloading configuration...")

	// 重新读取配置文件
	if err := viper.ReadInConfig(); err != nil {
		logger.Errorf("❌ Failed to read config file: %v", err)
		return
	}

	// 重新解析配置
	if err := viper.Unmarshal(&config.GlobalConfig); err != nil {
		logger.Errorf("❌ Failed to unmarshal config: %v", err)
		return
	}

	logger.Infof("✅ Configuration reloaded successfully")

	// 执行回调函数， 去更新
	m.executeCallbacks()
}

// executeCallbacks 执行回调函数
func (m *HotReloadManager) executeCallbacks() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for configKey, callbacks := range m.callbacks {
		logger.Infof("🔄 Executing callbacks for config key: %s", configKey)
		for _, callback := range callbacks {
			// 在goroutine中执行回调，避免阻塞
			go func(cb func()) {
				defer func() {
					// 防止回调cb中有painc报错，进行接收处理
					if r := recover(); r != nil {
						logger.Errorf("❌ Callback panicked: %v", r)
					}
				}()
				cb()
			}(callback)
		}
	}
}

// Stop 停止监听
func (m *HotReloadManager) Stop() {
	// 关闭监听时间，在watchloop就会进行停止
	close(m.stopChan)
	if m.debounceTimer != nil {
		m.debounceTimer.Stop()
	}
}

// GetConfigValue 获取配置值
func (m *HotReloadManager) GetConfigValue(key string) interface{} {
	return viper.Get(key)
}

// SetConfigValue 设置配置值
func (m *HotReloadManager) SetConfigValue(key string, value interface{}) error {
	viper.Set(key, value)

	// 重新解析到结构体
	if err := viper.Unmarshal(&config.GlobalConfig); err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// 执行相关回调
	m.executeCallbacks()

	return nil
}

// SaveConfig 保存配置到文件
func (m *HotReloadManager) SaveConfig() error {
	return viper.WriteConfig()
}
