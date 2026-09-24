package pool

import (
	"context"
	"sync"
	"time"
	"xiaozhi-esp32-server-golang/internal/components/http"
	"xiaozhi-esp32-server-golang/internal/util"
	log "xiaozhi-esp32-server-golang/logger"

	"github.com/spf13/viper"
)

// StatsReporter 资源池统计上报器
type StatsReporter struct {
	client  *http.ManagerClient
	enabled bool
}

var (
	globalReporter *StatsReporter
	reporterOnce   sync.Once
)

// GetStatsReporter 获取全局统计上报器（单例）
func GetStatsReporter() *StatsReporter {
	reporterOnce.Do(func() {
		// 获取 manager backend URL，优先从环境变量获取，如果环境变量不存在则从配置获取
		baseURL := util.GetBackendURL()
		if baseURL == "" {
			baseURL = "http://localhost:8080" // 默认值
		}

		// 检查是否启用上报
		enabled := viper.GetBool("pool_stats.report_enabled")
		if !enabled {
			// 默认启用
			enabled = true
		}

		// 创建 HTTP 客户端
		managerClient := http.NewManagerClient(http.ManagerClientConfig{
			BaseURL:    baseURL,
			AuthToken:  util.GetManagerAuthToken(),
			Timeout:    5 * time.Second,
			MaxRetries: 2,
		})

		globalReporter = &StatsReporter{
			client:  managerClient,
			enabled: enabled,
		}

		log.Infof("资源池统计上报器已初始化，backend_url=%s, enabled=%v", baseURL, enabled)
	})
	return globalReporter
}

// StartReporting 启动统计上报（每5秒上报一次）
func (r *StatsReporter) StartReporting(ctx context.Context) {
	if !r.enabled {
		log.Info("资源池统计上报已禁用")
		return
	}

	// 上报间隔（5秒）
	interval := viper.GetDuration("pool_stats.report_interval")
	if interval == 0 {
		interval = 5 * time.Second
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		//log.Infof("资源池统计上报已启动，每 %v 上报一次", interval)

		for {
			select {
			case <-ctx.Done():
				log.Debugf("资源池统计上报已停止")
				return
			case <-ticker.C:
				r.reportStats(ctx)
			}
		}
	}()
}

// reportStats 上报统计数据
func (r *StatsReporter) reportStats(ctx context.Context) {
	// 获取统计数据
	stats := GetStats()

	// 如果没有数据，跳过上报
	if len(stats) == 0 {
		//log.Debugf("当前没有活跃的资源池，跳过上报")
		return
	}

	// 构建请求体
	requestBody := map[string]interface{}{
		"stats": stats,
	}

	// 发送上报请求
	err := r.client.DoRequest(ctx, http.RequestOptions{
		Method: "POST",
		Path:   "/api/internal/pool/stats",
		Body:   requestBody,
	})

	if err != nil {
		log.Warnf("资源池统计上报失败: %v", err)
	} else {
		//log.Debugf("资源池统计上报成功，资源池数量: %d", len(stats))
	}
}

// StartStatsReporter 启动全局统计上报器（便捷函数）
func StartStatsReporter(ctx context.Context) {
	reporter := GetStatsReporter()
	reporter.StartReporting(ctx)
}
