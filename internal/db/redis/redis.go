package redis

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	log "xiaozhi-esp32-server-golang/logger"
)

var (
	// 全局Redis客户端实例
	globalClient *redis.Client
	// 确保只初始化一次
	once sync.Once
	// 读写锁保护实例访问
	mu sync.RWMutex
)

// Config Redis配置结构体
type Config struct {
	Host     string `mapstructure:"host" json:"host"`
	Port     int    `mapstructure:"port" json:"port"`
	Password string `mapstructure:"password" json:"password"`
	DB       int    `mapstructure:"db" json:"db"`
	// 连接池配置
	PoolSize     int           `mapstructure:"pool_size" json:"pool_size"`
	MinIdleConns int           `mapstructure:"min_idle_conns" json:"min_idle_conns"`
	MaxRetries   int           `mapstructure:"max_retries" json:"max_retries"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout" json:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout" json:"write_timeout"`
	DialTimeout  time.Duration `mapstructure:"dial_timeout" json:"dial_timeout"`
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		Host:         "localhost",
		Port:         6379,
		Password:     "",
		DB:           0,
		PoolSize:     10,
		MinIdleConns: 5,
		MaxRetries:   3,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		DialTimeout:  5 * time.Second,
	}
}

// Init 初始化Redis客户端
func Init(config *Config) error {
	var initErr error

	once.Do(func() {
		if config == nil {
			config = DefaultConfig()
		}

		// 创建Redis客户端
		options := &redis.Options{
			Addr:         fmt.Sprintf("%s:%d", config.Host, config.Port),
			Password:     config.Password,
			DB:           config.DB,
			PoolSize:     config.PoolSize,
			MinIdleConns: config.MinIdleConns,
			MaxRetries:   config.MaxRetries,
			ReadTimeout:  config.ReadTimeout,
			WriteTimeout: config.WriteTimeout,
			DialTimeout:  config.DialTimeout,
		}

		client := redis.NewClient(options)

		// 测试连接
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := client.Ping(ctx).Err(); err != nil {
			initErr = fmt.Errorf("failed to connect to redis: %w", err)
			return
		}

		mu.Lock()
		globalClient = client
		mu.Unlock()

		log.Log().Info("Redis客户端初始化成功")
	})

	return initErr
}

// GetClient 获取Redis客户端实例
func GetClient() *redis.Client {
	mu.RLock()
	defer mu.RUnlock()

	if globalClient == nil {
		log.Log().Warn("Redis客户端未初始化")
		return nil
	}

	return globalClient
}

// GetClientWithOptions 使用指定配置获取Redis客户端
func GetClientWithOptions(options *redis.Options) *redis.Client {
	if options == nil {
		return GetClient()
	}

	client := redis.NewClient(options)

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		log.Log().Errorf("Redis连接失败: %v", err)
		return nil
	}

	return client
}

// IsHealthy 检查Redis连接健康状态
func IsHealthy() bool {
	client := GetClient()
	if client == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	return client.Ping(ctx).Err() == nil
}

// Close 关闭Redis客户端连接
func Close() error {
	mu.Lock()
	defer mu.Unlock()

	if globalClient != nil {
		err := globalClient.Close()
		globalClient = nil
		if err != nil {
			log.Log().Errorf("关闭Redis连接失败: %v", err)
			return err
		}
		log.Log().Info("Redis连接已关闭")
	}

	return nil
}

// GetKeyWithPrefix 获取带前缀的键名
func GetKeyWithPrefix(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return fmt.Sprintf("%s:%s", prefix, key)
}

// Reconnect 重新连接Redis（用于连接断开后的重连）
func Reconnect() error {
	mu.Lock()
	defer mu.Unlock()

	if globalClient != nil {
		// 关闭现有连接
		_ = globalClient.Close()
		globalClient = nil
	}

	// 重置once，允许重新初始化
	once = sync.Once{}

	return nil
}

// Stats 获取Redis连接池统计信息
func Stats() *redis.PoolStats {
	client := GetClient()
	if client == nil {
		return nil
	}

	stats := client.PoolStats()
	return stats
}

// LogStats 记录Redis连接池统计信息
func LogStats() {
	stats := Stats()
	if stats == nil {
		log.Log().Warn("无法获取Redis连接池统计信息")
		return
	}

	log.Log().Infof("Redis连接池统计 - 总连接: %d, 空闲连接: %d, 过期连接: %d, 命中: %d, 未命中: %d, 超时: %d",
		stats.TotalConns, stats.IdleConns, stats.StaleConns, stats.Hits, stats.Misses, stats.Timeouts)
}
