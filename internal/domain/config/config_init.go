package user_config

import (
	"context"
	"fmt"
	log "xiaozhi-esp32-server-golang/logger"

	"xiaozhi-esp32-server-golang/internal/domain/config/manager"
	"xiaozhi-esp32-server-golang/internal/domain/config/memory"
	redis_config "xiaozhi-esp32-server-golang/internal/domain/config/redis"

	"github.com/spf13/viper"
)

// 分组声明
var (
	// managerSystemConfigHandlers 收到 WebSocket system_config 推送时的回调列表，主程序可多次注册（如合并到 viper、热更服务）
	managerSystemConfigHandlers []func(map[string]interface{})
)

// RegisterManagerSystemConfigHandler 注册 manager 模式下系统配置推送的回调，应在 InitConfigSystem 之前调用；可多次调用以追加多个回调
func RegisterManagerSystemConfigHandler(fn func(map[string]interface{})) {
	managerSystemConfigHandlers = append(managerSystemConfigHandlers, fn)
}

// InitConfigSystem 初始化配置系统
// 根据config_provider.type的值调用对应配置包的Init方法
func InitConfigSystem(ctx context.Context) error {
	// 获取配置提供者类型
	providerType := viper.GetString("config_provider.type") // manager
	if providerType == "" {
		providerType = "redis" // 默认使用redis
		log.Infof("config_provider.type not set, using default: redis")
	}

	log.Infof("Initializing config system with provider: %s", providerType)

	// 根据配置提供者类型调用对应的Init方法
	switch providerType {
	case "manager":
		// manager类型的初始化配置系统
		manager.SetSystemConfigPushHandler(func(data map[string]interface{}) {
			// 通过回调，来消费配置参数，暂时不知道map中存储的是什么，应该在某个回调中，触发触发这个函数进行回调，注册到这里的都可以进行初始化
			for _, h := range managerSystemConfigHandlers {
				h(data)
			}
		})
		// 进行初始化，我先好奇这里为什么需要这个上下文呢
		return manager.Init(ctx)
	case "redis":
		return redis_config.Init(ctx)
	case "memory":
		return memory.Init(ctx)
	default:
		return fmt.Errorf("unsupported config provider type: %s", providerType)
	}
}
