package bootstrap

import (
	"asr_server_learn/config"
	"asr_server_learn/internal/config/hotreload"
	"asr_server_learn/internal/logger"
	"asr_server_learn/internal/middleware"
	"asr_server_learn/internal/pool"
	"asr_server_learn/internal/session"
	"fmt"
	"os"

	sherpa "github.com/k2-fsa/sherpa-onnx-go/sherpa_onnx"
)

// InitApp 初始化所有核心组件，返回依赖注入结构体
func InitApp(cfg *config.Config) (error, error) {
	logger.Infof("🔧 Initializing components...")

	// 初始化配置热加载管理器
	logger.Infof("🔧 Initializing hot reload manager...")
	hotReloadMgr, err := hotreload.NewHotReloadManager()
	if err != nil {
		logger.Errorf("Failed to initialize hot reload manager: %v", err)
		return nil, fmt.Errorf("failed to initialize hot reload manager: %v", err)
	}
	if err := hotReloadMgr.StartWatching("config.json"); err != nil {
		logger.Warnf("Failed to start config file watching, continuing without hot reload: %v", err)
	}

	// 初始化全局识别器（仅在recognition启用时初始化）  asr模型对象的加载
	var globalRecognizer *sherpa.OfflineRecognizer
	if cfg.Recognition.Enabled {
		// 初始化全局识别器
		logger.Infof("🔧 Initializing global recognizer...")
		globalRecognizer, err = createRecognizer(cfg)
		if err != nil {
			logger.Errorf("Failed to initialize global recognizer: %v", err)
			return nil, fmt.Errorf("failed to initialize global recognizer: %v", err)
		}
	}

	// 初始化VAD池（总是初始化，不依赖recognition.enabled）
	var vadPool pool.VADPoolInterface
	logger.Infof("🔧 Initializing VAD pool...")
	// 创建vad多类型工厂对象，里面有很短类型的vad， （vad1, vad1的池子创建方式）（vad2, vad2的池子创建方式）
	// 通过多类型工厂去创建vad池对象===> 首先通过配置获取多个工厂中的一个， 获取这个vad工厂的配置信息， 通过这个vad工厂创建线程池对象， vad线程池对象进行初始化，后续通过线程池对象获取一个vad实例
	vadFactory := pool.NewVADFactory()

	if config.GlobalConfig.VAD.Provider == pool.SILERO_TYPE {
		// 检查VAD模型文件是否存在（仅对silero需要）
		if _, err := os.Stat(cfg.VAD.SileroVAD.ModelPath); os.IsNotExist(err) {
			logger.Errorf("VAD model file not found, model_path=%s", cfg.VAD.SileroVAD.ModelPath)
			return nil, fmt.Errorf("VAD model file not found: %s", cfg.VAD.SileroVAD.ModelPath)
		}
	}

	// 使用工厂创建VAD池
	vadPool, err = vadFactory.CreateVADPool()
	if err != nil {
		logger.Errorf("Failed to create VAD pool: %v", err)
		return nil, fmt.Errorf("failed to create VAD pool: %v", err)
	}

	// 初始化VAD池
	logger.Infof("🔧 Initializing VAD pool... pool_size=%d", cfg.VAD.PoolSize)
	if err := vadPool.Initialize(); err != nil {
		logger.Errorf("Failed to initialize VAD pool: %v", err)
		return nil, fmt.Errorf("failed to initialize VAD pool: %v", err)
	}

	// 初始化会话管理器
	logger.Infof("🔧 Initializing session manager...")
	sessionManager := session.NewManager(globalRecognizer, vadPool)

	// 注册配置热加载回调
	registerHotReloadCallbacks(hotReloadMgr)

	// 初始化速率限制器
	logger.Infof("🔧 Initializing rate limiter... requests_per_second=%d, max_connections=%d", cfg.RateLimit.RequestsPerSecond, cfg.RateLimit.MaxConnections)
	rateLimiter := middleware.NewRateLimiter(
		cfg.RateLimit.Enabled,
		cfg.RateLimit.RequestsPerSecond,
		cfg.RateLimit.BurstSize,
		cfg.RateLimit.MaxConnections,
	)

	print(sessionManager, rateLimiter)
	return nil, nil
}

// registerHotReloadCallbacks 注册配置热加载回调
func registerHotReloadCallbacks(hotReloadMgr *hotreload.HotReloadManager) {
	if hotReloadMgr == nil {
		return
	}

	hotReloadMgr.RegisterCallback("logging.level", func() {
		logger.Infof("🔄 Log level changed to: %s", config.GlobalConfig.Logging.Level)
	})
	hotReloadMgr.RegisterCallback("vad", func() {
		logger.Infof("🔄 VAD configuration changed")
	})
	hotReloadMgr.RegisterCallback("session", func() {
		logger.Infof("🔄 Session configuration changed")
	})
	hotReloadMgr.RegisterCallback("rate_limit", func() {
		logger.Infof("🔄 Rate limit configuration changed")
	})
	hotReloadMgr.RegisterCallback("response", func() {
		logger.Infof("🔄 Response configuration changed")
	})
	logger.Infof("✅ Hot reload callbacks registered")
}

// createRecognizer 用于初始化 sherpa 识别器
func createRecognizer(cfg *config.Config) (*sherpa.OfflineRecognizer, error) {
	// 创建离线对象
	c := sherpa.OfflineRecognizerConfig{}
	// 设置配置
	c.FeatConfig.SampleRate = cfg.Audio.SampleRate
	c.FeatConfig.FeatureDim = cfg.Audio.FeatureDim

	c.ModelConfig.SenseVoice.Model = cfg.Recognition.ModelPath
	c.ModelConfig.Tokens = cfg.Recognition.TokensPath
	c.ModelConfig.NumThreads = cfg.Recognition.NumThreads
	c.ModelConfig.Debug = 0
	if cfg.Recognition.Debug {
		c.ModelConfig.Debug = 1
	}
	c.ModelConfig.Provider = cfg.Recognition.Provider

	recognizer := sherpa.NewOfflineRecognizer(&c)
	if recognizer == nil {
		return nil, fmt.Errorf("failed to create offline recognizer")
	}

	return recognizer, nil
}
