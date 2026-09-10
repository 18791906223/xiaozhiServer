package bootstrap

import (
	"asr_server_learn/config"
	"asr_server_learn/internal/config/hotreload"
	"asr_server_learn/internal/logger"
	"fmt"

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

	//// 初始化全局识别器（仅在recognition启用时初始化）  asr模型对象的加载
	//var globalRecognizer *sherpa.OfflineRecognizer
	//if cfg.Recognition.Enabled {
	//	// 初始化全局识别器
	//	logger.Infof("🔧 Initializing global recognizer...")
	//	globalRecognizer, err = createRecognizer(cfg)
	//	if err != nil {
	//		logger.Errorf("Failed to initialize global recognizer: %v", err)
	//		return nil, fmt.Errorf("failed to initialize global recognizer: %v", err)
	//	}
	//}
	//
	//// 初始化VAD池（总是初始化，不依赖recognition.enabled）
	//var vadPool pool.VADPoolInterface
	//logger.Infof("🔧 Initializing VAD pool...")
	//vadFactory := pool.NewVADFactory()

	return nil, nil
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
