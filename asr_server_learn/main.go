package main

import (
	"asr_server_learn/config"
	"asr_server_learn/internal/bootstrap"
	"asr_server_learn/internal/logger"
	// 显式导入 pool 包以触发 cgo 编译 ten_vad_cgo.go，确保 ten_vad.dll 被链接到 exe
	_ "asr_server_learn/internal/pool"
	"os"
)

func main() {
	// 加载配置并初始化日志
	if err := config.InitConfig("config.json"); err != nil {
		logger.Errorf("Failed to load configuration:%v", err)
		os.Exit(1)
	}

	// 设置日志级别
	logger.InitLoggerFromConfig(logger.LoggingConfig{
		Level:      config.GlobalConfig.Logging.Level,
		Format:     config.GlobalConfig.Logging.Format,
		Output:     config.GlobalConfig.Logging.Output,
		FilePath:   config.GlobalConfig.Logging.FilePath,
		MaxSize:    config.GlobalConfig.Logging.MaxSize,
		MaxBackups: config.GlobalConfig.Logging.MaxBackups,
		MaxAge:     config.GlobalConfig.Logging.MaxAge,
		Compress:   config.GlobalConfig.Logging.Compress,
	})
	logger.Infof("✅ Configuration loaded")
	config.PrintConfig()

	// 初始化所有依赖
	bootstrap.InitApp(&config.GlobalConfig)
}
