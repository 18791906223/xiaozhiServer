package main

import (
	"asr_server_learn/config"
	"asr_server_learn/internal/bootstrap"
	"asr_server_learn/internal/logger"
	// 显式 import pool 以触发 init() 中将内嵌 DLL 写入 exe 目录的逻辑
	// go run 时 exe 在 go-build 临时目录，init() 会自动把 DLL 写过去
	_ "asr_server_learn/internal/pool"
	"fmt"
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
	wd, _ := os.Getwd()
	exe, _ := os.Executable()

	fmt.Println("工作目录:", wd)
	fmt.Println("可执行文件:", exe)
	fmt.Println("命令行参数:", os.Args)
}
