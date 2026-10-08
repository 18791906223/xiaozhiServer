package main

import "log"

// config 汇总简化版的可调参数。
//
// 真实实现用 viper 从 yaml 配置 + 管理后台下发的参数合并出配置，
// 每个 provider 还有自己的一坨 config map（api_key、region、voice、
// 重试次数、超时……）。简化版只用命令行参数。
type config struct {
	asrURL, asrKey, asrModel           string
	llmURL, llmKey, llmModel           string
	ttsURL, ttsKey, ttsModel, ttsVoice string
}

// appConfig 是全局配置。
//
// 真实实现里同样到处直接读 viper 全局变量
// （例如 viper.GetString("websocket.port")），这里保持同一风格：
// 在 main 里初始化一次，其余地方直接读。
var appConfig config

// newASR 见 asr.go，newLLM 见 llm.go，newTTS 见 tts.go。
// 把它们放在各自文件里，是因为每个实现都只和它自己的 provider 有关。

func logProviders(cfg config) {
	if cfg.asrURL == "" && cfg.llmURL == "" && cfg.ttsURL == "" {
		log.Print("三个 provider 都未配置，将全部使用内置模拟实现（可以听到「哔——」提示音）")
	}
}
