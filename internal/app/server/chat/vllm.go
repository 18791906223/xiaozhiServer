package chat

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/spf13/viper"

	"xiaozhi-esp32-server-golang/internal/domain/llm"
	log "xiaozhi-esp32-server-golang/logger"
)

func HandleVllm(deviceId string, file []byte, text string) (string, error) {
	//使用deviceId对应的vllm provider
	provider := viper.GetString("vision.vllm.provider")
	vllmConfig := viper.GetStringMap(fmt.Sprintf("vision.vllm.%s", provider))

	mimeType := http.DetectContentType(file[:512])

	llmProvider, err := llm.GetLLMProvider(provider, vllmConfig)
	if err != nil {
		log.Errorf("获取VLLM Provider失败: %v", err)
		return "", err
	}
	responseText, err := llmProvider.ResponseWithVllm(context.Background(), file, text, mimeType)
	if err != nil {
		log.Errorf("图片识别失败: %v", err)
		return "", err
	}

	return responseText, nil
}

func GetVisionUrl() string {
	return viper.GetString("vision.vision_url")
}

func GenToken(clientId string) string {
	return clientId + "_" + time.Now().Format("20060102150405")
}

func VisvionAuth(token string) error {
	return nil
}
