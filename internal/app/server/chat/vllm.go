// 文件：vllm.go
// 职责：视觉识别（图片理解）的入口。
// 设备/客户端把一张图片与一个问题 POST 上来，这里调用配置的视觉 LLM（vllm）得到文字回答。
//
// 在流程中的位置：它不属于「语音对话主链路」，而是由 HTTP 接口单独触发
// （internal/app/server/websocket/vision.go 的 handleVisionAPI 调用 HandleVllm）。
// 典型用法：设备拍一张照片问「这是什么」。
//
// 与哪些文件配合：
//   - websocket/vision.go：HTTP 入口，负责取 header、读文件、组装参数与返回响应。
//   - internal/domain/llm：GetLLMProvider 创建视觉 provider，ResponseWithVllm 执行推理。
//   - 配置（viper）：vision.vllm.provider 指定用哪个视觉模型，vision.vision_url 用于能力声明。
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

// HandleVllm 用配置好的视觉 LLM 识别图片并回答问题，返回模型输出文本。
// 注意：deviceId 目前不参与 provider 选择（用的是全局 vision.vllm 配置），
// 参数保留是为了将来支持「按设备路由到不同视觉模型」而不必改调用方签名。
func HandleVllm(deviceId string, file []byte, text string) (string, error) {
	//使用deviceId对应的vllm provider
	provider := viper.GetString("vision.vllm.provider")
	vllmConfig := viper.GetStringMap(fmt.Sprintf("vision.vllm.%s", provider))

	// MIME 类型由内容推断而不是相信上传方声明：
	// 客户端（嵌入式设备）常常不带 Content-Type 或带错，
	// 用 DetectContentType 读前 512 字节嗅探更可靠（这也是它只吃前 512 字节的原因）。
	mimeType := http.DetectContentType(file[:512])

	llmProvider, err := llm.GetLLMProvider(provider, vllmConfig)
	if err != nil {
		log.Errorf("获取VLLM Provider失败: %v", err)
		return "", err
	}
	// 这里不走池化：视觉请求是低频的人为触发的操作，
	// 且图片体积大、耗时长，按次创建 provider 更简单也不影响主链路。
	responseText, err := llmProvider.ResponseWithVllm(context.Background(), file, text, mimeType)
	if err != nil {
		log.Errorf("图片识别失败: %v", err)
		return "", err
	}

	return responseText, nil
}

// GetVisionUrl 返回配置的视觉服务地址（vision.vision_url）。
// 该地址会作为能力信息下发给设备端（见 mcp.go 在 MCP initialize 里注入 vision），
// 让设备知道把图片往哪里上传。
func GetVisionUrl() string {
	return viper.GetString("vision.vision_url")
}

// GenToken 生成视觉接口的鉴权 token：clientId + 时间戳。
// 拼时间戳是为了让同一客户端每次拿到的 token 不同，降低被重放的风险。
func GenToken(clientId string) string {
	return clientId + "_" + time.Now().Format("20060102150405")
}

// VisvionAuth 校验视觉接口的鉴权 token。
// 目前是空实现（恒返回 nil），即实际不做校验：
// 开关仍由配置 vision.enable_auth 控制，调用方（websocket/vision.go）只在开启时才调用本函数。
// 保留这个空壳是为了给后续接入真正的 token 校验留出位置，而不改动调用方。
func VisvionAuth(token string) error {
	return nil
}
