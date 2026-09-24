package websocket

import (
	"io"
	"net/http"
	"strings"
	"xiaozhi-esp32-server-golang/internal/app/server/chat"
	log "xiaozhi-esp32-server-golang/logger"

	"github.com/spf13/viper"
)

// handleVisionAPI 处理图片识别API
func (s *WebSocketServer) handleVisionAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		log.Warnf("图片识别请求方法错误: %s", r.Method)
		http.Error(w, "仅支持POST请求", http.StatusMethodNotAllowed)
		return
	}

	//从header头部获取Device-Id和Client-Id
	deviceId := r.Header.Get("Device-Id")
	clientId := r.Header.Get("Client-Id")
	_ = clientId
	if deviceId == "" {
		log.Errorf("图片识别请求缺少Device-Id")
		http.Error(w, "缺少Device-Id", http.StatusBadRequest)
		return
	}
	log.Infof("图片识别请求 deviceId=%s", deviceId)

	if viper.GetBool("vision.enable_auth") {

		//从header Authorization中获取Bearer token
		authToken := r.Header.Get("Authorization")
		if authToken == "" {
			log.Errorf("图片识别请求缺少Authorization deviceId=%s", deviceId)
			http.Error(w, "缺少Authorization", http.StatusBadRequest)
			return
		}
		authToken = strings.TrimPrefix(authToken, "Bearer ")

		err := chat.VisvionAuth(authToken)
		if err != nil {
			log.Errorf("图片识别认证失败 deviceId=%s err=%v", deviceId, err)
			http.Error(w, "图片识别认证失败", http.StatusUnauthorized)
			return
		}
		log.Infof("图片识别认证通过 deviceId=%s", deviceId)
	}

	// 解析 multipart 表单，最大 10MB
	question := r.FormValue("question")
	if question == "" {
		log.Warnf("图片识别请求缺少question deviceId=%s", deviceId)
		http.Error(w, "缺少question参数", http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		log.Errorf("图片识别请求缺少file或读取失败 deviceId=%s err=%v", deviceId, err)
		http.Error(w, "缺少file参数或文件读取失败", http.StatusBadRequest)
		return
	}
	defer file.Close()

	fileBytes, err := io.ReadAll(file)
	if err != nil {
		log.Errorf("图片识别文件读取失败 deviceId=%s err=%v", deviceId, err)
		http.Error(w, "文件读取失败", http.StatusInternalServerError)
		return
	}

	file.Close()
	log.Infof("图片识别收到文件 deviceId=%s filename=%s size=%d question=%s", deviceId, header.Filename, len(fileBytes), question)

	result, err := chat.HandleVllm(deviceId, fileBytes, question)
	if err != nil {
		log.Errorf("图片识别失败 deviceId=%s err=%v", deviceId, err)
		http.Error(w, "图片识别失败", http.StatusInternalServerError)
		return
	}

	log.Infof("图片识别成功 deviceId=%s resultLen=%d", deviceId, len(result))
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(result))
}
