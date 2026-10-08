package main

import (
	"log"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
)

// upgrader 与真实实现 internal/app/server/websocket/websocket_server.go 保持一致的配置：
// 允许所有来源（真机设备不带 Origin，不做校验）。
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// runServer 启动 WebSocket 服务。路由 /xiaozhi/v1/ 与真实服务相同，
// 所以设备端把 OTA 地址指过来就能直接连。
func runServer(addr, path string) error {
	mux := http.NewServeMux()
	mux.HandleFunc(path, handleChat)

	log.Printf("简化版小智服务已启动，请让设备连接 ws://%s%s", addr, path)
	return http.ListenAndServe(addr, mux)
}

// handleChat 处理一条设备连接。
//
// 对应真实实现：websocket_server.go 的 internalHandleChat。
// 真实实现这里还有鉴权、MQTT/UDP 分流、OTA 等分支，简化版全部去掉。
func handleChat(w http.ResponseWriter, r *http.Request) {
	deviceID := extractDeviceID(r)
	if deviceID == "" {
		log.Printf("拒绝连接：缺少 device-id")
		http.Error(w, "缺少 device-id（Header 或 URL 参数）", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket 升级失败: %v", err)
		return
	}

	log.Printf("[%s] 设备已连接", deviceID)
	newSession(newTransport(conn, deviceID), deviceID).run()
	log.Printf("[%s] 设备已断开", deviceID)
}

// extractDeviceID 依次尝试 Header 和 URL 参数，Header 优先。
// 真实实现支持的写法更多（各种大小写组合），逻辑是一样的。
func extractDeviceID(r *http.Request) string {
	for _, key := range []string{"Device-Id", "device-id", "device_id", "deviceId"} {
		if v := strings.TrimSpace(r.Header.Get(key)); v != "" {
			return v
		}
	}
	for _, key := range []string{"device-id", "device_id", "deviceId"} {
		if v := strings.TrimSpace(r.URL.Query().Get(key)); v != "" {
			return v
		}
	}
	return ""
}
