// 简化版小智服务端入口。
//
// 运行：
//
//	go run ./internal/app/server/chat/simplified
//
// 默认监听 :8989，设备连 ws://<你的内网IP>:8989/xiaozhi/v1/。
package main

import (
	"flag"
	"log"
)

func main() {
	addr := flag.String("addr", ":8989", "WebSocket 监听地址")
	path := flag.String("path", "/xiaozhi/v1/", "WebSocket 路由（与真实服务保持一致）")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	if err := runServer(*addr, *path); err != nil {
		log.Fatalf("服务启动失败: %v", err)
	}
}
