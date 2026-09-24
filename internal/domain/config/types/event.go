package types

import "context"

type EventHandler func(ctx context.Context, eventType string, eventData map[string]interface{}) (string, error)

// 上行push事件 主程序 => 管理内控
const (
	EventDeviceOnline  = "/api/device/active"   //设备上线
	EventDeviceOffline = "/api/device/inactive" //设备下线
)

// 下行pull事件 管理内控 => 主程序
const (
	EventHandleMessageInject = "/api/device/inject_msg" //处理消息注入
)
