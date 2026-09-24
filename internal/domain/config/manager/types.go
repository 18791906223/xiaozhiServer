package manager

import (
	"xiaozhi-esp32-server-golang/internal/domain/config/types"
)

const (
	EventDeviceOnlinePath  = "/api/device/active"
	EventDeviceOfflinePath = "/api/device/inactive"
	EventInjectMessagePath = "/api/device/message"
)

var event2Path = map[string]string{
	types.EventDeviceOnline:  EventDeviceOnlinePath,
	types.EventDeviceOffline: EventDeviceOfflinePath,
}

var path2Event = map[string]string{
	EventInjectMessagePath: types.EventHandleMessageInject,
}
