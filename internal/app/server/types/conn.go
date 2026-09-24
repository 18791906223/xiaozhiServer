package types

import "context"

// IConn 是协议无关的连接接口，由 websocket/mqtt_udp 等协议适配器实现
// 你可以根据实际需要扩展方法

const (
	TransportTypeWebsocket = "websocket"
	TransportTypeMqttUdp   = "udp"
)

type IConn interface {
	// 发送命令/信令数据
	SendCmd(msg []byte) error
	// 接收命令/信令数据
	RecvCmd(ctx context.Context, timeout int) ([]byte, error)
	// 发送语音数据
	SendAudio(audio []byte) error
	// 接收语音数据
	RecvAudio(ctx context.Context, timeout int) ([]byte, error)

	GetDeviceID() string

	Close() error
	OnClose(func(deviceId string))

	CloseAudioChannel() error

	GetTransportType() string

	//获取私有数据
	GetData(key string) (interface{}, error)
}

type OnNewConnection func(conn IConn)
