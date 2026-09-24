package websocket

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"time"
	"xiaozhi-esp32-server-golang/internal/app/server/types"
	log "xiaozhi-esp32-server-golang/logger"

	"github.com/gorilla/websocket"
)

// WebSocketConn 实现 types.IConn 接口，适配 WebSocket 连接
type WebSocketConn struct {
	ctx    context.Context
	cancel context.CancelFunc

	onCloseCbList []func(deviceId string)

	conn     *websocket.Conn
	deviceID string

	isMqttUdpBridge bool
	recvCmdChan     chan []byte
	recvAudioChan   chan []byte

	closed bool
	sync.RWMutex
}

// NewWebSocketConn 创建一个新的 WebSocketConn 实例
func NewWebSocketConn(conn *websocket.Conn, deviceID string, isMqttUdpBridge bool) *WebSocketConn {
	ctx, cancel := context.WithCancel(context.Background())
	instance := &WebSocketConn{
		ctx:             ctx,
		cancel:          cancel,
		conn:            conn,
		deviceID:        deviceID,
		isMqttUdpBridge: isMqttUdpBridge,
		recvCmdChan:     make(chan []byte, 100),
		recvAudioChan:   make(chan []byte, 100),
	}

	// 设置pong处理器
	conn.SetPongHandler(func(appData string) error {
		log.Debugf("收到pong消息，设备ID: %s", deviceID)
		return nil
	})

	// 启动心跳检测goroutine
	go func() {
		ticker := time.NewTicker(30 * time.Second) // 每30秒发送一次ping
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := instance.conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(5*time.Second)); err != nil {
					log.Errorf("发送ping消息失败，设备ID: %s, 错误: %v", deviceID, err)
					// 心跳失败，关闭连接
					for _, cb := range instance.onCloseCbList {
						cb(instance.deviceID)
					}
					return
				}
				log.Debugf("发送ping消息成功，设备ID: %s", deviceID)
			case <-instance.ctx.Done():
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case <-instance.ctx.Done():
				return
			default:
				msgType, audio, err := instance.conn.ReadMessage()
				if err != nil {
					log.Errorf("read message error: %v", err)
					for _, cb := range instance.onCloseCbList {
						cb(instance.deviceID) //通知注册方退出
					}
					return
				}

				if msgType == websocket.TextMessage {
					select {
					case instance.recvCmdChan <- audio:
					default:
						log.Errorf("recv cmd channel is full")
					}
				} else if msgType == websocket.BinaryMessage {
					if instance.isMqttUdpBridge {
						audio = instance.tryUnpackUdpBridgeAudioPacket(audio)
					}
					select {
					case instance.recvAudioChan <- audio:
					default:
						log.Errorf("recv audio channel is full")
					}
				}
			}
		}
	}()

	return instance
}

// 适配mqtt udp bridge的数据格式
// 前8个字节为0, 12-16字节为音频数据长度, 16字节后为音频数据
func (c *WebSocketConn) tryUnpackUdpBridgeAudioPacket(buffer []byte) []byte {
	if len(buffer) < 16 {
		return buffer
	}
	// 检查前8字节是否全为0
	for i := 0; i < 8; i++ {
		if buffer[i] != 0 {
			return buffer
		}
	}
	dataLen := binary.BigEndian.Uint32(buffer[12:16])
	if int(dataLen) != len(buffer)-16 {
		return buffer
	}
	audioData := buffer[16:]
	return audioData
}

func (c *WebSocketConn) packUdpBridgeAudioPacket(buffer []byte) []byte {
	header := make([]byte, 16)
	// 前8字节全为0，已初始化
	// 9~12字节写入当前时间戳（秒）
	timestamp := uint32(time.Now().Unix())
	binary.BigEndian.PutUint32(header[8:12], timestamp)
	// 13~16字节写入音频长度
	binary.BigEndian.PutUint32(header[12:16], uint32(len(buffer)))
	// 拼接header和音频数据
	return append(header, buffer...)
}

func (w *WebSocketConn) SendCmd(msg []byte) error {
	w.Lock()
	defer w.Unlock()

	if w.closed {
		return errors.New("connection is closed")
	}

	log.Debugf("send cmd: %s", string(msg))

	err := w.conn.WriteMessage(websocket.TextMessage, msg)
	if err != nil {
		log.Errorf("send cmd error: %v", err)
		return err
	}
	return nil
}

func (w *WebSocketConn) SendAudio(audio []byte) error {
	w.Lock()
	defer w.Unlock()

	if w.closed {
		return errors.New("connection is closed")
	}

	if w.isMqttUdpBridge {
		audio = w.packUdpBridgeAudioPacket(audio)
	}
	err := w.conn.WriteMessage(websocket.BinaryMessage, audio)
	if err != nil {
		log.Errorf("send audio error: %v", err)
		return err
	}
	return nil
}

func (w *WebSocketConn) RecvCmd(ctx context.Context, timeout int) ([]byte, error) {
	for {
		select {
		case <-ctx.Done():
			log.Debugf("recv cmd context done")
			return nil, ctx.Err()
		case msg, ok := <-w.recvCmdChan:
			if !ok {
				return nil, errors.New("connection is closed")
			}
			return msg, nil
		case <-time.After(time.Duration(timeout) * time.Second):
			return nil, errors.New("timeout")
		}
	}
}

func (w *WebSocketConn) RecvAudio(ctx context.Context, timeout int) ([]byte, error) {
	for {
		select {
		case <-ctx.Done():
			log.Debugf("recv audio context done")
			return nil, ctx.Err()
		case audio, ok := <-w.recvAudioChan:
			if !ok {
				return nil, errors.New("connection is closed")
			}
			return audio, nil
		case <-time.After(time.Duration(timeout) * time.Second):
			return nil, errors.New("timeout")
		}
	}
}

func (w *WebSocketConn) Close() error {
	w.Lock()
	defer w.Unlock()

	if w.closed {
		return nil // Already closed
	}

	w.closed = true
	w.cancel()
	w.conn.Close()
	close(w.recvCmdChan)
	close(w.recvAudioChan)
	return nil
}

func (w *WebSocketConn) OnClose(cb func(deviceId string)) {
	w.onCloseCbList = append(w.onCloseCbList, cb)
}

func (w *WebSocketConn) GetDeviceID() string {
	return w.deviceID
}

func (w *WebSocketConn) GetTransportType() string {
	return types.TransportTypeWebsocket
}

func (w *WebSocketConn) GetData(key string) (interface{}, error) {
	return nil, errors.New("not implemented")
}

func (w *WebSocketConn) CloseAudioChannel() error {
	return nil
}
