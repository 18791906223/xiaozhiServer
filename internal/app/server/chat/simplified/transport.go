package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// transport 是 WebSocket 连接的薄封装。
//
// 对应真实实现：internal/app/server/websocket/websocket_conn.go
// （它实现了 types.IConn 接口，简化版不需要那么抽象，直接用具体类型）。
//
// 两件关键事：
//  1. 只有一个读 goroutine —— gorilla/websocket 不允许并发读，读到的帧
//     按文本(控制指令)/二进制(音频)分流到两个 channel。
//  2. 写操作加锁 —— gorilla/websocket 同样不允许并发写。
type transport struct {
	conn     *websocket.Conn
	deviceID string

	cmdChan   chan []byte // 文本帧
	audioChan chan []byte // 二进制帧（Opus）

	closed    chan struct{}
	closeOnce sync.Once

	writeMu sync.Mutex
}

func newTransport(conn *websocket.Conn, deviceID string) *transport {
	t := &transport{
		conn:      conn,
		deviceID:  deviceID,
		cmdChan:   make(chan []byte, 100),
		audioChan: make(chan []byte, 100),
		closed:    make(chan struct{}),
	}
	go t.readLoop()
	go t.pingLoop()
	return t
}

// readLoop 是唯一的读 goroutine。
func (t *transport) readLoop() {
	defer t.Close()
	for {
		msgType, data, err := t.conn.ReadMessage()
		if err != nil {
			log.Printf("[%s] 连接读取结束: %v", t.deviceID, err)
			return
		}
		ch := t.audioChan
		if msgType == websocket.TextMessage {
			ch = t.cmdChan
		}
		select {
		case ch <- data:
		case <-t.closed:
			return
		}
	}
}

// pingLoop 定时发心跳，避免中间设备/网关断开空闲连接。
// 真实实现也是 30 秒一次。
func (t *transport) pingLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := t.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
				log.Printf("[%s] 心跳发送失败: %v", t.deviceID, err)
				t.Close()
				return
			}
		case <-t.closed:
			return
		}
	}
}

// SendCmd 发送一条 JSON 文本帧。
func (t *transport) SendCmd(msg ServerMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	select {
	case <-t.closed:
		return nil
	default:
	}
	if err := t.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		return err
	}
	log.Printf("[%s] 下发 %s", t.deviceID, data)
	return nil
}

// SendAudio 发送一个二进制帧（一帧 Opus）。
func (t *transport) SendAudio(frame []byte) error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	select {
	case <-t.closed:
		return nil
	default:
	}
	return t.conn.WriteMessage(websocket.BinaryMessage, frame)
}

// Cmd 返回文本帧的只读通道。
func (t *transport) Cmd() <-chan []byte { return t.cmdChan }

// Audio 返回二进制帧的只读通道。
func (t *transport) Audio() <-chan []byte { return t.audioChan }

// Done 在连接关闭后被关闭。
func (t *transport) Done() <-chan struct{} { return t.closed }

func (t *transport) Close() {
	t.closeOnce.Do(func() {
		close(t.closed)
		_ = t.conn.Close()
	})
}
