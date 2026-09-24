package xiaozhi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	log "xiaozhi-esp32-server-golang/logger"

	"github.com/gorilla/websocket"
)

var deviceIdList = []string{
	"ba:8f:17:de:94:94",
	"f2:85:44:27:7b:51",
	"4f:57:fb:d4:69:fa",
	"b3:1e:1c:80:cc:78",
	"32:a5:cc:b7:c0:e4",
	"2b:60:6a:5a:72:10",
	"ca:a6:8b:20:f1:6f",
	"26:1a:d7:27:9f:f8",
	"03:02:26:58:2b:06",
	"5f:f3:85:8b:5d:da",
}

// 记录最近出错的deviceId及其禁用到期时间
var (
	deviceIdBlocklist     = make(map[string]time.Time)
	deviceIdBlocklistLock sync.Mutex
	// 设备ID禁用时间（出错后多久内不使用）
	deviceIdBlockDuration = 5 * time.Second
)

// XiaozhiProvider 小智TTS WebSocket Provider
// 支持流式文本转语音
type XiaozhiProvider struct {
	ServerAddr  string
	DeviceID    string
	AudioFormat map[string]interface{}
	Header      http.Header
}

// 定期清理过期的deviceId禁用列表
func init() {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			// 清理过期的deviceId禁用列表
			deviceIdBlocklistLock.Lock()
			now := time.Now()
			for id, expireTime := range deviceIdBlocklist {
				if now.After(expireTime) {
					delete(deviceIdBlocklist, id)
					log.Debugf("设备ID禁用已过期，重新启用: %s", id)
				}
			}
			deviceIdBlocklistLock.Unlock()
		}
	}()
}

// 将deviceId添加到禁用列表
func blockDeviceId(deviceId string) {
	deviceIdBlocklistLock.Lock()
	defer deviceIdBlocklistLock.Unlock()

	deviceIdBlocklist[deviceId] = time.Now().Add(deviceIdBlockDuration)
	log.Warnf("设备ID %s 已添加到禁用列表，将在 %v 后重新启用", deviceId, deviceIdBlockDuration)
}

// 检查deviceId是否在禁用列表中
func isDeviceIdBlocked(deviceId string) bool {
	deviceIdBlocklistLock.Lock()
	defer deviceIdBlocklistLock.Unlock()

	expireTime, exists := deviceIdBlocklist[deviceId]
	if !exists {
		return false
	}

	// 如果过期时间已过，则从禁用列表中移除
	if time.Now().After(expireTime) {
		delete(deviceIdBlocklist, deviceId)
		log.Debugf("设备ID禁用已过期，重新启用: %s", deviceId)
		return false
	}

	return true
}

// NewXiaozhiProvider 创建新的小智TTS Provider
func NewXiaozhiProvider(config map[string]interface{}) *XiaozhiProvider {
	serverAddr, _ := config["server_addr"].(string)
	deviceID, _ := config["device_id"].(string)
	clientID, _ := config["client_id"].(string)
	token, _ := config["token"].(string)
	format := map[string]interface{}{
		"sample_rate":    16000,
		"channels":       1,
		"frame_duration": 20,
		"format":         "opus",
	}

	header := http.Header{}
	header.Set("Device-Id", deviceID)
	header.Set("Content-Type", "application/json")
	header.Set("Authorization", "Bearer "+token)
	header.Set("Protocol-Version", "1")
	header.Set("Client-Id", clientID)

	return &XiaozhiProvider{
		ServerAddr:  serverAddr,
		DeviceID:    deviceID,
		AudioFormat: format,
		Header:      header,
	}
}

// selectDeviceId 选择一个可用的设备ID
func (p *XiaozhiProvider) selectDeviceId() string {
	// 从deviceIdList中找出未被禁用的deviceId
	for _, deviceId := range deviceIdList {
		if !isDeviceIdBlocked(deviceId) {
			log.Debugf("选择未被禁用的设备ID: %s", deviceId)
			return deviceId
		}
	}

	// 如果所有deviceId都被禁用，则从所有deviceId中轮询选择
	if len(deviceIdList) > 0 {
		// 使用简单的轮询策略（基于时间）
		selectedIndex := int(time.Now().Unix()) % len(deviceIdList)
		selectedDeviceId := deviceIdList[selectedIndex]
		log.Warnf("所有deviceId均被禁用，轮询选择设备ID: %s (索引: %d)", selectedDeviceId, selectedIndex)
		return selectedDeviceId
	}

	// 如果deviceIdList为空，使用传入的deviceId
	if p.DeviceID != "" {
		log.Warnf("deviceIdList为空，使用当前设备ID: %s", p.DeviceID)
		return p.DeviceID
	}

	// 如果都没有，返回第一个设备ID（如果存在）
	if len(deviceIdList) > 0 {
		return deviceIdList[0]
	}

	return ""
}

// createWSConnection 创建新的WebSocket连接
func (p *XiaozhiProvider) createWSConnection(ctx context.Context) (*websocket.Conn, string, error) {
	// 选择一个可用的设备ID
	selectedDeviceId := p.selectDeviceId()
	if selectedDeviceId == "" {
		return nil, "", fmt.Errorf("无法选择设备ID")
	}

	// 更新当前p.DeviceID和Header
	p.DeviceID = selectedDeviceId
	p.Header.Set("Device-Id", selectedDeviceId)

	// 创建新连接
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, p.ServerAddr, p.Header)
	if err != nil {
		log.Errorf("创建WebSocket连接失败: %v, 设备ID: %s", err, selectedDeviceId)
		blockDeviceId(selectedDeviceId) // 将失败的deviceId加入禁用列表
		return nil, "", err
	}

	// 设置保持连接
	conn.SetPingHandler(func(appData string) error {
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(5*time.Second))
	})

	// 新建连接时发送hello消息
	helloMsg := map[string]interface{}{
		"type":         "hello",
		"device_id":    selectedDeviceId,
		"transport":    "websocket",
		"version":      1,
		"audio_params": p.AudioFormat,
	}
	log.Debugf("创建新连接并发送hello消息，设备ID: %s", selectedDeviceId)
	if err := conn.WriteJSON(helloMsg); err != nil {
		conn.Close()
		return nil, "", fmt.Errorf("发送hello消息失败: %v", err)
	}

	return conn, selectedDeviceId, nil
}

type RecvMsg struct {
	Type    string `json:"type"`
	State   string `json:"state"`
	Text    string `json:"text"`
	Version int    `json:"version"`
}

// sendStopMessage 发送stop消息并关闭连接
func sendStopMessage(conn *websocket.Conn, deviceId string) {
	stopMsg := map[string]interface{}{
		"type":      "listen",
		"device_id": deviceId,
		"state":     "stop",
	}
	if err := conn.WriteJSON(stopMsg); err != nil {
		log.Warnf("发送stop消息失败: %v, 设备ID: %s", err, deviceId)
	} else {
		log.Debugf("发送stop消息成功，设备ID: %s", deviceId)
	}
}

// handleTTSConnection 封装获取连接、发送消息和接收消息的逻辑
func (p *XiaozhiProvider) handleTTSConnection(ctx context.Context, text string, outputChan chan []byte) error {
	// 创建新连接
	conn, deviceId, err := p.createWSConnection(ctx)
	if err != nil {
		return fmt.Errorf("创建小智TTS连接失败: %v", err)
	}
	defer func() {
		// 发送stop消息并关闭连接
		sendStopMessage(conn, deviceId)
		conn.Close()
	}()

	// 发送listen detect消息
	sendText := fmt.Sprintf("`%s`", text)
	listenMsg := map[string]interface{}{
		"type":      "listen",
		"device_id": deviceId,
		"state":     "detect",
		"text":      sendText,
	}
	log.Debugf("发送xiaozhi服务端消息: %v", listenMsg)

	if err := conn.WriteJSON(listenMsg); err != nil {
		log.Errorf("发送listen消息失败: %v，设备ID: %s", err, deviceId)
		blockDeviceId(deviceId) // 将出错的deviceId加入禁用列表
		return fmt.Errorf("发送消息失败: %v", err)
	}

	// 读取并处理消息
	startTs := time.Now().UnixMilli()
	var firstFrameTs bool
	i := 0
	receivedFrames := false

	for {
		select {
		case <-ctx.Done():
			log.Debugf("xiaozhi服务端消息ctx.Done(), 设备ID: %s", deviceId)
			return nil
		default:
		}
		msgType, msg, err := conn.ReadMessage()
		if err != nil {
			// 连接出错
			log.Errorf("读取消息错误: %v，设备ID: %s", err, deviceId)

			// 如果还没有收到任何音频帧，说明连接可能有问题，将deviceId加入禁用列表
			if !receivedFrames {
				blockDeviceId(deviceId)
			}

			return fmt.Errorf("读取消息错误: %v", err)
		}
		if msgType == websocket.TextMessage {
			log.Debugf("收到xiaozhi服务端消息: %s", string(msg))
			var recvMsg RecvMsg
			err := json.Unmarshal(msg, &recvMsg)
			if err != nil {
				continue
			}
			if recvMsg.Type == "tts" {
				if recvMsg.State == "stop" {
					log.Debugf("xiaozhi服务端消息tts stop消息")
					return nil
				}
			}
		} else if msgType == websocket.BinaryMessage {
			receivedFrames = true
			if !firstFrameTs {
				firstFrameTs = true
				log.Debugf("tts耗时统计: xiaozhi服务tts 第一个音频帧时间: %d", time.Now().UnixMilli()-startTs)
			}
			outputChan <- msg
			if i%20 == 0 {
				log.Debugf("xiaozhi服务端音频消息, 已收到%d个音频帧", i)
			}
			i++
		}
	}
}

// TextToSpeechStream 实现流式TTS，返回opus音频帧chan
func (p *XiaozhiProvider) TextToSpeechStream(ctx context.Context, text string, sampleRate int, channels int, frameDuration int) (chan []byte, error) {
	outputChan := make(chan []byte, 1000)

	// 尝试处理TTS连接，支持重试
	go func() {
		defer close(outputChan)

		retryCount := 0
		maxRetries := 2
		var lastError error

		// 最多尝试maxRetries次
		for retryCount <= maxRetries {
			if retryCount > 0 {
				log.Infof("尝试重新获取连接，第 %d/%d 次重试", retryCount, maxRetries)

				// 在重试前检查上下文是否已取消
				select {
				case <-ctx.Done():
					log.Debugf("上下文已取消，停止重试")
					return
				default:
					// 继续重试
				}
			}

			// 处理TTS连接
			err := p.handleTTSConnection(ctx, text, outputChan)

			if err == nil {
				// 连接处理成功，无需重试
				return
			}

			lastError = err
			log.Errorf("TTS连接处理失败: %v (重试: %d/%d)", err, retryCount, maxRetries)

			retryCount++
		}

		if retryCount > maxRetries {
			log.Warnf("达到最大重试次数 %d，放弃重试，最后错误: %v", maxRetries, lastError)
		}
	}()

	return outputChan, nil
}

// GetVoiceInfo 获取TTS配置信息
func (p *XiaozhiProvider) GetVoiceInfo() map[string]interface{} {
	return map[string]interface{}{
		"type":         "xiaozhi_ws",
		"server_addr":  p.ServerAddr,
		"device_id":    p.DeviceID,
		"audio_format": p.AudioFormat,
	}
}

// SetVoice 设置音色参数（Xiaozhi Provider 不支持动态设置音色）
func (p *XiaozhiProvider) SetVoice(voiceConfig map[string]interface{}) error {
	return fmt.Errorf("Xiaozhi TTS Provider 不支持动态设置音色")
}

// Close 关闭资源（无状态 Provider，无需关闭）
func (p *XiaozhiProvider) Close() error {
	return nil
}

// IsValid 检查资源是否有效
func (p *XiaozhiProvider) IsValid() bool {
	return p != nil
}

// TextToSpeech 实现 BaseTTSProvider 接口，直接聚合流式帧
func (p *XiaozhiProvider) TextToSpeech(ctx context.Context, text string, sampleRate int, channels int, frameDuration int) ([][]byte, error) {
	ch, err := p.TextToSpeechStream(ctx, text, sampleRate, channels, frameDuration)
	if err != nil {
		return nil, err
	}
	var frames [][]byte
	for frame := range ch {
		frames = append(frames, frame)
	}
	return frames, nil
}
