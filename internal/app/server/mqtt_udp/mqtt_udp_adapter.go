package mqtt_udp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"xiaozhi-esp32-server-golang/internal/app/server/types"
	"xiaozhi-esp32-server-golang/internal/data/client"
	. "xiaozhi-esp32-server-golang/internal/data/client"
	msgdata "xiaozhi-esp32-server-golang/internal/data/msg"
	. "xiaozhi-esp32-server-golang/logger"
	log "xiaozhi-esp32-server-golang/logger"
)

//&MqttConfig{
//    Broker:   "127.0.0.1",           // broker 主机地址
//    Type:     "tcp",                       // 连接协议
//    Port:     1883,                        // broker 端口
//    ClientID: "xiaozhi-server-001",        // 客户端 ID
//    Username: "myuser",                    // 用户名
//    Password: "mypassword",                // 密码
//}

type MqttConfig struct {
	Broker   string
	Type     string
	Port     int
	ClientID string
	Username string
	Password string
}

// MqttUdpAdapter MQTT-UDP适配器结构
type MqttUdpAdapter struct {
	client             mqtt.Client
	udpServer          *UdpServer
	mqttConfig         *MqttConfig
	deviceId2Conn      *sync.Map
	lifecycleStates    *sync.Map
	msgChan            chan mqtt.Message
	onNewConnection    types.OnNewConnection
	onDeviceOnline     func(deviceID string)
	onDeviceOffline    func(deviceID string)
	onTransportReady   func(deviceID string)
	offlineGracePeriod time.Duration
	stopCtx            context.Context
	stopCancel         context.CancelFunc
	sync.RWMutex
}

type mqttDeviceLifecycleState struct {
	mu             sync.Mutex
	brokerOnline   bool
	lastEventTs    int64
	cleanupTimer   *time.Timer
	cleanupVersion uint64
}

const defaultOfflineGracePeriod = 2 * time.Minute

// MqttUdpAdapterOption 用于可选参数
type MqttUdpAdapterOption func(*MqttUdpAdapter)

// WithUdpServer 设置 udpServer
func WithUdpServer(udpServer *UdpServer) MqttUdpAdapterOption {
	return func(s *MqttUdpAdapter) {
		s.udpServer = udpServer
	}
}

func WithOnNewConnection(onNewConnection types.OnNewConnection) MqttUdpAdapterOption {
	return func(s *MqttUdpAdapter) {
		s.onNewConnection = onNewConnection
	}
}

func WithOnDeviceOnline(onDeviceOnline func(deviceID string)) MqttUdpAdapterOption {
	return func(s *MqttUdpAdapter) {
		s.onDeviceOnline = onDeviceOnline
	}
}

func WithOnDeviceOffline(onDeviceOffline func(deviceID string)) MqttUdpAdapterOption {
	return func(s *MqttUdpAdapter) {
		s.onDeviceOffline = onDeviceOffline
	}
}

func WithOnTransportReady(onTransportReady func(deviceID string)) MqttUdpAdapterOption {
	return func(s *MqttUdpAdapter) {
		s.onTransportReady = onTransportReady
	}
}

func WithOfflineGracePeriod(gracePeriod time.Duration) MqttUdpAdapterOption {
	return func(s *MqttUdpAdapter) {
		s.offlineGracePeriod = gracePeriod
	}
}

// NewMqttUdpAdapter 创建新的MQTT-UDP适配器，config为必传，其它参数用Option
func NewMqttUdpAdapter(config *MqttConfig, opts ...MqttUdpAdapterOption) *MqttUdpAdapter {
	ctx, cancel := context.WithCancel(context.Background())
	s := &MqttUdpAdapter{
		mqttConfig:         config,
		deviceId2Conn:      &sync.Map{},
		lifecycleStates:    &sync.Map{},
		msgChan:            make(chan mqtt.Message, 10000),
		offlineGracePeriod: defaultOfflineGracePeriod,
		stopCtx:            ctx,
		stopCancel:         cancel,
	}
	for _, opt := range opts {
		opt(s)
	}

	go s.processMessage()
	return s
}

func (s *MqttUdpAdapter) getClient() mqtt.Client {
	s.RLock()
	client := s.client
	s.RUnlock()
	return client
}

func (s *MqttUdpAdapter) setClient(client mqtt.Client) {
	s.Lock()
	s.client = client
	s.Unlock()
	s.updateSessionsClient(client)
}

func (s *MqttUdpAdapter) getUdpServer() *UdpServer {
	s.RLock()
	udpServer := s.udpServer
	s.RUnlock()
	return udpServer
}

func (s *MqttUdpAdapter) setUdpServer(udpServer *UdpServer) {
	s.Lock()
	s.udpServer = udpServer
	s.Unlock()
}

func (s *MqttUdpAdapter) updateSessionsClient(client mqtt.Client) {
	s.deviceId2Conn.Range(func(key, value interface{}) bool {
		if conn, ok := value.(*MqttUdpConn); ok {
			conn.SetMqttClient(client)
		}
		return true
	})
}

func (s *MqttUdpAdapter) clearDeviceSessions() {
	s.deviceId2Conn.Range(func(key, value interface{}) bool {
		if conn, ok := value.(*MqttUdpConn); ok {
			conn.Destroy()
		}
		s.deviceId2Conn.Delete(key)
		return true
	})
}

func (s *MqttUdpAdapter) clearLifecycleStates() {
	if s.lifecycleStates == nil {
		return
	}
	s.lifecycleStates.Range(func(key, value interface{}) bool {
		if state, ok := value.(*mqttDeviceLifecycleState); ok {
			state.mu.Lock()
			if state.cleanupTimer != nil {
				state.cleanupTimer.Stop()
				state.cleanupTimer = nil
			}
			state.mu.Unlock()
		}
		s.lifecycleStates.Delete(key)
		return true
	})
}

// Start 启动 MQTT 客户端（非阻塞）：在后台 goroutine 中连接并重试，不阻塞程序运行
func (s *MqttUdpAdapter) Start() error {
	Infof("MqttUdpAdapter开始启动，后台连接MQTT服务器 Broker=%s:%d ClientID=%s", s.mqttConfig.Broker, s.mqttConfig.Port, s.mqttConfig.ClientID)
	go s.connectAndRetry()
	return nil
}

// connectAndRetry 在后台循环连接 MQTT，连接失败时按间隔重试，与 mqtt_server 解耦不阻塞主流程
func (s *MqttUdpAdapter) connectAndRetry() {
	const retryInterval = 5 * time.Second

	s.RLock()
	cfg := s.mqttConfig
	s.RUnlock()
	if cfg == nil {
		return
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("%s://%s:%d", cfg.Type, cfg.Broker, cfg.Port))
	opts.SetClientID(cfg.ClientID)
	opts.SetUsername(cfg.Username)
	opts.SetPassword(cfg.Password)

	opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		Errorf("MQTT连接丢失: %v", err)
	})

	opts.SetOnConnectHandler(func(client mqtt.Client) {
		Info("MQTT已连接")
		topic := ServerSubTopicPrefix
		if token := client.Subscribe(topic, 0, s.handleMessage); token.Wait() && token.Error() != nil {
			Errorf("订阅主题失败: %v", token.Error())
		}
	})

	var retryCount int
	for {
		select {
		case <-s.stopCtx.Done():
			return
		default:
		}
		client := mqtt.NewClient(opts)
		s.setClient(client)
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			retryCount++
			Errorf("连接MQTT服务器失败(第%d次): %v，%d秒后重试", retryCount, token.Error(), int(retryInterval.Seconds()))
			select {
			case <-s.stopCtx.Done():
				return
			case <-time.After(retryInterval):
				continue
			}
		}
		break
	}

	_ = s.checkClientActive()
}

func (s *MqttUdpAdapter) checkClientActive() error {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-s.stopCtx.Done():
				return
			case <-ticker.C:
				s.deviceId2Conn.Range(func(key, value interface{}) bool {
					conn := value.(*MqttUdpConn)
					if !conn.IsActive() {
						conn.Destroy()
					}
					return true
				})
			}
		}
	}()
	return nil
}

func (s *MqttUdpAdapter) SetDeviceSession(deviceId string, conn *MqttUdpConn) {
	Debugf("SetDeviceSession, deviceId: %s", deviceId)
	s.deviceId2Conn.Store(deviceId, conn)
}

func (s *MqttUdpAdapter) getDeviceSession(deviceId string) *MqttUdpConn {
	Debugf("getDeviceSession, deviceId: %s", deviceId)
	if conn, ok := s.deviceId2Conn.Load(deviceId); ok {
		return conn.(*MqttUdpConn)
	}
	return nil
}

// handleMessage 将消息丢进队列
func (s *MqttUdpAdapter) handleMessage(client mqtt.Client, msg mqtt.Message) {
	select {
	case s.msgChan <- msg:
		return
	default:
		Debugf("handleMessage msg chan is full, topic: %s, payload: %s", msg.Topic(), string(msg.Payload()))
	}
}

// 断开连接，超时或goodbye主动断开
func (s *MqttUdpAdapter) handleDisconnect(deviceId string, closingConn *MqttUdpConn) {
	Debugf("handleDisconnect, deviceId: %s", deviceId)

	conn := s.getDeviceSession(deviceId)
	if closingConn != nil && conn != nil && conn != closingConn {
		Debugf("handleDisconnect, ignore stale conn for deviceId: %s", deviceId)
		return
	}

	notifyOffline := false
	if state := s.getLifecycleState(deviceId); state != nil {
		shouldDeleteState := false
		state.mu.Lock()
		if state.brokerOnline {
			state.brokerOnline = false
			state.cleanupVersion++
			if state.cleanupTimer != nil {
				state.cleanupTimer.Stop()
				state.cleanupTimer = nil
			}
			notifyOffline = true
		}
		shouldDeleteState = !state.brokerOnline && state.cleanupTimer == nil
		state.mu.Unlock()
		if shouldDeleteState {
			s.lifecycleStates.Delete(deviceId)
		}
	}

	if conn == nil {
		Debugf("handleDisconnect, deviceId: %s not found", deviceId)
		if notifyOffline && s.onDeviceOffline != nil {
			s.onDeviceOffline(deviceId)
		}
		return
	}
	conn.ReleaseUdpSession()
	s.deviceId2Conn.Delete(deviceId)
	if notifyOffline && s.onDeviceOffline != nil {
		s.onDeviceOffline(deviceId)
	}
}

// Stop 停止适配器：取消 context、断开 MQTT、关闭 UDP、清理会话（供热更前调用）
func (s *MqttUdpAdapter) Stop() {
	Debugf("enter MqttUdpAdapter Stop ")
	defer Debugf("exit MqttUdpAdapter Stop ")
	s.stopCancel()
	client := s.getClient()
	if client != nil && client.IsConnected() {
		Debugf("MqttUdpAdapter Stop, disconnect mqtt client")
		client.Disconnect(250)
	}
	udpServer := s.getUdpServer()
	Debugf("MqttUdpAdapter Stop, udpServer: %v", udpServer)
	if udpServer != nil {
		Debugf("MqttUdpAdapter Stop, close udpServer")
		_ = udpServer.Close()
	}
	s.clearLifecycleStates()
	s.clearDeviceSessions()
}

// ReloadMqttClient 仅重连 MQTT（保持 UDP 服务器实例）
func (s *MqttUdpAdapter) ReloadMqttClient(newConfig *MqttConfig) {
	if newConfig == nil {
		return
	}
	s.Lock()
	s.mqttConfig = newConfig
	oldClient := s.client
	s.Unlock()
	if oldClient != nil && oldClient.IsConnected() {
		oldClient.Disconnect(250)
	}
	s.clearLifecycleStates()
	s.clearDeviceSessions()
	go s.connectAndRetry()
}

// ReloadUdpServer 仅重启 UDP（保持 MQTT 连接）
func (s *MqttUdpAdapter) ReloadUdpServer(newUdpServer *UdpServer) {
	if newUdpServer == nil {
		return
	}
	oldUdp := s.getUdpServer()
	s.clearLifecycleStates()
	s.clearDeviceSessions()
	s.setUdpServer(newUdpServer)
	if oldUdp != nil {
		_ = oldUdp.Close()
	}
}

func (s *MqttUdpAdapter) getLifecycleState(deviceID string) *mqttDeviceLifecycleState {
	if s.lifecycleStates == nil || deviceID == "" {
		return nil
	}
	if state, ok := s.lifecycleStates.Load(deviceID); ok {
		if lifecycleState, ok := state.(*mqttDeviceLifecycleState); ok {
			return lifecycleState
		}
	}
	return nil
}

func (s *MqttUdpAdapter) getOrCreateLifecycleState(deviceID string) *mqttDeviceLifecycleState {
	if s.lifecycleStates == nil || deviceID == "" {
		return nil
	}
	if state := s.getLifecycleState(deviceID); state != nil {
		return state
	}
	newState := &mqttDeviceLifecycleState{}
	actual, _ := s.lifecycleStates.LoadOrStore(deviceID, newState)
	return actual.(*mqttDeviceLifecycleState)
}

// 标记设备在线
// 返回值：
// 状态是否从"离线"翻转为"在线"
// 这个事件是否被接受处理
func (s *MqttUdpAdapter) markDeviceOnline(deviceID string, eventTs int64) (bool, bool) {
	state := s.getOrCreateLifecycleState(deviceID)
	if state == nil {
		return false, false
	}
	if eventTs <= 0 {
		eventTs = time.Now().UnixMilli()
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	if state.lastEventTs > 0 && eventTs < state.lastEventTs {
		return false, false
	}

	if eventTs > state.lastEventTs {
		state.lastEventTs = eventTs
	}

	wasOnline := state.brokerOnline
	state.brokerOnline = true
	state.cleanupVersion++
	if state.cleanupTimer != nil {
		state.cleanupTimer.Stop()
		state.cleanupTimer = nil
	}
	return !wasOnline, true
}

func (s *MqttUdpAdapter) markDeviceOffline(deviceID string, eventTs int64) (bool, uint64) {
	state := s.getOrCreateLifecycleState(deviceID)
	if state == nil {
		return false, 0
	}
	if eventTs <= 0 {
		eventTs = time.Now().UnixMilli()
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	if state.lastEventTs > 0 && eventTs < state.lastEventTs {
		return false, 0
	}

	if eventTs > state.lastEventTs {
		state.lastEventTs = eventTs
	}

	wasOnline := state.brokerOnline
	state.brokerOnline = false
	state.cleanupVersion++
	version := state.cleanupVersion
	if state.cleanupTimer != nil {
		state.cleanupTimer.Stop()
	}
	state.cleanupTimer = time.AfterFunc(s.offlineGracePeriod, func() {
		s.cleanupOfflineTransport(deviceID, version)
	})
	return wasOnline, version
}

func (s *MqttUdpAdapter) cleanupOfflineTransport(deviceID string, version uint64) {
	state := s.getLifecycleState(deviceID)
	if state == nil {
		return
	}

	state.mu.Lock()
	if state.brokerOnline || state.cleanupVersion != version {
		state.mu.Unlock()
		return
	}
	state.cleanupTimer = nil
	state.mu.Unlock()

	conn := s.getDeviceSession(deviceID)
	if conn == nil {
		s.lifecycleStates.Delete(deviceID)
		return
	}
	conn.Destroy()
}

func (s *MqttUdpAdapter) EnsureDeviceTransport(deviceId string) (*MqttUdpConn, error) {
	deviceSession := s.getDeviceSession(deviceId)
	if deviceSession != nil {
		deviceSession.MarkBrokerOnline()
		return deviceSession, nil
	}

	udpServer, udpSession, err := s.createUdpSession(deviceId)
	if err != nil {
		return nil, fmt.Errorf("创建 udpSession 失败, deviceId: %s, err: %w", deviceId, err)
	}

	topicMacAddr := strings.ReplaceAll(deviceId, ":", "_")
	publicTopic := fmt.Sprintf("%s%s", client.ServerPubTopicPrefix, topicMacAddr)

	mqttClient := s.getClient()
	if mqttClient == nil {
		udpServer.CloseSessionByRef(udpSession)
		return nil, fmt.Errorf("mqtt client is nil, deviceId: %s", deviceId)
	}

	deviceSession = NewMqttUdpConn(deviceId, publicTopic, mqttClient, udpServer, udpSession)
	s.bindUdpSessionData(deviceSession, udpSession)
	deviceSession.MarkBrokerOnline()

	s.SetDeviceSession(deviceId, deviceSession)
	deviceSession.OnClose(func(closedDeviceID string) {
		s.handleDisconnect(closedDeviceID, deviceSession)
	})

	if s.onNewConnection != nil {
		s.onNewConnection(deviceSession)
	}
	return deviceSession, nil
}

func (s *MqttUdpAdapter) promoteDeviceOnline(deviceID string, eventTs int64) (*MqttUdpConn, bool, bool, error) {
	notifyOnline, acceptedOnline := s.markDeviceOnline(deviceID, eventTs)
	if !acceptedOnline {
		return s.getDeviceSession(deviceID), false, false, nil
	}
	conn, err := s.EnsureDeviceTransport(deviceID)
	if err != nil {
		return nil, notifyOnline, true, err
	}
	if conn != nil {
		conn.MarkBrokerOnline()
	}
	return conn, notifyOnline, true, nil
}

func (s *MqttUdpAdapter) handleLifecycleMessage(payload []byte) {
	var lifecycleEvent msgdata.MqttLifecycleEvent
	if err := json.Unmarshal(payload, &lifecycleEvent); err != nil {
		Errorf("解析 MQTT 生命周期消息失败: %v", err)
		return
	}
	deviceID := strings.TrimSpace(lifecycleEvent.DeviceID)
	if deviceID == "" {
		Errorf("MQTT 生命周期消息缺少 device_id: %s", string(payload))
		return
	}

	switch strings.TrimSpace(lifecycleEvent.State) {
	case msgdata.MqttLifecycleStateOnline:
		_, notifyOnline, acceptedOnline, err := s.promoteDeviceOnline(deviceID, lifecycleEvent.Ts)
		if err != nil {
			Errorf("处理 MQTT 上线事件失败: device=%s err=%v", deviceID, err)
			return
		}
		if !acceptedOnline {
			return
		}
		if notifyOnline && s.onDeviceOnline != nil {
			s.onDeviceOnline(deviceID)
		}
		// 设备重启后即便 broker 在线状态没有翻转，online 广播仍然意味着设备侧 runtime 可能已重建，
		// 需要触发 transport ready 链路重置 IoT MCP runtime 并重新 initialize。
		if s.onTransportReady != nil {
			s.onTransportReady(deviceID)
		}
	case msgdata.MqttLifecycleStateOffline:
		notifyOffline, cleanupVersion := s.markDeviceOffline(deviceID, lifecycleEvent.Ts)
		if cleanupVersion == 0 {
			return
		}
		conn := s.getDeviceSession(deviceID)
		if conn != nil {
			conn.MarkBrokerOffline(s.offlineGracePeriod)
		}
		if notifyOffline && s.onDeviceOffline != nil {
			s.onDeviceOffline(deviceID)
		}
	default:
		Warnf("忽略未知 MQTT 生命周期状态: device=%s state=%s", deviceID, lifecycleEvent.State)
	}
}

// 处理消息
func (s *MqttUdpAdapter) processMessage() {
	for {
		select {
		case <-s.stopCtx.Done():
			return
		case mqttMsg := <-s.msgChan:
			Debugf("mqtt handleMessage, topic: %s, payload: %s", mqttMsg.Topic(), string(mqttMsg.Payload()))
			if mqttMsg.Topic() == msgdata.MDeviceLifecycleTopic {
				s.handleLifecycleMessage(mqttMsg.Payload())
				continue
			}
			var clientMsg ClientMessage
			if err := json.Unmarshal(mqttMsg.Payload(), &clientMsg); err != nil {
				Errorf("解析JSON失败: %v", err)
				continue
			}
			_, deviceId := s.getDeviceIdByTopic(mqttMsg.Topic())
			if deviceId == "" {
				Errorf("mac_addr解析失败: %v", mqttMsg.Topic())
				continue
			}

			existingSession := s.getDeviceSession(deviceId)
			deviceSession, notifyOnline, _, err := s.promoteDeviceOnline(deviceId, time.Now().UnixMilli())
			if err != nil {
				Errorf("确保 MQTT transport 在线失败: device=%s err=%v", deviceId, err)
				continue
			}
			if notifyOnline && s.onDeviceOnline != nil {
				s.onDeviceOnline(deviceId)
			}
			if notifyOnline && s.onTransportReady != nil {
				s.onTransportReady(deviceId)
			}
			if existingSession != nil && clientMsg.Type == "hello" {
				newUdpSession, err := s.rotateDeviceUdpSession(deviceSession, deviceId)
				if err != nil {
					Errorf("hello 重建 udpSession 失败, deviceId: %s, err: %v", deviceId, err)
					continue
				}
				Debugf("hello 重建 udpSession 成功, deviceId: %s, connID: %s", deviceId, newUdpSession.ConnId)
			}

			if err := deviceSession.PushMsgToRecvCmd(mqttMsg.Payload()); err != nil {
				Errorf("InternalRecvCmd失败: %v", err)
				continue
			}
		}
	}
}

func (s *MqttUdpAdapter) createUdpSession(deviceId string) (*UdpServer, *UdpSession, error) {
	udpServer := s.getUdpServer()
	if udpServer == nil {
		return nil, nil, fmt.Errorf("udpServer is nil")
	}
	udpSession := udpServer.CreateSession(deviceId, "")
	if udpSession == nil {
		return nil, nil, fmt.Errorf("udpSession is nil")
	}
	return udpServer, udpSession, nil
}

func (s *MqttUdpAdapter) bindUdpSessionData(deviceSession *MqttUdpConn, udpSession *UdpSession) {
	if deviceSession == nil || udpSession == nil {
		return
	}
	deviceSession.SetUdpSession(udpSession)
	strAesKey, strFullNonce := udpSession.GetAesKeyAndNonce()
	deviceSession.SetData("aes_key", strAesKey)
	deviceSession.SetData("full_nonce", strFullNonce)
}

func (s *MqttUdpAdapter) rotateDeviceUdpSession(deviceSession *MqttUdpConn, deviceId string) (*UdpSession, error) {
	if deviceSession == nil {
		return nil, fmt.Errorf("deviceSession is nil")
	}
	udpServer, udpSession, err := s.createUdpSession(deviceId)
	if err != nil {
		return nil, err
	}
	oldSession := deviceSession.GetUdpSession()
	s.bindUdpSessionData(deviceSession, udpSession)
	if oldSession != nil {
		udpServer.CloseSessionByRef(oldSession)
	}
	return udpSession, nil
}

func (s *MqttUdpAdapter) getDeviceIdByTopic(topic string) (string, string) {
	var topicMacAddr, deviceId string
	//根据topic(/p2p/device_public/mac_addr)解析出来mac_addr
	strList := strings.Split(topic, "/")
	if len(strList) == 4 {
		topicMacAddr = strList[3]

		// 检查是否为新格式: "GID_test@@@ba_8f_17_de_94_94@@@e4b0c442-98fc-4e1b-8c3d-6a5b6a5b6a6d"
		if strings.Contains(topicMacAddr, "@@@") {
			parts := strings.Split(topicMacAddr, "@@@")
			if len(parts) >= 2 {
				// 提取中间部分作为MAC地址
				macAddr := parts[1]
				deviceId = strings.ReplaceAll(macAddr, "_", ":")
			}
		} else {
			deviceId = strings.ReplaceAll(topicMacAddr, "_", ":")
		}
	}

	log.Log().Debugf("topicMacAddr: %s, deviceId: %s", topicMacAddr, deviceId)
	return topicMacAddr, deviceId
}
