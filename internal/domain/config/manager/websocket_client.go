package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cloudwego/eino/components/tool"
	einoschema "github.com/cloudwego/eino/schema"
	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	cmap "github.com/orcaman/concurrent-map/v2"

	"xiaozhi-esp32-server-golang/internal/domain/config/types"
	"xiaozhi-esp32-server-golang/internal/domain/mcp"
	"xiaozhi-esp32-server-golang/internal/domain/openclaw"
	"xiaozhi-esp32-server-golang/internal/util"
	log "xiaozhi-esp32-server-golang/logger"
)

// 请求回调函数，暂时不知道作用
type MessageHandleFunc func(*WebSocketRequest) (string, error)

// websocket客户端
type WebSocketClient struct {
	conn           *websocket.Conn // 记录websocket 连接， 在connect中进行赋值
	baseURL        string
	requestTimeout time.Duration
	responseChans  map[string]chan *WebSocketResponse // 添加通道是，将会使用mu锁， key 为uuid， 接收请求回调的响应数据
	callbacks      map[string]func(*WebSocketResponse)
	requestHandler func(*WebSocketRequest) // 处理收到的请求
	mu             sync.RWMutex            // 关闭responseChans和callbacks通道
	writeMu        sync.Mutex              // 保护WebSocket写入操作，防止并发写入
	isConnected    bool                    // 是否已连接上
	connectMu      sync.Mutex              // 连接锁对象, 为了控制conn的赋值，同时只有一个可以进行创建和操作
	messageQueue   chan *WebSocketRequest  // 无缓存的通道， 将发送的消息推送到这个通道，三个go程读取并进行发送， 只有你close掉，三个go程才会停止，workers才不会等待了
	workers        sync.WaitGroup          // 工作线程 创建了三个go程去处理消息

	messageHandle cmap.ConcurrentMap[string, MessageHandleFunc] // 啊后续消息处理器
	uuid          string

	// 重连相关字段
	retryStopChan  chan struct{}  // 重连协程停止信号
	retryWg        sync.WaitGroup // 重连协程等待组
	retryMu        sync.Mutex     // 保护重连相关操作
	isRetrying     bool           // 是否正在重连
	isShuttingDown bool           // 是否正在关闭（主动断开，不重连）
}

// 请求对象， id， 方法， 路径， 请求头， 请求体
type WebSocketRequest struct {
	ID      string                 `json:"id"`
	Method  string                 `json:"method"`
	Path    string                 `json:"path"`
	Headers map[string]string      `json:"headers,omitempty"`
	Body    map[string]interface{} `json:"body,omitempty"`
}

// 响应对象，记录id， 状态， 响应头， 响应体， 错误信息
type WebSocketResponse struct {
	ID      string                 `json:"id"`
	Status  int                    `json:"status"`
	Headers map[string]string      `json:"headers,omitempty"`
	Body    map[string]interface{} `json:"body,omitempty"`
	Error   string                 `json:"error,omitempty"`
}

// 连接manager时的websocket请求权， 就是存token的
type managerWSClientClaims struct {
	Purpose              string `json:"purpose"` // 目的
	UUID                 string `json:"uuid"`    // uuid
	jwt.RegisteredClaims        // 注册的请求权
}

var (
	defaultClient           *WebSocketClient             // 获取客户端对象，只是刚开始没有websocket对象
	clientOnce              sync.Once                    // 只会进行一次调用，就是创建default
	systemConfigPushHandler func(map[string]interface{}) // 回调函数，当系统配置发送改变，通知的时候，将会被调用进行通知
)

// SetSystemConfigPushHandler 设置收到 system_config 推送时的回调（主程序用于合并到 viper 等），由 user_config 在 Init 时注入
func SetSystemConfigPushHandler(fn func(map[string]interface{})) {
	systemConfigPushHandler = fn
}

// 获取默认websocket客户端
// 这里的sync.Once真好用，程序只会执行一次，非常适合单例模式
func GetDefaultClient() *WebSocketClient {
	clientOnce.Do(func() {
		defaultClient = NewWebSocketClient()
	})
	return defaultClient
}

// 创建一个websocket客户端
func NewWebSocketClient() *WebSocketClient {
	// 优先从环境变量获取，如果环境变量不存在则从配置获取
	baseURL := util.GetBackendURL() // "http://127.0.0.1:8080", 获取后台地址
	if baseURL == "" {
		baseURL = "http://localhost:8080" // 没有就是默认的
	}

	return &WebSocketClient{
		baseURL:        baseURL,                                   // 基础地址
		requestTimeout: 30 * time.Second,                          // 请求超时时间
		responseChans:  make(map[string]chan *WebSocketResponse),  // 一个map集合存到管道，不知道作用
		callbacks:      make(map[string]func(*WebSocketResponse)), // 一个map集合存储回调函数，不知道作用
		messageQueue:   make(chan *WebSocketRequest, 100),         // 100个大小的请求通道，不知道作用
		messageHandle:  cmap.New[MessageHandleFunc](),             // 并发安全 map， key为string， 值为函数回调
		uuid:           uuid.New().String(),                       // 生成uuid字符串
		retryStopChan:  make(chan struct{}),                       // 无缓存的通道
		isRetrying:     false,                                     // 是否重试中
	}
}

func NewWebSocketClientWithHandler(requestHandler func(*WebSocketRequest)) *WebSocketClient {
	client := NewWebSocketClient()
	client.requestHandler = requestHandler
	return client
}

// 客户端创建连接
func (c *WebSocketClient) Connect(ctx context.Context) error {
	// 连接加锁
	c.connectMu.Lock()
	// 执行完释放锁
	defer c.connectMu.Unlock()
	// 如果已连接，就退出
	if c.isConnected {
		return nil
	}

	// 将HTTP URL转换为WebSocket URL
	wsURL := "ws://" + c.baseURL[7:] + "/ws" // 去掉 "http://" 并添加 "/ws"   // ws://0.0.0.0:8000
	wsToken, err := c.generateWSToken()      // 生成wstoken
	if err != nil {
		return fmt.Errorf("生成WebSocket认证token失败: %v", err)
	}

	// 建立WebSocket连接 发起 WebSocket 拨号连接
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, http.Header{
		"Origin": []string{c.baseURL},
		"UUID":   []string{c.uuid},
		"Authorization": []string{
			"Bearer " + wsToken,
		},
	})
	if err != nil {
		return fmt.Errorf("WebSocket连接失败: %v", err)
	}
	// 将创建的对象进行保存
	c.conn = conn
	// 设置连接成功
	c.isConnected = true

	// 设置ping处理器， 拿到没有心跳检测没有道理啊， 后面有通过go程
	conn.SetPongHandler(func(appData string) error {
		log.Debugf("收到pong消息")
		return nil
	})

	// 启动消息处理循环，回头看这个 如果读取报错进行断开重新并退出
	go c.handleMessages()

	// 启动消息发送工作线程, 3个工作go程，读取通道的请求，进行websocket的发送，中间加了锁，保证不会并发
	c.startWorkers()

	// 启动心跳检测， 一个go程 每30秒发送一次心跳，如果报错进行断开重新并退出
	go c.startHeartbeat()

	log.Debugf("WebSocket客户端已连接到: %s", wsURL)
	return nil
}

// 生成token，
// 这个主要生成一个自己uuid表示唯一的自己
// 使用hs256对自己身份信息加密，使用同一的密码，进行加密生成token
func (c *WebSocketClient) generateWSToken() (string, error) {
	claims := managerWSClientClaims{
		Purpose: "manager-ws-client", // mangager的客户端ws请求权
		UUID:    c.uuid,              // 自己的uuid
		RegisteredClaims: jwt.RegisteredClaims{
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(15 * time.Minute)),
		}, // 配置发行时间和15分钟的过期时间，但是我没有看到其他表示的配置，难道不需要吗， 需要的，下面获取了
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims) // 通过s256进行加密生成token，太草率了吧，唯一密钥呢，不需要吗， 需要，工具中获取的
	secret := []byte(util.GetManagerEndpointAuthToken())       // 获取服务器需要的唯一密钥喽xiaozhi_mcp_openclaw_secret_key
	//eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJQdXJwb3NlIjoibWFuYWdlci13cy1jbGllbnQiLCJVVUlEIjoiLi4uIiwiaWF0IjoxNzI2MDQ4MDAwLCJleHAiOjE3MjYwNDg5MDB9.签名部分
	return token.SignedString(secret) // 生成签名密钥，服务端验证加密是否一致，不能方向解密签名部分， 内容在中间这段
}

// 供外部使用
func (c *WebSocketClient) Disconnect() error {
	return c.disconnect(false)
}

// disconnect 内部断开连接方法
// manualDisconnect: true表示主动断开（不触发重连），false表示错误断开（触发重连）
func (c *WebSocketClient) disconnect(manualDisconnect bool) error {
	c.connectMu.Lock()
	defer c.connectMu.Unlock()
	// 如果没有连接就返回，不进行端口了， 只看见了设置true，的没有看见设置false的
	if !c.isConnected {
		return nil
	}
	// 如果主动断开，就设置表示为真
	if manualDisconnect {
		c.isShuttingDown = true
	}
	// 如果连接不为空，就关闭连接并设置为空
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			log.Debugf("关闭WebSocket连接时出错: %v", err)
		}
		c.conn = nil
	}

	c.isConnected = false
	c.mu.Lock()
	// 关闭所有响应通道，并重新创建
	for _, ch := range c.responseChans {
		close(ch)
	}
	c.responseChans = make(map[string]chan *WebSocketResponse)
	c.callbacks = make(map[string]func(*WebSocketResponse))
	c.mu.Unlock()

	// 停止工作线程， 接收websocket请求发送
	close(c.messageQueue)
	c.workers.Wait() // 当前发送的三个go程退出
	// 重新创建消息队列 重新创建发送通道
	c.messageQueue = make(chan *WebSocketRequest, 100)

	log.Debugf("WebSocket连接已断开")
	return nil
}

// 获取是否已连接
func (c *WebSocketClient) IsConnected() bool {
	c.connectMu.Lock()
	defer c.connectMu.Unlock()
	return c.isConnected
}

// 发送请求
func (c *WebSocketClient) SendRequest(ctx context.Context, method, path string, body map[string]interface{}) (*WebSocketResponse, error) {
	if !c.IsConnected() {
		if err := c.Connect(ctx); err != nil {
			return nil, fmt.Errorf("连接失败: %v", err)
		}
	}

	// 生成UUID作为请求ID
	requestID := uuid.New().String()

	request := WebSocketRequest{
		ID:     requestID,
		Method: method,
		Path:   path,
		Body:   body,
	}

	// 创建响应通道
	responseChan := make(chan *WebSocketResponse, 1)
	c.mu.Lock()
	c.responseChans[requestID] = responseChan
	c.mu.Unlock()

	// 清理响应通道
	defer func() {
		c.mu.Lock()
		// 删除这个请求
		delete(c.responseChans, requestID)
		c.mu.Unlock()
		// 关闭响应通道
		close(responseChan)
	}()

	// 发送请求（使用写入锁保护）
	c.writeMu.Lock()
	// 发送请求， 直接发送
	err := c.conn.WriteJSON(request)
	c.writeMu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("发送请求失败: %v", err)
	}

	// 等待响应
	select {
	case response := <-responseChan:
		return response, nil
	case <-time.After(c.requestTimeout):
		return nil, fmt.Errorf("请求超时")
	case <-ctx.Done():
		return nil, fmt.Errorf("上下文取消")
	}
}

// 便捷方法 - 使用WebSocket原生ping
func (c *WebSocketClient) Ping() error {
	if !c.IsConnected() {
		return fmt.Errorf("WebSocket未连接")
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(10*time.Second))
}

// 获取的状态
func (c *WebSocketClient) GetStatus(ctx context.Context) (*WebSocketResponse, error) {
	return c.SendRequest(ctx, "GET", "/api/ws/status", nil)
}

func (c *WebSocketClient) Echo(ctx context.Context, message string) (*WebSocketResponse, error) {
	return c.SendRequest(ctx, "POST", "/api/ws/echo", map[string]interface{}{
		"message": message,
	})
}

// 全局便捷方法
func ConnectManagerWebSocket(ctx context.Context) error {
	return GetDefaultClient().Connect(ctx)
}

// 关闭连接
func DisconnectManagerWebSocket() error {
	client := GetDefaultClient()
	client.StopReconnect()
	return client.disconnect(true) // 主动断开，不触发重连
}

// 发送消息
func SendManagerRequest(ctx context.Context, method, path string, body map[string]interface{}) (*WebSocketResponse, error) {
	return GetDefaultClient().SendRequest(ctx, method, path, body)
}

// 发送ping消息
func ManagerWebSocketPing(ctx context.Context) error {
	return GetDefaultClient().Ping()
}

// 获取状态
func ManagerWebSocketStatus(ctx context.Context) (*WebSocketResponse, error) {
	return GetDefaultClient().GetStatus(ctx)
}

// 发送echo
func ManagerWebSocketEcho(ctx context.Context, message string) (*WebSocketResponse, error) {
	return GetDefaultClient().Echo(ctx, message)
}

// 发送websocket连接
func IsManagerWebSocketConnected() bool {
	return GetDefaultClient().IsConnected()
}

// 发送设备请求
func SendDeviceRequest(ctx context.Context, path string, body map[string]interface{}) (*WebSocketResponse, error) {
	return GetDefaultClient().SendRequest(ctx, "POST", path, body)
}

// startWorkers 启动消息发送工作线程
func (c *WebSocketClient) startWorkers() {
	workerCount := 3 // 启动3个工作线程

	for i := 0; i < workerCount; i++ {
		c.workers.Add(1)
		go func(workerID int) {
			defer c.workers.Done()

			log.Debugf("Manager WebSocket工作线程 %d 已启动", workerID)
			// 一直等待通道有值，有值就处理，没值就阻塞，除非关闭这个通道
			for request := range c.messageQueue {
				// 连接断开就跳过当前选好
				if !c.IsConnected() {
					log.Debugf("工作线程 %d: WebSocket未连接，丢弃请求", workerID)
					continue
				}

				// 发送请求（使用写入锁保护）
				c.writeMu.Lock()
				// 这步是将请求信息转换为json，通过websocket发送出去
				err := c.conn.WriteJSON(request)
				c.writeMu.Unlock()
				if err != nil {
					log.Debugf("工作线程 %d: 发送请求失败: %v", workerID, err)
					// 连接可能已断开，触发重连
					c.handleConnectionError()
					continue
				}

				log.Debugf("工作线程 %d: 已发送请求 %s", workerID, request.ID)
			}

			log.Debugf("Manager WebSocket工作线程 %d 已停止", workerID)
		}(i)
	}
}

// handleConnectionError 处理连接错误
func (c *WebSocketClient) handleConnectionError() {
	// 如果连接着，断开，然后重新连接
	if c.IsConnected() {
		log.Warn("检测到WebSocket连接错误，正在断开连接...")
		c.disconnect(false) // 错误断开，会触发重连
		// 触发重连
		c.triggerReconnect()
	}
}

// startHeartbeat 启动心跳检测
func (c *WebSocketClient) startHeartbeat() {
	ticker := time.NewTicker(30 * time.Second) // 每30秒发送一次ping
	defer ticker.Stop()

	for {
		select {
		// 读取定时器器，到30秒就是通道就会有一个值，然后消费掉，等待下一次
		case <-ticker.C:
			// 连接断开了就可以退出了
			if !c.IsConnected() {
				return
			}

			// 发送ping消息
			c.writeMu.Lock()
			// 给服务器发送一个ping数据
			err := c.conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(10*time.Second))
			c.writeMu.Unlock()

			if err != nil {
				log.Warnf("发送ping失败，连接可能已断开: %v", err)
				// 关闭重新连接
				c.disconnect(false) // 错误断开，会触发重连
				// 触发重连
				c.triggerReconnect()
				return
			}
			log.Debugf("发送ping消息成功")

		case <-c.retryStopChan:
			// 收到重连协议停止的信号，退出不发送信号了
			return
		}
	}
}

// triggerReconnect 触发重连（非阻塞）
func (c *WebSocketClient) triggerReconnect() {
	c.retryMu.Lock()
	defer c.retryMu.Unlock()

	// 如果正在关闭，不触发重连
	if c.isShuttingDown {
		log.Debug("正在关闭中，不触发重连")
		return
	}

	// 如果已经在重连，不重复触发
	if c.isRetrying {
		return
	}

	c.isRetrying = true
	// 启动重连协程
	c.retryWg.Add(1)
	go c.startReconnectLoop()
}

// startReconnectLoop 启动重连循环（使用指数退避算法）
func (c *WebSocketClient) startReconnectLoop() {
	defer func() {
		c.retryMu.Lock()
		c.isRetrying = false
		c.retryMu.Unlock()
		c.retryWg.Done()
	}()

	// 硬编码的退避算法参数
	initialDelay := 3 * time.Second // 初始延迟3秒
	maxDelay := 1 * time.Minute     // 最大延迟1分钟
	backoffMultiplier := 2.0        // 退避倍数

	delay := initialDelay
	retryCount := 0

	log.Infof("Manager WebSocket连接重试协程已启动")

	for {
		// 检查是否应该停止重连
		select {
		case <-c.retryStopChan:
			log.Info("收到停止信号，停止重连")
			return
		default:
		}

		// 如果正在关闭，停止重连
		c.retryMu.Lock()
		shuttingDown := c.isShuttingDown
		c.retryMu.Unlock()
		if shuttingDown {
			log.Info("正在关闭中，停止重连")
			return
		}

		// 如果已经连接，停止重连
		if c.IsConnected() {
			log.Info("Manager WebSocket连接已恢复，停止重连")
			return
		}
		// 累加重连次数
		retryCount++
		log.Warnf("Manager WebSocket连接失败 (第%d次)，等待 %v 后重试连接...", retryCount, delay)

		// 等待延迟时间
		select {
		case <-time.After(delay):
			// 继续重连
		case <-c.retryStopChan:
			log.Info("收到停止信号，停止重连")
			return
		}

		// 尝试连接
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := c.Connect(ctx)
		cancel()

		if err != nil {
			log.Warnf("Manager WebSocket连接失败 (第%d次): %v", retryCount, err)
			// 计算下一次延迟时间（指数退避）
			delay = time.Duration(float64(delay) * backoffMultiplier)
			if delay > maxDelay {
				delay = maxDelay
			}
			continue
		}

		// 连接成功
		log.Info("Manager WebSocket连接成功")
		return
	}
}

// StopReconnect 停止重连协程
func (c *WebSocketClient) StopReconnect() {
	c.retryMu.Lock()
	c.isShuttingDown = true
	shouldClose := c.retryStopChan != nil
	c.retryMu.Unlock()

	if shouldClose {
		// 有值的情况
		// 使用 select 避免重复关闭通道
		select {
		case <-c.retryStopChan:
			// 通道已经关闭
		default:
			close(c.retryStopChan)
		}
		c.retryWg.Wait()
		log.Info("Manager WebSocket重连协程已优雅关闭")
	}
}

// SendRequestWithCallback 发送请求并使用回调处理响应
func (c *WebSocketClient) SendRequestWithCallback(ctx context.Context, method, path string, body map[string]interface{}, callback func(*WebSocketResponse)) error {
	if !c.IsConnected() {
		if err := c.Connect(ctx); err != nil {
			return fmt.Errorf("连接失败: %v", err)
		}
	}

	// 生成UUID作为请求ID
	requestID := uuid.New().String()

	request := WebSocketRequest{
		ID:     requestID,
		Method: method,
		Path:   path,
		Body:   body,
	}

	// 注册回调
	c.mu.Lock()
	c.callbacks[requestID] = callback
	c.mu.Unlock()

	// 清理回调
	defer func() {
		c.mu.Lock()
		delete(c.callbacks, requestID)
		c.mu.Unlock()
	}()

	// 将请求放入队列
	select {
	case c.messageQueue <- &request:
		log.Debugf("请求 %s 已加入队列", requestID)
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("消息队列已满，请求超时")
	case <-ctx.Done():
		return fmt.Errorf("上下文取消")
	}
}

// SendRequestAsync 异步发送请求， 也不需要返回值
func (c *WebSocketClient) SendRequestAsync(ctx context.Context, method, path string, body map[string]interface{}) (string, error) {
	if !c.IsConnected() {
		if err := c.Connect(ctx); err != nil {
			return "", fmt.Errorf("连接失败: %v", err)
		}
	}

	// 生成UUID作为请求ID
	requestID := uuid.New().String()

	request := WebSocketRequest{
		ID:     requestID,
		Method: method,
		Path:   path,
		Body:   body,
	}

	// 将请求放入队列
	select {
	case c.messageQueue <- &request:
		log.Debugf("异步请求 %s 已加入队列", requestID)
		return requestID, nil
	case <-time.After(5 * time.Second):
		return "", fmt.Errorf("消息队列已满，请求超时")
	case <-ctx.Done():
		return "", fmt.Errorf("上下文取消")
	}
}

// GetResponse 获取指定请求ID的响应（用于异步请求）
func (c *WebSocketClient) GetResponse(requestID string, timeout time.Duration) (*WebSocketResponse, error) {
	responseChan := make(chan *WebSocketResponse, 1)

	// 注册临时回调
	c.mu.Lock()
	c.callbacks[requestID] = func(response *WebSocketResponse) {
		responseChan <- response
	}
	c.mu.Unlock()

	// 清理回调
	defer func() {
		c.mu.Lock()
		delete(c.callbacks, requestID)
		c.mu.Unlock()
		close(responseChan)
	}()

	select {
	case response := <-responseChan:
		return response, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("等待响应超时")
	}
}

// handleSystemConfigPush 处理服务端推送的系统配置变更，异步调用已注册的回调
func (c *WebSocketClient) handleSystemConfigPush(data map[string]interface{}) {
	if systemConfigPushHandler == nil {
		log.Debugf("收到 system_config 推送，但未注册处理回调")
		return
	}
	go systemConfigPushHandler(data)
}

// handleMessages 处理接收到的WebSocket消息
func (c *WebSocketClient) handleMessages() {
	// 死循环
	for {
		// 如果连接端口，就不会有消息了
		if !c.isConnected {
			return
		}

		// 读取消息类型
		messageType, reader, err := c.conn.NextReader()
		if err != nil {
			// 如果错误就断开，然后重新连接
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Debugf("WebSocket读取错误: %v", err)
			}
			c.disconnect(false) // 错误断开，会触发重连
			// 触发重连
			c.triggerReconnect()
			return
		}

		// 处理不同类型的消息
		switch messageType {
		case websocket.TextMessage:
			// 处理JSON消息
			var rawMessage map[string]interface{}
			// 对文本消息进行转换字典对象，如果解析失败就重新等待，
			if err := json.NewDecoder(reader).Decode(&rawMessage); err != nil {
				log.Errorf("解析JSON消息失败: %v", err)
				continue
			}

			// 根据消息类型判断：服务端推送(system_config)、请求、响应
			//{
			//	"type": "system_config",
			//	"data": {},
			//	"method": "",
			//	"status": ""
			//}
			if msgType, _ := rawMessage["type"].(string); msgType == "system_config" {
				// 系统配置发生改变时触发
				if data, ok := rawMessage["data"].(map[string]interface{}); ok {
					c.handleSystemConfigPush(data)
				} else {
					log.Warnf("收到 system_config 推送但 data 格式无效")
				}
			} else if method, exists := rawMessage["method"]; exists && method != nil {
				// 这是收到的请求
				c.handleIncomingRequest(rawMessage)
			} else if status, exists := rawMessage["status"]; exists && status != nil {
				// 这是收到的响应
				c.handleIncomingResponse(rawMessage)
			} else {
				log.Warnf("收到无法识别的WebSocket消息: %+v", rawMessage)
			}

		case websocket.PingMessage:
			// 处理ping消息，自动回复pong（使用写入锁保护）
			log.Debugf("收到ping消息，自动回复pong")
			c.writeMu.Lock()
			err := c.conn.WriteControl(websocket.PongMessage, []byte{}, time.Now().Add(10*time.Second))
			c.writeMu.Unlock()
			if err != nil {
				log.Errorf("发送pong失败: %v", err)
			}

		case websocket.PongMessage:
			// 处理pong消息
			log.Debugf("收到pong消息")

		case websocket.CloseMessage:
			// 处理关闭消息
			log.Debugf("收到关闭消息")
			c.disconnect(false) // 错误断开，会触发重连
			// 触发重连
			c.triggerReconnect()
			return

		default:
			log.Warnf("收到未知类型的WebSocket消息: %d", messageType)
		}
	}
}

// handleIncomingRequest 处理收到的请求
func (c *WebSocketClient) handleIncomingRequest(rawMessage map[string]interface{}) {
	var request WebSocketRequest
	if err := mapToStruct(rawMessage, &request); err != nil {
		log.Errorf("解析WebSocket请求失败: %v", err)
		return
	}

	log.Debugf("收到请求: ID=%s, Method=%s, Path=%s", request.ID, request.Method, request.Path)

	// 如果有注册的请求处理器，调用它
	if c.requestHandler != nil {
		go c.requestHandler(&request)
	} else {
		// 如果没有注册处理器，使用默认处理器处理已知路径
		c.handleDefaultRequest(&request)
	}
}

func (c *WebSocketClient) RegisterMessageHandler(ctx context.Context, path string, handler types.EventHandler) {
	f := func(request *WebSocketRequest) (string, error) {
		return handler(ctx, request.Path, request.Body)
	}
	c.messageHandle.Set(path, f)
}

// handleDefaultRequest 默认请求处理器
func (c *WebSocketClient) handleDefaultRequest(request *WebSocketRequest) {
	switch request.Path {
	case "/api/config/test":
		// 配置测试可能较耗时（VAD/ASR/LLM/TTS 串行执行），放入独立 goroutine 避免阻塞读循环，支持多请求并发
		go c.handleConfigTestRequest(request)

	case "/api/mcp/tools":
		// 处理MCP工具列表请求
		c.handleMcpToolListRequest(request)

	case "/api/mcp/status":
		c.handleMcpStatusRequest(request)

	case "/api/mcp/call":
		// 处理MCP工具调用请求
		c.handleMcpToolCallRequest(request)

	case "/api/openclaw/status":
		c.handleOpenClawStatusRequest(request)

	case "/api/openclaw/chat":
		c.handleOpenClawChatRequest(request)

	case "/api/server/info":
		// 返回服务器信息
		response := map[string]interface{}{
			"server_name": "xiaozhi-server",
			"version":     "1.0.0",
			"uptime":      time.Now().Format(time.RFC3339),
			"request_id":  request.ID,
		}

		if err := c.SendResponse(request.ID, 200, response, ""); err != nil {
			log.Errorf("发送服务器信息响应失败: %v", err)
		}

	case "/api/server/ping":
		// 简单的ping响应
		response := map[string]interface{}{
			"message": "pong from server",
			"time":    time.Now().Format(time.RFC3339),
		}

		if err := c.SendResponse(request.ID, 200, response, ""); err != nil {
			log.Errorf("发送ping响应失败: %v", err)
		}
	default:
		handler, exists := c.messageHandle.Get(request.Path)
		if exists {
			// 调用处理器并处理返回值
			result, err := handler(request)
			if err != nil {
				log.Errorf("处理请求 %s 失败: %v", request.Path, err)
				// 发送错误响应
				if err := c.SendResponse(request.ID, 500, nil, err.Error()); err != nil {
					log.Errorf("发送错误响应失败: %v", err)
				}
			} else {
				// 发送成功响应
				response := map[string]interface{}{
					"result": result,
				}
				if err := c.SendResponse(request.ID, 200, response, ""); err != nil {
					log.Errorf("发送成功响应失败: %v", err)
				}
			}
		} else {
			log.Warnf("收到未知的WebSocket请求路径: %s, ID: %s", request.Path, request.ID)

			// 发送404响应
			if err := c.SendResponse(request.ID, 404, nil, "Unknown endpoint"); err != nil {
				log.Errorf("发送错误响应失败: %v", err)
			}
		}
	}
}

// configTestTotalTimeout 配置测试整体超时（VAD+ASR+LLM+TTS 合计）
const configTestTotalTimeout = 90 * time.Second

// handleConfigTestRequest 处理配置测试请求：VAD/ASR/LLM/TTS 使用下发的配置与固定 WAV/文本执行轻量测试
func (c *WebSocketClient) handleConfigTestRequest(request *WebSocketRequest) {
	// 应该是规定好了，直接从data中获取数据
	data, _ := request.Body["data"].(map[string]interface{})
	if data == nil {
		log.Debugf("[config_test] 请求 ID=%s 缺少 data 字段", request.ID)
		_ = c.SendResponse(request.ID, 400, nil, "缺少 data 字段")
		return
	}
	// 获取测试文本
	testText, _ := request.Body["test_text"].(string)
	// debug: 请求中各类型配置数量（不含 provider）， 获取模型提供商的数量
	log.Debugf("[config_test] 请求 ID=%s test_text=%q data 各类型条目数: vad=%d asr=%d llm=%d tts=%d",
		request.ID, testText,
		countConfigKeys(data["vad"]), countConfigKeys(data["asr"]),
		countConfigKeys(data["llm"]), countConfigKeys(data["tts"]))
	// 定义测试结果结构体
	type configTestResult struct {
		vad, asr, llm, tts map[string]interface{}
	}
	// 定义一个通道
	done := make(chan configTestResult, 1)
	go func() {
		// data相当于配置，testText相当于文本
		vadR, asrR, llmR, ttsR := RunConfigTest(data, testText)
		done <- configTestResult{vadR, asrR, llmR, ttsR}
	}()

	var vadR, asrR, llmR, ttsR map[string]interface{}
	// 阻塞等待结果
	select {
	case res := <-done:
		vadR, asrR, llmR, ttsR = res.vad, res.asr, res.llm, res.tts
	case <-time.After(configTestTotalTimeout):
		// 超时了
		log.Warnf("[config_test] 请求 ID=%s 整体超时 %v", request.ID, configTestTotalTimeout)
		body := map[string]interface{}{
			"vad": map[string]interface{}{"_error": map[string]interface{}{"ok": false, "message": "配置测试总超时"}},
			"asr": map[string]interface{}{"_error": map[string]interface{}{"ok": false, "message": "配置测试总超时"}},
			"llm": map[string]interface{}{"_error": map[string]interface{}{"ok": false, "message": "配置测试总超时"}},
			"tts": map[string]interface{}{"_error": map[string]interface{}{"ok": false, "message": "配置测试总超时"}},
		}
		// 发送数据回去
		_ = c.SendResponse(request.ID, 200, body, "")
		return
	}

	// 请求中带了某类型但无任何可测配置时，返回 _none 便于前端展示原因
	fillEmptyConfigTestResult(data, "vad", vadR)
	fillEmptyConfigTestResult(data, "asr", asrR)
	fillEmptyConfigTestResult(data, "llm", llmR)
	fillEmptyConfigTestResult(data, "tts", ttsR)
	body := map[string]interface{}{
		"vad": vadR,
		"asr": asrR,
		"llm": llmR,
		"tts": ttsR,
	}
	log.Debugf("[config_test] 响应 ID=%s 各类型结果数: vad=%d asr=%d llm=%d tts=%d",
		request.ID, len(vadR), len(asrR), len(llmR), len(ttsR))
	_ = c.SendResponse(request.ID, 200, body, "")
}

// fillEmptyConfigTestResult 当请求包含该类型但测试结果为空时，写入 _none 条目
func fillEmptyConfigTestResult(data map[string]interface{}, typ string, result map[string]interface{}) {
	// 获取指定类型如asr， 有数据就返回
	if _, has := data[typ]; !has || len(result) > 0 {
		return
	}
	msg := "未配置或未启用" + strings.ToUpper(typ)
	result["_none"] = map[string]interface{}{"ok": false, "message": msg}
	log.Debugf("[config_test] 类型 %s 无结果，已写入 _none: %s", typ, msg)
}

// countConfigKeys 统计 data 中除 provider 外的 config 条目数，用于 debug
func countConfigKeys(v interface{}) int {
	m, ok := v.(map[string]interface{})
	if !ok {
		return 0
	}
	n := 0
	for k := range m {
		if k != "provider" {
			n++
		}
	}
	return n
}

// handleIncomingResponse 处理收到的响应
func (c *WebSocketClient) handleIncomingResponse(rawMessage map[string]interface{}) {
	var response WebSocketResponse
	if err := mapToStruct(rawMessage, &response); err != nil {
		log.Errorf("解析WebSocket响应失败: %v", err)
		return
	}

	log.Debugf("收到响应: ID=%s, Status=%d", response.ID, response.Status)

	// 查找对应的响应通道和回调
	c.mu.RLock()
	responseChan, exists := c.responseChans[response.ID]
	callback, callbackExists := c.callbacks[response.ID]
	c.mu.RUnlock()

	if exists {
		select {
		case responseChan <- &response:
		default:
			log.Debugf("响应通道已满，丢弃响应: %s", response.ID)
		}
	}

	if callbackExists {
		go callback(&response)
	}

	if !exists && !callbackExists {
		log.Debugf("收到未知的响应ID: %s", response.ID)
	}
}

// SendResponse 发送响应给收到的请求
func (c *WebSocketClient) SendResponse(requestID string, status int, body map[string]interface{}, errorMsg string) error {
	if !c.IsConnected() {
		return fmt.Errorf("WebSocket未连接")
	}

	response := WebSocketResponse{
		ID:     requestID,
		Status: status,
		Body:   body,
		Error:  errorMsg,
	}

	// 使用写入锁保护
	c.writeMu.Lock()
	err := c.conn.WriteJSON(response)
	c.writeMu.Unlock()
	if err != nil {
		return fmt.Errorf("发送响应失败: %v", err)
	}

	log.Debugf("已发送响应: ID=%s, Status=%d", requestID, status)
	return nil
}

// SetRequestHandler 设置请求处理器
func (c *WebSocketClient) SetRequestHandler(handler func(*WebSocketRequest)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requestHandler = handler
}

// mapToStruct 辅助函数：将map转换为struct
func mapToStruct(data map[string]interface{}, target interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonData, target)
}

func toolInfoToSchemaMap(paramsOneOf interface{}) map[string]interface{} {
	if paramsOneOf == nil {
		return nil
	}

	// ParamsOneOf 内部字段未导出，直接 json.Marshal 可能得到 {}。
	// 优先走官方 ToOpenAPIV3()，确保能取到真实参数 schema。
	if p, ok := paramsOneOf.(*einoschema.ParamsOneOf); ok && p != nil {
		if openAPISchema, err := p.ToOpenAPIV3(); err == nil && openAPISchema != nil {
			raw, err := json.Marshal(openAPISchema)
			if err == nil {
				decoded := map[string]interface{}{}
				if err = json.Unmarshal(raw, &decoded); err == nil {
					if len(decoded) > 0 {
						return decoded
					}
				}
			}
		}
	}

	raw, err := json.Marshal(paramsOneOf)
	if err != nil {
		return nil
	}

	decoded := map[string]interface{}{}
	if err = json.Unmarshal(raw, &decoded); err != nil {
		return nil
	}

	if openAPIV3, ok := decoded["openAPIV3"].(map[string]interface{}); ok {
		return openAPIV3
	}
	if openAPIV3, ok := decoded["open_api_v3"].(map[string]interface{}); ok {
		return openAPIV3
	}
	if len(decoded) == 0 {
		return nil
	}
	return decoded
}

func convertReportedToolsToToolList(reportedTools map[string]tool.InvokableTool) ([]map[string]interface{}, error) {
	toolList := make([]map[string]interface{}, 0)

	names := make([]string, 0, len(reportedTools))
	for name := range reportedTools {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		invokable := reportedTools[name]
		toolInfo := map[string]interface{}{
			"name":        name,
			"description": fmt.Sprintf("MCP工具: %s", name),
			"schema":      true,
		}

		if info, err := invokable.Info(context.Background()); err == nil && info != nil {
			if info.Desc != "" {
				toolInfo["description"] = info.Desc
			}
			inputSchema := toolInfoToSchemaMap(info.ParamsOneOf)
			if inputSchema != nil {
				toolInfo["input_schema"] = inputSchema
			}
		}

		toolList = append(toolList, toolInfo)
	}

	return toolList, nil
}

func getDeviceMcpTools(deviceID string) ([]map[string]interface{}, error) {
	reportedTools, err := mcp.RefreshReportedToolsByDeviceID(deviceID)
	if err != nil {
		log.Errorf("刷新设备上报MCP工具列表失败: %v", err)
		return nil, err
	}

	return convertReportedToolsToToolList(reportedTools)
}

func getAgentMcpTools(agentID string) ([]map[string]interface{}, error) {
	reportedTools, err := mcp.RefreshReportedToolsByAgentID(agentID)
	if err != nil {
		log.Errorf("刷新智能体上报MCP工具列表失败: %v", err)
		return nil, err
	}

	return convertReportedToolsToToolList(reportedTools)
}

// handleMcpToolListRequest 处理MCP工具列表请求
func (c *WebSocketClient) handleMcpToolListRequest(request *WebSocketRequest) {
	// 从请求体中获取agent_id/device_id
	agentID := ""
	deviceID := ""
	if request.Body != nil {
		if id, ok := request.Body["agent_id"].(string); ok {
			agentID = id
		}
		if id, ok := request.Body["device_id"].(string); ok {
			deviceID = id
		}
	}

	if agentID == "" && deviceID == "" {
		log.Warnf("收到MCP工具列表请求，但缺少agent_id/device_id")
		if err := c.SendResponse(request.ID, 400, nil, "缺少agent_id或device_id参数"); err != nil {
			log.Errorf("发送错误响应失败: %v", err)
		}
		return
	}

	log.Infof("处理MCP工具列表请求，agent_id: %s, device_id: %s", agentID, deviceID)

	if agentID != "" && deviceID != "" {
		if err := c.SendResponse(request.ID, 400, nil, "agent_id与device_id不能同时传入"); err != nil {
			log.Errorf("发送错误响应失败: %v", err)
		}
		return
	}

	var (
		toolList []map[string]interface{}
		err      error
	)
	if deviceID != "" {
		toolList, err = getDeviceMcpTools(deviceID)
	} else {
		toolList, err = getAgentMcpTools(agentID)
	}
	if err != nil {
		log.Errorf("获取MCP工具列表失败: %v", err)
		if err := c.SendResponse(request.ID, 500, nil, fmt.Sprintf("获取工具列表失败: %v", err)); err != nil {
			log.Errorf("发送错误响应失败: %v", err)
		}
		return
	}

	// 构造响应
	response := map[string]interface{}{
		"agent_id":  agentID,
		"device_id": deviceID,
		"tools":     toolList,
		"count":     len(toolList),
	}

	// 发送响应
	if err := c.SendResponse(request.ID, 200, response, ""); err != nil {
		log.Errorf("发送MCP工具列表响应失败: %v", err)
	}
}

func (c *WebSocketClient) handleMcpStatusRequest(request *WebSocketRequest) {
	agentID := ""
	if request.Body != nil {
		if id, ok := request.Body["agent_id"].(string); ok {
			agentID = strings.TrimSpace(id)
		}
	}

	if agentID == "" {
		_ = c.SendResponse(request.ID, 400, nil, "缺少agent_id参数")
		return
	}

	connected, clientCount := mcp.GetWsEndpointConnectionStatus(agentID)
	status := "offline"
	if connected {
		status = "online"
	}

	response := map[string]interface{}{
		"agent_id":     agentID,
		"connected":    connected,
		"status":       status,
		"client_count": clientCount,
	}
	_ = c.SendResponse(request.ID, 200, response, "")
}

// 全局便捷方法（异步版本）
func SendManagerRequestAsync(ctx context.Context, method, path string, body map[string]interface{}) (string, error) {
	return GetDefaultClient().SendRequestAsync(ctx, method, path, body)
}

func SendManagerRequestWithCallback(ctx context.Context, method, path string, body map[string]interface{}, callback func(*WebSocketResponse)) error {
	return GetDefaultClient().SendRequestWithCallback(ctx, method, path, body, callback)
}

func GetManagerResponse(requestID string, timeout time.Duration) (*WebSocketResponse, error) {
	return GetDefaultClient().GetResponse(requestID, timeout)
}

// 双向通信支持方法
func SetManagerRequestHandler(handler func(*WebSocketRequest)) {
	GetDefaultClient().SetRequestHandler(handler)
}

func SendManagerResponse(requestID string, status int, body map[string]interface{}, errorMsg string) error {
	return GetDefaultClient().SendResponse(requestID, status, body, errorMsg)
}

// 创建带有请求处理器的客户端
func NewManagerClientWithHandler(handler func(*WebSocketRequest)) *WebSocketClient {
	return NewWebSocketClientWithHandler(handler)
}

// SendMcpToolListRequest 发送MCP工具列表请求
func SendMcpToolListRequest(ctx context.Context, agentID string) (*WebSocketResponse, error) {
	body := map[string]interface{}{
		"agent_id": agentID,
	}
	return SendManagerRequest(ctx, "GET", "/api/mcp/tools", body)
}

// SendMcpToolListRequestAsync 异步发送MCP工具列表请求
func SendMcpToolListRequestAsync(ctx context.Context, agentID string) (string, error) {
	body := map[string]interface{}{
		"agent_id": agentID,
	}
	return SendManagerRequestAsync(ctx, "GET", "/api/mcp/tools", body)
}

// SendMcpToolListRequestWithCallback 使用回调发送MCP工具列表请求
func SendMcpToolListRequestWithCallback(ctx context.Context, agentID string, callback func(*WebSocketResponse)) error {
	body := map[string]interface{}{
		"agent_id": agentID,
	}
	return SendManagerRequestWithCallback(ctx, "GET", "/api/mcp/tools", body, callback)
}

// Init 初始化Manager配置提供者
// 包括WebSocket连接的初始化和重连机制
func Init(ctx context.Context) error {
	log.Infof("Initializing Manager config provider with WebSocket client")

	// 创建WebSocket客户端， 只不过这个客户端，里面的websocket还没有进行创建，
	//后面看声明时候进行创建赋值
	client := GetDefaultClient()

	// 尝试连接到WebSocket服务器
	if err := client.Connect(ctx); err != nil {
		log.Warnf("初始连接Manager WebSocket失败: %v，将启动重连机制", err)
		// 即使初始连接失败，也启动重连机制
		client.triggerReconnect()
	} else {
		log.Infof("Manager config provider initialized successfully")
	}

	return nil
}

// Close 关闭Manager配置提供者，清理资源
func Close() error {
	log.Infof("Closing Manager config provider")

	// 停止重连协程
	client := GetDefaultClient()
	client.StopReconnect()

	// 主动断开连接（不触发重连）
	client.disconnect(true)

	return nil
}

// IsConnected 检查Manager配置提供者是否已连接
func IsConnected() bool {
	return IsManagerWebSocketConnected()
}

// handleMcpToolCallRequest 处理MCP工具调用请求
func (c *WebSocketClient) handleMcpToolCallRequest(request *WebSocketRequest) {
	agentID := ""
	deviceID := ""
	toolName := ""
	arguments := map[string]interface{}{}
	if request.Body != nil {
		if id, ok := request.Body["agent_id"].(string); ok {
			agentID = id
		}
		if id, ok := request.Body["device_id"].(string); ok {
			deviceID = id
		}
		if t, ok := request.Body["tool_name"].(string); ok {
			toolName = t
		}
		if args, ok := request.Body["arguments"].(map[string]interface{}); ok {
			arguments = args
		}
	}

	if toolName == "" || (agentID == "" && deviceID == "") {
		_ = c.SendResponse(request.ID, 400, nil, "缺少tool_name或agent_id/device_id参数")
		return
	}

	if agentID != "" && deviceID != "" {
		_ = c.SendResponse(request.ID, 400, nil, "agent_id与device_id不能同时传入")
		return
	}

	var (
		invokable tool.InvokableTool
		ok        bool
	)
	if deviceID != "" {
		invokable, ok = mcp.GetReportedToolByDeviceIDAndName(deviceID, toolName)
	} else {
		invokable, ok = mcp.GetReportedToolByAgentIDAndName(agentID, toolName)
	}
	if !ok {
		var (
			result    string
			rawCalled bool
			err       error
		)
		if deviceID != "" {
			result, rawCalled, err = mcp.RawCallReportedToolByDeviceID(deviceID, toolName, arguments)
		} else {
			result, rawCalled, err = mcp.RawCallReportedToolByAgentID(agentID, toolName, arguments)
		}
		if rawCalled {
			if err != nil {
				_ = c.SendResponse(request.ID, 500, nil, fmt.Sprintf("工具调用失败(raw call): %v", err))
				return
			}
			log.Warnf("工具 %s 未出现在工具列表中，已通过 raw call 兜底: device=%s agent=%s", toolName, deviceID, agentID)
			_ = c.SendResponse(request.ID, 200, map[string]interface{}{
				"agent_id":  agentID,
				"device_id": deviceID,
				"tool_name": toolName,
				"result":    result,
			}, "")
			return
		}
		_ = c.SendResponse(request.ID, 404, nil, fmt.Sprintf("工具不存在: %s", toolName))
		return
	}

	argBytes, _ := json.Marshal(arguments)
	result, err := invokable.InvokableRun(context.Background(), string(argBytes))
	if err != nil {
		_ = c.SendResponse(request.ID, 500, nil, fmt.Sprintf("工具调用失败: %v", err))
		return
	}

	_ = c.SendResponse(request.ID, 200, map[string]interface{}{
		"agent_id":  agentID,
		"device_id": deviceID,
		"tool_name": toolName,
		"result":    result,
	}, "")
}

func (c *WebSocketClient) handleOpenClawStatusRequest(request *WebSocketRequest) {
	agentID := ""
	if request.Body != nil {
		if id, ok := request.Body["agent_id"].(string); ok {
			agentID = strings.TrimSpace(id)
		}
	}
	if agentID == "" {
		_ = c.SendResponse(request.ID, 400, nil, "missing agent_id")
		return
	}

	manager := openclaw.GetManager()
	connected := manager.GetAgentSession(agentID) != nil
	status := "offline"
	if connected {
		status = "online"
	}

	_ = c.SendResponse(request.ID, 200, map[string]interface{}{
		"agent_id":  agentID,
		"connected": connected,
		"status":    status,
	}, "")
}

const (
	defaultOpenClawChatTimeoutMs = 10 * 60 * 1000
	minOpenClawChatTimeoutMs     = 1000
	maxOpenClawChatTimeoutMs     = 10 * 60 * 1000
	openClawChatTestSessionID    = "openclaw-chat-test-global"
)

func buildOpenClawTestDeviceID(agentID string) string {
	trimmed := strings.TrimSpace(agentID)
	if trimmed == "" {
		trimmed = "unknown"
	}
	return "__openclaw_test__:" + trimmed
}

func buildOpenClawTestSessionID() string {
	return openClawChatTestSessionID
}

func parseOpenClawTimeoutMs(v interface{}) int {
	timeout := defaultOpenClawChatTimeoutMs
	switch x := v.(type) {
	case int:
		timeout = x
	case int32:
		timeout = int(x)
	case int64:
		timeout = int(x)
	case float64:
		timeout = int(x)
	case float32:
		timeout = int(x)
	}
	if timeout < minOpenClawChatTimeoutMs {
		timeout = minOpenClawChatTimeoutMs
	}
	if timeout > maxOpenClawChatTimeoutMs {
		timeout = maxOpenClawChatTimeoutMs
	}
	return timeout
}

func parseOpenClawStreamEvents(v interface{}) bool {
	switch x := v.(type) {
	case bool:
		return x
	case string:
		switch strings.ToLower(strings.TrimSpace(x)) {
		case "1", "true", "yes", "on":
			return true
		}
	case int:
		return x != 0
	case int32:
		return x != 0
	case int64:
		return x != 0
	case float32:
		return x != 0
	case float64:
		return x != 0
	}
	return false
}

func openClawStreamSnippet(text string, maxRunes int) string {
	if maxRunes <= 0 {
		return ""
	}
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return ""
	}
	runes := []rune(trimmed)
	if len(runes) <= maxRunes {
		return string(runes)
	}
	return string(runes[:maxRunes]) + "..."
}

func (c *WebSocketClient) handleOpenClawChatRequest(request *WebSocketRequest) {
	agentID := ""
	message := ""
	sessionID := ""
	timeoutMs := defaultOpenClawChatTimeoutMs
	streamEvents := false

	if request.Body != nil {
		if id, ok := request.Body["agent_id"].(string); ok {
			agentID = strings.TrimSpace(id)
		}
		if msg, ok := request.Body["message"].(string); ok {
			message = strings.TrimSpace(msg)
		}
		if rawSessionID, ok := request.Body["session_id"].(string); ok && strings.TrimSpace(rawSessionID) != "" {
			sessionID = strings.TrimSpace(rawSessionID)
		}
		timeoutMs = parseOpenClawTimeoutMs(request.Body["timeout_ms"])
		streamEvents = parseOpenClawStreamEvents(request.Body["stream_events"])
	}

	if agentID == "" {
		_ = c.SendResponse(request.ID, 400, nil, "missing agent_id")
		return
	}
	if message == "" {
		_ = c.SendResponse(request.ID, 400, nil, "missing message")
		return
	}
	if sessionID == "" {
		sessionID = buildOpenClawTestSessionID()
	}

	manager := openclaw.GetManager()
	if manager.GetAgentSession(agentID) == nil {
		_ = c.SendResponse(request.ID, 409, nil, fmt.Sprintf("openclaw session not connected for agent %s", agentID))
		return
	}

	testDeviceID := buildOpenClawTestDeviceID(agentID)
	// 清理测试设备历史缓存，避免串到上一轮测试结果。
	manager.ReplayOfflineMessages(testDeviceID, func(msg openclaw.OfflineMessage) error {
		return nil
	})

	start := time.Now()
	messageID, err := manager.SendMessage(agentID, testDeviceID, message, sessionID)
	if err != nil {
		errMsg := strings.ToLower(strings.TrimSpace(err.Error()))
		if strings.Contains(errMsg, "session not found") {
			_ = c.SendResponse(request.ID, 409, nil, fmt.Sprintf("openclaw session not connected for agent %s", agentID))
			return
		}
		_ = c.SendResponse(request.ID, 500, nil, fmt.Sprintf("openclaw send failed: %v", err))
		return
	}
	if streamEvents {
		log.Infof(
			"openclaw chat stream started: request_id=%s agent=%s message_id=%s session=%s timeout_ms=%d",
			request.ID,
			agentID,
			messageID,
			sessionID,
			timeoutMs,
		)
	}

	deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	var replyBuilder strings.Builder
	chunks := make([]string, 0, 8)
	done := false
	firstChunkLatencyMs := -1
	for time.Now().Before(deadline) {
		manager.ReplayOfflineMessages(testDeviceID, func(msg openclaw.OfflineMessage) error {
			correlationID := strings.TrimSpace(msg.CorrelationID)
			if correlationID != "" && correlationID != messageID {
				return nil
			}
			chunk := strings.TrimSpace(msg.Text)
			if chunk != "" {
				replyBuilder.WriteString(chunk)
				chunks = append(chunks, chunk)
				if firstChunkLatencyMs < 0 {
					firstChunkLatencyMs = int(time.Since(start).Milliseconds())
				}
				if streamEvents {
					log.Infof(
						"openclaw chat stream chunk received: request_id=%s agent=%s message_id=%s chunk_index=%d chunk_len=%d chunk_snippet=%q",
						request.ID,
						agentID,
						messageID,
						len(chunks),
						len(chunk),
						openClawStreamSnippet(chunk, 64),
					)
				}
				if streamEvents {
					partialBody := map[string]interface{}{
						"agent_id":    agentID,
						"message_id":  messageID,
						"chunk":       chunk,
						"chunk_index": len(chunks),
						"reply":       strings.TrimSpace(replyBuilder.String()),
						"latency_ms":  int(time.Since(start).Milliseconds()),
						"done":        false,
					}
					if firstChunkLatencyMs >= 0 {
						partialBody["first_chunk_latency_ms"] = firstChunkLatencyMs
					}
					if err := c.SendResponse(request.ID, http.StatusPartialContent, partialBody, ""); err != nil {
						log.Warnf("openclaw chat stream partial response send failed: request_id=%s, err=%v", request.ID, err)
					}
				}
			}
			if msg.IsEnd {
				if streamEvents {
					log.Infof(
						"openclaw chat stream end marker received: request_id=%s agent=%s message_id=%s chunk_count=%d partial_reply_len=%d elapsed_ms=%d",
						request.ID,
						agentID,
						messageID,
						len(chunks),
						len(strings.TrimSpace(replyBuilder.String())),
						int(time.Since(start).Milliseconds()),
					)
				}
				done = true
			}
			return nil
		})
		if done {
			break
		}
		time.Sleep(120 * time.Millisecond)
	}
	reply := strings.TrimSpace(replyBuilder.String())

	if !done {
		// 清理测试设备离线缓存，避免累积。
		manager.ReplayOfflineMessages(testDeviceID, func(msg openclaw.OfflineMessage) error {
			return nil
		})
		if reply == "" {
			if streamEvents {
				log.Warnf(
					"openclaw chat stream timeout without reply: request_id=%s agent=%s message_id=%s timeout_ms=%d",
					request.ID,
					agentID,
					messageID,
					timeoutMs,
				)
			}
			_ = c.SendResponse(request.ID, 504, nil, "openclaw response timeout")
			return
		}
		if streamEvents {
			log.Warnf(
				"openclaw chat stream timeout with partial reply: request_id=%s agent=%s message_id=%s chunk_count=%d reply_len=%d elapsed_ms=%d",
				request.ID,
				agentID,
				messageID,
				len(chunks),
				len(reply),
				int(time.Since(start).Milliseconds()),
			)
		}
		_ = c.SendResponse(request.ID, 504, map[string]interface{}{
			"agent_id":               agentID,
			"message_id":             messageID,
			"reply":                  reply,
			"chunks":                 chunks,
			"chunk_count":            len(chunks),
			"latency_ms":             int(time.Since(start).Milliseconds()),
			"first_chunk_latency_ms": firstChunkLatencyMs,
			"timeout_ms":             timeoutMs,
			"finished":               false,
		}, "openclaw response timeout (partial reply received)")
		return
	}

	latencyMs := int(time.Since(start).Milliseconds())
	if streamEvents {
		log.Infof(
			"openclaw chat stream completed: request_id=%s agent=%s message_id=%s chunk_count=%d reply_len=%d latency_ms=%d",
			request.ID,
			agentID,
			messageID,
			len(chunks),
			len(reply),
			latencyMs,
		)
	}
	var firstChunkLatency interface{}
	if firstChunkLatencyMs >= 0 {
		firstChunkLatency = firstChunkLatencyMs
	}
	_ = c.SendResponse(request.ID, 200, map[string]interface{}{
		"agent_id":               agentID,
		"message_id":             messageID,
		"reply":                  reply,
		"chunks":                 chunks,
		"chunk_count":            len(chunks),
		"latency_ms":             latencyMs,
		"first_chunk_latency_ms": firstChunkLatency,
		"timeout_ms":             timeoutMs,
		"finished":               true,
	}, "")
}
