package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"xiaozhi-esp32-server-golang/internal/domain/asr/doubao/request"
	"xiaozhi-esp32-server-golang/internal/domain/asr/doubao/response"
	"xiaozhi-esp32-server-golang/internal/util"

	log "xiaozhi-esp32-server-golang/logger"
)

type AsrWsClient struct {
	seq            int
	url            string
	connect        *websocket.Conn
	appId          string
	accessKey      string
	resourceID     string
	connectID      string
	debugID        string
	requestOptions request.FullClientRequestOptions
	mu             sync.RWMutex // Protects connect from concurrent access

	// 延迟连接相关字段
	connectOnce  sync.Once     // 确保连接只建立一次
	connectReady chan struct{} // 通知接收 goroutine 连接已建立
	connectErr   error         // 连接建立时的错误
	connectErrMu sync.Mutex    // 保护 connectErr
}

func NewAsrWsClient(url string, appKey, accessKey, resourceID, connectID, debugID string, requestOptions request.FullClientRequestOptions) *AsrWsClient {
	return &AsrWsClient{
		seq:            1,
		url:            url,
		appId:          appKey,
		accessKey:      accessKey,
		resourceID:     resourceID,
		connectID:      connectID,
		debugID:        debugID,
		requestOptions: requestOptions,
		connectReady:   make(chan struct{}),
	}
}

func (c *AsrWsClient) logPrefix() string {
	if c.debugID == "" {
		return "[doubao-asr:unknown]"
	}
	return fmt.Sprintf("[doubao-asr:%s]", c.debugID)
}

func previewText(text string, maxRunes int) string {
	if maxRunes <= 0 {
		maxRunes = 32
	}
	runes := []rune(text)
	if len(runes) <= maxRunes {
		return text
	}
	return string(runes[:maxRunes]) + "..."
}

func firstNonEmptyUtteranceText(payload *response.AsrResponsePayload) string {
	if payload == nil {
		return ""
	}
	for _, utterance := range payload.Result.Utterances {
		if utterance.Text != "" {
			return utterance.Text
		}
	}
	return ""
}

func (c *AsrWsClient) CreateConnection(ctx context.Context) error {
	header := request.NewAuthHeader(c.appId, c.accessKey, c.resourceID, c.connectID)
	conn, resp, err := websocket.DefaultDialer.DialContext(ctx, c.url, header)
	if err != nil {
		if resp != nil {
			var body string
			if resp.Body != nil {
				bodyBytes, readErr := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if readErr == nil {
					body = string(bodyBytes)
				}
			}
			return fmt.Errorf("dial websocket err: %w, status=%d, body=%s", err, resp.StatusCode, body)
		}
		return fmt.Errorf("dial websocket err: %w", err)
	}
	logID := ""
	if resp != nil {
		logID = resp.Header.Get("X-Tt-Logid")
		if logID == "" {
			logID = resp.Header.Get("x-tt-logid")
		}
	}
	log.Debugf("%s websocket 连接建立成功: connect_id=%s, logid=%s", c.logPrefix(), c.connectID, logID)
	c.mu.Lock()
	c.connect = conn
	c.mu.Unlock()
	return nil
}

func (c *AsrWsClient) SendFullClientRequest() error {
	c.mu.RLock()
	conn := c.connect
	c.mu.RUnlock()

	if conn == nil {
		return fmt.Errorf("websocket connection is nil")
	}

	fullClientRequest := request.NewFullClientRequest(c.requestOptions)
	c.seq++
	err := conn.WriteMessage(websocket.BinaryMessage, fullClientRequest)
	if err != nil {
		return fmt.Errorf("full client message write websocket err: %w", err)
	}
	_, resp, err := conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("full client message read err: %w", err)
	}
	_ = resp
	//respStruct := response.ParseResponse(resp)
	//log.Println(respStruct)
	return nil
}

// ensureConnection 确保连接已建立（延迟连接，带重试机制）
func (c *AsrWsClient) ensureConnection(ctx context.Context) error {
	var err error
	c.connectOnce.Do(func() {
		log.Debugf("%s 延迟建立连接：收到第一个音频包，开始建立连接", c.logPrefix())

		// 重试配置
		const (
			maxRetries = 3                      // 最大重试次数（总共尝试4次：初始1次 + 重试3次）
			retryDelay = 500 * time.Millisecond // 重试延迟
		)

		for attempt := 1; attempt <= maxRetries+1; attempt++ {
			// 尝试建立连接
			err = c.CreateConnection(ctx)
			if err != nil {
				if attempt <= maxRetries {
					log.Warnf("%s 延迟建立连接失败(第%d次): %v，%v后重试", c.logPrefix(), attempt, err, retryDelay)
					select {
					case <-ctx.Done():
						err = fmt.Errorf("连接建立被取消: %w", ctx.Err())
						c.connectErrMu.Lock()
						c.connectErr = err
						c.connectErrMu.Unlock()
						return
					case <-time.After(retryDelay):
						// 固定延迟后重试
					}
					continue
				} else {
					// 最后一次重试失败
					log.Errorf("%s 延迟建立连接失败(第%d次，已达最大重试次数): %v", c.logPrefix(), attempt, err)
					c.connectErrMu.Lock()
					c.connectErr = err
					c.connectErrMu.Unlock()
					return
				}
			}

			// 连接建立成功，发送初始化请求
			err = c.SendFullClientRequest()
			if err != nil {
				// 发送初始化请求失败，关闭连接并重试
				log.Warnf("%s 发送初始化请求失败(第%d次): %v", c.logPrefix(), attempt, err)
				c.Close()

				if attempt <= maxRetries {
					log.Warnf("%s %v后重试建立连接", c.logPrefix(), retryDelay)
					select {
					case <-ctx.Done():
						err = fmt.Errorf("连接建立被取消: %w", ctx.Err())
						c.connectErrMu.Lock()
						c.connectErr = err
						c.connectErrMu.Unlock()
						return
					case <-time.After(retryDelay):
						// 固定延迟后重试
					}
					continue
				} else {
					// 最后一次重试失败
					log.Errorf("%s 发送初始化请求失败(第%d次，已达最大重试次数): %v", c.logPrefix(), attempt, err)
					c.connectErrMu.Lock()
					c.connectErr = err
					c.connectErrMu.Unlock()
					return
				}
			}

			// 连接和初始化都成功
			if attempt > 1 {
				log.Infof("%s 延迟建立连接成功(第%d次尝试)", c.logPrefix(), attempt)
			} else {
				log.Debugf("%s 延迟建立连接成功", c.logPrefix())
			}
			// 通知接收 goroutine 连接已建立
			close(c.connectReady)
			return
		}
	})
	return err
}

func (c *AsrWsClient) SendMessages(ctx context.Context, audioStream <-chan []float32, stopChan <-chan struct{}) error {
	messageChan := make(chan []byte)
	packetCount := 0
	totalSamples := 0
	exitReason := "unknown"
	defer func() {
		log.Debugf(
			"%s SendMessages exit: reason=%s, packets=%d, total_samples=%d, next_seq=%d",
			c.logPrefix(),
			exitReason,
			packetCount,
			totalSamples,
			c.seq,
		)
	}()
	go func() {
		for message := range messageChan {
			c.mu.RLock()
			conn := c.connect
			c.mu.RUnlock()

			if conn == nil {
				log.Debugf("%s websocket connection is nil, stopping message writer", c.logPrefix())
				return
			}

			err := conn.WriteMessage(websocket.TextMessage, message)
			if err != nil {
				log.Debugf("%s write message err: %s", c.logPrefix(), err)
				return
			}
		}
	}()

	defer close(messageChan)
	firstPacket := true
	for {
		select {
		case <-ctx.Done():
			exitReason = "context_done"
			return fmt.Errorf("send messages context done")
		case <-stopChan:
			exitReason = "stop_chan"
			return fmt.Errorf("send messages stop chan")
		case audioData, ok := <-audioStream:
			if !ok {
				exitReason = "audio_stream_closed"
				log.Debugf("%s sendMessages audioStream closed", c.logPrefix())
				// 如果连接未建立（静音情况），直接返回
				c.mu.RLock()
				conn := c.connect
				c.mu.RUnlock()
				if conn == nil {
					log.Debugf("%s audioStream 关闭且连接未建立，直接返回（静音情况）", c.logPrefix())
					return nil
				}
				// 连接已建立，发送结束消息
				endMessage := request.NewAudioOnlyRequest(-c.seq, []byte{})
				messageChan <- endMessage
				log.Debugf("%s 发送结束音频包: seq=%d", c.logPrefix(), -c.seq)
				return nil
			}

			// 收到第一个音频包时，建立连接
			if firstPacket {
				firstPacket = false
				err := c.ensureConnection(ctx)
				if err != nil {
					exitReason = "ensure_connection_failed"
					log.Errorf("%s 建立连接失败: %v", c.logPrefix(), err)
					return fmt.Errorf("ensure connection err: %w", err)
				}
			}

			packetCount++
			totalSamples += len(audioData)
			if packetCount <= 3 || packetCount%25 == 0 {
				log.Debugf(
					"%s 发送音频包: idx=%d, seq=%d, samples=%d, total_samples=%d",
					c.logPrefix(),
					packetCount,
					c.seq,
					len(audioData),
					totalSamples,
				)
			}

			byteData := make([]byte, len(audioData)*2)
			util.Float32ToPCMBytes(audioData, byteData)
			message := request.NewAudioOnlyRequest(c.seq, byteData)
			messageChan <- message
			c.seq++
		}
	}
}

func (c *AsrWsClient) recvMessages(ctx context.Context, resChan chan<- *response.AsrResponse, stopChan chan<- struct{}) {
	recvCount := 0
	for {
		c.mu.RLock()
		conn := c.connect
		c.mu.RUnlock()

		if conn == nil {
			log.Debugf("%s websocket connection is nil, stopping message receiver", c.logPrefix())
			return
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Warnf("%s 读取豆包响应失败: recv_count=%d, err=%v", c.logPrefix(), recvCount, err)
			return
		}
		resp := response.ParseResponse(message)
		recvCount++

		textLen := 0
		textSnippet := ""
		utteranceCount := 0
		firstUtterance := ""
		audioDuration := 0
		if resp.PayloadMsg != nil {
			textLen = len([]rune(resp.PayloadMsg.Result.Text))
			textSnippet = previewText(resp.PayloadMsg.Result.Text, 24)
			utteranceCount = len(resp.PayloadMsg.Result.Utterances)
			firstUtterance = previewText(firstNonEmptyUtteranceText(resp.PayloadMsg), 24)
			audioDuration = resp.PayloadMsg.AudioInfo.Duration
		}
		log.Debugf(
			"%s 收到响应包: idx=%d, payload_seq=%d, event=%d, last=%v, code=%d, text_len=%d, text=%q, utterances=%d, first_utterance=%q, audio_duration=%d",
			c.logPrefix(),
			recvCount,
			resp.PayloadSequence,
			resp.Event,
			resp.IsLastPackage,
			resp.Code,
			textLen,
			textSnippet,
			utteranceCount,
			firstUtterance,
			audioDuration,
		)
		select {
		case <-ctx.Done():
			return
		case resChan <- resp:
		}
		if resp.IsLastPackage {
			log.Debugf("%s 收到最后一个响应包，停止接收: recv_count=%d", c.logPrefix(), recvCount)
			return
		}
		if resp.Code != 0 {
			log.Warnf("%s 响应包返回错误码，通知发送协程停止: recv_count=%d, code=%d", c.logPrefix(), recvCount, resp.Code)
			close(stopChan)
			return
		}
	}
}

func (c *AsrWsClient) StartAudioStream(ctx context.Context, audioStream <-chan []float32, resChan chan<- *response.AsrResponse) error {
	stopChan := make(chan struct{})
	sendDoneChan := make(chan error, 1) // 发送完成通知（nil表示正常完成，error表示出错）
	log.Debugf("%s StartAudioStream begin", c.logPrefix())

	// 启动发送 goroutine
	go func() {
		err := c.SendMessages(ctx, audioStream, stopChan)
		// 无论成功还是失败，都发送通知
		sendDoneChan <- err
	}()

	// 等待连接建立或发送完成
	select {
	case <-ctx.Done():
		log.Debugf("%s StartAudioStream context done before connect", c.logPrefix())
		return fmt.Errorf("start audio stream context done")
	case <-c.connectReady:
		// 连接已建立，启动接收 goroutine
		log.Debugf("%s 连接已建立，启动接收 goroutine", c.logPrefix())
		c.recvMessages(ctx, resChan, stopChan)
		return nil
	case err := <-sendDoneChan:
		// 发送完成（可能是正常完成或出错）
		if err != nil {
			// 发送过程中出错
			log.Errorf("%s 发送音频流失败: %v", c.logPrefix(), err)
			return err
		}
		// 检查是否是静音情况（连接未建立）
		c.mu.RLock()
		conn := c.connect
		c.mu.RUnlock()
		if conn == nil {
			// 静音情况：audioStream 关闭但连接未建立
			log.Debugf("%s 静音情况：连接未建立，发送空结果", c.logPrefix())
			payload := &response.AsrResponsePayload{}
			payload.Result.Text = ""
			resChan <- &response.AsrResponse{
				Code:          0,
				IsLastPackage: true,
				PayloadMsg:    payload,
			}
			return nil
		}
		// 连接已建立，启动接收 goroutine（处理剩余的响应）
		log.Debugf("%s SendMessages 已结束，开始接收剩余响应", c.logPrefix())
		c.recvMessages(ctx, resChan, stopChan)
		return nil
	}
}

func (c *AsrWsClient) Excute(ctx context.Context, audioStream chan []float32, resChan chan<- *response.AsrResponse) error {
	c.seq = 1
	if c.url == "" {
		return errors.New("url is empty")
	}
	err := c.CreateConnection(ctx)
	if err != nil {
		return fmt.Errorf("create connection err: %w", err)
	}
	err = c.SendFullClientRequest()
	if err != nil {
		return fmt.Errorf("send full request err: %w", err)
	}

	err = c.StartAudioStream(ctx, audioStream, resChan)
	if err != nil {
		return fmt.Errorf("start audio stream err: %w", err)
	}
	return nil
}

func (c *AsrWsClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connect != nil {
		err := c.connect.Close()
		c.connect = nil
		return err
	}
	return nil
}
