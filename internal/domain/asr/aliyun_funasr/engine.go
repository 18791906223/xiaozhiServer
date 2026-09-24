package aliyun_funasr

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"xiaozhi-esp32-server-golang/constants"
	"xiaozhi-esp32-server-golang/internal/domain/asr/types"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// AliyunFunASR is the DashScope FunASR engine.
type AliyunFunASR struct {
	config Config
	dialer *websocket.Dialer
	conn   *websocket.Conn
	connMu sync.Mutex
	taskMu sync.Mutex
}

// NewAliyunFunASR creates a new instance.
func NewAliyunFunASR(config Config) (*AliyunFunASR, error) {
	if config.WsURL == "" {
		return nil, fmt.Errorf("ws_url is empty")
	}
	format := strings.ToLower(strings.TrimSpace(config.Format))
	if format == "" {
		format = "pcm"
	}
	if format != "pcm" {
		return nil, fmt.Errorf("aliyun funasr only supports pcm format")
	}
	if config.SampleRate == 0 {
		config.SampleRate = 16000
	}
	if config.SampleRate != 16000 {
		return nil, fmt.Errorf("aliyun funasr only supports 16000 sample_rate")
	}
	config.Format = format

	return &AliyunFunASR{
		config: config,
		dialer: websocket.DefaultDialer,
	}, nil
}

func (a *AliyunFunASR) getConn(ctx context.Context) (*websocket.Conn, error) {
	a.connMu.Lock()
	defer a.connMu.Unlock()
	if a.conn != nil {
		return a.conn, nil
	}
	apiKey := a.config.APIKey
	if apiKey == "" {
		apiKey = os.Getenv("DASHSCOPE_API_KEY")
	}
	if apiKey == "" {
		return nil, fmt.Errorf("missing api key: DASHSCOPE_API_KEY is empty")
	}
	header := make(http.Header)
	header.Add("Authorization", fmt.Sprintf("bearer %s", apiKey))
	conn, _, err := a.dialer.DialContext(ctx, a.config.WsURL, header)
	if err != nil {
		return nil, fmt.Errorf("connect websocket failed: %w", err)
	}
	a.conn = conn
	return conn, nil
}

func (a *AliyunFunASR) invalidateConn() {
	a.connMu.Lock()
	defer a.connMu.Unlock()
	if a.conn != nil {
		_ = a.conn.Close()
		a.conn = nil
	}
}

// StreamingRecognize performs streaming ASR recognition.
func (a *AliyunFunASR) StreamingRecognize(ctx context.Context, audioStream <-chan []float32) (chan types.StreamingResult, error) {
	a.taskMu.Lock()
	var unlockOnce sync.Once
	unlock := func() {
		unlockOnce.Do(func() {
			a.taskMu.Unlock()
		})
	}

	conn, err := a.getConn(ctx)
	if err != nil {
		unlock()
		return nil, err
	}

	taskID := uuid.New().String()
	runCmd := Event{
		Header: Header{
			Action:    "run-task",
			TaskID:    taskID,
			Streaming: "duplex",
		},
		Payload: Payload{
			TaskGroup: "audio",
			Task:      "asr",
			Function:  "recognition",
			Model:     a.config.Model,
			Parameters: Params{
				Format:                     a.config.Format,
				SampleRate:                 a.config.SampleRate,
				LanguageHints:              a.config.LanguageHints,
				VocabularyID:               a.config.VocabularyID,
				DisfluencyRemovalEnabled:   a.config.DisfluencyRemovalEnabled,
				SemanticPunctuationEnabled: a.config.SemanticPunctuationEnabled,
			},
			Input: Input{},
		},
	}

	runCmdBytes, err := json.Marshal(runCmd)
	if err != nil {
		unlock()
		return nil, fmt.Errorf("marshal run-task failed: %w", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, runCmdBytes); err != nil {
		a.invalidateConn()
		unlock()
		return nil, fmt.Errorf("send run-task failed: %w", err)
	}

	resultChan := make(chan types.StreamingResult, 20)
	taskStarted := make(chan struct{})
	done := make(chan struct{})
	var startOnce sync.Once

	var lastTextMu sync.Mutex
	var lastText string

	var sendErrMu sync.Mutex
	var sendErr error

	sendResult := func(r types.StreamingResult) {
		if !r.IsFinal {
			select {
			case resultChan <- r:
			default:
			}
			return
		}
		select {
		case resultChan <- r:
			return
		default:
			for {
				select {
				case <-resultChan:
				default:
					resultChan <- r
					return
				}
			}
		}
	}

	// Receiver
	go func() {
		defer close(done)
		defer close(resultChan)
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				sendErrMu.Lock()
				localErr := sendErr
				sendErrMu.Unlock()
				if localErr == nil {
					localErr = fmt.Errorf("read message failed: %w", err)
				}
				sendResult(types.StreamingResult{
					Error:   localErr,
					IsFinal: true,
					AsrType: constants.AsrTypeAliyunFunASR,
				})
				a.invalidateConn()
				return
			}
			var event Event
			if err := json.Unmarshal(message, &event); err != nil {
				continue
			}
			switch event.Header.Event {
			case "task-started":
				startOnce.Do(func() { close(taskStarted) })
			case "result-generated":
				if event.Payload.Output.Sentence.Heartbeat {
					continue
				}
				text := event.Payload.Output.Sentence.Text
				if text != "" {
					lastTextMu.Lock()
					lastText = text
					lastTextMu.Unlock()
					sendResult(types.StreamingResult{
						Text:    text,
						IsFinal: false,
						AsrType: constants.AsrTypeAliyunFunASR,
						Mode:    "online",
					})
				}
			case "task-finished":
				lastTextMu.Lock()
				finalText := lastText
				lastTextMu.Unlock()
				var emptyReason string
				if finalText == "" {
					emptyReason = types.EmptyReasonProviderEmptyFinal
				}
				sendResult(types.StreamingResult{
					Text:        finalText,
					IsFinal:     true,
					AsrType:     constants.AsrTypeAliyunFunASR,
					Mode:        "online",
					EmptyReason: emptyReason,
				})
				return
			case "task-failed":
				errMsg := event.Header.ErrorMessage
				if errMsg == "" {
					errMsg = "task failed"
				}
				sendResult(types.StreamingResult{
					Error:   fmt.Errorf("aliyun funasr task failed: %s", errMsg),
					IsFinal: true,
					AsrType: constants.AsrTypeAliyunFunASR,
				})
				a.invalidateConn()
				return
			default:
				// ignore
			}
		}
	}()

	go func() {
		<-done
		unlock()
	}()

	// Sender
	go func() {
		waitTimeout := a.config.Timeout
		if waitTimeout <= 0 {
			waitTimeout = 10 * time.Second
		}
		timer := time.NewTimer(waitTimeout)
		select {
		case <-taskStarted:
			timer.Stop()
		case <-timer.C:
			sendErrMu.Lock()
			sendErr = fmt.Errorf("wait task-started timeout after %s", waitTimeout)
			sendErrMu.Unlock()
			a.invalidateConn()
			return
		case <-ctx.Done():
			sendErrMu.Lock()
			sendErr = ctx.Err()
			sendErrMu.Unlock()
			a.invalidateConn()
			return
		}

		for {
			select {
			case <-ctx.Done():
				sendErrMu.Lock()
				sendErr = ctx.Err()
				sendErrMu.Unlock()
				a.invalidateConn()
				return
			case pcm, ok := <-audioStream:
				if !ok {
					finishCmd := Event{
						Header: Header{
							Action:    "finish-task",
							TaskID:    taskID,
							Streaming: "duplex",
						},
						Payload: Payload{Input: Input{}},
					}
					if bytes, err := json.Marshal(finishCmd); err == nil {
						_ = conn.WriteMessage(websocket.TextMessage, bytes)
					}
					finishWait := a.config.Timeout
					if finishWait <= 0 {
						finishWait = 10 * time.Second
					}
					timer := time.NewTimer(finishWait)
					select {
					case <-done:
					case <-ctx.Done():
						a.invalidateConn()
					case <-timer.C:
						a.invalidateConn()
					}
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					return
				}
				audioBytes := float32SliceToBytes(pcm)
				if err := conn.WriteMessage(websocket.BinaryMessage, audioBytes); err != nil {
					sendErrMu.Lock()
					sendErr = fmt.Errorf("send audio failed: %w", err)
					sendErrMu.Unlock()
					a.invalidateConn()
					return
				}
			}
		}
	}()

	return resultChan, nil
}

// Process performs a one-shot recognition using streaming API.
func (a *AliyunFunASR) Process(pcmData []float32) (string, error) {
	ctx := context.Background()
	if a.config.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, a.config.Timeout)
		defer cancel()
	}

	audioStream := make(chan []float32, 1)
	go func() {
		audioStream <- pcmData
		close(audioStream)
	}()

	resultChan, err := a.StreamingRecognize(ctx, audioStream)
	if err != nil {
		return "", err
	}
	var finalText string
	for result := range resultChan {
		if result.Error != nil {
			return "", result.Error
		}
		if result.Text != "" {
			finalText = result.Text
		}
		if result.IsFinal {
			return finalText, nil
		}
	}
	if finalText != "" {
		return finalText, nil
	}
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	return "", fmt.Errorf("no asr result")
}

// Close releases the reusable connection.
func (a *AliyunFunASR) Close() error {
	a.invalidateConn()
	return nil
}

// IsValid reports whether the engine instance is usable.
func (a *AliyunFunASR) IsValid() bool {
	return a != nil
}

func float32ToInt16(sample float32) int16 {
	if sample > 1.0 {
		sample = 1.0
	} else if sample < -1.0 {
		sample = -1.0
	}
	return int16(sample * 32767)
}

func float32SliceToBytes(samples []float32) []byte {
	data := make([]byte, len(samples)*2)
	for i, s := range samples {
		i16 := float32ToInt16(s)
		data[2*i] = byte(i16)
		data[2*i+1] = byte(i16 >> 8)
	}
	return data
}
