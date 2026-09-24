package speaker

import (
	"context"
	"fmt"
	"sync"

	log "xiaozhi-esp32-server-golang/logger"
)

// AsrServerProvider asr_server 声纹识别提供者
type AsrServerProvider struct {
	streamingClient *StreamingClient
	threshold       float32 // 声纹识别阈值
	isActive        bool
	mutex           sync.Mutex
}

// NewAsrServerProvider 创建 asr_server 声纹识别提供者
func NewAsrServerProvider(config map[string]interface{}) (*AsrServerProvider, error) {
	baseURL, ok := config["base_url"].(string)
	if !ok || baseURL == "" {
		return nil, fmt.Errorf("配置中缺少 service.base_url 字段")
	}

	// 读取阈值配置，默认值为 0.4
	threshold := float32(0.4)
	if thresholdVal, ok := config["threshold"]; ok {
		switch v := thresholdVal.(type) {
		case float64:
			threshold = float32(v)
		case float32:
			threshold = v
		case int:
			threshold = float32(v)
		case int64:
			threshold = float32(v)
		}
		// 验证阈值范围
		if threshold < 0 || threshold > 1 {
			log.Warnf("阈值 %.4f 超出有效范围 [0.0, 1.0]，使用默认值 0.4", threshold)
			threshold = 0.4
		}
	}

	streamingClient := NewStreamingClient(baseURL)
	return &AsrServerProvider{
		streamingClient: streamingClient,
		threshold:       threshold,
		isActive:        false,
	}, nil
}

// StartStreaming 启动流式识别
func (p *AsrServerProvider) StartStreaming(ctx context.Context, sampleRate int, agentId string) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.isActive {
		return nil // 已经激活，直接返回
	}

	err := p.streamingClient.Connect(sampleRate, agentId, p.threshold)
	if err != nil {
		log.Warnf("启动声纹识别流失败: %v", err)
		return err
	}

	p.isActive = true
	log.Debugf("声纹识别流已启动，采样率: %d Hz, agent_id: %s, 阈值: %.4f", sampleRate, agentId, p.threshold)
	return nil
}

// SendAudioChunk 发送音频块
func (p *AsrServerProvider) SendAudioChunk(ctx context.Context, pcmData []float32) error {
	p.mutex.Lock()
	isActive := p.isActive
	streamingClient := p.streamingClient
	p.mutex.Unlock()

	if !isActive {
		return nil // 未激活，静默忽略
	}

	err := streamingClient.SendAudioChunk(pcmData)
	if err != nil {
		log.Warnf("发送音频块到声纹识别服务失败: %v", err)
		// 发送失败时，标记为非激活状态
		p.mutex.Lock()
		p.isActive = false
		p.mutex.Unlock()
		return err
	}

	return nil
}

// FinishAndIdentify 完成识别并获取结果
func (p *AsrServerProvider) FinishAndIdentify(ctx context.Context) (*IdentifyResult, error) {
	p.mutex.Lock()
	if !p.isActive {
		p.mutex.Unlock()
		return nil, nil // 未激活，返回 nil
	}
	p.isActive = false
	streamingClient := p.streamingClient
	p.mutex.Unlock()

	result, err := streamingClient.FinishAndIdentify(ctx)

	if err != nil {
		log.Warnf("获取声纹识别结果失败: %v", err)
		return nil, err
	}

	return result, nil
}

// PeekAndIdentify 获取中间识别结果（不结束当前轮次）
// 返回: 识别结果, 是否被服务端防抖, 错误
func (p *AsrServerProvider) PeekAndIdentify(ctx context.Context, requestID string) (*IdentifyResult, bool, error) {
	select {
	case <-ctx.Done():
		return nil, false, ctx.Err()
	default:
	}

	p.mutex.Lock()
	isActive := p.isActive
	streamingClient := p.streamingClient
	p.mutex.Unlock()

	if !isActive {
		return nil, false, nil
	}

	result, throttled, err := streamingClient.PeekAndIdentify(ctx, requestID)
	if err != nil {
		if !streamingClient.IsConnected() {
			p.mutex.Lock()
			p.isActive = false
			p.mutex.Unlock()
		}
		log.Warnf("获取声纹中间识别结果失败: %v", err)
		return nil, throttled, err
	}

	return result, throttled, nil
}

// Close 关闭声纹提供者
func (p *AsrServerProvider) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.isActive = false
	if p.streamingClient != nil {
		return p.streamingClient.Close()
	}
	return nil
}

// IsActive 检查是否处于激活状态
func (p *AsrServerProvider) IsActive() bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.isActive
}
