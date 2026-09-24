package chat

import (
	"context"

	"xiaozhi-esp32-server-golang/internal/domain/speaker"
)

// SpeakerManager 声纹识别管理器（包装 SpeakerProvider）
type SpeakerManager struct {
	provider speaker.SpeakerProvider
}

type peekableSpeakerProvider interface {
	PeekAndIdentify(ctx context.Context, requestID string) (*speaker.IdentifyResult, bool, error)
}

// NewSpeakerManager 创建声纹管理器
func NewSpeakerManager(provider speaker.SpeakerProvider) *SpeakerManager {
	return &SpeakerManager{
		provider: provider,
	}
}

// StartStreaming 启动流式识别
func (sm *SpeakerManager) StartStreaming(ctx context.Context, sampleRate int, agentId string) error {
	return sm.provider.StartStreaming(ctx, sampleRate, agentId)
}

// SendAudioChunk 发送音频块
func (sm *SpeakerManager) SendAudioChunk(ctx context.Context, pcmData []float32) error {
	return sm.provider.SendAudioChunk(ctx, pcmData)
}

// FinishAndIdentify 完成识别并获取结果
func (sm *SpeakerManager) FinishAndIdentify(ctx context.Context) (*speaker.IdentifyResult, error) {
	return sm.provider.FinishAndIdentify(ctx)
}

// Close 关闭声纹管理器
func (sm *SpeakerManager) Close() error {
	return sm.provider.Close()
}

// IsActive 检查是否处于激活状态
func (sm *SpeakerManager) IsActive() bool {
	return sm.provider.IsActive()
}

// PeekAndIdentify 获取声纹中间识别结果（不结束当前轮次）
// 返回: 识别结果, 是否被服务端防抖, 错误
func (sm *SpeakerManager) PeekAndIdentify(ctx context.Context, requestID string) (*speaker.IdentifyResult, bool, error) {
	if sm == nil || sm.provider == nil {
		return nil, false, nil
	}
	peekProvider, ok := sm.provider.(peekableSpeakerProvider)
	if !ok {
		return nil, false, nil
	}
	return peekProvider.PeekAndIdentify(ctx, requestID)
}
