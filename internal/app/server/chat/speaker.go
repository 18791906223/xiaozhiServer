// 文件：speaker.go
// 职责：声纹识别 provider 的薄封装。
// 它在 chat 包与 internal/domain/speaker 之间加一层，让上层只依赖 SpeakerManager，
// 而不必感知具体 provider（asr_server / streaming 等）的实现差异。
//
// 核心类型：SpeakerManager（包装 speaker.SpeakerProvider）。
//
// 在流程中的位置：ASR 拿到最终文本前后，会话会通过它启动声纹流、送音频、取识别结果，
// 结果用于「是否允许该说话人对话」与「按说话人切换音色」等判断（见 asr.go / llm.go）。
//
// 与哪些文件配合：asr.go（送音频、取结果）、llm.go / session.go（按声纹切音色、判权限）。
package chat

import (
	"context"

	"xiaozhi-esp32-server-golang/internal/domain/speaker"
)

// SpeakerManager 声纹识别管理器（包装 SpeakerProvider）
type SpeakerManager struct {
	// provider 具体声纹识别实现。这里用接口而不是具体类型，
	// 是为了让测试可替换、也让不同后端（服务端 ASR 声纹 / 流式声纹）可以互换。
	provider speaker.SpeakerProvider
}

// peekableSpeakerProvider 是「可中间取结果」的 provider 能力接口。
// 为什么需要它：并非所有声纹 provider 都支持「不结束当前轮次就提前看一眼结果」
// （PeekAndIdentify）。把它单独抽成接口，就可以对可支持的 provider 用类型断言开启，
// 不支持的直接跳过——避免为了一个可选能力污染主接口、逼所有实现都去写空方法。
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
// sampleRate 必须与上行音频格式一致，否则声纹特征会失真（识别率大幅下降）。
func (sm *SpeakerManager) StartStreaming(ctx context.Context, sampleRate int, agentId string) error {
	return sm.provider.StartStreaming(ctx, sampleRate, agentId)
}

// SendAudioChunk 发送音频块
// 传入的是 PCM float32（已由 VAD 阶段解码好），不是原始 Opus。
func (sm *SpeakerManager) SendAudioChunk(ctx context.Context, pcmData []float32) error {
	return sm.provider.SendAudioChunk(ctx, pcmData)
}

// FinishAndIdentify 完成识别并获取结果
// 「完成」意味着结束当前轮次：之后要继续识别就得重新 StartStreaming。
func (sm *SpeakerManager) FinishAndIdentify(ctx context.Context) (*speaker.IdentifyResult, error) {
	return sm.provider.FinishAndIdentify(ctx)
}

// Close 关闭声纹管理器
func (sm *SpeakerManager) Close() error {
	return sm.provider.Close()
}

// IsActive 检查是否处于激活状态
// 上层用它判断「现在能不能取声纹结果」，避免对未启动的识别器做无意义调用。
func (sm *SpeakerManager) IsActive() bool {
	return sm.provider.IsActive()
}

// PeekAndIdentify 获取声纹中间识别结果（不结束当前轮次）
// 返回: 识别结果, 是否被服务端防抖, 错误
//
// 类型断言的作用：只有底层 provider 实现了 peekableSpeakerProvider 才真正调用；
// 否则返回「没有结果」而不是报错——因为「不支持 peek」是正常情况，不是故障。
// realtime 模式下用它可以在用户还没说完时就提前识别出说话人，从而提前切换音色或触发打断。
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
