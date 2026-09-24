package webrtc_vad

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"xiaozhi-esp32-server-golang/internal/domain/vad/inter"

	"github.com/hackers365/go-webrtcvad"
)

const (
	// DefaultSampleRate WebRTC VAD 支持的采样率 (8000, 16000, 32000, 48000)
	DefaultSampleRate = 16000
	// DefaultMode VAD 敏感度模式 (0: 最不敏感, 3: 最敏感)
	DefaultMode = 2
	// FrameDuration 帧持续时间 (ms)，WebRTC VAD 支持 10ms, 20ms, 30ms
	FrameDuration = 20
)

// WebRTCVAD WebRTC VAD 实现，现在实现了 Resource 接口
type WebRTCVAD struct {
	webrtcVad      *webrtcvad.VAD
	sampleRate     int          // 采样率
	mode           int          // VAD 模式
	frameSize      int          // 每帧采样数
	frameSizeBytes int          // 每帧字节数
	initialized    bool         // 是否已初始化
	lastUsed       time.Time    // 最后使用时间
	mu             sync.RWMutex // 读写锁
}

// AcquireVAD 创建并返回 WebRTC VAD 实例（由全局资源池管理）
func AcquireVAD(config map[string]interface{}) (inter.VAD, error) {
	vadConfig := getVadConfigFromMap(config)

	vad := &WebRTCVAD{
		sampleRate: vadConfig.SampleRate,
		mode:       vadConfig.Mode,
		lastUsed:   time.Now(),
	}

	// 初始化实例
	if err := vad.init(); err != nil {
		return nil, fmt.Errorf("failed to initialize WebRTC VAD: %w", err)
	}

	return vad, nil
}

// ReleaseVAD 释放 VAD 实例
func ReleaseVAD(vad inter.VAD) error {
	if vad != nil {
		return vad.Close()
	}
	return nil
}

// NewWebRTCVAD 创建新的 WebRTC VAD 实例
func NewWebRTCVAD() inter.VAD {
	return &WebRTCVAD{
		sampleRate: DefaultSampleRate,
		mode:       DefaultMode,
		lastUsed:   time.Now(),
	}
}

// NewWebRTCVADWithConfig 使用指定配置创建 WebRTC VAD 实例
func NewWebRTCVADWithConfig(sampleRate, mode int) (inter.VAD, error) {
	if !isValidSampleRate(sampleRate) {
		return nil, fmt.Errorf("unsupported sample rate: %d, supported rates: 8000, 16000, 32000, 48000", sampleRate)
	}
	if mode < 0 || mode > 3 {
		return nil, fmt.Errorf("invalid VAD mode: %d, must be 0-3", mode)
	}

	vad := &WebRTCVAD{
		sampleRate: sampleRate,
		mode:       mode,
		lastUsed:   time.Now(),
	}

	err := vad.init()
	if err != nil {
		return nil, err
	}

	return vad, nil
}

// init 初始化 WebRTC VAD
func (w *WebRTCVAD) init() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.initialized {
		return nil
	}

	// 计算帧大小
	w.frameSize = w.sampleRate / 1000 * FrameDuration
	w.frameSizeBytes = w.frameSize * 2 // 16-bit PCM

	// 创建 VAD 实例
	var err error
	w.webrtcVad, err = webrtcvad.New()
	if w.webrtcVad == nil {
		return fmt.Errorf("failed to create WebRTC VAD instance")
	}

	err = w.webrtcVad.SetMode(w.mode)
	if err != nil {
		webrtcvad.Free(w.webrtcVad)
		return fmt.Errorf("failed to set WebRTC VAD mode: %+v", err)
	}

	w.initialized = true
	w.lastUsed = time.Now()
	return nil
}

func (w *WebRTCVAD) IsVAD(pcmData []float32) (bool, error) {
	return w.isVad(pcmData, w.sampleRate, w.frameSize)
}

// IsVAD 检测音频数据中的语音活动
func (w *WebRTCVAD) isVad(pcmData []float32, sampleRate int, frameSize int) (bool, error) {
	if len(pcmData) == 0 {
		return false, nil
	}

	//log.Debugf("isVad, pcmData len: %d, frameSize: %d", len(pcmData), frameSize)

	// 更新最后使用时间
	w.lastUsed = time.Now()

	//pcmBytes := pcmData
	// 将 float32 数据转换为 int16 PCM 数据
	pcmBytes := w.float32ToPCMBytes(pcmData)

	// 如果数据长度不够一帧，返回 false
	if len(pcmBytes) < frameSize {
		return false, nil
	}

	// 处理多帧数据，取最后一帧的结果
	var isActive bool
	var err error

	activityCount := 0
	for i := 0; i+frameSize <= len(pcmBytes); i += frameSize {
		frameData := pcmBytes[i : i+frameSize]

		isActive, err = w.webrtcVad.Process(sampleRate, frameData)
		if err != nil {
			return false, fmt.Errorf("WebRTC VAD process error: %w", err)
		}
		if isActive {
			activityCount++
		}
	}

	frameCount := len(pcmBytes) / frameSize
	isActive = activityCount >= frameCount/2

	//log.Debugf("isVad, isActive: %v, activityCount: %d", isActive, activityCount)
	return isActive, nil
}

func (w *WebRTCVAD) IsVADExt(pcmData []float32, sampleRate int, frameSize int) (bool, error) {
	return w.isVad(pcmData, sampleRate, frameSize)
}

// Reset 重置检测器状态
func (w *WebRTCVAD) Reset() error {
	return nil
}

// Close 关闭并释放资源 (实现 Resource 接口)
func (w *WebRTCVAD) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.initialized && w.webrtcVad != nil {
		webrtcvad.Free(w.webrtcVad)
		w.initialized = false
	}
	return nil
}

// IsValid 检查资源是否有效 (实现 Resource 接口)
func (w *WebRTCVAD) IsValid() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.initialized && w.webrtcVad != nil
}

// float32ToPCMBytes 将 float32 数组转换为 16-bit PCM 字节数组
func (w *WebRTCVAD) float32ToPCMBytes(samples []float32) []byte {
	pcmBytes := make([]byte, len(samples)*2)

	for i, sample := range samples {
		// 将 float32 (-1.0 到 1.0) 转换为 int16 (-32768 到 32767)
		var intSample int16
		if sample > 1.0 {
			intSample = 32767
		} else if sample < -1.0 {
			intSample = -32768
		} else {
			intSample = int16(sample * 32767)
		}

		// 小端序写入字节数组
		binary.LittleEndian.PutUint16(pcmBytes[i*2:], uint16(intSample))
	}

	return pcmBytes
}

// isValidSampleRate 检查采样率是否被 WebRTC VAD 支持
func isValidSampleRate(sampleRate int) bool {
	validRates := []int{8000, 16000, 32000, 48000}
	for _, rate := range validRates {
		if rate == sampleRate {
			return true
		}
	}
	return false
}

// SetMode 设置 VAD 敏感度模式
func (w *WebRTCVAD) SetMode(mode int) error {
	if mode < 0 || mode > 3 {
		return fmt.Errorf("invalid VAD mode: %d, must be 0-3", mode)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	w.mode = mode

	if w.initialized {
		return w.webrtcVad.SetMode(mode)
	}

	return nil
}

// SetSampleRate 设置采样率
func (w *WebRTCVAD) SetSampleRate(sampleRate int) error {
	if !isValidSampleRate(sampleRate) {
		return fmt.Errorf("unsupported sample rate: %d, supported rates: 8000, 16000, 32000, 48000", sampleRate)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// 如果已经初始化，需要重新初始化
	if w.initialized {
		w.Close()
	}

	w.sampleRate = sampleRate
	return nil
}

// GetSampleRate 获取当前采样率
func (w *WebRTCVAD) GetSampleRate() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.sampleRate
}

// GetMode 获取当前 VAD 模式
func (w *WebRTCVAD) GetMode() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.mode
}

// GetLastUsed 获取最后使用时间
func (w *WebRTCVAD) GetLastUsed() time.Time {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.lastUsed
}
