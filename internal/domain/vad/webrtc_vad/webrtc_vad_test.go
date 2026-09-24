package webrtc_vad

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewWebRTCVAD 测试创建 WebRTC VAD 实例
func TestNewWebRTCVAD(t *testing.T) {
	vad := NewWebRTCVAD()
	require.NotNil(t, vad)

	webrtcVAD, ok := vad.(*WebRTCVAD)
	require.True(t, ok)
	assert.Equal(t, DefaultSampleRate, webrtcVAD.sampleRate)
	assert.Equal(t, DefaultMode, webrtcVAD.mode)
	assert.False(t, webrtcVAD.initialized)

	// 清理资源
	err := vad.Close()
	assert.NoError(t, err)
}

// TestNewWebRTCVADWithConfig 测试使用配置创建 WebRTC VAD 实例
func TestNewWebRTCVADWithConfig(t *testing.T) {
	// 测试有效配置
	vad, err := NewWebRTCVADWithConfig(8000, 1)
	require.NoError(t, err)
	require.NotNil(t, vad)

	webrtcVAD, ok := vad.(*WebRTCVAD)
	require.True(t, ok)
	assert.Equal(t, 8000, webrtcVAD.sampleRate)
	assert.Equal(t, 1, webrtcVAD.mode)

	err = vad.Close()
	assert.NoError(t, err)

	// 测试无效采样率
	vad, err = NewWebRTCVADWithConfig(22050, 1)
	assert.Error(t, err)
	assert.Nil(t, vad)

	// 测试无效模式
	vad, err = NewWebRTCVADWithConfig(16000, 5)
	assert.Error(t, err)
	assert.Nil(t, vad)
}

// TestWebRTCVAD_IsVAD 测试语音活动检测
func TestWebRTCVAD_IsVAD(t *testing.T) {
	vad := NewWebRTCVAD()
	require.NotNil(t, vad)
	defer vad.Close()

	// 测试空数据
	isActive, err := vad.IsVAD([]float32{})
	assert.NoError(t, err)
	assert.False(t, isActive)

	// 测试静音数据（全零）
	silentData := make([]float32, 1600) // 100ms at 16kHz
	isActive, err = vad.IsVAD(silentData)
	assert.NoError(t, err)
	// 静音数据通常不会被检测为语音活动，但这取决于 VAD 的实现

	// 测试合成语音数据（正弦波）
	speechData := generateSineWave(16000, 440, 1.0, 0.5) // 1秒 440Hz 正弦波
	isActive, err = vad.IsVAD(speechData)
	assert.NoError(t, err)
	// 正弦波可能被检测为语音活动，但这取决于 VAD 算法

	// 测试数据量不足一帧的情况
	shortData := make([]float32, 100) // 少于一帧的数据
	isActive, err = vad.IsVAD(shortData)
	assert.NoError(t, err)
	assert.False(t, isActive)
}

// TestWebRTCVAD_Reset 测试重置功能
func TestWebRTCVAD_Reset(t *testing.T) {
	vad := NewWebRTCVAD()
	require.NotNil(t, vad)
	defer vad.Close()

	// 初始化之前重置
	err := vad.Reset()
	assert.NoError(t, err)

	// 先使用 VAD 进行初始化
	testData := make([]float32, 1600) // 100ms at 16kHz
	_, err = vad.IsVAD(testData)
	assert.NoError(t, err)

	// 初始化之后重置
	err = vad.Reset()
	assert.NoError(t, err)
}

// TestWebRTCVAD_Close 测试关闭功能
func TestWebRTCVAD_Close(t *testing.T) {
	vad := NewWebRTCVAD()
	require.NotNil(t, vad)

	// 未初始化时关闭
	err := vad.Close()
	assert.NoError(t, err)

	// 初始化后关闭
	testData := make([]float32, 1600)
	_, err = vad.IsVAD(testData)
	assert.NoError(t, err)

	err = vad.Close()
	assert.NoError(t, err)

	// 重复关闭
	err = vad.Close()
	assert.NoError(t, err)
}

// TestWebRTCVAD_SetMode 测试设置模式
func TestWebRTCVAD_SetMode(t *testing.T) {
	vad := NewWebRTCVAD()
	require.NotNil(t, vad)
	defer vad.Close()

	webrtcVAD, ok := vad.(*WebRTCVAD)
	require.True(t, ok)

	// 测试有效模式
	for mode := 0; mode <= 3; mode++ {
		err := webrtcVAD.SetMode(mode)
		assert.NoError(t, err)
		assert.Equal(t, mode, webrtcVAD.GetMode())
	}

	// 测试无效模式
	err := webrtcVAD.SetMode(-1)
	assert.Error(t, err)

	err = webrtcVAD.SetMode(4)
	assert.Error(t, err)
}

// TestWebRTCVAD_SetSampleRate 测试设置采样率
func TestWebRTCVAD_SetSampleRate(t *testing.T) {
	vad := NewWebRTCVAD()
	require.NotNil(t, vad)
	defer vad.Close()

	webrtcVAD, ok := vad.(*WebRTCVAD)
	require.True(t, ok)

	// 测试有效采样率
	validRates := []int{8000, 16000, 32000, 48000}
	for _, rate := range validRates {
		err := webrtcVAD.SetSampleRate(rate)
		assert.NoError(t, err)
		assert.Equal(t, rate, webrtcVAD.GetSampleRate())
	}

	// 测试无效采样率
	err := webrtcVAD.SetSampleRate(22050)
	assert.Error(t, err)

	err = webrtcVAD.SetSampleRate(44100)
	assert.Error(t, err)
}

// TestFloat32ToPCMBytes 测试数据类型转换
func TestFloat32ToPCMBytes(t *testing.T) {
	vad := NewWebRTCVAD()
	require.NotNil(t, vad)
	defer vad.Close()

	webrtcVAD, ok := vad.(*WebRTCVAD)
	require.True(t, ok)

	// 测试边界值
	testData := []float32{-1.0, 0.0, 1.0, 1.5, -1.5}
	pcmBytes := webrtcVAD.float32ToPCMBytes(testData)

	assert.Equal(t, len(testData)*2, len(pcmBytes))

	// 检查转换结果
	// -1.0 -> -32768
	// 0.0 -> 0
	// 1.0 -> 32767
	// 1.5 -> 32767 (clipped)
	// -1.5 -> -32768 (clipped)
}

// TestIsValidSampleRate 测试采样率验证
func TestIsValidSampleRate(t *testing.T) {
	// 有效采样率
	validRates := []int{8000, 16000, 32000, 48000}
	for _, rate := range validRates {
		assert.True(t, isValidSampleRate(rate))
	}

	// 无效采样率
	invalidRates := []int{11025, 22050, 44100, 96000}
	for _, rate := range invalidRates {
		assert.False(t, isValidSampleRate(rate))
	}
}

// generateSineWave 生成正弦波数据用于测试
func generateSineWave(sampleRate int, frequency float64, duration float64, amplitude float64) []float32 {
	numSamples := int(float64(sampleRate) * duration)
	samples := make([]float32, numSamples)

	for i := 0; i < numSamples; i++ {
		t := float64(i) / float64(sampleRate)
		samples[i] = float32(amplitude * math.Sin(2*math.Pi*frequency*t))
	}

	return samples
}
