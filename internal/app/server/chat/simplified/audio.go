package main

import (
	"fmt"
	"math"

	"gopkg.in/hraban/opus.v2"
)

// codec 封装 Opus 编解码。
//
// 对应真实实现：internal/domain/audio/audio_handler.go 的 AudioProcesser。
//
// 一个关键点：Opus 一帧最长 120ms，解码前并不知道这一帧实际多长，
// 所以先按 120ms 预分配缓冲，解码后用返回的实际长度截断。
// 真实实现里也是这么做的（asr.go 里的 maxFrameSize）。
type codec struct {
	sampleRate    int
	channels      int
	frameDuration int // 每帧毫秒数（下行 60ms）

	decoder *opus.Decoder
	encoder *opus.Encoder
}

func newCodec(f AudioFormat) (*codec, error) {
	// 解码器和编码器必须用和音频流一致的采样率/声道数，否则出来是噪声。
	decoder, err := opus.NewDecoder(f.SampleRate, f.Channels)
	if err != nil {
		return nil, fmt.Errorf("创建 Opus 解码器失败: %w", err)
	}
	encoder, err := opus.NewEncoder(f.SampleRate, f.Channels, opus.AppAudio)
	if err != nil {
		return nil, fmt.Errorf("创建 Opus 编码器失败: %w", err)
	}
	return &codec{
		sampleRate:    f.SampleRate,
		channels:      f.Channels,
		frameDuration: f.FrameDuration,
		decoder:       decoder,
		encoder:       encoder,
	}, nil
}

// maxFrameSamples 解码缓冲区大小，按最长 120ms 计算。
func (c *codec) maxFrameSamples() int {
	return c.sampleRate * c.channels * 120 / 1000
}

// samplesPerMs 每毫秒有多少个采样点，用来把采样点数换算成时长。
func (c *codec) samplesPerMs() int {
	return c.sampleRate * c.channels / 1000
}

// samplesPerFrame 一帧有多少个采样点。
func (c *codec) samplesPerFrame() int {
	return c.sampleRate * c.channels * c.frameDuration / 1000
}

// decode 解码一帧 Opus 为 PCM。
// 返回 float32（取值约 [-1,1]）是刻意的：VAD 和 ASR 都吃这个格式，
// 避免在每一环之间来回转换（真实实现里同样用 float32）。
func (c *codec) decode(frame []byte) ([]float32, error) {
	buf := make([]float32, c.maxFrameSamples())
	n, err := c.decoder.DecodeFloat32(frame, buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

// encode 把一帧 PCM 编成 Opus。
//
// 注意：Opus 只接受固定长度的帧（2.5/5/10/20/40/60ms），
// 所以调用方必须传正好 samplesPerFrame() 个采样点，
// 长度不够要自己补零（见 tts.go 里收尾那一帧的处理）。
func (c *codec) encode(pcm []float32) ([]byte, error) {
	buf := make([]byte, 4000) // 一帧 Opus 远小于 4000 字节，够用
	n, err := c.encoder.EncodeFloat32(pcm, buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

// floatToInt16 把 [-1,1] 的浮点采样转成 16bit 整型。
// ASR 需要的 WAV、TTS 返回的裸 PCM，都是这个格式。
func floatToInt16(s float32) int16 {
	switch {
	case s >= 1:
		return 32767
	case s <= -1:
		return -32768
	default:
		return int16(s * 32767)
	}
}

// int16ToFloat32 反方向转换。
func int16ToFloat32(v int16) float32 {
	return float32(v) / 32768
}

// tone 生成一段正弦波（振幅 0.5）。
// 内置的模拟 TTS 用它产出「能听见的声音」，测试用它模拟人声。
func tone(ms, sampleRate int, freq float64) []float32 {
	n := sampleRate * ms / 1000
	out := make([]float32, n)
	for i := range out {
		out[i] = 0.5 * float32(math.Sin(2*math.Pi*freq*float64(i)/float64(sampleRate)))
	}
	return out
}
