package main

import (
	"fmt"

	"gopkg.in/hraban/opus.v2"
)

// codec 封装 Opus 编解码。
//
// 对应真实实现：internal/domain/audio/audio_handler.go 的 AudioProcesser。
// 简化版只留 decode（阶段 5 再加 encode），也去掉了「按帧时长算错就报错」之类的校验。
//
// 一个关键点：Opus 一帧最长 120ms，解码前并不知道这一帧实际多长，
// 所以先按 120ms 预分配缓冲，解码后用返回的实际长度截断。
// 真实实现里也是这么做的（asr.go: maxFrameSize 那段）。
type codec struct {
	sampleRate int
	channels   int

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
		sampleRate: f.SampleRate,
		channels:   f.Channels,
		decoder:    decoder,
		encoder:    encoder,
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

// floatToInt16 把 [-1,1] 的浮点采样转成 16bit 整型。
// ASR 需要的 WAV、Opus 编码器需要的输入，都是这个格式。
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
