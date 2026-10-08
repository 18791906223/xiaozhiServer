package main

import (
	"math"
	"time"
)

// vad 是一个极简的能量门限 VAD（Voice Activity Detection，人声活动检测）。
//
// 对应真实实现：internal/domain/vad/ 下的 silero_vad / ten_vad。
// 那些是神经网络模型（几十 MB onnx 权重 + 每次推理），判定质量高得多；
// 但它们的输入输出契约和这里完全一样：给一帧 PCM，回答「这一帧有没有人声」。
// 这里换成 RMS 能量阈值，零依赖、零模型文件，足够把流程跑清楚。
//
// 它承担的核心职责是：把连绵不断的音频流切成「一句话」。
// 没有 VAD，服务端就不知道该在哪一刻把音频送去识别。
type vad struct {
	threshold      float32       // RMS 能量阈值，超过就认为有人声
	silenceTimeout time.Duration // 连续静音多久算「这句话说完了」
	minSpeech      time.Duration // 一段话短于这个时长，当噪声丢弃

	speaking  bool // 当前是否处于一段语音中间
	speechMs  int  // 这一段累计说了多久
	silenceMs int  // 这一段末尾已经静音了多久
}

// vadResult 是 VAD 对一帧的判定。
type vadResult int

const (
	vadSilence   vadResult = iota // 没人在说话 / 仍在同一句话中间的停顿
	vadSpeech                     // 检测到人声
	vadSpeechEnd                  // 这一帧标志着一段话结束了
)

func newVAD() *vad {
	return &vad{
		threshold: 0.01,
		// 400ms 对应真实实现的 chat.chat_max_silence_duration 默认值。
		// 调大 → 更耐断句间的停顿，但响应更慢；调小 → 响应快，但容易把一句话切成两半。
		silenceTimeout: 400 * time.Millisecond,
		// 太短的声响（咳嗽、键盘、碰桌子）不该触发一次对话。
		minSpeech: 200 * time.Millisecond,
	}
}

// Push 送入一帧 PCM，frameMs 是这一帧的时长（毫秒）。
//
// 返回值 spokenMs 只在 result == vadSpeechEnd 时有意义，
// 表示刚刚结束的这一段一共说了多久。
func (v *vad) Push(pcm []float32, frameMs int) (result vadResult, spokenMs int) {
	if rms(pcm) >= v.threshold {
		v.speaking = true
		v.speechMs += frameMs
		v.silenceMs = 0
		return vadSpeech, 0
	}

	// 静音帧。还没开始说话，什么都不用做。
	if !v.speaking {
		return vadSilence, 0
	}

	// 说话中间的正常停顿：继续攒静音时长，还不算结束。
	v.silenceMs += frameMs
	if time.Duration(v.silenceMs)*time.Millisecond < v.silenceTimeout {
		return vadSilence, 0
	}

	// 静音够久了 → 这段话结束。
	spoken := v.speechMs
	longEnough := time.Duration(spoken)*time.Millisecond >= v.minSpeech
	v.Reset()
	if !longEnough {
		return vadSilence, 0 // 太短，当噪声丢掉，不触发对话
	}
	return vadSpeechEnd, spoken
}

// Reset 清空状态，准备接收下一段语音。
func (v *vad) Reset() {
	v.speaking = false
	v.speechMs = 0
	v.silenceMs = 0
}

// rms 计算一帧的均方根能量，落在 [0,1]。这就是「音量大小」。
// 真实实现把这一帧喂给神经网络，这里只需要一个数字。
func rms(pcm []float32) float32 {
	if len(pcm) == 0 {
		return 0
	}
	var sum float32
	for _, s := range pcm {
		sum += s * s
	}
	return float32(math.Sqrt(float64(sum / float32(len(pcm)))))
}
