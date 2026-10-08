package main

import (
	"strings"
	"unicode/utf8"
)

// sentenceSplitter 把流式文本切成句子。
//
// 对应真实实现 internal/util/sentence.go 的 ExtractSmartSentences，
// 以及 plugins/output_segmenter.go 那个 stream-transform 插件。
//
// 为什么必须切句：TTS 的粒度是「一句话一次请求」，只有拿到一个完整的句子
// 才能开始合成。如果等整段回复写完再合成，首字延迟就等于 LLM 的总耗时，
// 用户会觉得音箱「反应很慢」。
//
// 切句的两难：
//   - 切得太急（一见到逗号就切）→ 语气生硬，音频碎
//   - 切得太晚（非等到句号）    → 首字延迟高
//
// 真实实现里 minLen=2 / maxLen=100 并允许第一句在逗号处切开；
// 简化版只按句末标点切，外加一个最长长度兜底。
type sentenceSplitter struct {
	buf string
}

// 句末标点。真实实现在 internal/util/sentence.go 里维护了一套更全的集合。
const sentenceEnders = "。！？；!?;…\n"

// maxSentenceRunes 是一句话的字符数上限。
// 超过就强制切开 —— 否则 LLM 一直不出标点时，用户要等很久才有声音。
const maxSentenceRunes = 40

// Push 送入一段新文本，返回其中已经可以成句的部分（可能 0 个或多个）。
func (s *sentenceSplitter) Push(delta string) []string {
	s.buf += delta
	var out []string

	for {
		runes := []rune(s.buf)

		// 找到第一个句末标点
		idx := -1
		for i, r := range runes {
			if strings.ContainsRune(sentenceEnders, r) {
				idx = i
				break
			}
		}
		if idx < 0 {
			break // 还没有完整的句子，继续等
		}

		// 吞掉紧跟其后的连续标点，让「真的吗？！」算一句而不是两句
		end := idx + 1
		for end < len(runes) && strings.ContainsRune(sentenceEnders, runes[end]) {
			end++
		}

		if t := strings.TrimSpace(string(runes[:end])); t != "" {
			out = append(out, t)
		}
		s.buf = string(runes[end:])
	}

	// 兜底：一直没遇到句末标点也要切，否则迟迟没有音频输出。
	runes := []rune(s.buf)
	if len(runes) >= maxSentenceRunes {
		if t := strings.TrimSpace(string(runes[:maxSentenceRunes])); t != "" {
			out = append(out, t)
		}
		s.buf = string(runes[maxSentenceRunes:])
	}
	return out
}

// Flush 在本轮结束时调用，把缓冲区里剩下的半句话也交出来。
// 少了这一步，回复的最后一句（通常不带句号）就会被丢掉。
func (s *sentenceSplitter) Flush() string {
	rest := strings.TrimSpace(s.buf)
	s.buf = ""
	return rest
}

// Len 返回缓冲区里还剩多少个字符。
func (s *sentenceSplitter) Len() int {
	return utf8.RuneCountInString(s.buf)
}
