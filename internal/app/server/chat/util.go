package chat

import (
	"strings"
	"unicode"

	"github.com/spf13/viper"
)

// removePunctuation 移除文本中的标点符号
func removePunctuation(text string) string {
	// 创建一个字符串构建器
	var builder strings.Builder
	builder.Grow(len(text))

	for _, r := range text {
		if !unicode.IsPunct(r) && !unicode.IsSpace(r) {
			builder.WriteRune(r)
		}
	}

	return builder.String()
}

// isWakeupWord 检查文本是否是唤醒词
func isWakeupWord(text string) bool {
	wakeupWords := viper.GetStringSlice("wakeup_words")
	for _, word := range wakeupWords {
		if text == word {
			return true
		}
	}
	return false
}
