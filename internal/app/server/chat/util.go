// 文件：util.go
// 职责：chat 包内的零散小工具：文本去标点、唤醒词判断。
// 这些函数被多个模块共用，放在单独文件避免散落各处重复实现。
//
// 在流程中的位置：
//   - removePunctuation：给关键词匹配用（realtime_media_gate.go 做指令归一化、
//     openclaw_warmup.go 提炼主题短语），因为 ASR 输出常带标点，不清理会导致匹配失败。
//   - isWakeupWord：session.go 判断用户是否以唤醒词打招呼，据此决定要不要播欢迎语。
//
// 与哪些文件配合：realtime_media_gate.go、openclaw_warmup.go、session.go。
package chat

import (
	"strings"
	"unicode"

	"github.com/spf13/viper"
)

// removePunctuation 移除文本中的标点符号
//
// 除了标点，连空格也一并去掉：ASR 结果里常出现「暂停 ， 」这类带空格的标点组合，
// 只去标点会留下空格，仍可能让关键词包含匹配失败；统一清成连续字符最稳。
func removePunctuation(text string) string {
	// 创建一个字符串构建器
	var builder strings.Builder
	// 预分配容量避免中途扩容：输入长度已知，且绝大多数字符会被保留。
	builder.Grow(len(text))

	for _, r := range text {
		// unicode.IsPunct 覆盖中英文标点；IsSpace 覆盖空格、换行、制表符等。
		if !unicode.IsPunct(r) && !unicode.IsSpace(r) {
			builder.WriteRune(r)
		}
	}

	return builder.String()
}

// isWakeupWord 检查文本是否是唤醒词
//
// 唤醒词列表来自配置（wakeup_words），采用完全相等匹配而非包含匹配：
// 这是刻意的保守设计——若用包含匹配，一句话里出现唤醒词就会被当成唤醒，
// 可能触发不该有的欢迎语播报；这里的语义是「用户就是在叫这个名字」。
func isWakeupWord(text string) bool {
	wakeupWords := viper.GetStringSlice("wakeup_words")
	for _, word := range wakeupWords {
		if text == word {
			return true
		}
	}
	return false
}
