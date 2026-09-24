package main

import (
	"fmt"
)

func containsRune(slice []rune, target rune) bool {
	for _, r := range slice {
		if r == target {
			return true
		}
	}
	return false
}

func extractSmartSentences(text string, minLen, maxLen int) (sentences []string, remaining string) {
	// 有效分割符集合（可自定义扩展）
	splitTokens := []rune{'。', '！', '？', '；', '\n', '.', '!', '?', ';'}

	current := []rune(text)
	for len(current) >= minLen {
		// 计算当前窗口大小
		windowSize := maxLen
		if windowSize > len(current) {
			windowSize = len(current)
		}

		// 在有效窗口中寻找分割点
		splitPos := -1
		for i := windowSize - 1; i >= minLen-1; i-- {
			if containsRune(splitTokens, current[i]) {
				splitPos = i
				break
			}
		}

		if splitPos == -1 {
			break // 未找到有效分割点
		}

		// 分割并保存有效句子
		sentences = append(sentences, string(current[:splitPos+1]))
		current = current[splitPos+1:]
	}

	return
}

func main() {
	text := "大家好！今天天气不错。我们一起学习自然语言处理。这个例子演示文本分割功能。"
	sentences, remaining := extractSmartSentences(text, 3, 20)
	fmt.Println(sentences)
	fmt.Println(remaining)
}
