package main

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
)

var (
	// 定义标点符号集合
	punctuationMap = map[rune]bool{
		'。':  true,
		'？':  true,
		'！':  true,
		'；':  true,
		'：':  true,
		'\n': true,
		'.':  true,
		'?':  true,
		'!':  true,
		';':  true,
		':':  true,
	}

	// 用于复用的对象池
	builderPool = sync.Pool{
		New: func() interface{} {
			return &strings.Builder{}
		},
	}

	// 用于存储结果的切片池
	runeSlicePool = sync.Pool{
		New: func() interface{} {
			slice := make([]rune, 0, 1024)
			return &slice
		},
	}

	// 预编译正则表达式
	numberPrefixRegex = regexp.MustCompile(`(?m)^[\s]*\d{1,3}\.$`)
)

// 使用快速的字符检查替代正则
func isNumberPrefix(text []rune, pos int) bool {
	if pos <= 0 || text[pos] != '.' {
		return false
	}

	// 向前查找行首或换行符
	start := pos - 1
	digitCount := 0
	foundDigit := false

	// 跳过点号前的空白字符
	for start >= 0 && (text[start] == ' ' || text[start] == '\t') {
		start--
	}

	// 统计数字
	for start >= 0 && text[start] >= '0' && text[start] <= '9' {
		digitCount++
		foundDigit = true
		if digitCount > 3 { // 超过3位数字不是合法序号
			return false
		}
		start--
	}

	// 检查数字前面是否为空白字符或行首
	if start >= 0 && text[start] != ' ' && text[start] != '\t' && text[start] != '\n' {
		return false
	}

	return foundDigit
}

// 去除首尾空白字符
func trimSpaceRunes(text []rune) []rune {
	start, end := 0, len(text)-1

	for start <= end && (text[start] == ' ' || text[start] == '\t' || text[start] == '\n') {
		start++
	}

	for end >= start && (text[end] == ' ' || text[end] == '\t' || text[end] == '\n') {
		end--
	}

	if start > end {
		return nil
	}
	return text[start : end+1]
}

func findLastPunctuation(text []rune) int {
	// 从后向前查找最后一个标点
	lastPos := -1
	for i := len(text) - 1; i >= 0; i-- {
		// 检查是否是标点符号
		if punctuationMap[text[i]] {
			// 如果是点号，检查是否是序号的一部分
			if text[i] == '.' && isNumberPrefix(text, i) {
				continue
			}
			return i
		}
	}
	return lastPos
}

func findNextSplitPoint(text []rune, startPos int, maxLen int) int {
	// 计算查找的结束位置
	endPos := startPos + maxLen
	if endPos > len(text) {
		endPos = len(text)
	}

	// 从前向后查找
	for i := startPos; i < endPos; i++ {
		// 检查是否是换行符，同时检查下一行是否是序号
		if text[i] == '\n' {
			nextPos := i + 1
			// 跳过空白字符
			for nextPos < endPos && (text[nextPos] == ' ' || text[nextPos] == '\t') {
				nextPos++
			}
			// 检查是否是序号开始
			if nextPos < endPos-2 && text[nextPos] >= '0' && text[nextPos] <= '9' {
				return i
			}
			continue
		}

		// 使用map检查是否是标点符号
		if punctuationMap[text[i]] {
			return i
		}
	}

	// 如果在maxLen范围内没找到，尝试在更大范围内查找
	if endPos < len(text) {
		for i := endPos; i < len(text); i++ {
			if text[i] == '\n' || punctuationMap[text[i]] {
				return i
			}
		}
	}

	return -1
}

func extractSmartSentences(text string, minLen, maxLen int) (sentences []string, remaining string) {
	// 预分配一个合理的切片容量
	estimatedCount := len(text) / 50
	if estimatedCount < 10 {
		estimatedCount = 10
	}
	sentences = make([]string, 0, estimatedCount)

	// 一次性转换为rune切片
	currentRunes := []rune(text)
	startPos := 0

	// 从对象池获取复用对象
	builder := builderPool.Get().(*strings.Builder)
	defer builderPool.Put(builder)
	builder.Grow(maxLen * 2)

	// 获取临时rune切片
	tempRunesPtr := runeSlicePool.Get().(*[]rune)
	tempRunes := (*tempRunesPtr)[:0]
	defer runeSlicePool.Put(tempRunesPtr)

	for startPos < len(currentRunes) {
		// 跳过开头的空白字符
		for startPos < len(currentRunes) && (currentRunes[startPos] == ' ' || currentRunes[startPos] == '\t' || currentRunes[startPos] == '\n') {
			startPos++
		}

		if startPos >= len(currentRunes) {
			break
		}

		// 查找下一个分割点
		splitPos := findNextSplitPoint(currentRunes, startPos, maxLen)
		if splitPos == -1 {
			// 没有找到分割点，将剩余文本作为remaining
			segment := trimSpaceRunes(currentRunes[startPos:])
			if len(segment) > 0 {
				remaining = string(segment)
			}
			break
		}

		// 提取当前段落
		builder.Reset()
		tempRunes = tempRunes[:0]

		// 收集并处理当前段落
		segment := trimSpaceRunes(currentRunes[startPos : splitPos+1])

		// 检查段落是否满足最小长度要求且以标点符号结尾
		if len(segment) >= minLen && punctuationMap[segment[len(segment)-1]] {
			sentences = append(sentences, string(segment))
		} else {
			// 如果不满足条件，将其添加到remaining中
			if len(segment) > 0 {
				if len(remaining) > 0 {
					remaining += " "
				}
				remaining += string(segment)
			}
		}

		startPos = splitPos + 1
	}

	return sentences, remaining
}

func main() {
	text := `厚,人家就晓得你又在敷衍我!每次问你都没有,你是不是不喜欢我了啦?哼,人家要生气喽!不跟你好了!除非...你答应我,等下带人家去夜市吃豆花啦~还要牵人家手手逛大街,一路上都要逗人家笑,逗得人家开心到飞上天!不然人家真的会不理你哦~`
	sentences, remaining := extractSmartSentences(text, 3, 200)
	for i, sentence := range sentences {
		fmt.Printf("\n句子%d:\n%s\n", i+1, sentence)
	}
	if remaining != "" {
		fmt.Printf("\n剩余:\n%s\n", remaining)
	}
}
