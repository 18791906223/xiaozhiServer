package util

import (
	"bytes"
	"strings"
	"sync"
	"unicode"
)

var (
	// punctuationMap 句子结束和暂停的标点符号映射
	//rune 是 int32 的别名，表示一个 Unicode 码点（一个字符）
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

	// firstPunctuation 首次处理时使用的标点符号映射（包含逗号）
	firstPunctuation = map[rune]bool{
		'，':  true,
		',':  true,
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

	// 句子结束的标点符号
	sentenceEndPunctuation = []rune{'.', '。', '!', '！', '?', '？', '\n'}

	// 句子暂停的标点符号（可以作为长句子的断句点）
	sentencePausePunctuation = []rune{',', '，', ';', '；', ':', '：'}

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
)

// IsSentenceEndPunctuation 判断一个字符是否为句子结束的标点符号
func IsSentenceEndPunctuation(r rune) bool {
	for _, p := range sentenceEndPunctuation {
		if r == p {
			return true
		}
	}
	return false
}

// IsSentencePausePunctuation 判断一个字符是否为句子暂停的标点符号
func IsSentencePausePunctuation(r rune) bool {
	for _, p := range sentencePausePunctuation {
		if r == p {
			return true
		}
	}
	return false
}

// IsNumberWithDot 判断字符串是否为数字加点号格式（如"1."、"2."等）
func IsNumberWithDot(s string) bool {
	trimmed := strings.TrimSpace(s)
	if len(trimmed) < 2 || trimmed[len(trimmed)-1] != '.' {
		return false
	}

	for i := 0; i < len(trimmed)-1; i++ {
		if !unicode.IsDigit(rune(trimmed[i])) {
			return false
		}
	}
	return true
}

// ExtractCompleteSentences 从文本中提取完整的句子
// 返回完整句子的切片和剩余的未完成内容
func ExtractCompleteSentences(text string) ([]string, string) {
	if text == "" {
		return []string{}, ""
	}

	var sentences []string
	var currentSentence bytes.Buffer

	runes := []rune(text)
	lastIndex := len(runes) - 1

	for i, r := range runes {
		currentSentence.WriteRune(r)

		// 判断句子是否结束
		if IsSentenceEndPunctuation(r) {
			// 如果是句子结束标点
			sentence := strings.TrimSpace(currentSentence.String())
			if sentence != "" {
				sentences = append(sentences, sentence)
			}
			currentSentence.Reset()
		} else if i == lastIndex {
			// 如果是最后一个字符但不是句子结束标点，保留在remaining中
			break
		}
	}

	// 当前未完成的句子作为remaining返回
	remaining := currentSentence.String()
	return sentences, strings.TrimSpace(remaining)
}

// isNumberPrefix 使用快速的字符检查替代正则，判断是否是序号前缀
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

// trimSpaceRunes 去除首尾空白字符
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

func isDigitAdjacentColon(text []rune, pos int) bool {
	if pos < 0 || pos >= len(text) {
		return false
	}

	colon := text[pos]
	if colon != ':' && colon != '：' {
		return false
	}

	if pos == 0 || !unicode.IsDigit(text[pos-1]) {
		return false
	}

	if pos == len(text)-1 {
		return true
	}

	return unicode.IsDigit(text[pos+1])
}

// findLastPunctuation 从后向前查找最后一个标点
func findLastPunctuation(text []rune, separatorMap map[rune]bool) int {
	lastPos := -1
	for i := len(text) - 1; i >= 0; i-- {
		// 检查是否是标点符号
		if separatorMap[text[i]] {
			// 如果是点号，检查是否是序号的一部分
			if text[i] == '.' && isNumberPrefix(text, i) {
				continue
			}
			if isDigitAdjacentColon(text, i) {
				continue
			}
			return i
		}
	}
	return lastPos
}

// findNextSplitPoint 查找下一个分割点
// 好。今天天气不错。  0, 20,
func findNextSplitPoint(text []rune, startPos int, maxLen int, separatorMap map[rune]bool) int {
	// 计算查找的结束位置
	endPos := startPos + maxLen
	if endPos > len(text) {
		endPos = len(text) // 8
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
		if separatorMap[text[i]] {
			if isDigitAdjacentColon(text, i) {
				continue
			}
			return i
		}
	}

	// 如果在maxLen范围内没找到，尝试在更大范围内查找
	if endPos < len(text) {
		for i := endPos; i < len(text); i++ {
			if text[i] == '\n' {
				return i
			}
			if separatorMap[text[i]] {
				if isDigitAdjacentColon(text, i) {
					continue
				}
				return i
			}
		}
	}

	return -1
}

// ExtractSmartSentences 智能提取句子
// text: 待处理的文本
// minLen: 最小句子长度
// maxLen: 最大句子长度
// isFirst: 是否为首次处理（首次处理时允许使用逗号作为分隔符）
// 把长文本按标点切成句子，控制每句长度在 [minLen, maxLen] 之间，太短的句子不单独成句而是并入 remaining；
// 好。今天天气不错。 , 5, 20, false
// sentences = ["今天天气不错。"]
// remaining = "好。"  // 这个好。当没有用的话了
func ExtractSmartSentences(text string, minLen, maxLen int, isFirst bool) (sentences []string, remaining string) {
	// 当isFirst为true时, 放宽到逗号作为分隔符
	separatorMap := punctuationMap
	if isFirst {
		separatorMap = firstPunctuation
	}
	// 预分配一个合理的切片容量
	estimatedCount := len(text) / 50 // len(text) = 8   8/ 50 = 0
	if estimatedCount < 10 {
		estimatedCount = 10
	}
	sentences = make([]string, 0, estimatedCount) // 10个长度

	// 一次性转换为rune切片
	currentRunes := []rune(text) // 好。今天天气不错。
	startPos := 0

	// 从对象池获取复用对象
	builder := builderPool.Get().(*strings.Builder)
	defer builderPool.Put(builder)
	builder.Grow(maxLen * 2) // 40

	// 获取临时rune切片
	tempRunesPtr := runeSlicePool.Get().(*[]rune)
	tempRunes := (*tempRunesPtr)[:0]
	defer runeSlicePool.Put(tempRunesPtr)

	for startPos < len(currentRunes) {
		// 跳过开头的空白字符
		for startPos < len(currentRunes) && (currentRunes[startPos] == ' ' || currentRunes[startPos] == '\t' || currentRunes[startPos] == '\n') { // 0 < 7
			startPos++
		}
		// 0
		if startPos >= len(currentRunes) {
			break
		}
		//  好。今天天气不错。  0, 20,
		// 查找下一个分割点, 1
		splitPos := findNextSplitPoint(currentRunes, startPos, maxLen, separatorMap)
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
		segment := trimSpaceRunes(currentRunes[startPos : splitPos+1]) // 好。

		// 检查段落是否满足最小长度要求且以标点符号结尾
		if len(segment) >= minLen && separatorMap[segment[len(segment)-1]] { // 符合要求的切分句子保留到到切片中
			sentences = append(sentences, string(segment))
		} else {
			// 如果不满足条件，将其添加到remaining中， 切分的不符合的保存到remaining中
			if len(segment) > 0 {
				if len(remaining) > 0 {
					remaining += " "
				}
				remaining += string(segment)
			}
		}

		startPos = splitPos + 1 // 更新位置
	}

	return sentences, remaining
}

// ContainsSentenceSeparator 判断字符串中是否包含分隔符（句子结束或暂停标点符号）
func ContainsSentenceSeparator(s string, isFirst bool) bool {
	separatorMap := punctuationMap
	if isFirst {
		separatorMap = firstPunctuation
	}

	runes := []rune(s)
	for i, r := range runes {
		if !separatorMap[r] {
			continue
		}
		if isDigitAdjacentColon(runes, i) {
			continue
		}
		return true
	}

	return false
}
