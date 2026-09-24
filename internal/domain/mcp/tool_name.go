package mcp

import (
	"fmt"
	"hash/fnv"
	"strings"
)

const llmToolNameMaxLen = 64

func sanitizeLLMToolName(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return ""
	}

	var b strings.Builder
	b.Grow(len(trimmed))
	hasAlphaNum := false
	lastWasReplacement := false
	for _, r := range trimmed {
		if isLLMToolNameAlphaNum(r) {
			b.WriteRune(r)
			hasAlphaNum = true
			lastWasReplacement = false
			continue
		}
		if isLLMToolNamePunct(r) {
			b.WriteRune(r)
			lastWasReplacement = false
			continue
		}
		if !lastWasReplacement {
			b.WriteByte('_')
			lastWasReplacement = true
		}
	}

	sanitized := strings.Trim(b.String(), "_-")
	if sanitized == "" || !hasAlphaNum {
		sanitized = "tool_" + shortStableHash(trimmed)
	}
	return truncateLLMToolName(sanitized, trimmed)
}

func uniqueLLMToolName(candidate, original string, used map[string]string) string {
	if candidate == "" {
		candidate = "tool_" + shortStableHash(original)
	}
	if used == nil {
		return candidate
	}

	if prev, ok := used[candidate]; !ok || prev == original {
		used[candidate] = original
		return candidate
	}

	hashSuffix := "_" + shortStableHash(original)
	base := strings.Trim(candidate, "_-")
	if base == "" {
		base = "tool"
	}

	for i := 0; ; i++ {
		extraSuffix := ""
		if i > 0 {
			extraSuffix = fmt.Sprintf("_%d", i+1)
		}
		maxBaseLen := llmToolNameMaxLen - len(hashSuffix) - len(extraSuffix)
		if maxBaseLen < 1 {
			maxBaseLen = 1
		}

		trimmedBase := base
		if len(trimmedBase) > maxBaseLen {
			trimmedBase = strings.Trim(trimmedBase[:maxBaseLen], "_-")
			if trimmedBase == "" {
				trimmedBase = "tool"
			}
		}

		name := trimmedBase + hashSuffix + extraSuffix
		if prev, ok := used[name]; !ok || prev == original {
			used[name] = original
			return name
		}
	}
}

func isLLMToolNameAlphaNum(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

func isLLMToolNamePunct(r rune) bool {
	return r == '_' || r == '-'
}

func truncateLLMToolName(name, original string) string {
	if len(name) <= llmToolNameMaxLen {
		return name
	}

	hashSuffix := "_" + shortStableHash(original)
	maxBaseLen := llmToolNameMaxLen - len(hashSuffix)
	if maxBaseLen < 1 {
		maxBaseLen = 1
	}

	base := strings.Trim(name[:maxBaseLen], "_-")
	if base == "" {
		base = "tool"
	}
	return base + hashSuffix
}

func shortStableHash(value string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(value))
	return fmt.Sprintf("%08x", h.Sum32())
}
