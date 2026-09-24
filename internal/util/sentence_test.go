package util

import "testing"

func TestExtractSmartSentencesKeepsTimeTogether(t *testing.T) {
	text := "根据系统时间，现在是2026年3月20日 星期五 02:37:04。"

	sentences, remaining := ExtractSmartSentences(text, 2, 100, false)

	if remaining != "" {
		t.Fatalf("expected no remaining text, got %q", remaining)
	}
	if len(sentences) != 1 {
		t.Fatalf("expected 1 sentence, got %d: %#v", len(sentences), sentences)
	}
	if sentences[0] != text {
		t.Fatalf("expected full time expression to stay intact, got %q", sentences[0])
	}
}

func TestContainsSentenceSeparatorIgnoresStreamingTimeColon(t *testing.T) {
	if ContainsSentenceSeparator("现在是2026年3月20日 星期五 02:", false) {
		t.Fatal("expected trailing time colon not to trigger sentence split")
	}

	if !ContainsSentenceSeparator("现在是2026年3月20日 星期五 02:37:04。", false) {
		t.Fatal("expected final period to trigger sentence split")
	}
}
