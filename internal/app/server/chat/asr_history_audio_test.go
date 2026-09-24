package chat

import "testing"

func TestTrimFirstSpeechAudioKeepsCurrentFrameAndMaxPreSpeech(t *testing.T) {
	allData := make([]float32, 1000)
	for i := range allData {
		allData[i] = float32(i)
	}

	got := trimFirstSpeechAudio(allData, 20, 1000, 1)

	if len(got) != 220 {
		t.Fatalf("len = %d, want 220", len(got))
	}
	if got[0] != 780 {
		t.Fatalf("first sample = %v, want 780", got[0])
	}
	if got[len(got)-1] != 999 {
		t.Fatalf("last sample = %v, want 999", got[len(got)-1])
	}
}

func TestTrimFirstSpeechAudioKeepsShortBuffer(t *testing.T) {
	allData := []float32{1, 2, 3, 4, 5}

	got := trimFirstSpeechAudio(allData, 2, 1000, 1)

	if len(got) != len(allData) {
		t.Fatalf("len = %d, want %d", len(got), len(allData))
	}
	for i := range allData {
		if got[i] != allData[i] {
			t.Fatalf("sample[%d] = %v, want %v", i, got[i], allData[i])
		}
	}
}

func TestTrimFirstSpeechAudioInvalidFormatKeepsOriginal(t *testing.T) {
	allData := []float32{1, 2, 3, 4, 5}

	got := trimFirstSpeechAudio(allData, 2, 0, 1)

	if len(got) != len(allData) {
		t.Fatalf("len = %d, want %d", len(got), len(allData))
	}
}
