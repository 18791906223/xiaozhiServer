package manager

import "testing"

func TestVADTestSampleCountUsesSileroWindow(t *testing.T) {
	got := vadTestSampleCount("custom_vad", map[string]interface{}{
		"provider":    "silero_vad",
		"sample_rate": 16000,
	})
	if got != 512 {
		t.Fatalf("vadTestSampleCount() = %d, want 512", got)
	}
}

func TestVADTestSampleCountUsesSilero8KWindow(t *testing.T) {
	got := vadTestSampleCount("custom_vad", map[string]interface{}{
		"provider":    "silero_vad",
		"sample_rate": 8000,
	})
	if got != 256 {
		t.Fatalf("vadTestSampleCount() = %d, want 256", got)
	}
}

func TestVADTestSampleCountUsesTenHopSize(t *testing.T) {
	got := vadTestSampleCount("custom_vad", map[string]interface{}{
		"provider": "ten_vad",
		"hop_size": 320,
	})
	if got != 320 {
		t.Fatalf("vadTestSampleCount() = %d, want 320", got)
	}
}
