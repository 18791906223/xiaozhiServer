package pool

import (
	"fmt"
	"os"
	"testing"
)

func TestEmbedRead(t *testing.T) {
	data, err := tenVadDLL.ReadFile("lib/ten-vad/lib/Windows/x64/ten_vad.dll")
	fmt.Fprintf(os.Stderr, "[TestEmbedRead] len=%d err=%v\n", len(data), err)
	if err != nil {
		t.Fatalf("embed read failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("embed data is empty")
	}
	fmt.Fprintf(os.Stderr, "[TestEmbedRead] PASS\n")
}
