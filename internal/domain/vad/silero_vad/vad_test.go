package silero_vad

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestResolveNumSessionsDefaultsToCPUCount(t *testing.T) {
	got := resolveNumSessions(withDefaults(map[string]interface{}{}))
	want := runtime.NumCPU()
	if got != want {
		t.Fatalf("resolveNumSessions() = %d, want %d", got, want)
	}
}

func TestResolveNumSessionsUsesPoolSizeFallback(t *testing.T) {
	got := resolveNumSessions(withDefaults(map[string]interface{}{
		"pool_size": 4,
	}))
	if got != 4 {
		t.Fatalf("resolveNumSessions() = %d, want 4", got)
	}
}

func TestResolveNumSessionsPrefersNumSessions(t *testing.T) {
	got := resolveNumSessions(withDefaults(map[string]interface{}{
		"pool_size":    4,
		"num_sessions": 2,
	}))
	if got != 2 {
		t.Fatalf("resolveNumSessions() = %d, want 2", got)
	}
}

func TestResolveNumSessionsFallsBackToCPUCountForInvalidValue(t *testing.T) {
	got := resolveNumSessions(withDefaults(map[string]interface{}{
		"pool_size": 0,
	}))
	want := runtime.NumCPU()
	if got != want {
		t.Fatalf("resolveNumSessions() = %d, want %d", got, want)
	}
}

func TestBuildModelPathCandidatesAddsSourceToReleaseFallback(t *testing.T) {
	root := "app"
	got := buildModelPathCandidates("config/models/vad/silero_vad.onnx", root)
	want := []string{
		filepath.Join(root, "config", "models", "vad", "silero_vad.onnx"),
		filepath.Join(root, "models", "vad", "silero_vad.onnx"),
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("buildModelPathCandidates() = %#v, want %#v", got, want)
	}
}

func TestBuildModelPathCandidatesAddsReleaseToSourceFallback(t *testing.T) {
	root := "app"
	got := buildModelPathCandidates("models/vad/silero_vad.onnx", root)
	want := []string{
		filepath.Join(root, "models", "vad", "silero_vad.onnx"),
		filepath.Join(root, "config", "models", "vad", "silero_vad.onnx"),
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("buildModelPathCandidates() = %#v, want %#v", got, want)
	}
}

func TestResolveModelPathPrefersConfiguredPathWhenPresent(t *testing.T) {
	root := t.TempDir()
	runInDir(t, root)
	writeTestFile(t, filepath.Join(root, "config", "models", "vad", "silero_vad.onnx"))
	writeTestFile(t, filepath.Join(root, "models", "vad", "silero_vad.onnx"))

	got, err := resolveModelPath("config/models/vad/silero_vad.onnx")
	if err != nil {
		t.Fatalf("resolveModelPath() error = %v", err)
	}
	want := filepath.Join(root, "config", "models", "vad", "silero_vad.onnx")
	if got != want {
		t.Fatalf("resolveModelPath() = %q, want %q", got, want)
	}
}

func TestResolveModelPathFallsBackFromSourceLayoutToReleaseLayout(t *testing.T) {
	root := t.TempDir()
	runInDir(t, root)
	writeTestFile(t, filepath.Join(root, "models", "vad", "silero_vad.onnx"))

	got, err := resolveModelPath("config/models/vad/silero_vad.onnx")
	if err != nil {
		t.Fatalf("resolveModelPath() error = %v", err)
	}
	want := filepath.Join(root, "models", "vad", "silero_vad.onnx")
	if got != want {
		t.Fatalf("resolveModelPath() = %q, want %q", got, want)
	}
}

func TestResolveModelPathFallsBackFromReleaseLayoutToSourceLayout(t *testing.T) {
	root := t.TempDir()
	runInDir(t, root)
	writeTestFile(t, filepath.Join(root, "config", "models", "vad", "silero_vad.onnx"))

	got, err := resolveModelPath("models/vad/silero_vad.onnx")
	if err != nil {
		t.Fatalf("resolveModelPath() error = %v", err)
	}
	want := filepath.Join(root, "config", "models", "vad", "silero_vad.onnx")
	if got != want {
		t.Fatalf("resolveModelPath() = %q, want %q", got, want)
	}
}

func TestResolveModelPathReportsTriedCandidates(t *testing.T) {
	root := t.TempDir()
	runInDir(t, root)

	_, err := resolveModelPath("config/models/vad/silero_vad.onnx")
	if err == nil {
		t.Fatal("resolveModelPath() error = nil, want missing file error")
	}
	message := err.Error()
	for _, want := range []string{
		filepath.Join(root, "config", "models", "vad", "silero_vad.onnx"),
		filepath.Join(root, "models", "vad", "silero_vad.onnx"),
	} {
		if !strings.Contains(message, want) {
			t.Fatalf("resolveModelPath() error = %q, want it to include %q", message, want)
		}
	}
}

func runInDir(t *testing.T, dir string) {
	t.Helper()
	previous, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd() error = %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("os.Chdir(%q) error = %v", dir, err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(previous); err != nil {
			t.Fatalf("restore cwd to %q error = %v", previous, err)
		}
	})
}

func writeTestFile(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("os.MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(path, []byte("test"), 0o644); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
}
