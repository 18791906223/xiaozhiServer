package pool

import (
	"embed"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

//go:embed lib/ten-vad/lib/Windows/x64/ten_vad.dll
var tenVadDLL embed.FS

//go:embed lib/sherpa-onnx/onnxruntime.dll
var onnxruntimeDLL embed.FS

//go:embed lib/sherpa-onnx/sherpa-onnx-c-api.dll
var sherpaCApiDLL embed.FS

//go:embed lib/sherpa-onnx/sherpa-onnx-cxx-api.dll
var sherpaCxxApiDLL embed.FS

// ensureDLLsWritten 将内嵌的 DLL 写到项目根目录（exe 同目录）。
// go build -o app.exe 时，exe 在项目根目录，CWD = exe 目录，Windows 能找到 DLL。
// go run 时，exe 在 go-build 临时目录，但 CWD 仍然是项目根目录，
// Windows DLL 搜索顺序包含 CWD，因此也能找到。
// 注意：go run 时 PE loader 会先于 init() 搜索 DLL，
// 但 Windows 的 DLL 搜索在查找 CWD 时是从当前进程启动时的工作目录出发，
// go run 会在项目目录启动子进程，所以 CWD 就是项目根目录。
func init() {
	if runtime.GOOS != "windows" {
		return
	}
	dllEntries := []struct{ name, embedPath string }{
		{"ten_vad.dll", "lib/ten-vad/lib/Windows/x64/ten_vad.dll"},
		{"onnxruntime.dll", "lib/sherpa-onnx/onnxruntime.dll"},
		{"sherpa-onnx-c-api.dll", "lib/sherpa-onnx/sherpa-onnx-c-api.dll"},
		{"sherpa-onnx-cxx-api.dll", "lib/sherpa-onnx/sherpa-onnx-cxx-api.dll"},
	}

	// 目标目录：exe 所在目录（go build 时是项目根，go run 时是 go-build 临时目录）
	exePath, exeErr := os.Executable()
	destDir := ""
	if exeErr == nil {
		destDir = filepath.Dir(exePath)
	}
	// 兜底：如果 exePath 为空（极少情况），使用当前工作目录
	if destDir == "" {
		var wdErr error
		destDir, wdErr = os.Getwd()
		if wdErr != nil {
			fmt.Fprintf(os.Stderr, "[DLL init] failed to get target dir: %v\n", wdErr)
			return
		}
	}

	for _, d := range dllEntries {
		destPath := filepath.Join(destDir, d.name)
		// 已存在则跳过
		if info, err := os.Stat(destPath); err == nil && !info.IsDir() {
			continue
		}
		data, err := readEmbedded(d.embedPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[DLL init] read %s failed: %v\n", d.name, err)
			continue
		}
		if err := os.WriteFile(destPath, data, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "[DLL init] write %s to %s failed: %v\n", d.name, destDir, err)
		} else {
			fmt.Fprintf(os.Stderr, "[DLL init] wrote %s to %s\n", d.name, destDir)
		}
	}
}

func readEmbedded(path string) ([]byte, error) {
	switch path {
	case "lib/ten-vad/lib/Windows/x64/ten_vad.dll":
		return tenVadDLL.ReadFile(path)
	case "lib/sherpa-onnx/onnxruntime.dll":
		return onnxruntimeDLL.ReadFile(path)
	case "lib/sherpa-onnx/sherpa-onnx-c-api.dll":
		return sherpaCApiDLL.ReadFile(path)
	case "lib/sherpa-onnx/sherpa-onnx-cxx-api.dll":
		return sherpaCxxApiDLL.ReadFile(path)
	}
	return nil, fmt.Errorf("unknown embed path: %s", path)
}
