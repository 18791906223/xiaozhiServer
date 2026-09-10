package pool

import (
	"embed"
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

// EnsureDLLsCopied 将内嵌的 DLL 复制到 exe 同目录（如果尚未存在）
// Windows 上 CGO 动态链接需要在运行时能找到 DLL，embed 后手动释放是最稳定的方案。
func EnsureDLLsCopied() error {
	if runtime.GOOS != "windows" {
		return nil
	}
	exePath, err := os.Executable()
	if err != nil {
		return err
	}
	destDir := filepath.Dir(exePath)

	dlls := map[string][]byte{
		"ten_vad.dll":             nil,
		"onnxruntime.dll":         nil,
		"sherpa-onnx-c-api.dll":   nil,
		"sherpa-onnx-cxx-api.dll": nil,
	}

	// ten_vad.dll
	data, err := tenVadDLL.ReadFile("lib/ten-vad/lib/Windows/x64/ten_vad.dll")
	if err != nil {
		return err
	}
	dlls["ten_vad.dll"] = data

	// sherpa-onnx DLLs
	data, err = onnxruntimeDLL.ReadFile("lib/sherpa-onnx/onnxruntime.dll")
	if err != nil {
		return err
	}
	dlls["onnxruntime.dll"] = data

	data, err = sherpaCApiDLL.ReadFile("lib/sherpa-onnx/sherpa-onnx-c-api.dll")
	if err != nil {
		return err
	}
	dlls["sherpa-onnx-c-api.dll"] = data

	data, err = sherpaCxxApiDLL.ReadFile("lib/sherpa-onnx/sherpa-onnx-cxx-api.dll")
	if err != nil {
		return err
	}
	dlls["sherpa-onnx-cxx-api.dll"] = data

	// 写入 DLL（仅当目标文件不存在时）
	for name, data := range dlls {
		destPath := filepath.Join(destDir, name)
		if _, err := os.Stat(destPath); err == nil {
			continue
		}
		if err := os.WriteFile(destPath, data, 0644); err != nil {
			return err
		}
	}
	return nil
}
