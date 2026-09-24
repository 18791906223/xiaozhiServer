# ONNX Runtime (win-x64)

版本：见 `win-x64/VERSION_NUMBER`（来源：onnxruntime-win-x64 官方发行包）。

本目录为 `github.com/hackers365/silero-vad-go` 的 CGO 编译依赖：

- `include/`：CGO 编译头文件（`CGO_CFLAGS -I` 指向这里）
- `lib/onnxruntime.lib`：链接期 import library（`CGO_LDFLAGS -L` 指向这里）
- `lib/onnxruntime.dll`：运行时动态库，需与 `xiaozhi_server.exe` 同目录或在 `PATH` 中
- `lib/onnxruntime_providers_shared.dll`：运行时可选插件，建议与 dll 一同部署

完整包体积约 418 MB，其中 `onnxruntime.pdb`（调试符号，约 400 MB）与编译运行无关，未纳入仓库。

## 配置（每台机器一次）

```powershell
go env -w CGO_CFLAGS="-I<项目路径>/third_party/onnxruntime/win-x64/include"
go env -w CGO_LDFLAGS="-L<项目路径>/third_party/onnxruntime/win-x64/lib"
```

## 许可证

见 `win-x64/LICENSE`（MIT License）。
