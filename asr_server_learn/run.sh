#!/bin/bash
# 在 go run 之前将 DLL 复制到项目目录（go run 的工作目录）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cp "$SCRIPT_DIR/lib/ten-vad/lib/Windows/x64/ten_vad.dll" "$SCRIPT_DIR/" 2>/dev/null
cp "$SCRIPT_DIR/lib/sherpa-onnx/*.dll" "$SCRIPT_DIR/" 2>/dev/null
go run "$@"
