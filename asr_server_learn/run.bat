@echo off
rem 在 go run 之前将 DLL 复制到项目目录
copy /Y "%~dp0lib\ten-vad\lib\Windows\x64\ten_vad.dll" "%~dp0" >nul
copy /Y "%~dp0lib\sherpa-onnx\*.dll" "%~dp0" >nul
go run %*
