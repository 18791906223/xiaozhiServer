@echo off
chcp 65001 >nul
title FunASR 本地服务 (CPU)

set PYTHON=D:\devSoftware\anaconda3\envs\pyEnvs\xiaozhi-esp32-server\python.exe
set SCRIPT=%~dp0funasr_wss_server.py
set PORT=10096

echo ============================================================
echo   FunASR 本地部署（CPU 模式）
echo   环境: xiaozhi-esp32-server (Python 3.10.20)
echo   端口: %PORT%
echo ============================================================
echo.
echo 首次启动会自动下载模型（约 1-2GB），请耐心等待...
echo.
"%PYTHON%" "%SCRIPT%" ^
    --host 0.0.0.0 ^
    --port %PORT% ^
    --ngpu 0 ^
    --device cpu ^
    --ncpu 4

echo.
echo 服务已停止。
pause