@echo off
chcp 65001 >nul
title 停止小智项目全部服务

echo ============================================================
echo   正在停止项目全部服务 ...
echo ============================================================

echo [1/2] 停止 xiaozhi_server ...
taskkill /f /im xiaozhi_server.exe >nul 2>&1
if errorlevel 1 (
    echo        xiaozhi_server 未运行
) else (
    echo        xiaozhi_server 已停止
)

echo [2/2] 停止 FunASR 语音识别 ...
for /f "tokens=5" %%p in ('netstat -ano ^| findstr ":10096" ^| findstr "LISTENING"') do (
    taskkill /f /pid %%p >nul 2>&1
)
echo        FunASR 已停止

echo.
echo 全部服务已停止。
pause