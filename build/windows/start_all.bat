@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion
title 小智 xiaozhiServer 整体启动

cd /d "%~dp0"

echo ============================================================
echo   小智项目整体启动脚本
echo   1) FunASR 语音识别   : ws://127.0.0.1:10096
echo   2) xiaozhi_server    : ws://127.0.0.1:8989/xiaozhi/v1/
echo      (内嵌 asr_server : 9000,  管理后台: http://127.0.0.1:8080)
echo ============================================================
echo.

REM =====================================================================
REM 1. 启动 FunASR 语音识别服务（若未运行）
REM =====================================================================
echo [1/3] 检查 FunASR 服务 ...
powershell -NoProfile -Command "$c=New-Object System.Net.Sockets.TcpClient; try{$c.Connect('127.0.0.1',10096);'UP'}catch{'DOWN'}; $c.Close()" > "%TEMP%\funasr_check.txt" 2>&1
set /p FUNASR_STATE=<"%TEMP%\funasr_check.txt"
if /i "%FUNASR_STATE%"=="UP" (
    echo        FunASR 已在运行，跳过启动。
    goto funasr_done
)

echo        FunASR 未运行，正在新窗口启动（首次会下载模型，需几分钟）...
start "FunASR 语音识别" /min cmd /c ""%~dp0funasr\start_funasr_local.bat""

echo        等待 FunASR 就绪（端口 10096）...
set /a _wait=0
:wait_funasr
powershell -NoProfile -Command "$c=New-Object System.Net.Sockets.TcpClient; try{$c.Connect('127.0.0.1',10096);'UP'}catch{'DOWN'}; $c.Close()" > "%TEMP%\funasr_check.txt" 2>&1
set /p FUNASR_STATE=<"%TEMP%\funasr_check.txt"
if /i "%FUNASR_STATE%"=="UP" goto funasr_ready
set /a _wait+=10
timeout /t 10 /nobreak >nul
if !_wait! GEQ 600 (
    echo        [x] FunASR 启动超时（10 分钟），请检查 funasr\funasr.log
    echo            可能原因：模型正在首次下载，或 python 环境路径不对。
    goto funasr_done
)
goto wait_funasr

:funasr_ready
echo        FunASR 已就绪 (127.0.0.1:10096)
:funasr_done
echo.

REM =====================================================================
REM 2. 停止旧的 xiaozhi_server（避免端口冲突）
REM =====================================================================
echo [2/3] 停止旧 xiaozhi_server（若有）...
taskkill /f /im xiaozhi_server.exe >nul 2>&1
timeout /t 2 /nobreak >nul

REM =====================================================================
REM 3. 启动 xiaozhi_server
REM =====================================================================
echo [3/3] 启动 xiaozhi_server ...
start "xiaozhi_server" /min xiaozhi_server.exe -c main_config.yaml -asr-enable --asr-config asr_server.json --manager-enable --manager-config manager.json

timeout /t 3 /nobreak >nul
echo.
echo ============================================================
echo   启动完成！
echo   对话入口  : ws://127.0.0.1:8989/xiaozhi/v1/
echo   管理后台  : http://127.0.0.1:8080
echo   FunASR    : ws://127.0.0.1:10096
echo.
echo   常用命令：
echo     停止全部:  stop_all.bat
echo     查看日志:  logs\ 目录
echo ============================================================
echo.
pause