@echo off
REM =====================================================================
REM  本地部署 FunASR 语音识别服务（Docker 方式）
REM  启动后监听 0.0.0.0:10096，供 xiaozhiServer 的 asr.funasr 使用
REM  用法：双击本脚本 或 在命令行执行 deploy_funasr.bat
REM =====================================================================
setlocal enabledelayedexpansion

chcp 65001 >nul
title FunASR 本地部署

set "FUNASR_IMAGE=registry.cn-hangzhou.aliyuncs.com/funasr_repo/funasr"
REM 国内镜像源（拉不动官方源时使用）
set "FUNASR_IMAGE_MIRROR=docker.m.daocloud.io/registry.cn-hangzhou.aliyuncs.com/funasr_repo/funasr"
set "CONTAINER_NAME=funasr"
set "HOST_PORT=10096"
REM 模型缓存目录（挂载到容器，避免每次重启重新下载模型）
set "MODELS_DIR=%~dp0funasr_models"

REM ---------------------------------------------------------------------
REM 1. 确保 Docker Desktop 已启动
REM ---------------------------------------------------------------------
echo [1/5] 检查 Docker ...
docker info >nul 2>&1
if errorlevel 1 (
    echo       Docker 守护进程未运行，正在尝试启动 Docker Desktop ...
    if exist "D:\devSoftware\docker\Docker Desktop.exe" (
        start "" "D:\devSoftware\docker\Docker Desktop.exe"
    ) else (
        echo       [!] 未找到 Docker Desktop，请手动启动 Docker 后再运行本脚本
        pause
        exit /b 1
    )
    echo       等待 Docker 启动（最多 180 秒）...
    set /a _wait=0
    :wait_docker
    docker info >nul 2>&1
    if not errorlevel 1 goto docker_ready
    set /a _wait+=5
    timeout /t 5 /nobreak >nul
    if !_wait! GEQ 180 (
        echo       [x] 等待超时，Docker 未就绪，请手动检查 Docker Desktop
        pause
        exit /b 1
    )
    goto wait_docker
)
:docker_ready
echo       Docker 已就绪
docker info --format "{{.ServerVersion}}" 2>nul

REM ---------------------------------------------------------------------
REM 2. 拉取 FunASR 镜像
REM ---------------------------------------------------------------------
echo [2/5] 拉取 FunASR 镜像（首次约 2GB，请耐心等待）...
docker pull %FUNASR_IMAGE%
if errorlevel 1 (
    echo       官方源拉取失败，尝试国内镜像源 ...
    docker pull %FUNASR_IMAGE_MIRROR%
    if errorlevel 1 (
        echo       [x] 镜像拉取失败，请检查网络后重试
        pause
        exit /b 1
    )
    docker tag %FUNASR_IMAGE_MIRROR% %FUNASR_IMAGE%
)

REM ---------------------------------------------------------------------
REM 3. 清理旧容器（若存在）
REM ---------------------------------------------------------------------
echo [3/5] 检查旧容器 ...
docker rm -f %CONTAINER_NAME% >nul 2>&1

REM ---------------------------------------------------------------------
REM 4. 启动 FunASR 容器
REM    -p 10096:10096 : 暴露 WebSocket 端口
REM    -v 模型目录     : 持久化模型缓存（首次启动会自动下载模型，约 1-2GB）
REM    --restart unless-stopped : 开机自动重启
REM ---------------------------------------------------------------------
echo [4/5] 启动 FunASR 容器 ...
if not exist "%MODELS_DIR%" mkdir "%MODELS_DIR%"
docker run -itd --name %CONTAINER_NAME% ^
    -p %HOST_PORT%:10096 ^
    -v "%MODELS_DIR%":/root/.cache/modelscope/hub ^
    --restart unless-stopped ^
    %FUNASR_IMAGE%

REM ---------------------------------------------------------------------
REM 5. 等待服务就绪
REM ---------------------------------------------------------------------
echo [5/5] 等待 FunASR 服务就绪（首次会自动下载模型，可能需要几分钟）...
set /a _wait=0
:wait_funasr
docker ps --filter "name=%CONTAINER_NAME%" --filter "status=running" | findstr /i "%CONTAINER_NAME%" >nul
if errorlevel 1 (
    echo       [x] 容器未在运行，查看日志：
    docker logs %CONTAINER_NAME% --tail 30
    pause
    exit /b 1
)
REM 尝试 WebSocket 握手（收到 HTTP 101 或任何响应都视为服务已监听）
powershell -Command "try{$c=New-Object System.Net.Sockets.TcpClient; $c.Connect('127.0.0.1',%HOST_PORT%); $c.Close(); exit 0}catch{exit 1}" >nul 2>&1
if not errorlevel 1 goto funasr_ready
set /a _wait+=10
timeout /t 10 /nobreak >nul
if !_wait! GEQ 600 (
    echo       [x] 等待超时，请查看容器日志确认模型是否下载成功：
    docker logs %CONTAINER_NAME% --tail 50
    pause
    exit /b 1
)
goto wait_funasr

:funasr_ready
echo.
echo ============================================================
echo   FunASR 部署成功！服务监听 0.0.0.0:10096
echo   容器名: %CONTAINER_NAME%
echo   模型目录: %MODELS_DIR%
echo.
echo   接下来需要把 xiaozhiServer 的 ASR 配置 host 改为 127.0.0.1
echo   （main_config.yaml 和 管理后台的系统配置 都要改）
echo ============================================================
echo.
echo   常用命令：
echo     查看日志:    docker logs -f %CONTAINER_NAME%
echo     停止服务:    docker stop %CONTAINER_NAME%
echo     重新启动:    docker start %CONTAINER_NAME%
echo.
pause
