@echo off
echo Compiling OpenCode Zen Router for Tencent Ubuntu Server (Linux amd64)...
set GOOS=linux
set GOARCH=amd64
go build -ldflags="-s -w" -o opencode-router-linux .
if %ERRORLEVEL% EQU 0 (
    echo [OK] Successfully built standalone Linux binary: opencode-router-linux!
    echo Upload 'opencode-router-linux' to your Tencent Ubuntu server.
    echo Then run: chmod +x opencode-router-linux ^&^& ./opencode-router-linux
) else (
    echo [ERROR] Linux build failed!
)
set GOOS=
set GOARCH=
