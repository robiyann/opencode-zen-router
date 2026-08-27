@echo off
echo Compiling OpenCode Zen Router for Windows...
go build -ldflags="-s -w" -o opencode-router.exe .
if %ERRORLEVEL% EQU 0 (
    echo [OK] Successfully built opencode-router.exe!
) else (
    echo [ERROR] Build failed!
)
