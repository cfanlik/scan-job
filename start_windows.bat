@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion
cd /d "%~dp0"
if not exist "logs" mkdir "logs"
set "PROJ=%~dp0"

rem -- version info --
for /f "delims=" %%h in ('git rev-parse --short HEAD 2^>nul') do set "VER=%%h"
if not defined VER set "VER=dev"

if "%~1"=="stop" goto DO_STOP
if "%~1"=="STOP" goto DO_STOP
if "%~1"=="restart" goto DO_RESTART
if "%~1"=="RESTART" goto DO_RESTART
if "%~1"=="start" goto DO_START
if "%~1"=="START" goto DO_START
if "%~1"=="" goto SHOW_MENU
goto SHOW_MENU

:SHOW_MENU
echo.
echo   ==========================================
echo     scan-job v1.0  [!VER!]
echo     融资代币扫描平台
echo   ==========================================
echo     1. Start    - 启动服务
echo     2. Stop     - 停止服务
echo     3. Restart  - 重启服务
echo     0. Exit     - 退出
echo   ==========================================
echo.
set "CH="
set /p "CH=  请选择 [1/2/3/0]: "
if "!CH!"=="1" goto DO_START
if "!CH!"=="2" goto DO_STOP
if "!CH!"=="3" goto DO_RESTART
if "!CH!"=="0" goto THE_END
goto SHOW_MENU

:DO_STOP
echo.
echo [STOP] 停止服务...
set "KILLED=0"
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":3600 " ^| findstr "LISTENING"') do (
    echo   kill PID %%a
    taskkill /F /PID %%a >nul 2>&1
    set "KILLED=1"
)
if "!KILLED!"=="0" echo   未发现运行中的服务.
echo [STOP] 完成.
goto THE_END

:DO_START
echo.
rem -- 检查 Python 环境 --
python --version >nul 2>&1
if !errorlevel! neq 0 (
    echo   [FAIL] 未找到 Python，请先安装 Python 3.10+
    goto THE_END
)

rem -- 检查依赖 --
python -c "import fastapi" >nul 2>&1
if !errorlevel! neq 0 (
    echo   [SETUP] 安装依赖...
    pip install -r "%PROJ%requirements.txt" >> "%PROJ%logs\install.log" 2>&1
    if !errorlevel! neq 0 (
        echo   [FAIL] 依赖安装失败，详见 logs\install.log
        goto THE_END
    )
    echo   [SETUP] 依赖安装完成
)

echo [START] Backend :3600 ...
echo CreateObject("WScript.Shell").Run "cmd /c cd /d %PROJ% && python web\server.py >> logs\backend.log 2>&1", 0, False > "%TEMP%\scanjob_bg.vbs"
wscript "%TEMP%\scanjob_bg.vbs"
ping -n 5 127.0.0.1 >nul

set "BOK=0"
netstat -ano 2>nul | findstr ":3600 " | findstr "LISTENING" >nul 2>&1
if !errorlevel! equ 0 set "BOK=1"
echo.
if "!BOK!"=="1" (
    echo   [OK] Backend   http://localhost:3600
    echo   [OK] Dashboard http://localhost:3600/dashboard
) else (
    echo   [FAIL] Backend - see logs\backend.log
)
goto THE_END

:DO_RESTART
echo.
echo [RESTART] 先停止服务...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":3600 " ^| findstr "LISTENING"') do (
    echo   kill PID %%a
    taskkill /F /PID %%a >nul 2>&1
)
ping -n 3 127.0.0.1 >nul
goto DO_START

:THE_END
echo.
echo ==========================================
echo   用法: start_windows.bat [start / stop / restart]
echo ==========================================
echo.
pause
endlocal
