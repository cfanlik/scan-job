#!/usr/bin/env bash
# scan-job Service Manager (Linux / macOS)
set -e
cd "$(dirname "$0")"
mkdir -p logs

# -- version info --
VER=$(git rev-parse --short HEAD 2>/dev/null || echo "dev")

do_stop() {
    echo ""
    echo "[STOP] 停止服务..."
    local found=0
    if command -v lsof &>/dev/null; then
        for pid in $(lsof -ti :3600 2>/dev/null); do
            echo "  kill PID $pid"
            kill -9 "$pid" 2>/dev/null || true
            found=1
        done
    elif command -v fuser &>/dev/null; then
        fuser -k 3600/tcp 2>/dev/null && found=1 || true
    fi
    [ "$found" = "0" ] && echo "  未发现运行中的服务."
    echo "[STOP] 完成."
}

do_start() {
    echo ""

    # -- 检查 Python 环境 --
    if ! command -v python3 &>/dev/null; then
        echo "  [FAIL] 未找到 python3，请先安装 Python 3.10+"
        return 1
    fi

    # -- 检查依赖 --
    if ! python3 -c "import fastapi" 2>/dev/null; then
        echo "  [SETUP] 安装依赖..."
        pip3 install -r requirements.txt >> logs/install.log 2>&1
        if [ $? -ne 0 ]; then
            echo "  [FAIL] 依赖安装失败，详见 logs/install.log"
            return 1
        fi
        echo "  [SETUP] 依赖安装完成"
    fi

    echo "[START] Backend :3600 ..."
    nohup python3 web/server.py >> logs/backend.log 2>&1 &
    echo "  PID: $!"
    sleep 3

    # -- 验证 --
    echo ""
    if ss -tlnp 2>/dev/null | grep -q ':3600 '; then
        echo "  [OK] Backend   http://localhost:3600"
        echo "  [OK] Dashboard http://localhost:3600/dashboard"
    elif command -v lsof &>/dev/null && lsof -ti :3600 &>/dev/null; then
        echo "  [OK] Backend   http://localhost:3600"
        echo "  [OK] Dashboard http://localhost:3600/dashboard"
    else
        echo "  [FAIL] Backend - see logs/backend.log"
    fi
}

show_menu() {
    echo ""
    echo "  =========================================="
    echo "    scan-job v1.0  [$VER]"
    echo "    融资代币扫描平台"
    echo "  =========================================="
    echo "    1. Start    - 启动服务"
    echo "    2. Stop     - 停止服务"
    echo "    3. Restart  - 重启服务"
    echo "    0. Exit     - 退出"
    echo "  =========================================="
    echo ""
    read -rp "  请选择 [1/2/3/0]: " ch
    case "$ch" in
        1) do_start ;;
        2) do_stop ;;
        3) do_stop; sleep 2; do_start ;;
        0) exit 0 ;;
        *) show_menu ;;
    esac
}

case "${1,,}" in
    start)   do_start ;;
    stop)    do_stop ;;
    restart) do_stop; sleep 2; do_start ;;
    *)       show_menu ;;
esac

echo ""
echo "用法: ./start_linux.sh [start|stop|restart]"
echo ""
