#!/bin/bash
# Wrapper script untuk menjalankan Flask app YouTube Transcript Manager di localhost.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
VENV_PYTHON="$("$SCRIPT_DIR/get_venv.sh")"
LOG_DIR="$REPO_DIR/logs"
LOG_FILE="$LOG_DIR/app_localhost.log"
PID_FILE="$LOG_DIR/app_localhost.pid"
HOST="${APP_HOST:-127.0.0.1}"
PORT="${APP_PORT:-5000}"

mkdir -p "$LOG_DIR"

usage() {
    cat <<'EOF'
Usage: scripts/app.sh [start|stop|status|restart]

Environment:
  APP_HOST   Host bind default 127.0.0.1
  APP_PORT   Port bind default 5000
EOF
}

start_app() {
    if [ -f "$PID_FILE" ]; then
        old_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
        if [ -n "$old_pid" ] && kill -0 "$old_pid" 2>/dev/null; then
            echo "App sudah berjalan dengan PID $old_pid"
            return 0
        fi
    fi

    echo "Starting app on http://$HOST:$PORT"
    nohup env PYTHONUNBUFFERED=1 APP_HOST="$HOST" APP_PORT="$PORT" \
        "$VENV_PYTHON" -c 'from flask_app.app import run_server; import os; run_server(host=os.environ.get("APP_HOST", "127.0.0.1"), port=int(os.environ.get("APP_PORT", "5000")), debug=False)' \
        >"$LOG_FILE" 2>&1 &
    app_pid=$!
    echo "$app_pid" >"$PID_FILE"
    sleep 2
    if kill -0 "$app_pid" 2>/dev/null; then
        echo "Started PID $app_pid"
        echo "Log: $LOG_FILE"
    else
        echo "App gagal start, cek log: $LOG_FILE" >&2
        exit 1
    fi
}

stop_app() {
    if [ ! -f "$PID_FILE" ]; then
        echo "PID file tidak ditemukan"
        return 0
    fi

    pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid"
        sleep 1
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid" || true
        fi
        echo "Stopped PID $pid"
    else
        echo "PID $pid tidak aktif"
    fi
    rm -f "$PID_FILE"
}

status_app() {
    if [ -f "$PID_FILE" ]; then
        pid="$(cat "$PID_FILE" 2>/dev/null || true)"
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            echo "running PID=$pid"
            return 0
        fi
    fi
    echo "stopped"
}

case "${1:-start}" in
    start) start_app ;;
    stop) stop_app ;;
    status) status_app ;;
    restart)
        stop_app
        start_app
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        echo "Unknown command: $1" >&2
        usage >&2
        exit 1
        ;;
esac
