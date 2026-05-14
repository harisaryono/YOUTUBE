#!/bin/bash
# Orchestrator Shell Wrapper
# Usage:
#   ./scripts/orchestrator.sh once          # Run one cycle
#   ./scripts/orchestrator.sh run           # Run continuously
#   ./scripts/orchestrator.sh status        # Show latest status
#   ./scripts/orchestrator.sh report        # Show latest report JSON
#   ./scripts/orchestrator.sh stop          # Stop running daemon (via PID file)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
VENV_PYTHON="$("$SCRIPT_DIR/get_venv.sh")"
PID_FILE="/tmp/orchestrator_daemon.pid"

# Check if virtualenv exists
if [ ! -x "$VENV_PYTHON" ]; then
    echo "❌ Virtualenv tidak ditemukan atau tidak bisa dieksekusi." >&2
    echo "Pastikan .env/EXTERNAL_VENV_DIR benar atau .venv ada." >&2
    exit 1
fi

cd "$REPO_DIR"

# Ensure yaml is installed
"$VENV_PYTHON" -c "import yaml" 2>/dev/null || {
    echo "📦 Installing PyYAML..."
    "$VENV_PYTHON" -m pip install pyyaml -q
}

# Use PYTHONPATH so relative imports in orchestrator/ work
export PYTHONPATH="$REPO_DIR${PYTHONPATH:+:$PYTHONPATH}"

MODE="${1:-once}"
shift 2>/dev/null || true

case "$MODE" in
    once)
        echo "============================================="
        echo "🎯 Orchestrator — One Cycle"
        echo "============================================="
        echo "Run dir: $REPO_DIR"
        echo "Python: $VENV_PYTHON"
        echo "============================================="
        echo ""
        exec "$VENV_PYTHON" -m orchestrator.daemon once "$@"
        ;;

    run)
        echo "============================================="
        echo "🔄 Orchestrator — Continuous Mode"
        echo "============================================="
        echo "Run dir: $REPO_DIR"
        echo "Python: $VENV_PYTHON"
        echo "PID file: $PID_FILE"
        echo "============================================="
        echo ""

        # Write PID file
        echo $$ > "$PID_FILE"
        trap 'rm -f "$PID_FILE"; echo "Orchestrator stopped."' EXIT

        exec "$VENV_PYTHON" -m orchestrator.daemon run "$@"
        ;;

    status)
        echo "============================================="
        echo "📊 Orchestrator Status"
        echo "============================================="
        if [ -f "$PID_FILE" ]; then
            PID=$(cat "$PID_FILE")
            if kill -0 "$PID" 2>/dev/null; then
                echo "✅ Daemon running (PID: $PID)"
            else
                echo "⚠️  PID file exists but process not running (stale PID: $PID)"
                rm -f "$PID_FILE"
            fi
        else
            echo "ℹ️  Daemon not running"
        fi
        echo ""
        exec "$VENV_PYTHON" -m orchestrator.daemon status "$@"
        ;;

    report)
        exec "$VENV_PYTHON" -m orchestrator.daemon report "$@"
        ;;


    stop)
        echo "============================================="
        echo "⏹️  Orchestrator Stop"
        echo "============================================="
        if [ -f "$PID_FILE" ]; then
            PID=$(cat "$PID_FILE")
            if kill -0 "$PID" 2>/dev/null; then
                echo "Stopping daemon (PID: $PID)..."
                kill "$PID"
                sleep 2
                if kill -0 "$PID" 2>/dev/null; then
                    echo "Force stopping..."
                    kill -9 "$PID" 2>/dev/null || true
                fi
                rm -f "$PID_FILE"
                echo "✅ Daemon stopped"
            else
                echo "⚠️  Process not running, cleaning up PID file"
                rm -f "$PID_FILE"
            fi
        else
            echo "ℹ️  No PID file found (daemon not running)"
        fi
        ;;

    *)
        echo "Usage: $0 {once|run|status|report|stop} [options]"
        echo ""
        echo "Commands:"
        echo "  once              Run one orchestrator cycle and exit"
        echo "  run               Run orchestrator continuously"
        echo "  status            Show daemon status and latest report"
        echo "  report            Show latest report as JSON"
        echo "  stop              Stop running daemon"
        echo ""
        echo "Options:"
        echo "  --max-jobs N      Maximum jobs per cycle (default: 5)"
        echo "  --profile MODE    Override profile: safe|normal|fast"
        echo "  --config PATH     Path to orchestrator.yaml"
        exit 1
        ;;
esac
