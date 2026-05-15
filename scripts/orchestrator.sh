#!/bin/bash
# Orchestrator Shell Wrapper
# Usage:
#   ./scripts/orchestrator.sh once          # Run one cycle
#   ./scripts/orchestrator.sh run           # Run continuously
#   ./scripts/orchestrator.sh status        # Show latest status
#   ./scripts/orchestrator.sh active        # Show active jobs
#   ./scripts/orchestrator.sh logs --job-id X --tail 100
#   ./scripts/orchestrator.sh cancel --job-id X [--force]
#   ./scripts/orchestrator.sh cancel-stage transcript
#   ./scripts/orchestrator.sh cancel-group youtube
#   ./scripts/orchestrator.sh reconcile     # Reconcile stale running jobs
#   ./scripts/orchestrator.sh explain       # Explain why jobs are/aren't running
#   ./scripts/orchestrator.sh validate      # Validate config + AI_CONTEXT
#   ./scripts/orchestrator.sh doctor        # Diagnose backlog, cooldown, and failures
#   ./scripts/orchestrator.sh pause-stage transcript --minutes 60
#   ./scripts/orchestrator.sh resume-stage transcript
#   ./scripts/orchestrator.sh pause-group youtube --minutes 60
#   ./scripts/orchestrator.sh retry-failed --stage transcript --dry-run
#   ./scripts/orchestrator.sh quarantine-channel UCxxxx
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

        "$VENV_PYTHON" -m orchestrator.daemon run "$@" &
        CHILD_PID=$!
        echo "$CHILD_PID" > "$PID_FILE"

        cleanup() {
            if [ -n "${CHILD_PID:-}" ] && kill -0 "$CHILD_PID" 2>/dev/null; then
                kill "$CHILD_PID" 2>/dev/null || true
                sleep 1
                if kill -0 "$CHILD_PID" 2>/dev/null; then
                    kill -9 "$CHILD_PID" 2>/dev/null || true
                fi
            fi
            rm -f "$PID_FILE"
            echo "Orchestrator stopped."
        }
        trap cleanup EXIT INT TERM HUP

        wait "$CHILD_PID"

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

    active)
        exec "$VENV_PYTHON" -m orchestrator.daemon active "$@"
        ;;

    logs)
        exec "$VENV_PYTHON" -m orchestrator.daemon logs "$@"
        ;;

    cancel)
        exec "$VENV_PYTHON" -m orchestrator.daemon cancel "$@"
        ;;

    cancel-stage)
        STAGE_NAME="${1:-}"
        if [ -z "$STAGE_NAME" ]; then
            echo "Usage: $0 cancel-stage <stage> [--force] [--grace-seconds N]" >&2
            exit 1
        fi
        shift
        exec "$VENV_PYTHON" -m orchestrator.daemon cancel --stage "$STAGE_NAME" "$@"
        ;;

    cancel-group)
        GROUP_NAME="${1:-}"
        if [ -z "$GROUP_NAME" ]; then
            echo "Usage: $0 cancel-group <group> [--force] [--grace-seconds N]" >&2
            exit 1
        fi
        shift
        exec "$VENV_PYTHON" -m orchestrator.daemon cancel --group "$GROUP_NAME" "$@"
        ;;

    reconcile)
        exec "$VENV_PYTHON" -m orchestrator.daemon reconcile "$@"
        ;;

    validate)
        exec "$VENV_PYTHON" -m orchestrator.validate "$@"
        ;;

    doctor)
        exec "$VENV_PYTHON" -m orchestrator.doctor "$@"
        ;;

    pause-stage)
        exec "$VENV_PYTHON" -m orchestrator.actions pause-stage "$@"
        ;;

    resume-stage)
        exec "$VENV_PYTHON" -m orchestrator.actions resume-stage "$@"
        ;;

    pause-group)
        exec "$VENV_PYTHON" -m orchestrator.actions pause-group "$@"
        ;;

    resume-group)
        exec "$VENV_PYTHON" -m orchestrator.actions resume-group "$@"
        ;;

    retry-failed)
        exec "$VENV_PYTHON" -m orchestrator.actions retry-failed "$@"
        ;;

    quarantine-channel)
        exec "$VENV_PYTHON" -m orchestrator.actions quarantine-channel "$@"
        ;;

    unquarantine-channel)
        exec "$VENV_PYTHON" -m orchestrator.actions unquarantine-channel "$@"
        ;;

    explain)
        exec "$VENV_PYTHON" -m orchestrator.daemon explain "$@"
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
        echo "Usage: $0 {once|run|status|active|logs|cancel|cancel-stage|cancel-group|reconcile|validate|doctor|pause-stage|resume-stage|pause-group|resume-group|retry-failed|quarantine-channel|unquarantine-channel|explain|report|stop} [options]"
        echo ""
        echo "Commands:"
        echo "  once              Run one orchestrator cycle and exit"
        echo "  run               Run orchestrator continuously"
        echo "  status            Show daemon status and latest report"
        echo "  active            Show active jobs"
        echo "  logs              Tail a job log by job id"
        echo "  cancel            Cancel one or more running jobs"
        echo "  cancel-stage      Cancel all running jobs in a stage"
        echo "  cancel-group      Cancel all running jobs in a group"
        echo "  reconcile         Reconcile stale running jobs"
        echo "  validate          Validate config + AI_CONTEXT"
        echo "  doctor            Diagnose backlog, cooldown, and failures"
        echo "  pause-stage       Pause a stage for a number of minutes"
        echo "  resume-stage      Resume a paused stage"
        echo "  pause-group       Pause a control group"
        echo "  resume-group      Resume a paused control group"
        echo "  retry-failed      Show retry candidates for failed jobs"
        echo "  quarantine-channel Quarantine a channel"
        echo "  unquarantine-channel Release a channel from quarantine"
        echo "  explain           Explain current work inventory and blockers"
        echo "  report            Show latest report as JSON"
        echo "  stop              Stop running daemon"
        echo ""
        echo "Options:"
        echo "  --max-jobs N      Maximum jobs per cycle (0 uses config default)"
        echo "  --profile MODE    Override profile: safe|normal|fast"
        echo "  --config PATH     Path to orchestrator.yaml"
        exit 1
        ;;
esac
