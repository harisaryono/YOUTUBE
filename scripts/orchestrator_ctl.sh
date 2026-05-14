#!/bin/bash
# Control wrapper for orchestrator pause/resume commands.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
VENV_PYTHON="$("$SCRIPT_DIR/get_venv.sh")"

if [ ! -x "$VENV_PYTHON" ]; then
    echo "❌ Virtualenv tidak ditemukan atau tidak bisa dieksekusi." >&2
    echo "Pastikan .env/EXTERNAL_VENV_DIR benar atau .venv ada." >&2
    exit 1
fi

cd "$REPO_DIR"

MODE="${1:-}"
TARGET="${2:-}"

case "$MODE" in
    pause|resume)
        shift 2 || true
        if [ -z "$TARGET" ]; then
            echo "❌ Usage: $0 {pause|resume} <target> [--reason TEXT]" >&2
            exit 1
        fi
        exec "$VENV_PYTHON" -m orchestrator.daemon "$MODE" --target "$TARGET" "$@"
        ;;
    preflight)
        shift 1 || true
        exec "$VENV_PYTHON" -m orchestrator.preflight "$@"
        ;;
    status|explain|report|janitor)
        shift 1 || true
        exec "$VENV_PYTHON" -m orchestrator.daemon "$MODE" "$@"
        ;;
    *)
        echo "Usage: $0 {pause|resume|preflight|status|explain|report|janitor} [target] [options]" >&2
        exit 1
        ;;
esac
