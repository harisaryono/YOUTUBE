#!/bin/bash
# Preflight checks for orchestrator runtime.

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

exec "$VENV_PYTHON" -m orchestrator.preflight "$@"
