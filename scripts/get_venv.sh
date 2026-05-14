#!/bin/bash
# Helper script to resolve the Python executable from the external virtual
# environment defined in the .env file. The repo-local .venv has been removed.

set -euo pipefail

# Get the project root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

# Fallback to current directory if not in scripts/
if [[ "$SCRIPT_DIR" != *"/scripts" ]]; then
    REPO_DIR="$SCRIPT_DIR"
fi

# Load .env file if it exists
ENV_FILE="$REPO_DIR/.env"
EXTERNAL_VENV_DIR=""
DEFAULT_VENV_DIR="$REPO_DIR/.venv"

if [ -f "$ENV_FILE" ]; then
    # Parse EXTERNAL_VENV_DIR from .env
    EXTERNAL_VENV_DIR=$(grep "^EXTERNAL_VENV_DIR=" "$ENV_FILE" | cut -d'=' -f2- | tr -d '"'\'' ')
fi

VENV_DIR="${EXTERNAL_VENV_DIR:-$DEFAULT_VENV_DIR}"

if [ ! -d "$VENV_DIR" ]; then
    echo "Virtualenv tidak ditemukan di $VENV_DIR" >&2
    exit 1
fi

# Determine Python executable
VENV_PYTHON="$VENV_DIR/bin/python"

# If python doesn't exist, try python3
if [ ! -x "$VENV_PYTHON" ] && [ -x "$VENV_DIR/bin/python3" ]; then
    VENV_PYTHON="$VENV_DIR/bin/python3"
fi

# Output the path
echo "$VENV_PYTHON"
