#!/bin/bash
# Compatibility wrapper for the dedicated audio download stage.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec "$SCRIPT_DIR/audio.sh" "$@"
