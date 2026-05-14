#!/bin/bash
# Wrapper script untuk discovery video baru dari channel YouTube
# Script ini otomatis menggunakan .venv di repo ini

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
VENV_PYTHON="$("$SCRIPT_DIR/get_venv.sh")"
TRACKER_PY="$REPO_DIR/job_tracker.py"

# Check if virtualenv exists
if [ ! -x "$VENV_PYTHON" ]; then
    echo "❌ Virtualenv tidak ditemukan atau tidak bisa dieksekusi." >&2
    echo "Pastikan .env EXTERNAL_VENV_DIR benar atau .venv ada." >&2
    exit 1
fi

cd "$REPO_DIR"

echo "============================================="
echo "🔍 YouTube Discovery Tool"
echo "============================================="
echo "Run dir: $REPO_DIR"
echo "Python: $VENV_PYTHON"
echo "Coordinator: ${YT_PROVIDER_COORDINATOR_URL:-http://127.0.0.1:8788}"
echo "============================================="
echo ""

# Default arguments
SCAN_MODE=""
CHANNEL_LIMIT_VAL=""
RECENT_PER_CHANNEL_VAL="50"
DISCOVERY_ONLY="--discovery-only"
RUN_DIR_VALUE=""
TARGET_CHANNEL_ID=""
TARGET_CHANNEL_NAME=""
RATE_LIMIT_SAFE_VALUE="1"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --scan-all-missing  Scan full history and pick all missing/incomplete videos"
            echo "  --latest-only       Scan only latest N videos per channel (default: 50)"
            echo "  --recent-per-channel N  Number of latest videos per channel when using --latest-only"
            echo "  --channel-limit N   Limit number of channels to scan (0 = all channels)"
            echo "  --run-dir PATH      Custom run directory for output"
            echo "  --full-pipeline     Run full pipeline (discovery + transcript + resume), not discovery-only"
            echo "  --channel-id ID     Scan specific channel by ID"
            echo "  --channel-name NAME Scan specific channel by name"
            echo "  --rate-limit-safe   Skip upload-date fallback and slow down between channels (default on)"
            echo "  --help              Show this help message"
            exit 0
            ;;
        --scan-all-missing)
            SCAN_MODE="--scan-all-missing"
            shift
            ;;
        --latest-only)
            SCAN_MODE=""
            if [ -z "$RECENT_PER_CHANNEL_VAL" ]; then
                RECENT_PER_CHANNEL_VAL="50"
            fi
            shift
            ;;
        --channel-limit)
            CHANNEL_LIMIT_VAL="$2"
            shift 2
            ;;
        --recent-per-channel)
            RECENT_PER_CHANNEL_VAL="$2"
            shift 2
            ;;
        --run-dir)
            RUN_DIR_VALUE="$2"
            shift 2
            ;;
        --full-pipeline)
            DISCOVERY_ONLY=""
            shift
            ;;
        --channel-id)
            TARGET_CHANNEL_ID="$2"
            shift 2
            ;;
        --channel-name)
            TARGET_CHANNEL_NAME="$2"
            shift 2
            ;;
        --rate-limit-safe)
            RATE_LIMIT_SAFE_VALUE="1"
            shift
            ;;
        *)
            echo "⚠️  Unknown option: $1" >&2
            echo "Usage: $0 [OPTIONS]" >&2
            exit 1
            ;;
    esac
done

# Build command array
CMD_ARGS=("$VENV_PYTHON" "update_latest_channel_videos.py")
if [ -n "$SCAN_MODE" ]; then CMD_ARGS+=("$SCAN_MODE"); fi
if [ -n "$DISCOVERY_ONLY" ]; then CMD_ARGS+=("$DISCOVERY_ONLY"); fi
if [ -n "$CHANNEL_LIMIT_VAL" ]; then CMD_ARGS+=("--channel-limit" "$CHANNEL_LIMIT_VAL"); fi
if [ -n "$RECENT_PER_CHANNEL_VAL" ]; then CMD_ARGS+=("--recent-per-channel" "$RECENT_PER_CHANNEL_VAL"); fi
if [ -n "$RUN_DIR_VALUE" ]; then CMD_ARGS+=("--run-dir" "$RUN_DIR_VALUE"); fi
if [ -n "$TARGET_CHANNEL_ID" ]; then CMD_ARGS+=("--channel-id" "$TARGET_CHANNEL_ID"); fi
if [ -n "$TARGET_CHANNEL_NAME" ]; then CMD_ARGS+=("--channel-name" "$TARGET_CHANNEL_NAME"); fi
if [ "$RATE_LIMIT_SAFE_VALUE" = "1" ]; then CMD_ARGS+=("--rate-limit-safe"); fi

CMD_STR="${CMD_ARGS[*]}"

JOB_TYPE="discover"
JOB_ID="${JOB_ID:-${JOB_TYPE}_$(date +%Y%m%d_%H%M%S)_$$}"
JOB_SOURCE="${JOB_SOURCE:-cli-wrapper}"
JOB_RUN_DIR="${JOB_RUN_DIR:-$RUN_DIR_VALUE}"
JOB_LOG_PATH="${JOB_LOG_PATH:-$REPO_DIR/logs/${JOB_ID}.log}"
mkdir -p "$(dirname "$JOB_LOG_PATH")"
exec >>"$JOB_LOG_PATH" 2>&1

if [ "$RATE_LIMIT_SAFE_VALUE" = "1" ]; then
    export YT_DISCOVERY_SKIP_UPLOAD_DATE_LOOKUP=1
    export YT_DISCOVERY_CHANNEL_DELAY_SECONDS=2
fi

TRACKER_START_ARGS=(
    start
    --job-id "$JOB_ID"
    --job-type "$JOB_TYPE"
    --status running
    --source "$JOB_SOURCE"
    --command "$CMD_STR"
    --log-path "$JOB_LOG_PATH"
    --run-dir "$JOB_RUN_DIR"
    --pid "$$"
)
TRACKER_FINISH_ARGS=(
    finish
    --job-id "$JOB_ID"
    --job-type "$JOB_TYPE"
    --source "$JOB_SOURCE"
    --command "$CMD_STR"
    --log-path "$JOB_LOG_PATH"
    --run-dir "$JOB_RUN_DIR"
    --pid "$$"
)
if [ -n "$TARGET_CHANNEL_ID" ]; then
    TRACKER_START_ARGS+=(--target-channel-id "$TARGET_CHANNEL_ID")
    TRACKER_FINISH_ARGS+=(--target-channel-id "$TARGET_CHANNEL_ID")
fi

echo "🚀 Running: $CMD_STR"
echo ""

if ! "$VENV_PYTHON" "$TRACKER_PY" "${TRACKER_START_ARGS[@]}"; then
    echo "⚠️  Job tracker start failed, continuing without admin visibility" >&2
fi

echo "Job ID: $JOB_ID"
echo ""

# Execute
set +e
"${CMD_ARGS[@]}"
EXIT_CODE=$?
set -e

JOB_STATUS="completed"
if [ $EXIT_CODE -ne 0 ]; then
    JOB_STATUS="failed"
fi

TRACKER_FINISH_ARGS+=(--status "$JOB_STATUS" --exit-code "$EXIT_CODE")
if ! "$VENV_PYTHON" "$TRACKER_PY" "${TRACKER_FINISH_ARGS[@]}"; then
    echo "⚠️  Job tracker finish failed" >&2
fi

echo ""
echo "============================================="
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Discovery completed successfully"
else
    echo "❌ Discovery failed with exit code: $EXIT_CODE"
fi
echo "============================================="

# Auto-import results to DB
echo "📥 Importing pending updates to database..."
"$VENV_PYTHON" "$REPO_DIR/partial_py/import_pending_updates.py"

exit $EXIT_CODE
