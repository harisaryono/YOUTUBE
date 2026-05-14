#!/bin/bash
# Wrapper script untuk stage audio_download YouTube via yt-dlp download-only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
VENV_PYTHON="$("$SCRIPT_DIR/get_venv.sh")"
TRACKER_PY="$REPO_DIR/job_tracker.py"

if [ ! -x "$VENV_PYTHON" ]; then
    echo "❌ Virtualenv tidak ditemukan atau tidak bisa dieksekusi." >&2
    echo "Pastikan .env/EXTERNAL_VENV_DIR benar atau .venv ada." >&2
    exit 1
fi

cd "$REPO_DIR"

echo "============================================="
echo "🎵 YouTube Audio Download Tool"
echo "============================================="
echo "Run dir: $REPO_DIR"
echo "Python: $VENV_PYTHON"
echo "Coordinator: ${YT_PROVIDER_COORDINATOR_URL:-http://127.0.0.1:8788}"
echo "============================================="
echo ""

CSV_FILE=""
LIMIT_VALUE=0
RUN_DIR_VALUE=""
TARGET_VIDEO_ID=""
TARGET_CHANNEL_ID=""
PROVIDERS_VALUE="groq,nvidia"
MODEL_VALUE="whisper-large-v3"
LANGUAGE_VALUE="multi"
CHUNK_SECONDS_VALUE="45"
OVERLAP_SECONDS_VALUE="2"
VIDEO_WORKERS_VALUE="1"
RATE_LIMIT_SAFE_VALUE="0"

while [[ $# -gt 0 ]]; do
    case $1 in
        --csv)
            CSV_FILE="$2"
            shift 2
            ;;
        --limit)
            LIMIT_VALUE="$2"
            shift 2
            ;;
        --run-dir)
            RUN_DIR_VALUE="$2"
            shift 2
            ;;
        --video-id)
            TARGET_VIDEO_ID="$2"
            shift 2
            ;;
        --channel-id)
            TARGET_CHANNEL_ID="$2"
            shift 2
            ;;
        --providers)
            PROVIDERS_VALUE="$2"
            shift 2
            ;;
        --model)
            MODEL_VALUE="$2"
            shift 2
            ;;
        --language)
            LANGUAGE_VALUE="$2"
            shift 2
            ;;
        --chunk-seconds)
            CHUNK_SECONDS_VALUE="$2"
            shift 2
            ;;
        --overlap-seconds)
            OVERLAP_SECONDS_VALUE="$2"
            shift 2
            ;;
        --video-workers|--workers)
            VIDEO_WORKERS_VALUE="$2"
            shift 2
            ;;
        --rate-limit-safe)
            RATE_LIMIT_SAFE_VALUE="1"
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --csv FILE            CSV file containing video_id column"
            echo "  --limit N             Limit number of videos to process when using selection mode"
            echo "  --run-dir PATH        Custom run directory for output"
            echo "  --video-id ID         Warm cache for a single video"
            echo "  --channel-id ID       Warm cache for one channel"
            echo "  --providers LIST      Provider order, default: groq,nvidia"
            echo "  --model NAME          Default Whisper model for Groq, default: whisper-large-v3"
            echo "  --language CODE       Language hint, default: multi"
            echo "  --chunk-seconds N     Chunk duration in seconds, default: 45"
            echo "  --overlap-seconds N   Chunk overlap in seconds, default: 2"
            echo "  --workers N           Parallel download workers, default: 1"
            echo "  --rate-limit-safe     Enable safer yt-dlp pacing"
            echo "  --help                Show this help message"
            exit 0
            ;;
        *)
            echo "⚠️  Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

if [ -n "$CSV_FILE" ] && { [ -n "$TARGET_VIDEO_ID" ] || [ -n "$TARGET_CHANNEL_ID" ]; }; then
    echo "❌ Error: --csv cannot be combined with --video-id or --channel-id" >&2
    exit 1
fi
if [ -n "$TARGET_VIDEO_ID" ] && [ -n "$TARGET_CHANNEL_ID" ]; then
    echo "❌ Error: choose only one of --video-id or --channel-id" >&2
    exit 1
fi
if ! [[ "$LIMIT_VALUE" =~ ^[0-9]+$ ]]; then
    echo "❌ Error: --limit must be a non-negative integer" >&2
    exit 1
fi
if ! [[ "$CHUNK_SECONDS_VALUE" =~ ^[0-9]+$ ]]; then
    echo "❌ Error: --chunk-seconds must be a positive integer" >&2
    exit 1
fi
if ! [[ "$OVERLAP_SECONDS_VALUE" =~ ^[0-9]+$ ]]; then
    echo "❌ Error: --overlap-seconds must be a non-negative integer" >&2
    exit 1
fi
if ! [[ "$VIDEO_WORKERS_VALUE" =~ ^[0-9]+$ ]]; then
    echo "❌ Error: --video-workers must be a non-negative integer" >&2
    exit 1
fi

JOB_TYPE="audio_download"
JOB_ID="${JOB_ID:-${JOB_TYPE}_$(date +%Y%m%d_%H%M%S)_$$}"
JOB_SOURCE="${JOB_SOURCE:-cli-wrapper}"
JOB_RUN_DIR="${JOB_RUN_DIR:-$RUN_DIR_VALUE}"
if [ -z "$JOB_RUN_DIR" ]; then
    JOB_RUN_DIR="runs/${JOB_ID}"
fi
mkdir -p "$JOB_RUN_DIR"
JOB_LOG_PATH="${JOB_LOG_PATH:-$REPO_DIR/logs/${JOB_ID}.log}"
mkdir -p "$(dirname "$JOB_LOG_PATH")"
exec >>"$JOB_LOG_PATH" 2>&1

export ASR_AUDIO_DIR="${ASR_AUDIO_DIR:-$REPO_DIR/uploads/audio}"
if [ "$RATE_LIMIT_SAFE_VALUE" = "1" ]; then
    export YT_ASR_RATE_LIMIT_SAFE=1
fi

CMD_ARGS=(
    "recover_asr_transcripts.py"
    "--download-only"
    "--run-dir" "$JOB_RUN_DIR"
    "--providers" "$PROVIDERS_VALUE"
    "--model" "$MODEL_VALUE"
    "--language" "$LANGUAGE_VALUE"
    "--chunk-seconds" "$CHUNK_SECONDS_VALUE"
    "--overlap-seconds" "$OVERLAP_SECONDS_VALUE"
    "--video-workers" "$VIDEO_WORKERS_VALUE"
)
if [ -n "$CSV_FILE" ]; then
    CMD_ARGS+=("--csv" "$CSV_FILE")
fi
if [ -n "$TARGET_VIDEO_ID" ]; then
    CMD_ARGS+=("--video-id" "$TARGET_VIDEO_ID")
fi
if [ -n "$TARGET_CHANNEL_ID" ]; then
    CMD_ARGS+=("--channel-id" "$TARGET_CHANNEL_ID")
fi
if [ "$LIMIT_VALUE" -gt 0 ]; then
    CMD_ARGS+=("--limit" "$LIMIT_VALUE")
fi

CMD_STR="$VENV_PYTHON ${CMD_ARGS[*]}"

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
if [ -n "$TARGET_VIDEO_ID" ]; then
    TRACKER_START_ARGS+=(--target-video-id "$TARGET_VIDEO_ID")
    TRACKER_FINISH_ARGS+=(--target-video-id "$TARGET_VIDEO_ID")
fi
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

set +e
"$VENV_PYTHON" "${CMD_ARGS[@]}"
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
    echo "✅ Audio cache warmup completed successfully"
else
    echo "❌ Audio cache warmup failed with exit code: $EXIT_CODE"
fi
echo "============================================="
exit $EXIT_CODE
