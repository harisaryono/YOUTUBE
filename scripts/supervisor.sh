#!/bin/bash
# Wrapper script untuk aware supervisor pipeline

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
echo "🧭 YouTube Aware Supervisor"
echo "============================================="
echo "Run dir: $REPO_DIR"
echo "Python: $VENV_PYTHON"
echo "Coordinator: ${YT_PROVIDER_COORDINATOR_URL:-http://8.215.77.132:8788}"
echo "============================================="
echo ""

RUN_DIR_VALUE=""
ONCE_FLAG="0"
INTERVAL_SECONDS_VALUE="600"
DISCOVER_CHANNEL_LIMIT_VALUE="5"
DISCOVER_RECENT_PER_CHANNEL_VALUE="50"
TRANSCRIPT_LIMIT_VALUE="100"
AUDIO_LIMIT_VALUE="100"
ASR_LIMIT_VALUE="100"
RESUME_LIMIT_VALUE="100"
FORMAT_LIMIT_VALUE="100"
TRANSCRIPT_WORKERS_VALUE="10"
AUDIO_WORKERS_VALUE="2"
ASR_WORKERS_VALUE="2"
RESUME_WORKERS_VALUE="10"
FORMAT_WORKERS_VALUE="8"
CHANNEL_ID_VALUE=""
CHANNEL_NAME_VALUE=""
SKIP_DISCOVERY_FLAG="0"
SKIP_TRANSCRIPT_FLAG="0"
SKIP_AUDIO_FLAG="0"
SKIP_ASR_FLAG="0"
SKIP_RESUME_FLAG="0"
SKIP_FORMAT_FLAG="0"
DRY_RUN_FLAG="0"

while [[ $# -gt 0 ]]; do
    case $1 in
        --run-dir)
            RUN_DIR_VALUE="$2"
            shift 2
            ;;
        --once)
            ONCE_FLAG="1"
            shift
            ;;
        --interval-seconds)
            INTERVAL_SECONDS_VALUE="$2"
            shift 2
            ;;
        --discover-channel-limit)
            DISCOVER_CHANNEL_LIMIT_VALUE="$2"
            shift 2
            ;;
        --discover-recent-per-channel)
            DISCOVER_RECENT_PER_CHANNEL_VALUE="$2"
            shift 2
            ;;
        --transcript-limit)
            TRANSCRIPT_LIMIT_VALUE="$2"
            shift 2
            ;;
        --audio-limit)
            AUDIO_LIMIT_VALUE="$2"
            shift 2
            ;;
        --asr-limit)
            ASR_LIMIT_VALUE="$2"
            shift 2
            ;;
        --resume-limit)
            RESUME_LIMIT_VALUE="$2"
            shift 2
            ;;
        --format-limit)
            FORMAT_LIMIT_VALUE="$2"
            shift 2
            ;;
        --transcript-workers)
            TRANSCRIPT_WORKERS_VALUE="$2"
            shift 2
            ;;
        --audio-workers)
            AUDIO_WORKERS_VALUE="$2"
            shift 2
            ;;
        --asr-workers)
            ASR_WORKERS_VALUE="$2"
            shift 2
            ;;
        --resume-workers)
            RESUME_WORKERS_VALUE="$2"
            shift 2
            ;;
        --format-workers)
            FORMAT_WORKERS_VALUE="$2"
            shift 2
            ;;
        --channel-id)
            CHANNEL_ID_VALUE="$2"
            shift 2
            ;;
        --channel-name)
            CHANNEL_NAME_VALUE="$2"
            shift 2
            ;;
        --skip-discovery)
            SKIP_DISCOVERY_FLAG="1"
            shift
            ;;
        --skip-transcript)
            SKIP_TRANSCRIPT_FLAG="1"
            shift
            ;;
        --skip-audio)
            SKIP_AUDIO_FLAG="1"
            shift
            ;;
        --skip-asr)
            SKIP_ASR_FLAG="1"
            shift
            ;;
        --skip-resume)
            SKIP_RESUME_FLAG="1"
            shift
            ;;
        --skip-format)
            SKIP_FORMAT_FLAG="1"
            shift
            ;;
        --dry-run)
            DRY_RUN_FLAG="1"
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --run-dir PATH                 Custom run directory"
            echo "  --once                         Run one cycle then stop"
            echo "  --interval-seconds N           Sleep between cycles, default: 600"
            echo "  --discover-channel-limit N     Number of channels per discovery cycle"
            echo "  --discover-recent-per-channel N Window for latest-only discovery"
            echo "  --transcript-limit N           Limit transcript items per cycle"
            echo "  --audio-limit N                Limit audio warmup items per cycle"
            echo "  --asr-limit N                  Limit ASR items per cycle"
            echo "  --resume-limit N               Limit resume items per cycle"
            echo "  --format-limit N               Limit format items per cycle"
            echo "  --transcript-workers N         Transcript worker count"
            echo "  --audio-workers N              Audio warmup worker count"
            echo "  --asr-workers N                ASR worker count"
            echo "  --resume-workers N             Resume worker count"
            echo "  --format-workers N             Format worker count"
            echo "  --channel-id ID                Focus one channel"
            echo "  --channel-name NAME            Focus one channel by name"
            echo "  --skip-discovery               Skip discovery stage"
            echo "  --skip-transcript              Skip transcript stage"
            echo "  --skip-audio                   Skip audio warmup stage"
            echo "  --skip-asr                     Skip ASR stage"
            echo "  --skip-resume                  Skip resume stage"
            echo "  --skip-format                  Skip format stage"
            echo "  --dry-run                      Print commands only"
            echo "  --help                         Show this help message"
            exit 0
            ;;
        *)
            echo "⚠️  Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

if ! [[ "$INTERVAL_SECONDS_VALUE" =~ ^[0-9]+$ ]]; then
    echo "❌ --interval-seconds harus angka bulat" >&2
    exit 1
fi
for value_name in DISCOVER_CHANNEL_LIMIT_VALUE DISCOVER_RECENT_PER_CHANNEL_VALUE TRANSCRIPT_LIMIT_VALUE AUDIO_LIMIT_VALUE ASR_LIMIT_VALUE RESUME_LIMIT_VALUE FORMAT_LIMIT_VALUE TRANSCRIPT_WORKERS_VALUE AUDIO_WORKERS_VALUE ASR_WORKERS_VALUE RESUME_WORKERS_VALUE FORMAT_WORKERS_VALUE; do
    value="${!value_name}"
    if ! [[ "$value" =~ ^[0-9]+$ ]]; then
        echo "❌ $value_name harus angka bulat: $value" >&2
        exit 1
    fi
done

JOB_TYPE="supervisor"
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

CMD_ARGS=(
    "$VENV_PYTHON"
    "scripts/aware_supervisor.py"
    "--run-dir" "$JOB_RUN_DIR"
    "--interval-seconds" "$INTERVAL_SECONDS_VALUE"
    "--discover-channel-limit" "$DISCOVER_CHANNEL_LIMIT_VALUE"
    "--discover-recent-per-channel" "$DISCOVER_RECENT_PER_CHANNEL_VALUE"
    "--transcript-limit" "$TRANSCRIPT_LIMIT_VALUE"
    "--audio-limit" "$AUDIO_LIMIT_VALUE"
    "--asr-limit" "$ASR_LIMIT_VALUE"
    "--resume-limit" "$RESUME_LIMIT_VALUE"
    "--format-limit" "$FORMAT_LIMIT_VALUE"
    "--transcript-workers" "$TRANSCRIPT_WORKERS_VALUE"
    "--audio-workers" "$AUDIO_WORKERS_VALUE"
    "--asr-workers" "$ASR_WORKERS_VALUE"
    "--resume-workers" "$RESUME_WORKERS_VALUE"
    "--format-workers" "$FORMAT_WORKERS_VALUE"
)
if [ "$ONCE_FLAG" = "1" ]; then
    CMD_ARGS+=("--once")
fi
if [ -n "$CHANNEL_ID_VALUE" ]; then
    CMD_ARGS+=("--channel-id" "$CHANNEL_ID_VALUE")
fi
if [ -n "$CHANNEL_NAME_VALUE" ]; then
    CMD_ARGS+=("--channel-name" "$CHANNEL_NAME_VALUE")
fi
if [ "$SKIP_DISCOVERY_FLAG" = "1" ]; then
    CMD_ARGS+=("--skip-discovery")
fi
if [ "$SKIP_TRANSCRIPT_FLAG" = "1" ]; then
    CMD_ARGS+=("--skip-transcript")
fi
if [ "$SKIP_AUDIO_FLAG" = "1" ]; then
    CMD_ARGS+=("--skip-audio")
fi
if [ "$SKIP_ASR_FLAG" = "1" ]; then
    CMD_ARGS+=("--skip-asr")
fi
if [ "$SKIP_RESUME_FLAG" = "1" ]; then
    CMD_ARGS+=("--skip-resume")
fi
if [ "$SKIP_FORMAT_FLAG" = "1" ]; then
    CMD_ARGS+=("--skip-format")
fi
if [ "$DRY_RUN_FLAG" = "1" ]; then
    CMD_ARGS+=("--dry-run")
fi

CMD_STR="${CMD_ARGS[*]}"

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
if [ -n "$CHANNEL_ID_VALUE" ]; then
    TRACKER_START_ARGS+=(--target-channel-id "$CHANNEL_ID_VALUE")
    TRACKER_FINISH_ARGS+=(--target-channel-id "$CHANNEL_ID_VALUE")
fi

echo "🚀 Running: $CMD_STR"
echo ""

if ! "$VENV_PYTHON" "$TRACKER_PY" "${TRACKER_START_ARGS[@]}"; then
    echo "⚠️  Job tracker start failed, continuing without admin visibility" >&2
fi

echo "Job ID: $JOB_ID"
echo ""

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
    echo "✅ Supervisor completed successfully"
else
    echo "❌ Supervisor failed with exit code: $EXIT_CODE"
fi
echo "============================================="
exit $EXIT_CODE
