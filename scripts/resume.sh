#!/bin/bash
# Wrapper script untuk generate resume dari transcript video YouTube
# Script ini otomatis menggunakan .venv di repo ini

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
VENV_PYTHON="$("$SCRIPT_DIR/get_venv.sh")"
TRACKER_PY="$REPO_DIR/job_tracker.py"

# Check if virtualenv exists
if [ ! -x "$VENV_PYTHON" ]; then
    echo "❌ Virtualenv tidak ditemukan atau tidak bisa dieksekusi." >&2
    echo "Pastikan .env/EXTERNAL_VENV_DIR benar atau .venv ada." >&2
    exit 1
fi

cd "$REPO_DIR"

echo "============================================="
echo "📄 YouTube Resume Generation Tool"
echo "============================================="
echo "Run dir: $REPO_DIR"
echo "Python: $VENV_PYTHON"
echo "Coordinator: ${YT_PROVIDER_COORDINATOR_URL:-http://127.0.0.1:8788}"
echo "============================================="
echo ""

# Default arguments
TASKS_CSV_STR=""
LIMIT_VAL=""
RUN_DIR_VALUE=""
MODEL_STR=""
TARGET_VIDEO_ID=""
TARGET_CHANNEL_ID=""
TASKS_CSV_PATH=""
MAX_WORKERS_VALUE=""
NVIDIA_ONLY_VALUE=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --tasks-csv)
            TASKS_CSV_STR="$2"
            shift 2
            ;;
        --limit)
            LIMIT_VAL="$2"
            shift 2
            ;;
        --run-dir)
            RUN_DIR_VALUE="$2"
            shift 2
            ;;
        --model)
            MODEL_STR="$2"
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
        --max-workers)
            MAX_WORKERS_VALUE="$2"
            shift 2
            ;;
        --nvidia-only)
            NVIDIA_ONLY_VALUE="1"
            shift 1
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --tasks-csv FILE   CSV file containing video_id and channel_name columns"
            echo "  --limit N          Limit number of videos to process"
            echo "  --run-dir PATH     Custom run directory for output"
            echo "  --model MODEL      Model to use for resume generation"
            echo "  --video-id ID      Process single video by ID"
            echo "  --channel-id ID    Process missing summaries from one channel"
            echo "  --max-workers N    Limit coordinator accounts/workers used"
            echo "  --nvidia-only      Disable Groq and use Nvidia accounts only"
            echo "  --help             Show this help message"
            exit 0
            ;;
        *)
            echo "⚠️  Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

if [ -z "$MAX_WORKERS_VALUE" ]; then
    MAX_WORKERS_VALUE="12"
fi

if [ -n "$TASKS_CSV_STR" ] && { [ -n "$TARGET_VIDEO_ID" ] || [ -n "$TARGET_CHANNEL_ID" ]; }; then
    echo "❌ Error: --tasks-csv cannot be combined with --video-id or --channel-id" >&2
    exit 1
fi

JOB_TYPE="resume"
JOB_ID="${JOB_ID:-${JOB_TYPE}_$(date +%Y%m%d_%H%M%S)_$$}"
JOB_SOURCE="${JOB_SOURCE:-cli-wrapper}"
JOB_RUN_DIR="${JOB_RUN_DIR:-$RUN_DIR_VALUE}"
if [ -z "$JOB_RUN_DIR" ]; then
    JOB_RUN_DIR="runs/resume_${JOB_ID}"
fi
export JOB_ID JOB_SOURCE JOB_RUN_DIR
mkdir -p "$JOB_RUN_DIR"
JOB_LOG_PATH="${JOB_LOG_PATH:-$REPO_DIR/logs/${JOB_ID}.log}"
export JOB_LOG_PATH
mkdir -p "$(dirname "$JOB_LOG_PATH")"
exec >>"$JOB_LOG_PATH" 2>&1

if [ -z "$TASKS_CSV_STR" ] && { [ -n "$TARGET_VIDEO_ID" ] || [ -n "$TARGET_CHANNEL_ID" ]; }; then
    TASKS_CSV_PATH="$JOB_RUN_DIR/tasks.csv"
    export REPO_ROOT="$REPO_DIR"
    export TASKS_CSV_PATH
    export TARGET_VIDEO_ID
    export TARGET_CHANNEL_ID
    export LIMIT_NUM_VAL="${LIMIT_VAL:-0}"

    "$VENV_PYTHON" - <<'PY'
import csv
import os
import sqlite3
from pathlib import Path

from orchestrator.video_claims import active_video_claim_clause, claim_rows_by_query, release_claims

repo_root = Path(os.environ["REPO_ROOT"])
db_path = repo_root / "youtube_transcripts.db"
tasks_csv = Path(os.environ["TASKS_CSV_PATH"])
video_id = str(os.environ.get("TARGET_VIDEO_ID", "")).strip()
channel_id = str(os.environ.get("TARGET_CHANNEL_ID", "")).strip()
limit_raw = str(os.environ.get("LIMIT_NUM_VAL", "0")).strip()
limit_value = int(limit_raw) if limit_raw else 0
claim_owner = str(os.environ.get("JOB_ID") or "").strip()
if not claim_owner:
    raise SystemExit("missing JOB_ID for resume claim owner")

tasks_csv.parent.mkdir(parents=True, exist_ok=True)
params = []

if video_id:
    select_sql = f"""
        SELECT v.id, v.video_id, c.channel_name
        FROM videos v
        JOIN channels c ON c.id = v.channel_id
        WHERE v.video_id = ?
          AND v.transcript_downloaded = 1
          AND COALESCE(v.summary_file_path, '') = ''
          AND {active_video_claim_clause('v')}
        ORDER BY c.channel_name ASC, v.id DESC
    """
    params.append(video_id)
else:
    select_sql = f"""
        SELECT v.id, v.video_id, c.channel_name
        FROM videos v
        JOIN channels c ON c.id = v.channel_id
        WHERE v.transcript_downloaded = 1
          AND COALESCE(v.summary_file_path, '') = ''
          AND {active_video_claim_clause('v')}
    """
    if not channel_id:
        raise SystemExit("missing channel_id")
    select_sql += " AND (c.channel_id = ? OR c.channel_id = ?)"
    params.extend([channel_id, channel_id.lstrip("@")])
    select_sql += " ORDER BY c.channel_name ASC, v.id DESC"
    if limit_value > 0:
        select_sql += " LIMIT ?"
        params.append(limit_value)

con = sqlite3.connect(str(db_path))
con.row_factory = sqlite3.Row
try:
    rows = claim_rows_by_query(
        con,
        select_sql=select_sql,
        params=params,
        owner=claim_owner,
        stage="resume",
        ttl_seconds=4 * 60 * 60,
    )
finally:
    con.close()

with tasks_csv.open("w", encoding="utf-8", newline="") as fp:
    writer = csv.DictWriter(fp, fieldnames=["video_id", "channel_name"])
    writer.writeheader()
    for row in rows:
        writer.writerow({"video_id": row["video_id"], "channel_name": row["channel_name"]})
print(len(rows))
PY
    TASKS_CSV_STR="$TASKS_CSV_PATH"
fi

# Build array for safer execution
CMD_ARGS=("$VENV_PYTHON" "launch_resume_queue.py")
if [ -n "$TASKS_CSV_STR" ]; then CMD_ARGS+=("--tasks-csv" "$TASKS_CSV_STR"); fi
if [ -n "$LIMIT_VAL" ]; then CMD_ARGS+=("--limit" "$LIMIT_VAL"); fi
if [ -n "$RUN_DIR_VALUE" ]; then CMD_ARGS+=("--run-dir" "$RUN_DIR_VALUE"); fi
if [ -n "$MODEL_STR" ]; then CMD_ARGS+=("--model" "$MODEL_STR"); fi
if [ -n "$MAX_WORKERS_VALUE" ]; then CMD_ARGS+=("--max-workers" "$MAX_WORKERS_VALUE"); fi
if [ -n "$NVIDIA_ONLY_VALUE" ]; then CMD_ARGS+=("--nvidia-only"); fi

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
    echo "✅ Resume generation completed successfully"
else
    echo "❌ Resume generation failed with exit code: $EXIT_CODE"
fi
echo "============================================="

echo "📦 Releasing resume claims..."
"$VENV_PYTHON" - <<'PY' || true
import os

from database_optimized import OptimizedDatabase
from orchestrator.video_claims import release_claims

job_id = str(os.environ.get("JOB_ID") or "").strip()
if job_id:
    db = OptimizedDatabase("youtube_transcripts.db", "uploads")
    try:
        released = release_claims(db.conn, owner=job_id)
        print(f"released_claims={released}")
    finally:
        db.close()
PY

# Auto-import results to DB
echo "📥 Importing pending updates to database..."
"$VENV_PYTHON" "$REPO_DIR/partial_py/import_pending_updates.py"

exit $EXIT_CODE
