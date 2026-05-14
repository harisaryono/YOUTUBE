#!/bin/bash
# Wrapper script untuk format transcript video YouTube
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
echo "📝 YouTube Transcript Formatter Tool"
echo "============================================="
echo "Run dir: $REPO_DIR"
echo "Python: $VENV_PYTHON"
echo "Coordinator: ${YT_PROVIDER_COORDINATOR_URL:-http://127.0.0.1:8788}"
echo "============================================="
echo ""

LIMIT_VALUE=0
RUN_DIR_VALUE=""
VIDEO_ID_VALUE=""
CHANNEL_ID_VALUE=""
PROVIDER_PLAN_VAL="nvidia_only"
WORKERS_VALUE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --limit)
            LIMIT_VALUE="$2"
            shift 2
            ;;
        --run-dir)
            RUN_DIR_VALUE="$2"
            shift 2
            ;;
        --video-id)
            VIDEO_ID_VALUE="$2"
            shift 2
            ;;
        --channel-id)
            CHANNEL_ID_VALUE="$2"
            shift 2
            ;;
        --provider-plan)
            PROVIDER_PLAN_VAL="$2"
            shift 2
            ;;
        --workers)
            WORKERS_VALUE="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --limit N            Limit number of transcripts to format"
            echo "  --run-dir PATH       Custom run directory for output"
            echo "  --video-id ID        Format specific video by ID"
            echo "  --channel-id ID      Format transcripts from specific channel"
            echo "  --provider-plan PLAN Provider plan (nvidia_only, groq_only, etc.)"
            echo "  --workers N          Number of parallel formatting workers"
            echo "  --help               Show this help message"
            exit 0
            ;;
        *)
            echo "⚠️  Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

if [ -z "$WORKERS_VALUE" ]; then
    WORKERS_VALUE="1"
fi

JOB_TYPE="format"
JOB_ID="${JOB_ID:-${JOB_TYPE}_$(date +%Y%m%d_%H%M%S)_$$}"
JOB_SOURCE="${JOB_SOURCE:-cli-wrapper}"
JOB_RUN_DIR="${JOB_RUN_DIR:-$RUN_DIR_VALUE}"
if [ -z "$JOB_RUN_DIR" ]; then
    JOB_RUN_DIR="runs/format_${JOB_ID}"
fi
mkdir -p "$JOB_RUN_DIR"
JOB_LOG_PATH="${JOB_LOG_PATH:-$REPO_DIR/logs/${JOB_ID}.log}"
mkdir -p "$(dirname "$JOB_LOG_PATH")"
exec >>"$JOB_LOG_PATH" 2>&1

TASKS_CSV_PATH="$JOB_RUN_DIR/tasks.csv"
RESULTS_CSV_PATH="$JOB_RUN_DIR/results.csv"

export REPO_ROOT="$REPO_DIR"
export TASKS_CSV_PATH
export VIDEO_ID_VALUE
export CHANNEL_ID_VALUE
export LIMIT_VALUE

"$VENV_PYTHON" - <<'PY'
import csv
import os
import sqlite3
from pathlib import Path

from database_optimized import OptimizedDatabase

repo_root = Path(os.environ["REPO_ROOT"])
db_path = repo_root / "youtube_transcripts.db"
tasks_csv = Path(os.environ["TASKS_CSV_PATH"])
video_id_filter = str(os.environ.get("VIDEO_ID_VALUE", "")).strip()
channel_filter = str(os.environ.get("CHANNEL_ID_VALUE", "")).strip()
limit_raw = str(os.environ.get("LIMIT_VALUE", "0")).strip()
limit_value = int(limit_raw) if limit_raw else 0

query = """
    SELECT v.id, v.video_id, c.channel_id AS channel_slug, v.title
    FROM videos v
    JOIN channels c ON c.id = v.channel_id
    WHERE v.transcript_downloaded = 1
      AND COALESCE(v.transcript_formatted_path, '') = ''
"""
params = []
if video_id_filter:
    query += " AND v.video_id = ?"
    params.append(video_id_filter)
elif channel_filter:
    query += " AND (c.channel_id = ? OR c.channel_id = ?)"
    params.extend([channel_filter, channel_filter.lstrip("@")])
query += " ORDER BY v.id ASC"
if limit_value > 0:
    query += " LIMIT ?"
    params.append(limit_value)

con = sqlite3.connect(str(db_path))
con.row_factory = sqlite3.Row
try:
    rows = con.execute(query, params).fetchall()
finally:
    con.close()

db = OptimizedDatabase(str(db_path))

tasks_csv.parent.mkdir(parents=True, exist_ok=True)
written = 0
with tasks_csv.open("w", encoding="utf-8", newline="") as fp:
    writer = csv.DictWriter(fp, fieldnames=["id", "video_id", "channel_slug", "title", "transcript_file_path", "transcript_text"])
    writer.writeheader()
    for row in rows:
        transcript_text = str(db.read_transcript(str(row["video_id"])) or "").strip()
        writer.writerow(
            {
                "id": row["id"],
                "video_id": row["video_id"],
                "channel_slug": row["channel_slug"],
                "title": row["title"],
                "transcript_file_path": "",
                "transcript_text": transcript_text,
            }
        )
        written += 1

try:
    db.close()
except Exception:
    pass

if written == 0:
    raise SystemExit("no format tasks found")
print(written)
PY

# Build array for safer execution
CMD_ARGS=("$VENV_PYTHON" "format_transcripts_pool.py" "--tasks-csv" "$TASKS_CSV_PATH" "--results-csv" "$RESULTS_CSV_PATH" "--provider-plan" "$PROVIDER_PLAN_VAL")
if [ "$LIMIT_VALUE" -gt 0 ]; then
    CMD_ARGS+=("--limit" "$LIMIT_VALUE")
fi
if [ -n "$WORKERS_VALUE" ]; then
    CMD_ARGS+=("--workers" "$WORKERS_VALUE")
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
if [ -n "$VIDEO_ID_VALUE" ]; then
    TRACKER_START_ARGS+=(--target-video-id "$VIDEO_ID_VALUE")
    TRACKER_FINISH_ARGS+=(--target-video-id "$VIDEO_ID_VALUE")
fi
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
    echo "✅ Transcript formatting completed successfully"
else
    echo "❌ Transcript formatting failed with exit code: $EXIT_CODE"
fi
echo "============================================="

# Auto-import results to DB
echo "📥 Importing pending updates to database..."
IMPORT_SCRIPT="$REPO_DIR/partial_py/import_pending_updates.py"
if [ -f "$IMPORT_SCRIPT" ]; then
    "$VENV_PYTHON" "$IMPORT_SCRIPT"
else
    echo "⚠️  import_pending_updates.py not found at $IMPORT_SCRIPT" >&2
fi

exit $EXIT_CODE
