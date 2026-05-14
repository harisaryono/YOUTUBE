#!/bin/bash
# Wrapper script untuk download transcript dari video YouTube
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
echo "📝 YouTube Transcript Recovery Tool"
echo "============================================="
echo "Run dir: $REPO_DIR"
echo "Python: $VENV_PYTHON"
echo "Coordinator: ${YT_PROVIDER_COORDINATOR_URL:-http://127.0.0.1:8788}"
echo "============================================="
echo ""

# Default arguments
CSV_FILE=""
LIMIT_VALUE=0
RUN_DIR_VALUE=""
TARGET_VIDEO_ID=""
TARGET_CHANNEL_ID=""
WORKERS_VALUE="1"
WEBSHARE_ONLY_VALUE="0"
WEBSHARE_FIRST_VALUE="0"
AUDIT_NO_SUBTITLE_VALUE="0"
RATE_LIMIT_SAFE_VALUE="0"

# Parse arguments
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
        --workers)
            WORKERS_VALUE="$2"
            shift 2
            ;;
        --webshare-only)
            WEBSHARE_ONLY_VALUE="1"
            shift
            ;;
        --webshare-first)
            WEBSHARE_FIRST_VALUE="1"
            shift
            ;;
        --audit-no-subtitle)
            AUDIT_NO_SUBTITLE_VALUE="1"
            shift
            ;;
        --rate-limit-safe)
            RATE_LIMIT_SAFE_VALUE="1"
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --csv FILE      CSV file containing video_id column"
            echo "  --limit N       Limit number of videos to process when using --csv/--channel-id/global pending queue"
            echo "  --run-dir PATH  Custom run directory for output"
            echo "  --video-id ID   Process single video by ID"
            echo "  --channel-id ID Process missing transcripts from one channel"
            echo "  --workers N     Run N parallel transcript workers (default: 1)"
            echo "  --webshare-first Prioritize Webshare before SaveSubs/API/yt-dlp"
            echo "  --webshare-only Use Webshare proxy path only; skip SaveSubs/API/yt-dlp"
            echo "  --audit-no-subtitle Recheck no_subtitle rows and requeue ones with captions"
            echo "  --rate-limit-safe Enable pacing, cap workers, and skip expensive fallback"
            echo "  --help          Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0 --csv tasks.csv --run-dir runs/transcript_batch"
            echo "  $0 --video-id dQw4w9WgXcQ"
            echo "  $0 --channel-id UC1234567890 --limit 200"
            echo "  $0 --workers 20 --run-dir runs/transcript_parallel"
            echo "  $0 --webshare-first --audit-no-subtitle --workers 20 --limit 50"
            echo "  $0 --webshare-only --limit 50"
            echo "  $0 --limit 200"
            exit 0
            ;;
        *)
            echo "⚠️  Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

# Validate arguments
if [ -n "$CSV_FILE" ] && { [ -n "$TARGET_VIDEO_ID" ] || [ -n "$TARGET_CHANNEL_ID" ]; }; then
    echo "❌ Error: --csv cannot be combined with --video-id or --channel-id" >&2
    exit 1
fi
if [ -n "$TARGET_VIDEO_ID" ] && [ -n "$TARGET_CHANNEL_ID" ]; then
    echo "❌ Error: choose only one of --video-id or --channel-id" >&2
    exit 1
fi
if ! [[ "$WORKERS_VALUE" =~ ^[0-9]+$ ]]; then
    echo "❌ Error: --workers must be a positive integer" >&2
    exit 1
fi
if [ "$WORKERS_VALUE" -lt 1 ]; then
    WORKERS_VALUE="1"
fi
# Note: --csv is already mutually exclusive with --video-id/--channel-id (validated above),
# so --webshare-only + --csv + --video-id cannot happen simultaneously.
# This check is kept for safety in case future changes alter the validation order.
if [ "$WEBSHARE_ONLY_VALUE" = "1" ] && [ -n "$CSV_FILE" ] && [ -n "$TARGET_VIDEO_ID" ]; then
    echo "❌ Error: --webshare-only cannot be combined with --csv and --video-id at the same time" >&2
    exit 1
fi
if [ "$RATE_LIMIT_SAFE_VALUE" = "1" ] && [ "$WORKERS_VALUE" -gt 2 ]; then
    WORKERS_VALUE="2"
fi

JOB_TYPE="transcript"
JOB_ID="${JOB_ID:-${JOB_TYPE}_$(date +%Y%m%d_%H%M%S)_$$}"
JOB_SOURCE="${JOB_SOURCE:-cli-wrapper}"
JOB_RUN_DIR="${JOB_RUN_DIR:-$RUN_DIR_VALUE}"
if [ -z "$JOB_RUN_DIR" ]; then
    JOB_RUN_DIR="runs/transcript_${JOB_ID}"
fi
export JOB_ID JOB_SOURCE JOB_RUN_DIR
mkdir -p "$JOB_RUN_DIR"
JOB_LOG_PATH="${JOB_LOG_PATH:-$REPO_DIR/logs/${JOB_ID}.log}"
export JOB_LOG_PATH
mkdir -p "$(dirname "$JOB_LOG_PATH")"
exec >>"$JOB_LOG_PATH" 2>&1

if [ "$WEBSHARE_ONLY_VALUE" = "1" ]; then
    export YT_TRANSCRIPT_WEBSHARE_ONLY=1
fi
if [ "$WEBSHARE_FIRST_VALUE" = "1" ]; then
    export YT_TRANSCRIPT_WEBSHARE_FIRST=1
fi
if [ "$AUDIT_NO_SUBTITLE_VALUE" = "1" ]; then
    export YT_TRANSCRIPT_AUDIT_NO_SUBTITLE=1
fi
if [ "$RATE_LIMIT_SAFE_VALUE" = "1" ]; then
    export YT_TRANSCRIPT_DISABLE_INTER_VIDEO_PACING=0
    export YT_TRANSCRIPT_INTER_VIDEO_DELAY_MIN=8
    export YT_TRANSCRIPT_INTER_VIDEO_DELAY_MAX=15
    export YT_TRANSCRIPT_SKIP_EXPENSIVE_FALLBACK=1
    export YT_TRANSCRIPT_MAX_CONSECUTIVE_HARD_BLOCKS=3
    unset YT_TRANSCRIPT_WEBSHARE_FIRST
fi

TASKS_CSV_PATH=""
if [ -z "$CSV_FILE" ]; then
    TASKS_CSV_PATH="$JOB_RUN_DIR/tasks.csv"
    export REPO_ROOT="$REPO_DIR"
    export TASKS_CSV_PATH
    export TARGET_VIDEO_ID
    export TARGET_CHANNEL_ID
    export LIMIT_VALUE
    export YT_TRANSCRIPT_WEBSHARE_ONLY
    export YT_TRANSCRIPT_WEBSHARE_FIRST
    export YT_TRANSCRIPT_SKIP_EXPENSIVE_FALLBACK
    export YT_TRANSCRIPT_AUDIT_NO_SUBTITLE
    export JOB_RUN_DIR
    AUDIT_REPORT_PATH="$JOB_RUN_DIR/audit_no_subtitle_report.csv"
    export AUDIT_REPORT_PATH

"$VENV_PYTHON" - <<'PY'
import csv
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from orchestrator.video_claims import active_video_claim_clause, claim_rows_by_query
from recover_transcripts import TranscriptRecoverer

repo_root = Path(os.environ["REPO_ROOT"])
db_path = repo_root / "youtube_transcripts.db"
tasks_csv = Path(os.environ["TASKS_CSV_PATH"])
video_id = str(os.environ.get("TARGET_VIDEO_ID", "")).strip()
channel_id = str(os.environ.get("TARGET_CHANNEL_ID", "")).strip()
limit_raw = str(os.environ.get("LIMIT_VALUE", "0")).strip()
limit_value = int(limit_raw) if limit_raw else 0
audit_enabled = str(os.environ.get("YT_TRANSCRIPT_AUDIT_NO_SUBTITLE", "0")).strip().lower() in {"1", "true", "yes", "on"}
audit_report = Path(os.environ.get("AUDIT_REPORT_PATH", str(tasks_csv.parent / "audit_no_subtitle_report.csv")))

tasks_csv.parent.mkdir(parents=True, exist_ok=True)

params: list[object] = []
claim_owner = str(os.environ.get("JOB_ID") or "").strip()
if not claim_owner:
    raise SystemExit("missing JOB_ID for transcript claim owner")

if video_id:
    select_sql = f"""
        SELECT v.id, v.video_id
        FROM videos v
        WHERE v.video_id = ?
          AND COALESCE(v.transcript_downloaded, 0) = 0
          AND (v.transcript_language IS NULL OR v.transcript_language != 'no_subtitle')
          AND COALESCE(v.is_short, 0) = 0
          AND COALESCE(v.is_member_only, 0) = 0
          AND {active_video_claim_clause('v')}
        ORDER BY v.created_at DESC, v.id DESC
    """
    params.append(video_id)
else:
    select_sql = f"""
        SELECT v.id, v.video_id
        FROM videos v
        LEFT JOIN channels c ON c.id = v.channel_id
        WHERE COALESCE(v.transcript_downloaded, 0) = 0
          AND (v.transcript_language IS NULL OR v.transcript_language != 'no_subtitle')
          AND COALESCE(v.is_short, 0) = 0
          AND COALESCE(v.is_member_only, 0) = 0
          AND {active_video_claim_clause('v')}
    """
    if channel_id:
        select_sql += " AND (c.channel_id = ? OR c.channel_id = ?)"
        params.extend([channel_id, channel_id.lstrip("@")])
    select_sql += " ORDER BY v.created_at DESC, v.id DESC"
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
        stage="transcript",
        ttl_seconds=4 * 60 * 60,
    )

    if not rows and audit_enabled and not video_id:
        recoverer = TranscriptRecoverer()
        audit_rows = con.execute(
            """
            SELECT v.video_id, c.channel_name
            FROM videos v
            JOIN channels c ON c.id = v.channel_id
            WHERE COALESCE(v.transcript_downloaded, 0) = 0
              AND COALESCE(v.transcript_language, '') = 'no_subtitle'
            ORDER BY v.created_at DESC
            """
        ).fetchall()

        requeued = 0
        audit_report.parent.mkdir(parents=True, exist_ok=True)
        with audit_report.open("w", encoding="utf-8", newline="") as audit_fp:
            audit_writer = csv.DictWriter(
                audit_fp,
                fieldnames=[
                    "video_id",
                    "channel_name",
                    "inventory_state",
                    "action",
                    "detail",
                ],
            )
            audit_writer.writeheader()
            total_audit = len(audit_rows)
            audit_workers = max(1, min(20, total_audit))

            def check_inventory(audit_row):
                video_id_audit = str(audit_row["video_id"])
                channel_name_audit = str(audit_row["channel_name"])
                state, inventory, detail = recoverer._yt_dlp_subtitle_inventory(video_id_audit)
                return (
                    video_id_audit,
                    channel_name_audit,
                    state,
                    detail,
                )

            with ThreadPoolExecutor(max_workers=audit_workers) as executor:
                future_map = {
                    executor.submit(check_inventory, audit_row): audit_row
                    for audit_row in audit_rows
                }
                for idx, future in enumerate(as_completed(future_map), start=1):
                    video_id_audit, channel_name_audit, state, detail = future.result()
                    action = "keep_no_subtitle"
                    if state == "available":
                        con.execute(
                            """
                            UPDATE videos
                            SET transcript_language = NULL,
                                transcript_retry_reason = NULL,
                                transcript_retry_after = NULL,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE video_id = ?
                            """,
                            (video_id_audit,),
                        )
                        requeued += 1
                        action = "requeued_for_transcript"
                    audit_writer.writerow(
                        {
                            "video_id": video_id_audit,
                            "channel_name": channel_name_audit,
                            "inventory_state": state,
                            "action": action,
                            "detail": str(detail)[:500],
                            }
                        )
                    if idx % 100 == 0 or action == "requeued_for_transcript":
                        print(
                            f"audit_progress={idx}/{total_audit} requeued={requeued}",
                            flush=True,
                        )
                    if idx % 25 == 0:
                        con.commit()
                        print(
                            f"audit_commit={idx}/{total_audit} requeued={requeued}",
                            flush=True,
                        )
        con.commit()
        rows = con.execute(query, params).fetchall()
        print(f"audit_requeued={requeued}")
finally:
    con.close()

if not rows:
    if video_id:
        raise SystemExit(f"video_id not found or not eligible: {video_id}")
    with tasks_csv.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=["video_id"])
        writer.writeheader()
    print("0")
    raise SystemExit(0)

with tasks_csv.open("w", encoding="utf-8", newline="") as fp:
    writer = csv.DictWriter(fp, fieldnames=["video_id"])
    writer.writeheader()
    for row in rows:
        writer.writerow({"video_id": row["video_id"]})

print(len(rows))
PY
    CSV_FILE="$TASKS_CSV_PATH"
fi
if [ -n "$CSV_FILE" ] && [ -z "$TASKS_CSV_PATH" ]; then
    TASKS_CSV_PATH="$CSV_FILE"
fi

WORKER_COUNT="$WORKERS_VALUE"
if [ "$WORKER_COUNT" -gt 1 ]; then
    WORKER_ROOT="$JOB_RUN_DIR/workers"
    mkdir -p "$WORKER_ROOT"
    export REPO_ROOT="$REPO_DIR"
    export TASKS_CSV_PATH
    export WORKER_ROOT
    export WORKER_COUNT
    export JOB_RUN_DIR
    export YT_TRANSCRIPT_WEBSHARE_ONLY
    export YT_TRANSCRIPT_WEBSHARE_FIRST

    "$VENV_PYTHON" - <<'PY'
import csv
import os
from pathlib import Path

tasks_csv = Path(os.environ["TASKS_CSV_PATH"])
worker_root = Path(os.environ["WORKER_ROOT"])
worker_count = max(1, int(os.environ["WORKER_COUNT"]))

with tasks_csv.open("r", encoding="utf-8", newline="") as f:
    rows = list(csv.DictReader(f))

worker_rows = [[] for _ in range(worker_count)]
for idx, row in enumerate(rows):
    worker_rows[idx % worker_count].append(row)

for i, shard_rows in enumerate(worker_rows, start=1):
    shard_path = worker_root / f"shard_{i:02d}.csv"
    shard_path.parent.mkdir(parents=True, exist_ok=True)
    with shard_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["video_id"])
        writer.writeheader()
        for row in shard_rows:
            video_id = str(row.get("video_id") or "").strip()
            if not video_id:
                continue
            writer.writerow({"video_id": video_id})
    print(f"{shard_path} {len(shard_rows)}")
PY

    echo "🚀 Running parallel transcript workers: $WORKER_COUNT"
    PIDS=()
    WORKER_EXIT=0
    for i in $(seq 1 "$WORKER_COUNT"); do
        shard_id="$(printf '%02d' "$i")"
        shard_csv="$WORKER_ROOT/shard_${shard_id}.csv"
        if [ ! -s "$shard_csv" ]; then
            continue
        fi
        shard_run_dir="$WORKER_ROOT/worker_${shard_id}"
        mkdir -p "$shard_run_dir"
        worker_stdout="$shard_run_dir/stdout.log"
        worker_log="$shard_run_dir/transcript.log"
        env \
            PYTHONUNBUFFERED=1 \
            YT_TRANSCRIPT_WEBSHARE_ONLY="${YT_TRANSCRIPT_WEBSHARE_ONLY:-}" \
            YT_TRANSCRIPT_WEBSHARE_FIRST="${YT_TRANSCRIPT_WEBSHARE_FIRST:-}" \
            YT_TRANSCRIPT_SKIP_EXPENSIVE_FALLBACK="${YT_TRANSCRIPT_SKIP_EXPENSIVE_FALLBACK:-}" \
            YT_TRANSCRIPT_AUDIT_NO_SUBTITLE="${YT_TRANSCRIPT_AUDIT_NO_SUBTITLE:-}" \
            YT_TRANSCRIPT_LOG_FILE="$worker_log" \
            "$VENV_PYTHON" recover_transcripts_from_csv.py --csv "$shard_csv" --run-dir "$shard_run_dir" \
            >"$worker_stdout" 2>&1 &
        PIDS+=("$!")
        echo "  worker $shard_id started for $(basename "$shard_csv")"
    done

    for pid in "${PIDS[@]}"; do
        if ! wait "$pid"; then
            WORKER_EXIT=1
        fi
    done

    "$VENV_PYTHON" - <<'PY'
import csv
import os
from pathlib import Path

run_dir = Path(os.environ["JOB_RUN_DIR"])
worker_root = run_dir / "workers"
report_paths = sorted(worker_root.glob("worker_*/recover_report.csv"))
retry_paths = sorted(worker_root.glob("worker_*/retry_later.csv"))

out_report = run_dir / "recover_report.csv"
out_retry = run_dir / "retry_later.csv"

if report_paths:
    with out_report.open("w", encoding="utf-8", newline="") as out_f:
        writer = None
        for path in report_paths:
            with path.open("r", encoding="utf-8", newline="") as in_f:
                reader = csv.reader(in_f)
                try:
                    header = next(reader)
                except StopIteration:
                    continue
                if writer is None:
                    writer = csv.writer(out_f)
                    writer.writerow(header)
                for row in reader:
                    writer.writerow(row)

if retry_paths:
    with out_retry.open("w", encoding="utf-8", newline="") as out_f:
        writer = None
        for path in retry_paths:
            with path.open("r", encoding="utf-8", newline="") as in_f:
                reader = csv.reader(in_f)
                try:
                    header = next(reader)
                except StopIteration:
                    continue
                if writer is None:
                    writer = csv.writer(out_f)
                    writer.writerow(header)
                for row in reader:
                    writer.writerow(row)
PY

    CMD_STR="recover_transcripts_from_csv.py parallel-workers=$WORKER_COUNT run_dir=$JOB_RUN_DIR"
    JOB_STATUS="completed"
    [ $WORKER_EXIT -ne 0 ] && JOB_STATUS="failed"

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
    if ! "$VENV_PYTHON" "$TRACKER_PY" "${TRACKER_START_ARGS[@]}"; then
        echo "⚠️  Job tracker start failed, continuing without admin visibility" >&2
    fi
    TRACKER_FINISH_ARGS+=(--status "$JOB_STATUS" --exit-code "$WORKER_EXIT")
    if ! "$VENV_PYTHON" "$TRACKER_PY" "${TRACKER_FINISH_ARGS[@]}"; then
        echo "⚠️  Job tracker finish failed" >&2
    fi

    echo ""
    echo "============================================="
    if [ $WORKER_EXIT -eq 0 ]; then
        echo "✅ Parallel transcript completed successfully"
    else
        echo "❌ Parallel transcript failed with exit code: $WORKER_EXIT"
    fi
    echo "============================================="
    echo "📦 Releasing transcript claims..."
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
    exit $WORKER_EXIT
fi

# Build array for safer execution
CMD_ARGS=("$VENV_PYTHON" "recover_transcripts_from_csv.py" "--csv" "$CSV_FILE" "--run-dir" "$JOB_RUN_DIR")
if [ -n "$LIMIT_VALUE" ] && [ "$LIMIT_VALUE" -gt 0 ] && [ -n "$CSV_FILE" ] && [ -z "$TARGET_VIDEO_ID" ] && [ -z "$TARGET_CHANNEL_ID" ]; then
    CMD_ARGS+=("--limit" "$LIMIT_VALUE")
fi

# For command tracking, we convert array to a single string
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
    echo "✅ Transcript recovery completed successfully"
else
    echo "❌ Transcript recovery failed with exit code: $EXIT_CODE"
fi
echo "============================================="

# Auto-import results to DB
echo "📦 Releasing transcript claims..."
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

echo "📥 Importing pending updates to database..."
"$VENV_PYTHON" "$REPO_DIR/partial_py/import_pending_updates.py"

exit $EXIT_CODE
