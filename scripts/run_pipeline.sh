#!/bin/bash
set -euo pipefail

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do
    DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
    SOURCE="$(readlink "$SOURCE")"
    [[ "$SOURCE" != /* ]] && SOURCE="$DIR/$SOURCE"
done
SCRIPT_DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_PYTHON="$("$REPO_DIR/scripts/get_venv.sh")"
DB_PATH="$REPO_DIR/youtube_transcripts.db"

if [ ! -x "$VENV_PYTHON" ]; then
    echo "Virtualenv tidak ditemukan atau tidak bisa dieksekusi: $VENV_PYTHON" >&2
    exit 1
fi

cd "$REPO_DIR"

JOB_ID="${JOB_ID:-pipeline_$(date +%Y%m%d_%H%M%S)_$$}"
RUN_DIR_VALUE=""
TARGET_CHANNEL_ID=""
TARGET_CHANNEL_NAME=""
ALL_CHANNELS_VALUE="1"
PIPELINE_MODE=""
SKIP_DISCOVERY_VALUE="0"
SKIP_TRANSCRIPT_VALUE="0"
SKIP_RESUME_VALUE="0"
SKIP_FORMAT_VALUE="0"
DISCOVER_RATE_LIMIT_SAFE_VALUE="1"
TRANSCRIPT_WEBSHARE_FIRST_VALUE="0"
TRANSCRIPT_WEBSHARE_ONLY_VALUE="0"
TRANSCRIPT_RATE_LIMIT_SAFE_VALUE="0"
TRANSCRIPT_WORKERS_VALUE="10"
RESUME_WORKERS_VALUE="10"
RESUME_MODEL_VALUE="openai/gpt-oss-120b"
RESUME_NVIDIA_ONLY_VALUE="1"
FORMAT_WORKERS_VALUE="8"
FORMAT_PROVIDER_PLAN_VALUE="nvidia_only"
TRANSCRIPT_LIMIT_VALUE="0"
RESUME_LIMIT_VALUE="0"
FORMAT_LIMIT_VALUE="0"
DISCOVER_STRATEGY="auto"
DISCOVER_RECENT_PER_CHANNEL_VALUE="50"
DISCOVER_CHANNEL_LIMIT_VALUE=""

usage() {
    cat >&2 <<'EOF'
Penggunaan:
  ./run_pipeline.sh [--all-channels]
  ./run_pipeline.sh --channel-id <CHANNEL_ID>
  ./run_pipeline.sh --channel-name <CHANNEL_NAME>

Opsi utama:
  --run-dir <PATH>                 Direktori run utama
  --discovery-only                 Jalankan discovery saja
  --transcript-only                Jalankan transcript saja
  --resume-only                    Jalankan resume saja
  --format-only                    Jalankan format saja
  --skip-discovery                 Lewati fase discovery
  --skip-transcript                Lewati fase transcript
  --skip-resume                    Lewati fase resume
  --skip-format                    Lewati fase format

  --transcript-workers <N>         Default 10
  --transcript-limit <N>           Batasi item transcript
  --transcript-webshare-first      Prioritaskan Webshare dulu
  --transcript-webshare-only       Paksa jalur Webshare saja
  --transcript-rate-limit-safe     Pacing lebih konservatif

  --resume-workers <N>             Default 10
  --resume-limit <N>               Batasi item resume
  --resume-model <MODEL>           Default openai/gpt-oss-120b
  --resume-any-provider            Izinkan Groq dan provider lain

  --format-workers <N>             Default 8
  --format-limit <N>               Batasi item format
  --format-provider-plan <PLAN>    Default nvidia_only

  --discover-latest-only           Discovery jendela terbaru
  --discover-scan-all-missing      Discovery seluruh riwayat channel
  --discover-auto                  Discovery otomatis berdasarkan status backlog per channel
  --discover-recent-per-channel <N>
  --discover-channel-limit <N>
  --discover-rate-limit-safe       Default on

  --channel-id <ID>                Filter satu channel
  --channel-name <NAME>            Filter satu channel by nama
  --all-channels                   Paksa tanpa filter channel
  --help                           Tampilkan bantuan
EOF
}

resolve_channel_id() {
    local channel_name="$1"
    if [ -z "$channel_name" ]; then
        return 0
    fi
    "$VENV_PYTHON" - "$DB_PATH" "$channel_name" <<'PY'
import sqlite3
import sys

db_path = sys.argv[1]
channel_name = str(sys.argv[2]).strip()
con = sqlite3.connect(db_path)
con.row_factory = sqlite3.Row
try:
    row = con.execute(
        """
        SELECT channel_id, channel_name
        FROM channels
        WHERE channel_name = ?
           OR channel_id = ?
           OR channel_id = ?
        ORDER BY id ASC
        LIMIT 1
        """,
        (channel_name, channel_name, channel_name.lstrip("@")),
    ).fetchone()
finally:
    con.close()

if row:
    print(row["channel_id"])
PY
}

run_stage() {
    local stage_label="$1"
    shift
    echo "[${stage_label}] $*"
    "$@"
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --run-dir)
            RUN_DIR_VALUE="${2:-}"
            shift 2
            ;;
        --channel-id)
            TARGET_CHANNEL_ID="${2:-}"
            ALL_CHANNELS_VALUE="0"
            shift 2
            ;;
        --channel-name)
            TARGET_CHANNEL_NAME="${2:-}"
            ALL_CHANNELS_VALUE="0"
            shift 2
            ;;
        --all-channels)
            ALL_CHANNELS_VALUE="1"
            TARGET_CHANNEL_ID=""
            TARGET_CHANNEL_NAME=""
            shift
            ;;
        --discovery-only)
            PIPELINE_MODE="discovery-only"
            shift
            ;;
        --transcript-only)
            PIPELINE_MODE="transcript-only"
            shift
            ;;
        --resume-only)
            PIPELINE_MODE="resume-only"
            shift
            ;;
        --format-only)
            PIPELINE_MODE="format-only"
            shift
            ;;
        --skip-discovery)
            SKIP_DISCOVERY_VALUE="1"
            shift
            ;;
        --skip-transcript)
            SKIP_TRANSCRIPT_VALUE="1"
            shift
            ;;
        --skip-resume)
            SKIP_RESUME_VALUE="1"
            shift
            ;;
        --skip-format)
            SKIP_FORMAT_VALUE="1"
            shift
            ;;
        --discover-rate-limit-safe)
            DISCOVER_RATE_LIMIT_SAFE_VALUE="1"
            shift
            ;;
        --discover-latest-only)
            DISCOVER_STRATEGY="latest_only"
            shift
            ;;
        --discover-scan-all-missing)
            DISCOVER_STRATEGY="all_missing"
            shift
            ;;
        --discover-auto)
            DISCOVER_STRATEGY="auto"
            shift
            ;;
        --discover-recent-per-channel)
            DISCOVER_RECENT_PER_CHANNEL_VALUE="${2:-}"
            shift 2
            ;;
        --discover-channel-limit)
            DISCOVER_CHANNEL_LIMIT_VALUE="${2:-}"
            shift 2
            ;;
        --transcript-workers)
            TRANSCRIPT_WORKERS_VALUE="${2:-}"
            shift 2
            ;;
        --transcript-limit)
            TRANSCRIPT_LIMIT_VALUE="${2:-}"
            shift 2
            ;;
        --transcript-webshare-first)
            TRANSCRIPT_WEBSHARE_FIRST_VALUE="1"
            shift
            ;;
        --transcript-webshare-only)
            TRANSCRIPT_WEBSHARE_ONLY_VALUE="1"
            shift
            ;;
        --transcript-rate-limit-safe)
            TRANSCRIPT_RATE_LIMIT_SAFE_VALUE="1"
            shift
            ;;
        --resume-workers)
            RESUME_WORKERS_VALUE="${2:-}"
            shift 2
            ;;
        --resume-limit)
            RESUME_LIMIT_VALUE="${2:-}"
            shift 2
            ;;
        --resume-model)
            RESUME_MODEL_VALUE="${2:-}"
            shift 2
            ;;
        --resume-any-provider)
            RESUME_NVIDIA_ONLY_VALUE="0"
            shift
            ;;
        --format-workers)
            FORMAT_WORKERS_VALUE="${2:-}"
            shift 2
            ;;
        --format-limit)
            FORMAT_LIMIT_VALUE="${2:-}"
            shift 2
            ;;
        --format-provider-plan)
            FORMAT_PROVIDER_PLAN_VALUE="${2:-}"
            shift 2
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            echo "⚠️  Unknown option: $1" >&2
            usage
            exit 1
            ;;
    esac
done

MODE_FLAGS=0
for candidate in discovery-only transcript-only resume-only format-only; do
    if [ "$PIPELINE_MODE" = "$candidate" ]; then
        MODE_FLAGS=$((MODE_FLAGS + 1))
    fi
done
if [ "$MODE_FLAGS" -gt 1 ]; then
    echo "❌ Pilih hanya satu mode: discovery-only, transcript-only, resume-only, atau format-only" >&2
    exit 1
fi

case "$PIPELINE_MODE" in
    discovery-only)
        SKIP_TRANSCRIPT_VALUE="1"
        SKIP_RESUME_VALUE="1"
        SKIP_FORMAT_VALUE="1"
        ;;
    transcript-only)
        SKIP_DISCOVERY_VALUE="1"
        SKIP_RESUME_VALUE="1"
        SKIP_FORMAT_VALUE="1"
        ;;
    resume-only)
        SKIP_DISCOVERY_VALUE="1"
        SKIP_TRANSCRIPT_VALUE="1"
        SKIP_FORMAT_VALUE="1"
        ;;
    format-only)
        SKIP_DISCOVERY_VALUE="1"
        SKIP_TRANSCRIPT_VALUE="1"
        SKIP_RESUME_VALUE="1"
        ;;
esac

if [ -n "$TARGET_CHANNEL_ID" ] && [ -n "$TARGET_CHANNEL_NAME" ]; then
    echo "❌ Pilih salah satu antara --channel-id atau --channel-name" >&2
    exit 1
fi

for value_name in TRANSCRIPT_LIMIT_VALUE RESUME_LIMIT_VALUE FORMAT_LIMIT_VALUE TRANSCRIPT_WORKERS_VALUE RESUME_WORKERS_VALUE FORMAT_WORKERS_VALUE; do
    value="${!value_name}"
    if ! [[ "$value" =~ ^[0-9]+$ ]]; then
        echo "❌ $value_name harus angka bulat: $value" >&2
        exit 1
    fi
done

if [ -z "$RUN_DIR_VALUE" ]; then
    RUN_DIR_VALUE="$REPO_DIR/runs/pipeline_${JOB_ID}"
fi

mkdir -p "$RUN_DIR_VALUE"
LOG_FILE="$RUN_DIR_VALUE/run.log"
exec >>"$LOG_FILE" 2>&1

echo "============================================="
echo "YouTube phase pipeline"
echo "Job ID   : $JOB_ID"
echo "Run dir  : $RUN_DIR_VALUE"
echo "Python   : $VENV_PYTHON"
echo "DB       : $DB_PATH"
echo "Channel  : ${TARGET_CHANNEL_ID:-${TARGET_CHANNEL_NAME:-<all channels>}}"
echo "Mode     : ${PIPELINE_MODE:-full}"
echo "Discover : ${DISCOVER_STRATEGY}"
echo "============================================="

RESOLVED_CHANNEL_ID="${TARGET_CHANNEL_ID:-}"
if [ -z "$RESOLVED_CHANNEL_ID" ] && [ -n "$TARGET_CHANNEL_NAME" ]; then
    RESOLVED_CHANNEL_ID="$(resolve_channel_id "$TARGET_CHANNEL_NAME" || true)"
fi

DISCOVERY_DIR="$RUN_DIR_VALUE/01_discovery"
TRANSCRIPT_DIR="$RUN_DIR_VALUE/02_transcript"
RESUME_DIR="$RUN_DIR_VALUE/03_resume"
FORMAT_DIR="$RUN_DIR_VALUE/04_format"
mkdir -p "$DISCOVERY_DIR" "$TRANSCRIPT_DIR" "$RESUME_DIR" "$FORMAT_DIR"

generate_discovery_plan() {
    local plan_path="$1"
    "$VENV_PYTHON" - "$DB_PATH" "$plan_path" "$DISCOVER_STRATEGY" "$DISCOVER_CHANNEL_LIMIT_VALUE" "${RESOLVED_CHANNEL_ID:-}" "${TARGET_CHANNEL_NAME:-}" <<'PY'
import sqlite3
import sys
from pathlib import Path

db_path = Path(sys.argv[1])
plan_path = Path(sys.argv[2])
strategy = str(sys.argv[3] or "auto").strip() or "auto"
channel_limit_raw = str(sys.argv[4] or "0").strip()
channel_limit = int(channel_limit_raw) if channel_limit_raw else 0
target_channel_id = str(sys.argv[5] or "").strip()
target_channel_name = str(sys.argv[6] or "").strip()

con = sqlite3.connect(str(db_path))
con.row_factory = sqlite3.Row
try:
    channels_sql = """
        SELECT id, channel_name, channel_id, channel_url
        FROM channels
    """
    params: list[object] = []
    if target_channel_id:
        channels_sql += " WHERE channel_id = ? OR channel_id = ? "
        params.extend([target_channel_id, target_channel_id.lstrip("@")])
    elif target_channel_name:
        channels_sql += " WHERE channel_name = ? "
        params.append(target_channel_name)
    channels_sql += " ORDER BY id ASC"
    if not target_channel_id and not target_channel_name and channel_limit > 0:
        channels_sql += f" LIMIT {channel_limit}"
    channels = con.execute(channels_sql, params).fetchall()

    def backlog_count(channel_db_id: int) -> int:
        row = con.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM videos
            WHERE channel_id = ?
              AND (
                COALESCE(transcript_downloaded, 0) = 0
                OR COALESCE(summary_file_path, '') = ''
                OR COALESCE(transcript_formatted_path, '') = ''
              )
            """,
            (channel_db_id,),
        ).fetchone()
        return int(row["cnt"] or 0)

    def slug(text: str) -> str:
        out = []
        for ch in text:
            if ch.isalnum():
                out.append(ch)
            else:
                out.append("_")
        cleaned = "".join(out).strip("_")
        return cleaned or "channel"

    plan_path.parent.mkdir(parents=True, exist_ok=True)
    with plan_path.open("w", encoding="utf-8", newline="") as fp:
        for row in channels:
            pending = backlog_count(int(row["id"]))
            if strategy == "all_missing":
                mode = "scan-all-missing"
            elif strategy == "latest_only":
                mode = "latest-only"
            else:
                mode = "scan-all-missing" if pending > 0 else "latest-only"
            fp.write(
                "\t".join(
                    [
                        mode,
                        str(row["channel_id"] or ""),
                        str(row["channel_name"] or ""),
                        str(row["channel_url"] or ""),
                        f"{int(row['id']):05d}_{slug(str(row['channel_id'] or row['channel_name'] or 'channel'))}",
                        str(pending),
                    ]
                )
                + "\n"
            )
finally:
    con.close()
PY
}

if [ "$SKIP_DISCOVERY_VALUE" != "1" ]; then
    DISCOVERY_PLAN="$DISCOVERY_DIR/discovery_plan.tsv"
    generate_discovery_plan "$DISCOVERY_PLAN"
    while IFS=$'\t' read -r DISCOVER_MODE DISCOVER_CH_ID DISCOVER_CH_NAME DISCOVER_CH_URL DISCOVER_CH_KEY DISCOVER_BACKLOG_COUNT; do
        [ -z "$DISCOVER_CH_ID" ] && continue
        DISCOVER_SUBDIR="$DISCOVERY_DIR/${DISCOVER_MODE}/${DISCOVER_CH_KEY}"
        mkdir -p "$DISCOVER_SUBDIR"
        DISCOVER_CMD=("$REPO_DIR/scripts/discover.sh")
        if [ "$DISCOVER_MODE" = "latest-only" ]; then
            DISCOVER_CMD+=("--latest-only")
            if [ -n "$DISCOVER_RECENT_PER_CHANNEL_VALUE" ]; then
                DISCOVER_CMD+=("--recent-per-channel" "$DISCOVER_RECENT_PER_CHANNEL_VALUE")
            fi
        else
            DISCOVER_CMD+=("--scan-all-missing")
        fi
        DISCOVER_CMD+=("--run-dir" "$DISCOVER_SUBDIR" "--channel-id" "$DISCOVER_CH_ID")
        if [ "$DISCOVER_RATE_LIMIT_SAFE_VALUE" = "1" ]; then
            DISCOVER_CMD+=("--rate-limit-safe")
        fi
        echo "[DISCOVERY][$DISCOVER_MODE][backlog=${DISCOVER_BACKLOG_COUNT}] $DISCOVER_CH_ID | $DISCOVER_CH_NAME"
        run_stage "DISCOVERY[$DISCOVER_MODE]" "${DISCOVER_CMD[@]}"
    done < "$DISCOVERY_PLAN"
    if [ -z "$RESOLVED_CHANNEL_ID" ] && [ -n "$TARGET_CHANNEL_NAME" ]; then
        RESOLVED_CHANNEL_ID="$(resolve_channel_id "$TARGET_CHANNEL_NAME" || true)"
    fi
fi

if [ -n "$TARGET_CHANNEL_NAME" ] && [ -z "$RESOLVED_CHANNEL_ID" ]; then
    echo "❌ Channel tidak ditemukan di DB setelah discovery: $TARGET_CHANNEL_NAME" >&2
    exit 1
fi

if [ "$SKIP_TRANSCRIPT_VALUE" != "1" ]; then
    TRANSCRIPT_CMD=("$REPO_DIR/scripts/transcript.sh" "--run-dir" "$TRANSCRIPT_DIR" "--workers" "$TRANSCRIPT_WORKERS_VALUE")
    if [ -n "$RESOLVED_CHANNEL_ID" ]; then
        TRANSCRIPT_CMD+=(--channel-id "$RESOLVED_CHANNEL_ID")
    fi
    if [ "$TRANSCRIPT_LIMIT_VALUE" -gt 0 ]; then
        TRANSCRIPT_CMD+=(--limit "$TRANSCRIPT_LIMIT_VALUE")
    fi
    if [ "$TRANSCRIPT_WEBSHARE_FIRST_VALUE" = "1" ]; then
        TRANSCRIPT_CMD+=(--webshare-first)
    fi
    if [ "$TRANSCRIPT_WEBSHARE_ONLY_VALUE" = "1" ]; then
        TRANSCRIPT_CMD+=(--webshare-only)
    fi
    if [ "$TRANSCRIPT_RATE_LIMIT_SAFE_VALUE" = "1" ]; then
        TRANSCRIPT_CMD+=(--rate-limit-safe)
    fi
    run_stage "TRANSCRIPT" "${TRANSCRIPT_CMD[@]}"
fi

if [ "$SKIP_RESUME_VALUE" != "1" ]; then
    RESUME_CMD=("$REPO_DIR/scripts/resume.sh" "--run-dir" "$RESUME_DIR" "--max-workers" "$RESUME_WORKERS_VALUE" "--model" "$RESUME_MODEL_VALUE")
    if [ -n "$RESOLVED_CHANNEL_ID" ]; then
        RESUME_CMD+=(--channel-id "$RESOLVED_CHANNEL_ID")
    fi
    if [ "$RESUME_LIMIT_VALUE" -gt 0 ]; then
        RESUME_CMD+=(--limit "$RESUME_LIMIT_VALUE")
    fi
    if [ "$RESUME_NVIDIA_ONLY_VALUE" = "1" ]; then
        RESUME_CMD+=(--nvidia-only)
    fi
    run_stage "RESUME" "${RESUME_CMD[@]}"
fi

if [ "$SKIP_FORMAT_VALUE" != "1" ]; then
    FORMAT_CMD=("$REPO_DIR/scripts/format.sh" "--run-dir" "$FORMAT_DIR" "--workers" "$FORMAT_WORKERS_VALUE" "--provider-plan" "$FORMAT_PROVIDER_PLAN_VALUE")
    if [ -n "$RESOLVED_CHANNEL_ID" ]; then
        FORMAT_CMD+=(--channel-id "$RESOLVED_CHANNEL_ID")
    fi
    if [ "$FORMAT_LIMIT_VALUE" -gt 0 ]; then
        FORMAT_CMD+=(--limit "$FORMAT_LIMIT_VALUE")
    fi
    run_stage "FORMAT" "${FORMAT_CMD[@]}"
fi

echo "============================================="
echo "Pipeline selesai"
echo "Run dir: $RUN_DIR_VALUE"
echo "============================================="
