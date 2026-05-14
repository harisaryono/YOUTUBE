#!/bin/bash
# Manual transcript chain: transcript -> resume -> format

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
VENV_PYTHON="$("$SCRIPT_DIR/get_venv.sh")"

VIDEO_ID=""
RUN_DIR_VALUE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --video-id)
            VIDEO_ID="$2"
            shift 2
            ;;
        --run-dir)
            RUN_DIR_VALUE="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 --video-id ID [--run-dir PATH]"
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

if [ -z "$VIDEO_ID" ]; then
    echo "video-id is required" >&2
    exit 1
fi

cd "$REPO_DIR"
export REPO_ROOT="$REPO_DIR"

RUN_DIR_VALUE="${RUN_DIR_VALUE:-$REPO_DIR/runs/manual_transcript_${VIDEO_ID}_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$RUN_DIR_VALUE"

TRANSCRIPT_RUN_DIR="$RUN_DIR_VALUE/transcript"
RESUME_RUN_DIR="$RUN_DIR_VALUE/resume"
FORMAT_RUN_DIR="$RUN_DIR_VALUE/format"

echo "[chain] transcript -> resume -> format for $VIDEO_ID"

bash "$SCRIPT_DIR/transcript.sh" --video-id "$VIDEO_ID" --webshare-first --run-dir "$TRANSCRIPT_RUN_DIR"
echo "[chain] transcript done; verifying DB state for $VIDEO_ID"

"$VENV_PYTHON" - "$VIDEO_ID" <<'PY'
import sqlite3
import os
import sys
from pathlib import Path

repo = Path(os.environ.get("REPO_ROOT", "/media/harry/DATA120B/GIT/YOUTUBE"))
video_id = sys.argv[1]
con = sqlite3.connect(str(repo / 'db' / 'youtube_transcripts.db'))
con.row_factory = sqlite3.Row
row = con.execute("SELECT transcript_downloaded FROM videos WHERE video_id = ?", (video_id,)).fetchone()
con.close()
if not row or not row['transcript_downloaded']:
    raise SystemExit(1)
PY

echo "[chain] transcript verified; continuing resume for $VIDEO_ID"
bash "$SCRIPT_DIR/resume.sh" --video-id "$VIDEO_ID" --run-dir "$RESUME_RUN_DIR" --max-workers 1 --nvidia-only
echo "[chain] resume done; continuing format for $VIDEO_ID"
bash "$SCRIPT_DIR/format.sh" --video-id "$VIDEO_ID" --run-dir "$FORMAT_RUN_DIR" --workers 1 --provider-plan nvidia_only
