#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database_blobs import BlobStorage


def connect_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=8000")
    except Exception:
        pass
    return conn


def backfill_text_blobs(db_path: Path, batch_size: int) -> dict[str, int]:
    conn = connect_db(db_path)
    blob = BlobStorage(str(db_path.with_name("youtube_transcripts_blobs.db")))

    rows = conn.execute(
        """
        SELECT video_id, transcript_text, summary_text
        FROM videos
        WHERE COALESCE(transcript_text, '') <> ''
           OR COALESCE(summary_text, '') <> ''
        ORDER BY id ASC
        """
    ).fetchall()

    transcript_saved = 0
    summary_saved = 0
    processed = 0

    for row in rows:
        video_id = str(row["video_id"])
        transcript_text = str(row["transcript_text"] or "")
        summary_text = str(row["summary_text"] or "")
        processed += 1
        if transcript_text:
            if blob.save_blob(video_id, "transcript", transcript_text):
                transcript_saved += 1
        if summary_text:
            if blob.save_blob(video_id, "resume", summary_text):
                summary_saved += 1

        if batch_size > 0 and processed % batch_size == 0:
            conn.commit()

    conn.commit()
    conn.close()
    return {
        "processed": processed,
        "transcript_saved": transcript_saved,
        "summary_saved": summary_saved,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill transcript_text/summary_text into blob storage.")
    parser.add_argument("--db", default="youtube_transcripts.db", help="Path to main SQLite DB")
    parser.add_argument("--batch-size", type=int, default=25, help="Commit checkpoint every N rows")
    args = parser.parse_args()

    result = backfill_text_blobs(Path(args.db), max(1, int(args.batch_size)))
    print(
        f"processed={result['processed']} "
        f"transcript_saved={result['transcript_saved']} "
        f"summary_saved={result['summary_saved']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
