#!/usr/bin/env python3
"""Clear duplicated transcript/summary text from videos after blob migration."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


REPO_DIR = Path(__file__).resolve().parent.parent
MAIN_DB_PATH = REPO_DIR / "db" / "youtube_transcripts.db"
BLOB_DB_PATH = REPO_DIR / "db" / "youtube_transcripts_blobs.db"


def open_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def get_counts(conn: sqlite3.Connection) -> dict[str, int]:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM videos WHERE COALESCE(transcript_text, '') <> ''")
    transcript_rows = int(cur.fetchone()[0] or 0)
    cur.execute("SELECT COUNT(*) FROM videos WHERE COALESCE(summary_text, '') <> ''")
    summary_rows = int(cur.fetchone()[0] or 0)
    cur.execute("SELECT COUNT(*) FROM videos WHERE COALESCE(transcript_text, '') <> '' AND COALESCE(summary_text, '') <> ''")
    both_rows = int(cur.fetchone()[0] or 0)
    return {
        "transcript_rows": transcript_rows,
        "summary_rows": summary_rows,
        "both_rows": both_rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Apply the cleanup instead of dry-run")
    parser.add_argument("--vacuum", action="store_true", help="Run VACUUM after clearing the columns")
    args = parser.parse_args()

    conn = open_db(MAIN_DB_PATH)
    conn.execute(f"ATTACH DATABASE '{BLOB_DB_PATH}' AS blobs")

    before = get_counts(conn)
    print(
        "Before: transcript_rows={transcript_rows} summary_rows={summary_rows} both_rows={both_rows}".format(
            **before
        )
    )

    transcript_safe = """
        COALESCE(transcript_text, '') <> ''
        AND EXISTS (
            SELECT 1
            FROM blobs.content_blobs b
            WHERE b.video_id = videos.video_id
              AND b.content_type = 'transcript'
        )
    """
    summary_safe = """
        COALESCE(summary_text, '') <> ''
        AND EXISTS (
            SELECT 1
            FROM blobs.content_blobs b
            WHERE b.video_id = videos.video_id
              AND b.content_type = 'resume'
        )
    """

    if not args.apply:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM videos WHERE {transcript_safe}")
        print(f"Dry-run transcript rows to clear: {int(cur.fetchone()[0] or 0)}")
        cur.execute(f"SELECT COUNT(*) FROM videos WHERE {summary_safe}")
        print(f"Dry-run summary rows to clear: {int(cur.fetchone()[0] or 0)}")
        conn.close()
        return 0

    cur = conn.cursor()
    cur.execute(f"UPDATE videos SET transcript_text = NULL WHERE {transcript_safe}")
    transcript_cleared = cur.rowcount if cur.rowcount is not None else 0
    cur.execute(f"UPDATE videos SET summary_text = NULL WHERE {summary_safe}")
    summary_cleared = cur.rowcount if cur.rowcount is not None else 0
    conn.commit()

    after = get_counts(conn)
    print(f"Cleared transcript_text rows: {transcript_cleared}")
    print(f"Cleared summary_text rows: {summary_cleared}")
    print(
        "After: transcript_rows={transcript_rows} summary_rows={summary_rows} both_rows={both_rows}".format(
            **after
        )
    )

    if args.vacuum:
        print("Running VACUUM ...")
        conn.execute("VACUUM")
        conn.commit()
        print("VACUUM done")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
