#!/usr/bin/env python3
"""Remove SaveSubs HTML error pages that were stored as transcripts."""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path


REPO_DIR = Path(__file__).resolve().parent.parent
DB_PATH = REPO_DIR / "db" / "youtube_transcripts.db"
BLOB_DB_PATH = REPO_DIR / "db" / "youtube_transcripts_blobs.db"

HTML_MARKERS = (
    "<!doctype html",
    "<html",
    "cloudflare used to restrict access",
    "you are being rate limited",
    "access denied | savesubs.com",
    "error 1015",
    "ray id:",
)


def looks_like_html_error(text: str) -> bool:
    normalized = (text or "").strip().lower()
    if not normalized:
        return False
    return any(marker in normalized for marker in HTML_MARKERS)


def open_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def iter_candidates(conn: sqlite3.Connection):
    query = """
        SELECT id, video_id, transcript_file_path, transcript_text
        FROM videos
        WHERE transcript_text LIKE '<!doctype html%'
           OR transcript_text LIKE '<html%'
           OR transcript_text LIKE '%Cloudflare used to restrict access%'
           OR transcript_text LIKE '%Access denied | savesubs.com%'
           OR transcript_text LIKE '%You are being rate limited%'
           OR transcript_text LIKE '%Ray ID:%'
           OR transcript_retry_reason = 'save_subs_html_error'
    """
    for row in conn.execute(query):
        yield row


def file_text_is_bad(path: Path) -> bool:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return False
    return looks_like_html_error(text)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Apply changes instead of dry-run")
    parser.add_argument("--delete-files", action="store_true", default=True, help="Delete bad transcript files from disk")
    args = parser.parse_args()

    conn = open_db(DB_PATH)
    blob_conn = open_db(BLOB_DB_PATH)
    blob_cursor = blob_conn.cursor()

    candidates = []
    for row in iter_candidates(conn):
        transcript_text = str(row["transcript_text"] or "")
        file_path = str(row["transcript_file_path"] or "").strip()
        bad = looks_like_html_error(transcript_text)
        if not bad and file_path:
            path = Path(file_path)
            if not path.is_absolute():
                path = REPO_DIR / path
            bad = path.exists() and file_text_is_bad(path)
        if bad:
            candidates.append((row["id"], row["video_id"], file_path))

    print(f"Candidates: {len(candidates)}")
    if not args.apply:
        for _, video_id, file_path in candidates[:20]:
            print(f"DRY-RUN {video_id} {file_path}")
        return 0

    deleted_files = 0
    repaired = 0
    for idx, (row_id, video_id, file_path) in enumerate(candidates, start=1):
        if file_path:
            path = Path(file_path)
            if not path.is_absolute():
                path = REPO_DIR / path
            if args.delete_files and path.exists():
                try:
                    path.unlink()
                    deleted_files += 1
                except Exception:
                    pass

        conn.execute(
            """
            UPDATE videos
            SET transcript_file_path = NULL,
                summary_file_path = NULL,
                transcript_downloaded = 0,
                transcript_language = NULL,
                word_count = 0,
                line_count = 0,
                transcript_text = '',
                transcript_retry_after = datetime('now', '+7 days'),
                transcript_retry_reason = 'save_subs_cloudflare_html',
                transcript_retry_count = COALESCE(transcript_retry_count, 0) + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (row_id,),
        )
        blob_cursor.execute(
            "DELETE FROM content_blobs WHERE video_id = ? AND content_type = 'transcript'",
            (video_id,),
        )
        repaired += 1
        if repaired % 50 == 0:
            conn.commit()
            blob_conn.commit()
            print(f"Repaired {repaired}/{len(candidates)}")

    conn.commit()
    blob_conn.commit()
    conn.close()
    blob_conn.close()

    print(f"Done repaired={repaired} deleted_files={deleted_files}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
