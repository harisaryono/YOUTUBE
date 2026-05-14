#!/usr/bin/env python3
"""Repair stale transcript rows and backfill formatted-path metadata.

This utility keeps DB state aligned with files on disk:
- clear orphan resume/summary state on `no_subtitle` rows
- backfill formatted transcript paths from `uploads/*/text_formatted/*.txt`
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from database_optimized import OptimizedDatabase


def _connect(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA busy_timeout=8000")
    except Exception:
        pass
    return con


def repair_stale_transcripts(con: sqlite3.Connection) -> int:
    rows = con.execute(
        """
        SELECT id
        FROM videos
        WHERE transcript_language = 'no_subtitle'
          AND COALESCE(summary_file_path, '') <> ''
        """
    ).fetchall()
    if not rows:
        return 0

    ids = [int(row["id"]) for row in rows]
    placeholders = ",".join("?" for _ in ids)
    con.execute(
        f"""
        UPDATE videos
        SET transcript_downloaded = 0,
            transcript_file_path = '',
            summary_file_path = '',
            transcript_formatted_path = '',
            word_count = 0,
            line_count = 0,
            updated_at = CURRENT_TIMESTAMP
        WHERE id IN ({placeholders})
        """,
        ids,
    )
    con.commit()
    return len(ids)


def backfill_formatted_paths(con: sqlite3.Connection, uploads_dir: Path) -> int:
    files = sorted(uploads_dir.glob("*/text_formatted/*.txt"))
    updated = 0
    for file_path in files:
        video_id = file_path.stem
        channel_slug = file_path.parent.parent.name
        rel_path = str(Path("uploads") / channel_slug / "text_formatted" / f"{video_id}.txt")
        row = con.execute(
            """
            SELECT id, transcript_downloaded, COALESCE(transcript_language, '') AS transcript_language,
                   COALESCE(transcript_formatted_path, '') AS transcript_formatted_path
            FROM videos
            WHERE video_id = ?
            """,
            (video_id,),
        ).fetchone()
        if not row:
            continue
        if int(row["transcript_downloaded"] or 0) != 1:
            continue
        if str(row["transcript_language"] or "") == "no_subtitle":
            continue
        current_path = str(row["transcript_formatted_path"] or "")
        if current_path == rel_path:
            continue
        con.execute(
            """
            UPDATE videos
            SET transcript_formatted_path = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (rel_path, int(row["id"])),
        )
        updated += 1
    con.commit()
    return updated


def main() -> int:
    parser = argparse.ArgumentParser(description="Repair stale transcript and formatted-path DB state.")
    parser.add_argument("--db", default="youtube_transcripts.db", help="SQLite DB path")
    parser.add_argument("--uploads-dir", default="uploads", help="Uploads directory root")
    args = parser.parse_args()

    db_path = Path(args.db)
    uploads_dir = Path(args.uploads_dir)
    con = _connect(db_path)
    try:
        stale_fixed = repair_stale_transcripts(con)
        formatted_backfilled = backfill_formatted_paths(con, uploads_dir)
    finally:
        con.close()

    print(f"stale_fixed={stale_fixed}")
    print(f"formatted_backfilled={formatted_backfilled}")
    try:
        db = OptimizedDatabase(str(db_path), str(uploads_dir))
        db._bump_stats_cache_version()
        db.close()
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
