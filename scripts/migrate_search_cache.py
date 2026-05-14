#!/usr/bin/env python3
"""
Backfill blob-first search cache and migrate away from legacy videos_fts.

This script:
1. Creates/refreshes videos_search_cache from blob-backed transcript/summary reads.
2. Drops the legacy videos_fts / triggers that still depend on videos.transcript_text
   and videos.summary_text.
3. Commits incrementally so long runs can survive interruption.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database_optimized import OptimizedDatabase

REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = REPO_ROOT / "db" / "youtube_transcripts.db"


def ensure_legacy_drop(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    cursor.execute("DROP TRIGGER IF EXISTS videos_ai")
    cursor.execute("DROP TRIGGER IF EXISTS videos_ad")
    cursor.execute("DROP TRIGGER IF EXISTS videos_au")
    cursor.execute("DROP TABLE IF EXISTS videos_fts")
    conn.commit()


def backfill_cache(batch_size: int = 25, drop_legacy: bool = True) -> int:
    db = OptimizedDatabase(str(DB_PATH))
    read_conn = sqlite3.connect(str(DB_PATH))
    read_conn.row_factory = sqlite3.Row

    try:
        with db._get_cursor() as cursor:
            cursor.execute("DELETE FROM videos_search_cache")

        read_cur = read_conn.cursor()
        read_cur.execute(
            """
            SELECT v.video_id, v.title, v.description
            FROM videos v
            ORDER BY v.id ASC
            """
        )

        total = 0
        batch: list[tuple[str, str, str, str, str]] = []

        while True:
            rows = read_cur.fetchmany(batch_size)
            if not rows:
                break

            for row in rows:
                video_id = str(row["video_id"] or "").strip()
                if not video_id:
                    continue
                batch.append(
                    (
                        video_id,
                        str(row["title"] or "").strip(),
                        str(row["description"] or ""),
                        db.read_transcript(video_id) or "",
                        db.read_summary(video_id) or "",
                    )
                )

            if batch:
                with db._get_cursor() as cursor:
                    cursor.executemany(
                        """
                        INSERT INTO videos_search_cache (
                            video_id, title, description, transcript_search, summary_search, updated_at
                        ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        ON CONFLICT(video_id) DO UPDATE SET
                            title = excluded.title,
                            description = excluded.description,
                            transcript_search = excluded.transcript_search,
                            summary_search = excluded.summary_search,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        batch,
                    )
                total += len(batch)
                print(f"[backfill] committed {total} rows")
                batch.clear()

        if batch:
            with db._get_cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO videos_search_cache (
                        video_id, title, description, transcript_search, summary_search, updated_at
                    ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(video_id) DO UPDATE SET
                        title = excluded.title,
                        description = excluded.description,
                        transcript_search = excluded.transcript_search,
                        summary_search = excluded.summary_search,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    batch,
                )
            total += len(batch)
            print(f"[backfill] committed {total} rows")

        if drop_legacy:
            ensure_legacy_drop(db.conn)
            print("[migrate] dropped legacy videos_fts triggers/tables")

        return total
    finally:
        try:
            read_conn.close()
        except Exception:
            pass
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill blob-first search cache and migrate legacy FTS")
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--keep-legacy-fts", action="store_true")
    args = parser.parse_args()

    total = backfill_cache(batch_size=max(1, int(args.batch_size)), drop_legacy=not args.keep_legacy_fts)
    print(f"[done] backfilled {total} search cache rows")


if __name__ == "__main__":
    main()
