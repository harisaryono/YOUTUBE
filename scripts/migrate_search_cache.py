#!/usr/bin/env python3
"""Rebuild the separate search DB with a slimmer search corpus."""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database_optimized import OptimizedDatabase

REPO_ROOT = Path(__file__).resolve().parent.parent
MAIN_DB_PATH = REPO_ROOT / "db" / "youtube_transcripts.db"
SEARCH_DB_PATH = REPO_ROOT / "db" / "youtube_transcripts_search.db"


def _remove_search_db_files() -> None:
    for suffix in ("", "-wal", "-shm"):
        path = Path(f"{SEARCH_DB_PATH}{suffix}")
        if path.exists():
            path.unlink()


def rebuild_search_db(batch_size: int = 250) -> int:
    _remove_search_db_files()
    db = OptimizedDatabase(str(MAIN_DB_PATH))
    read_conn = sqlite3.connect(str(MAIN_DB_PATH))
    read_conn.row_factory = sqlite3.Row

    try:
        db.search_storage.conn.execute("DELETE FROM videos_search_cache")
        db.search_storage.conn.commit()

        read_cur = read_conn.cursor()
        read_cur.execute(
            """
            SELECT v.video_id, v.title, v.description
            FROM videos v
            ORDER BY v.id ASC
            """
        )

        total = 0
        batch: list[tuple[str, str, str, str]] = []

        while True:
            rows = read_cur.fetchmany(max(1, int(batch_size)))
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
                    )
                )

            if batch:
                db.search_storage.bulk_replace_cache(batch)
                total += len(batch)
                print(f"[backfill] committed {total} rows")
                batch.clear()

        if batch:
            db.search_storage.bulk_replace_cache(batch)
            total += len(batch)
            print(f"[backfill] committed {total} rows")

        db.search_storage.rebuild()
        print("[backfill] rebuilt search FTS")
        return total
    finally:
        try:
            read_conn.close()
        except Exception:
            pass
        try:
            db.close()
        except Exception:
            pass


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-size", type=int, default=250)
    parser.add_argument("--keep-main-search", action="store_true", help="Keep legacy search objects in main DB")
    parser.add_argument("--skip-vacuum", action="store_true", help="Skip VACUUM on the main DB after cleanup")
    args = parser.parse_args()

    total = rebuild_search_db(batch_size=max(1, int(args.batch_size)))
    print(f"[done] rebuilt {total} search cache rows into the slimmer DB")

    if args.keep_main_search:
        print("[done] legacy main-db search objects kept")
        return 0

    cleanup_conn = sqlite3.connect(str(MAIN_DB_PATH), timeout=30)
    try:
        cursor = cleanup_conn.cursor()
        cursor.execute("DROP TRIGGER IF EXISTS videos_search_cache_ai")
        cursor.execute("DROP TRIGGER IF EXISTS videos_search_cache_ad")
        cursor.execute("DROP TRIGGER IF EXISTS videos_search_cache_au")
        cursor.execute("DROP INDEX IF EXISTS idx_videos_search_cache_video_id")
        cursor.execute("DROP TABLE IF EXISTS videos_search_fts")
        cursor.execute("DROP TABLE IF EXISTS videos_search_cache")
        cleanup_conn.commit()
        print("[cleanup] dropped legacy search objects from main DB")
        if not args.skip_vacuum:
            cleanup_conn.execute("VACUUM")
            cleanup_conn.commit()
            print("[cleanup] VACUUM completed on main DB")
    finally:
        cleanup_conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
