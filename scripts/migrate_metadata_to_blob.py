#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
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


def migrate_metadata(db_path: Path, *, clear_column: bool, batch_size: int) -> dict[str, int]:
    conn = connect_db(db_path)
    blob = BlobStorage(str(db_path.with_name("youtube_transcripts_blobs.db")))

    rows = conn.execute(
        """
        SELECT id, video_id, metadata
        FROM videos
        WHERE COALESCE(metadata, '') <> ''
        ORDER BY id ASC
        """
    ).fetchall()

    processed = 0
    saved = 0
    cleared = 0
    batch_updates: list[tuple[int, int]] = []

    for row in rows:
        processed += 1
        video_id = str(row["video_id"])
        metadata_raw = str(row["metadata"] or "").strip()
        if not metadata_raw:
            continue
        try:
            parsed = json.loads(metadata_raw)
            metadata_json = json.dumps(parsed, ensure_ascii=False) if isinstance(parsed, dict) else metadata_raw
        except Exception:
            metadata_json = metadata_raw

        if blob.save_blob(video_id, "metadata", metadata_json):
            saved += 1
        if clear_column:
            batch_updates.append((int(row["id"]),))
            cleared += 1

        if clear_column and len(batch_updates) >= batch_size:
            conn.executemany(
                "UPDATE videos SET metadata = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                batch_updates,
            )
            conn.commit()
            batch_updates.clear()

    if clear_column and batch_updates:
        conn.executemany(
            "UPDATE videos SET metadata = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            batch_updates,
        )
        conn.commit()

    conn.close()
    return {"processed": processed, "saved": saved, "cleared": cleared}


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill videos.metadata into blob storage.")
    parser.add_argument("--db", default="youtube_transcripts.db", help="Main SQLite DB path")
    parser.add_argument("--clear-column", action="store_true", help="Clear videos.metadata after backfill")
    parser.add_argument("--batch-size", type=int, default=25, help="Commit batch size when clearing metadata")
    args = parser.parse_args()

    result = migrate_metadata(Path(args.db), clear_column=bool(args.clear_column), batch_size=max(1, int(args.batch_size)))
    print(f"processed={result['processed']} saved={result['saved']} cleared={result['cleared']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
