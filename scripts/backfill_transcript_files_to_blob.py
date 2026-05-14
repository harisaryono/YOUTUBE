#!/usr/bin/env python3
"""Backfill transcript files into blob storage, then remove redundant files.

This script is intentionally conservative:
- it scans transcript files still present under `uploads/*/text/*`
- it writes transcript content into the `transcript` blob and `videos.transcript_text`
- it leaves `transcript_file_path` intact as a legacy marker
- it deletes the physical file only after the database batch commits
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = REPO_ROOT / "db" / "youtube_transcripts.db"
BLOB_DB_PATH = REPO_ROOT / "db" / "youtube_transcripts_blobs.db"
UPLOADS_DIR = REPO_ROOT / "uploads"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from database_blobs import BlobStorage  # noqa: E402


HTML_MARKERS = (
    "<!doctype html",
    "<html",
    "cloudflare",
    "access denied",
    "error 1015",
)


def _is_html_error(text: str) -> bool:
    low = str(text or "").strip().lower()
    return any(marker in low for marker in HTML_MARKERS)


def _word_count(text: str) -> int:
    return len(re.findall(r"\S+", str(text or "")))


def _line_count(text: str) -> int:
    return len([ln for ln in str(text or "").splitlines() if ln.strip()])


def _resolve_file_path(raw: str) -> Path:
    path = Path(str(raw or "").strip())
    if path.is_absolute():
        return path
    return (REPO_ROOT / path).resolve()


def _prune_empty_dirs(start: Path) -> None:
    current = start
    while current != UPLOADS_DIR and current != current.parent:
        try:
            current.rmdir()
        except OSError:
            break
        current = current.parent


def iter_files(limit: int | None = None) -> list[Path]:
    files = sorted(
        p for p in UPLOADS_DIR.rglob("*.txt")
        if p.is_file() and "/text/" in str(p)
    )
    if limit and int(limit) > 0:
        return files[: int(limit)]
    return files


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill remaining transcript files into blobs and delete the files")
    parser.add_argument("--db", default=str(DB_PATH), help="Path to the main SQLite database")
    parser.add_argument("--blob-db", default=str(BLOB_DB_PATH), help="Path to the blob SQLite database")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of rows to process")
    parser.add_argument("--batch-size", type=int, default=25, help="Commit database changes every N rows")
    parser.add_argument("--dry-run", action="store_true", help="Report only, do not write or delete anything")
    args = parser.parse_args()

    db_path = Path(args.db)
    blob_db_path = Path(args.blob_db)
    if not db_path.exists():
        raise FileNotFoundError(f"main database not found: {db_path}")
    if not blob_db_path.exists():
        raise FileNotFoundError(f"blob database not found: {blob_db_path}")

    blob = BlobStorage(str(blob_db_path))
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    cursor = con.cursor()

    files = iter_files(limit=args.limit or None)
    total = len(files)

    stats = {
        "total": total,
        "processed": 0,
        "backfilled": 0,
        "deleted": 0,
        "skipped_missing": 0,
        "skipped_html": 0,
        "failed": 0,
    }
    pending_deletes: list[Path] = []

    for idx, file_path in enumerate(files, start=1):
        file_path = file_path.resolve()
        stem = file_path.stem
        video_id = stem.split("_transcript_")[0] if "_transcript_" in stem else stem
        row = con.execute(
            """
            SELECT id, video_id, transcript_file_path, transcript_text
            FROM videos
            WHERE video_id = ?
            LIMIT 1
            """,
            (video_id,),
        ).fetchone()

        if not row:
            print(f"⚠️  {video_id}: no DB row found for file {file_path}")
            stats["skipped_missing"] += 1
            continue

        if not file_path.exists():
            stats["skipped_missing"] += 1
            continue

        try:
            content = file_path.read_text(encoding="utf-8")
        except Exception as exc:
            print(f"❌ {video_id}: read failed: {exc}")
            stats["failed"] += 1
            continue

        if _is_html_error(content):
            print(f"⚠️  {video_id}: HTML error page detected, leaving file untouched")
            stats["skipped_html"] += 1
            continue

        if args.dry_run:
            print(f"[dry-run] {video_id}: would backfill {file_path}")
            stats["backfilled"] += 1
            stats["processed"] += 1
            continue

        if not blob.save_blob(video_id, "transcript", content):
            print(f"❌ {video_id}: blob save failed")
            stats["failed"] += 1
            continue

        cursor.execute(
            """
            UPDATE videos
            SET transcript_text = ?,
                transcript_downloaded = 1,
                word_count = ?,
                line_count = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE video_id = ?
            """,
            (content, _word_count(content), _line_count(content), video_id),
        )
        pending_deletes.append(file_path)
        stats["backfilled"] += 1
        stats["processed"] += 1

        if len(pending_deletes) >= max(1, int(args.batch_size or 25)):
            con.commit()
            for path in pending_deletes:
                try:
                    if path.exists():
                        path.unlink()
                        stats["deleted"] += 1
                        _prune_empty_dirs(path.parent)
                except Exception as exc:
                    print(f"⚠️  delete failed for {path}: {exc}")
            pending_deletes.clear()
            print(
                f"✅ committed {stats['processed']}/{stats['total']} | "
                f"backfilled={stats['backfilled']} deleted={stats['deleted']} "
                f"missing={stats['skipped_missing']} html={stats['skipped_html']} failed={stats['failed']}"
            )

    if not args.dry_run:
        con.commit()
        for path in pending_deletes:
            try:
                if path.exists():
                    path.unlink()
                    stats["deleted"] += 1
                    _prune_empty_dirs(path.parent)
            except Exception as exc:
                print(f"⚠️  delete failed for {path}: {exc}")

    con.close()
    print(
        f"done total={stats['total']} processed={stats['processed']} "
        f"backfilled={stats['backfilled']} deleted={stats['deleted']} "
        f"missing={stats['skipped_missing']} html={stats['skipped_html']} failed={stats['failed']}"
    )
    return 0 if stats["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
