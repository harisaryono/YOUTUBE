#!/usr/bin/env python3
"""Delete redundant transcript text files whose content already exists in blob storage."""

from __future__ import annotations

import argparse
import csv
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


REPO_DIR = Path(__file__).resolve().parent.parent
DB_PATH = REPO_DIR / "db" / "youtube_transcripts.db"
BLOB_DB_PATH = REPO_DIR / "db" / "youtube_transcripts_blobs.db"


def open_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def iter_candidates(conn: sqlite3.Connection):
    query = """
        SELECT id, video_id, transcript_file_path
        FROM videos
        WHERE transcript_file_path LIKE 'uploads/%/text/%'
    """
    for row in conn.execute(query):
        yield row


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Apply deletions and DB updates")
    parser.add_argument("--batch-size", type=int, default=25, help="SQLite commit batch size")
    parser.add_argument("--run-dir", default="", help="Optional output dir for report files")
    args = parser.parse_args()

    conn = open_db(DB_PATH)
    blob = open_db(BLOB_DB_PATH)
    blob_cur = blob.cursor()

    candidates = []
    folder_counts = Counter()
    folder_bytes = Counter()
    for row in iter_candidates(conn):
        video_id = str(row["video_id"])
        file_path = str(row["transcript_file_path"] or "").strip()
        if not file_path:
            continue
        has_blob = blob.execute(
            "SELECT 1 FROM content_blobs WHERE video_id = ? AND content_type = 'transcript'",
            (video_id,),
        ).fetchone() is not None
        if not has_blob:
            continue
        path = REPO_DIR / file_path
        size = path.stat().st_size if path.exists() else 0
        folder = Path(file_path).parts[1] if len(Path(file_path).parts) >= 2 else "unknown"
        candidates.append((row["id"], video_id, file_path, size, folder))
        folder_counts[folder] += 1
        folder_bytes[folder] += size

    print(f"Candidates: {len(candidates)}")
    for folder, count in folder_counts.most_common():
        print(f"{folder}\t{count}\t{folder_bytes[folder]}")

    if not args.apply:
        return 0

    report_dir = None
    if args.run_dir:
        report_dir = Path(args.run_dir)
        report_dir.mkdir(parents=True, exist_ok=True)
    else:
        report_dir = REPO_DIR / "runs" / f"cleanup_redundant_transcript_files_{datetime.now():%Y%m%d_%H%M%S}"
        report_dir.mkdir(parents=True, exist_ok=True)

    report_csv = report_dir / "report.csv"
    batch = []
    deleted_files = 0
    updated_rows = 0
    with report_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["video_id", "file_path", "size_bytes", "folder", "file_deleted", "db_updated"])
        for idx, (row_id, video_id, file_path, size, folder) in enumerate(candidates, start=1):
            file_deleted = False
            path = REPO_DIR / file_path
            if path.exists():
                try:
                    path.unlink()
                    file_deleted = True
                    deleted_files += 1
                except Exception:
                    file_deleted = False

            conn.execute(
                """
                UPDATE videos
                SET transcript_file_path = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (row_id,),
            )
            updated_rows += 1
            blob_cur.execute(
                "DELETE FROM content_blobs WHERE video_id = ? AND content_type = 'transcript'",
                (video_id,),
            )
            writer.writerow([video_id, file_path, size, folder, int(file_deleted), 1])

            if idx % args.batch_size == 0:
                conn.commit()
                blob.commit()

        conn.commit()
        blob.commit()

    print(f"Done candidates={len(candidates)} deleted_files={deleted_files} updated_rows={updated_rows}")
    print(f"Report: {report_csv}")
    conn.close()
    blob.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
