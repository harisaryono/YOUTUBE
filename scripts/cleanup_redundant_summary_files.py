#!/usr/bin/env python3
"""Delete redundant summary files whose content already exists in blob storage."""

from __future__ import annotations

import argparse
import csv
import sqlite3
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
        SELECT id, video_id, summary_file_path
        FROM videos
        WHERE summary_file_path LIKE 'uploads/%/summary/%'
           OR summary_file_path LIKE 'uploads/%/summaries/%'
    """
    for row in conn.execute(query):
        yield row


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Apply deletions")
    parser.add_argument("--run-dir", default="", help="Optional output dir for report files")
    args = parser.parse_args()

    conn = open_db(DB_PATH)
    blob = open_db(BLOB_DB_PATH)

    candidates = []
    for row in iter_candidates(conn):
        video_id = str(row["video_id"])
        file_path = str(row["summary_file_path"] or "").strip()
        if not file_path:
            continue
        has_blob = blob.execute(
            "SELECT 1 FROM content_blobs WHERE video_id = ? AND content_type = 'resume'",
            (video_id,),
        ).fetchone() is not None
        if not has_blob:
            continue
        path = REPO_DIR / file_path
        size = path.stat().st_size if path.exists() else 0
        candidates.append((row["id"], video_id, file_path, size))

    total_bytes = sum(size for _, _, _, size in candidates)
    print(f"Candidates: {len(candidates)} bytes={total_bytes}")

    if not args.apply:
        for _, video_id, file_path, size in candidates[:20]:
            print(f"DRY-RUN {video_id} {file_path} {size}")
        return 0

    if args.run_dir:
        report_dir = Path(args.run_dir)
    else:
        report_dir = REPO_DIR / "runs" / f"cleanup_redundant_summary_files_{datetime.now():%Y%m%d_%H%M%S}"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_csv = report_dir / "report.csv"

    deleted_files = 0
    with report_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["video_id", "file_path", "size_bytes", "file_deleted"])
        for _, video_id, file_path, size in candidates:
            file_deleted = False
            path = REPO_DIR / file_path
            if path.exists():
                try:
                    path.unlink()
                    file_deleted = True
                    deleted_files += 1
                except Exception:
                    file_deleted = False
            writer.writerow([video_id, file_path, size, int(file_deleted)])

    # Remove empty summary directories after file deletion.
    for summary_dir in sorted(REPO_DIR.glob("uploads/*/summary")):
        if summary_dir.is_dir():
            try:
                next(summary_dir.iterdir())
            except StopIteration:
                try:
                    summary_dir.rmdir()
                except Exception:
                    pass

    conn.close()
    blob.close()
    print(f"Done candidates={len(candidates)} deleted_files={deleted_files}")
    print(f"Report: {report_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
