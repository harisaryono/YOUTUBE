#!/usr/bin/env python3
"""
Diet DB — Kurangi ukuran youtube_transcripts.db.

Yang dilakukan:
1. Pindahkan transcript_text dari tabel videos ke blobs.db (gzip, ~231 MB → ~50 MB)
2. Pindahkan summary_text dari tabel videos ke blobs.db (~53 MB → ~15 MB)
3. Vacuum untuk kompres sisa ruang

Tidak menyentuh videos_search_cache (content table FTS5 — diperlukan untuk search).

Estimasi penghematan: ~220 MB dari ~1 GB.
"""

import sqlite3
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "db" / "youtube_transcripts.db"
BLOBS_DB_PATH = PROJECT_ROOT / "db" / "youtube_transcripts_blobs.db"

sys.path.insert(0, str(PROJECT_ROOT))
from database_blobs import BlobStorage


def get_db_size(path: Path) -> str:
    if not path.exists():
        return "0 B"
    size = path.stat().st_size
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} GB"


def step(msg: str):
    print(f"  ▶ {msg}")


def main():
    print("=" * 60)
    print("  Diet DB — youtube_transcripts.db")
    print("=" * 60)
    print(f"\n  Before: {get_db_size(DB_PATH)}")
    print(f"  Blobs:  {get_db_size(BLOBS_DB_PATH)}")
    print()

    if not DB_PATH.exists():
        print("  ❌ DB not found:", DB_PATH)
        sys.exit(1)

    blob_storage = BlobStorage(str(BLOBS_DB_PATH))
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row

    # ── Step 1: Pindahkan transcript_text ke blobs.db ──
    print("  [1/3] Pindahkan transcript_text ke blobs.db...")
    rows = conn.execute(
        """SELECT video_id, transcript_text FROM videos
           WHERE transcript_text IS NOT NULL AND transcript_text != ''"""
    ).fetchall()
    step(f"Found {len(rows)} videos with transcript_text")

    moved_transcript = 0
    for row in rows:
        ok = blob_storage.save_blob(row["video_id"], "transcript", row["transcript_text"])
        if ok:
            moved_transcript += 1
    step(f"Moved {moved_transcript} transcripts to blobs.db")

    # Null-kan transcript_text di videos
    conn.execute("UPDATE videos SET transcript_text = NULL WHERE transcript_text IS NOT NULL AND transcript_text != ''")
    step("Cleared transcript_text from videos table")

    # ── Step 2: Pindahkan summary_text ke blobs.db ──
    print("\n  [2/3] Pindahkan summary_text ke blobs.db...")
    rows = conn.execute(
        """SELECT video_id, summary_text FROM videos
           WHERE summary_text IS NOT NULL AND summary_text != ''"""
    ).fetchall()
    step(f"Found {len(rows)} videos with summary_text")

    moved_summary = 0
    for row in rows:
        ok = blob_storage.save_blob(row["video_id"], "summary", row["summary_text"])
        if ok:
            moved_summary += 1
    step(f"Moved {moved_summary} summaries to blobs.db")

    # Null-kan summary_text di videos
    conn.execute("UPDATE videos SET summary_text = NULL WHERE summary_text IS NOT NULL AND summary_text != ''")
    step("Cleared summary_text from videos table")

    conn.commit()

    # ── Step 3: Vacuum ──
    print("\n  [3/3] Vacuum (kompres sisa ruang)...")
    step("This may take a while...")
    t0 = time.time()
    conn.execute("VACUUM")
    elapsed = time.time() - t0
    step(f"Vacuum completed in {elapsed:.1f}s")

    conn.close()

    # ── Hasil ──
    print("\n" + "=" * 60)
    print("  Hasil Diet DB")
    print("=" * 60)
    print(f"  After:  {get_db_size(DB_PATH)}")
    print(f"  Blobs:  {get_db_size(BLOBS_DB_PATH)}")
    print(f"  Transcripts moved to blobs: {moved_transcript}")
    print(f"  Summaries moved to blobs:   {moved_summary}")
    print(f"  videos_search_cache:        TIDAK DIHAPUS (diperlukan FTS)")
    print()
    print("  ⚠️  Script yang baca transcript_text / summary_text langsung")
    print("     dari videos table perlu diupdate untuk pakai BlobStorage.")
    print("=" * 60)


if __name__ == "__main__":
    main()
