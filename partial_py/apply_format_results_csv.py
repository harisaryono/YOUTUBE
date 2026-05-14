#!/usr/bin/env python3
"""
Apply formatting results (CSV) back into the main sqlite DB.

Expected CSV columns (from format_transcripts_pool.py --results-csv):
- id
- ok
- formatted_rel_path

This updates:
- videos.transcript_formatted_path
"""

import argparse
import csv
import sqlite3


def main() -> int:
    ap = argparse.ArgumentParser(description="Apply format results CSV into youtube_transcripts.db")
    ap.add_argument("--db", default="youtube_transcripts.db", help="Path to sqlite DB")
    ap.add_argument("--results-csv", required=True, help="Results CSV path")
    ap.add_argument("--dry-run", action="store_true", help="Do not write changes")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    cur = conn.cursor()
    conn.execute("PRAGMA busy_timeout=8000")
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except Exception:
        pass

    applied = 0
    skipped = 0
    with open(args.results_csv, "r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            raw_id = str((row.get("id") or "").strip())
            ok = str((row.get("ok") or "").strip())
            rel = str((row.get("formatted_rel_path") or "").strip())
            if not raw_id or ok != "1" or not rel:
                skipped += 1
                continue
            try:
                vid_id = int(raw_id)
            except Exception:
                skipped += 1
                continue
            applied += 1
            if args.dry_run:
                continue
            cur.execute(
                """
                UPDATE videos
                SET transcript_formatted_path = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (rel, vid_id),
            )

    if args.dry_run:
        conn.close()
        print(f"Dry-run: would apply {applied} updates; skipped {skipped} rows")
        return 0

    conn.commit()
    conn.close()
    print(f"Applied {applied} updates; skipped {skipped} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
