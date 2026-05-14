#!/usr/bin/env python3
"""
Export pending transcript-formatting tasks to a CSV file.

This lets you run formatting on another machine (e.g. yt-server) without shipping the full sqlite DB.

CSV columns:
- id
- video_id
- channel_slug
- title
- transcript_file_path
"""

import argparse
import csv
import sqlite3
from pathlib import Path


def _read_exclude_ids(csv_path: str) -> set[int]:
    p = Path(csv_path)
    if not p.exists():
        return set()
    out: set[int] = set()
    with open(p, "r", encoding="utf-8", newline="") as fp:
        r = csv.DictReader(fp)
        for row in r:
            raw = str(row.get("id") or "").strip()
            if not raw:
                continue
            try:
                out.add(int(raw))
            except ValueError:
                continue
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Export pending transcript-formatting tasks to CSV")
    ap.add_argument("--db", default="youtube_transcripts.db", help="Path to sqlite DB")
    ap.add_argument("--out", required=True, help="Output CSV path")
    ap.add_argument("--limit", type=int, default=1000, help="Max rows to export (default: 1000)")
    ap.add_argument("--order", choices=("asc", "desc"), default="asc", help="Order by videos.created_at (default: asc)")
    ap.add_argument(
        "--exclude-csv",
        action="append",
        default=[],
        help="Tasks/results CSV to exclude by id (repeatable)",
    )
    args = ap.parse_args()

    order_sql = "ASC" if args.order != "desc" else "DESC"
    exclude_ids: set[int] = set()
    for x in list(args.exclude_csv or []):
        exclude_ids |= _read_exclude_ids(x)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    base_query = f"""
    SELECT
        v.id,
        v.video_id,
        v.title,
        COALESCE(v.transcript_file_path, '') AS transcript_file_path,
        REPLACE(REPLACE(c.channel_id, '@', ''), '/', '_') AS channel_slug
    FROM videos v
    JOIN channels c ON v.channel_id = c.id
    WHERE v.transcript_downloaded = 1
      AND COALESCE(v.transcript_language, '') != 'no_subtitle'
      AND COALESCE(v.transcript_file_path, '') != ''
      AND (v.transcript_formatted_path IS NULL OR v.transcript_formatted_path = '')
    ORDER BY v.created_at {order_sql}
    """

    wanted = int(args.limit)
    page_size = max(500, min(5000, wanted * 2))
    offset = 0
    rows: list[sqlite3.Row] = []
    stale = 0
    while len(rows) < wanted:
        cur.execute(f"{base_query} LIMIT ? OFFSET ?", (page_size, offset))
        page = cur.fetchall()
        if not page:
            break
        offset += len(page)
        for r in page:
            if exclude_ids and int(r["id"]) in exclude_ids:
                continue
            p = str(r["transcript_file_path"] or "").strip()
            if not p:
                stale += 1
                continue
            # Paths are stored relative to repo root (e.g. uploads/<slug>/text/...txt).
            if not Path(p).exists():
                stale += 1
                continue
            rows.append(r)
            if len(rows) >= wanted:
                break
    conn.close()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(
            fp,
            fieldnames=["id", "video_id", "channel_slug", "title", "transcript_file_path"],
        )
        w.writeheader()
        for r in rows:
            w.writerow(
                {
                    "id": r["id"],
                    "video_id": r["video_id"],
                    "channel_slug": r["channel_slug"],
                    "title": r["title"],
                    "transcript_file_path": r["transcript_file_path"],
                }
            )

    msg = f"Exported {len(rows)} rows -> {out_path}"
    if stale:
        msg += f" (skipped {stale} stale/missing transcript files)"
    print(msg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
