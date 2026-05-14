#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from recover_transcripts import BASE_DIR, TranscriptRecoverer


def safe_channel_slug(channel_id: str) -> str:
    return (
        str(channel_id or "")
        .replace("@", "")
        .replace(" ", "_")
        .replace("?", "_")
        .replace(":", "_")
    )


def load_targets(db_path: Path) -> list[dict[str, str]]:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            """
            SELECT v.video_id, v.title, c.channel_name, c.channel_id
            FROM videos v
            JOIN channels c ON c.id = v.channel_id
            WHERE COALESCE(v.transcript_downloaded, 0) = 0
              AND COALESCE(v.transcript_language, '') = 'no_subtitle'
            ORDER BY v.created_at DESC
            """
        ).fetchall()
    finally:
        con.close()
    return [
        {
            "video_id": str(row["video_id"]),
            "title": str(row["title"] or ""),
            "channel_name": str(row["channel_name"] or ""),
            "channel_id": str(row["channel_id"] or ""),
        }
        for row in rows
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit seluruh no_subtitle dengan Webshare-only transcript recovery.")
    parser.add_argument("--db", default=str(ROOT / "youtube_transcripts.db"))
    parser.add_argument("--run-dir", default="")
    parser.add_argument("--workers", type=int, default=10)
    args = parser.parse_args()

    db_path = Path(args.db).resolve()
    run_dir = Path(args.run_dir) if args.run_dir else ROOT / "runs" / f"audit_no_subtitle_webshare_{time.strftime('%Y%m%d_%H%M%S')}"
    run_dir.mkdir(parents=True, exist_ok=True)

    os.environ["YT_TRANSCRIPT_WEBSHARE_ONLY"] = "1"
    os.environ.pop("YT_TRANSCRIPT_WEBSHARE_FIRST", None)
    os.environ.pop("YT_TRANSCRIPT_AUDIT_NO_SUBTITLE", None)

    targets = load_targets(db_path)
    if not targets:
        print("Tidak ada row no_subtitle yang perlu diaudit.", flush=True)
        return 0

    report_path = run_dir / "recover_report.csv"
    retry_path = run_dir / "retry_later.csv"
    lock = threading.Lock()
    counters = {
        "downloaded": 0,
        "blocked": 0,
        "proxy_block": 0,
        "retry_later": 0,
        "fatal_error": 0,
        "no_subtitle": 0,
    }

    print(f"run_dir={run_dir}", flush=True)
    print(f"total_no_subtitle={len(targets)}", flush=True)
    print(f"workers={max(1, int(args.workers or 1))}", flush=True)

    with report_path.open("w", encoding="utf-8", newline="") as report_fp, retry_path.open(
        "w", encoding="utf-8", newline=""
    ) as retry_fp:
        report_writer = csv.DictWriter(
            report_fp,
            fieldnames=["video_id", "channel_name", "status", "language", "transcript_file_path", "note"],
        )
        report_writer.writeheader()
        retry_writer = csv.DictWriter(
            retry_fp,
            fieldnames=["video_id", "channel_name", "reason", "retry_after_hours"],
        )
        retry_writer.writeheader()

        def process(row: dict[str, str]) -> dict[str, str]:
            rec = TranscriptRecoverer()
            video_id = row["video_id"]
            try:
                result, outcome = rec.download_transcript(video_id)
            except Exception as exc:
                rec.last_transcript_failure_reason = str(exc)
                return {
                    "video_id": video_id,
                    "channel_name": row["channel_name"],
                    "status": "fatal_error",
                    "language": "",
                    "transcript_file_path": "",
                    "note": str(exc)[:500],
                }

            note = str(getattr(rec, "last_transcript_failure_reason", "") or "").strip()
            if result:
                timestamp = time.strftime("%Y%m%d_%H%M%S")
                text_dir = Path(BASE_DIR) / safe_channel_slug(row["channel_id"]) / "text"
                text_dir.mkdir(parents=True, exist_ok=True)
                file_path = text_dir / f"{video_id}_transcript_{timestamp}.txt"
                file_path.write_text(str(result["formatted"]), encoding="utf-8")
                with lock:
                    rec.db.update_video_with_transcript(
                        video_id=video_id,
                        transcript_file_path=str(file_path),
                        summary_file_path="",
                        transcript_language=str(result["language"]),
                        word_count=int(result["word_count"]),
                        line_count=int(result["line_count"]),
                    )
                return {
                    "video_id": video_id,
                    "channel_name": row["channel_name"],
                    "status": "downloaded",
                    "language": str(result["language"]),
                    "transcript_file_path": str(file_path),
                    "note": "",
                }

            if outcome == "proxy_block":
                with lock:
                    rec.db.mark_video_transcript_retry_later(
                        video_id=video_id,
                        reason=note or "proxy_block",
                        retry_after_hours=24,
                    )
                return {
                    "video_id": video_id,
                    "channel_name": row["channel_name"],
                    "status": "proxy_block",
                    "language": "",
                    "transcript_file_path": "",
                    "note": note[:500] or "proxy_block",
                }

            if outcome == "retry_later":
                with lock:
                    rec.db.mark_video_transcript_retry_later(
                        video_id=video_id,
                        reason=note or "retry_later",
                        retry_after_hours=24,
                    )
                return {
                    "video_id": video_id,
                    "channel_name": row["channel_name"],
                    "status": "retry_later",
                    "language": "",
                    "transcript_file_path": "",
                    "note": note[:500] or "retry_later",
                }

            if outcome == "blocked":
                with lock:
                    with rec.db._get_cursor() as cursor:
                        cursor.execute(
                            """
                            UPDATE videos
                            SET transcript_language = 'no_subtitle',
                                transcript_downloaded = 0,
                                transcript_file_path = '',
                                summary_file_path = '',
                                transcript_retry_reason = ?,
                                transcript_retry_after = NULL,
                                word_count = 0,
                                line_count = 0,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE video_id = ?
                            """,
                            (note or "blocked_member_only", video_id),
                        )
                return {
                    "video_id": video_id,
                    "channel_name": row["channel_name"],
                    "status": "blocked",
                    "language": "no_subtitle",
                    "transcript_file_path": "",
                    "note": note[:500] or "blocked_member_only",
                }

            return {
                "video_id": video_id,
                "channel_name": row["channel_name"],
                "status": "fatal_error",
                "language": "",
                "transcript_file_path": "",
                "note": note[:500] or "subtitle unavailable",
            }

        done = 0
        with ThreadPoolExecutor(max_workers=max(1, int(args.workers or 1))) as executor:
            future_map = {executor.submit(process, row): row for row in targets}
            for future in as_completed(future_map):
                result = future.result()
                report_writer.writerow(result)
                if result["status"] == "retry_later":
                    retry_writer.writerow(
                        {
                            "video_id": result["video_id"],
                            "channel_name": result["channel_name"],
                            "reason": result["note"],
                            "retry_after_hours": 24,
                        }
                    )
                with lock:
                    done += 1
                    status = result["status"]
                    if status in counters:
                        counters[status] += 1
                    if done % 50 == 0 or status in {"downloaded", "blocked"}:
                        print(
                            f"progress={done}/{len(targets)} "
                            f"downloaded={counters['downloaded']} "
                            f"blocked={counters['blocked']} "
                            f"retry_later={counters['retry_later']} "
                            f"fatal_error={counters['fatal_error']} "
                            f"no_subtitle={counters['no_subtitle']}",
                            flush=True,
                        )

    print(f"SUMMARY={counters}", flush=True)
    print(f"REPORT={report_path}", flush=True)
    print(f"RETRY={retry_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
