#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import queue
import sqlite3
import sys
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from local_services import fetch_youtube_upload_date  # noqa: E402


DEFAULT_DB_PATH = PROJECT_ROOT / "youtube_transcripts.db"
RUNS_DIR = PROJECT_ROOT / "runs"


def log(message: str) -> None:
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{stamp}] {message}", flush=True)


@dataclass(frozen=True)
class Task:
    video_id: str
    channel_id: str
    channel_name: str
    video_url: str


def load_tasks(db_path: Path, channel_id_filter: str, limit: int) -> list[Task]:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        sql = """
            SELECT v.video_id,
                   c.channel_id,
                   c.channel_name,
                   v.video_url
            FROM videos v
            JOIN channels c ON c.id = v.channel_id
            WHERE COALESCE(v.upload_date, '') = ''
        """
        params: list[object] = []
        if channel_id_filter:
            sql += " AND c.channel_id = ? "
            params.append(channel_id_filter)
        sql += " ORDER BY c.channel_name ASC, v.id DESC "
        if limit > 0:
            sql += " LIMIT ? "
            params.append(limit)
        rows = con.execute(sql, params).fetchall()
        return [
            Task(
                video_id=str(row["video_id"] or "").strip(),
                channel_id=str(row["channel_id"] or "").strip(),
                channel_name=str(row["channel_name"] or "").strip(),
                video_url=str(row["video_url"] or "").strip(),
            )
            for row in rows
            if str(row["video_id"] or "").strip() and str(row["video_url"] or "").strip()
        ]
    finally:
        con.close()


def worker_loop(
    *,
    worker_name: str,
    db_path: Path,
    task_queue: "queue.Queue[Task | None]",
    writer: csv.DictWriter,
    report_handle,
    writer_lock: threading.Lock,
    counters: dict[str, int],
    counter_lock: threading.Lock,
) -> None:
    con = sqlite3.connect(str(db_path), timeout=60)
    try:
        while True:
            task = task_queue.get()
            if task is None:
                task_queue.task_done()
                return

            status = "failed"
            note = ""
            upload_date = ""
            try:
                upload_date = fetch_youtube_upload_date(task.video_url, timeout_seconds=45)
                if upload_date:
                    con.execute(
                        "UPDATE videos SET upload_date = ? WHERE video_id = ? AND COALESCE(upload_date, '') = ''",
                        (upload_date, task.video_id),
                    )
                    con.commit()
                    status = "updated"
                else:
                    status = "missing_from_source"
                    note = "upload_date unavailable from video metadata"
            except Exception as exc:
                status = "error"
                note = str(exc)[:1000]

            row = {
                "video_id": task.video_id,
                "channel_id": task.channel_id,
                "channel_name": task.channel_name,
                "status": status,
                "upload_date": upload_date,
                "worker": worker_name,
                "note": note,
            }
            with writer_lock:
                writer.writerow(row)
                report_handle.flush()

            with counter_lock:
                counters["processed"] += 1
                counters[status] = counters.get(status, 0) + 1
                processed = counters["processed"]
                total = counters["total"]
                if processed % 25 == 0 or processed == total:
                    log(
                        f"progress {processed}/{total} | "
                        f"updated={counters.get('updated', 0)} "
                        f"missing={counters.get('missing_from_source', 0)} "
                        f"error={counters.get('error', 0)}"
                    )
            task_queue.task_done()
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill upload_date yang kosong dari metadata video YouTube.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--run-dir", default="", help="Direktori run untuk log/report.")
    parser.add_argument("--channel-id", default="", help="Batasi ke satu channel_id.")
    parser.add_argument("--limit", type=int, default=0, help="Batasi jumlah task. 0 = semua.")
    parser.add_argument("--workers", type=int, default=6, help="Jumlah worker paralel.")
    args = parser.parse_args()

    db_path = Path(args.db_path).resolve()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = Path(args.run_dir).resolve() if args.run_dir else (RUNS_DIR / f"backfill_upload_dates_{timestamp}").resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    report_path = run_dir / "report.csv"
    meta_path = run_dir / "meta.txt"

    tasks = load_tasks(db_path, str(args.channel_id or "").strip(), int(args.limit or 0))
    total = len(tasks)
    log(f"tasks loaded: {total}")
    meta_path.write_text(
        "\n".join(
            [
                f"db_path={db_path}",
                f"channel_id={str(args.channel_id or '').strip()}",
                f"workers={max(1, int(args.workers or 1))}",
                f"task_count={total}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    if not tasks:
        with report_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["video_id", "channel_id", "channel_name", "status", "upload_date", "worker", "note"],
            )
            writer.writeheader()
        return 0

    task_queue: "queue.Queue[Task | None]" = queue.Queue()
    for task in tasks:
        task_queue.put(task)

    counters = {"processed": 0, "total": total}
    counter_lock = threading.Lock()
    writer_lock = threading.Lock()
    worker_count = max(1, int(args.workers or 1))

    with report_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["video_id", "channel_id", "channel_name", "status", "upload_date", "worker", "note"],
        )
        writer.writeheader()

        threads = []
        for idx in range(worker_count):
            thread = threading.Thread(
                target=worker_loop,
                kwargs={
                    "worker_name": f"w{idx + 1}",
                    "db_path": db_path,
                    "task_queue": task_queue,
                    "writer": writer,
                    "report_handle": f,
                    "writer_lock": writer_lock,
                    "counters": counters,
                    "counter_lock": counter_lock,
                },
                daemon=True,
            )
            thread.start()
            threads.append(thread)

        for _ in threads:
            task_queue.put(None)

        task_queue.join()
        for thread in threads:
            thread.join(timeout=1)

    log(
        "done | "
        f"updated={counters.get('updated', 0)} "
        f"missing={counters.get('missing_from_source', 0)} "
        f"error={counters.get('error', 0)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
