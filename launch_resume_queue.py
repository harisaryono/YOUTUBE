#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import subprocess
import threading
import time
from pathlib import Path

from local_services import coordinator_status_accounts

# Use relative path based on script location (portable across environments)
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR
from dotenv import load_dotenv

load_dotenv()
VENV_DIR = Path(
    os.environ.get("YOUTUBE_VENV_DIR")
    or os.environ.get("EXTERNAL_VENV_DIR")
    or "/media/harry/DATA120B/venv_youtube"
)
PYTHON_BIN = str(VENV_DIR / "bin" / "python")
WORKER_SCRIPT = str(ROOT / "fill_missing_resumes_youtube_db.py")
DB = str(ROOT / "youtube_transcripts.db")
DEFAULT_MODEL = "openai/gpt-oss-120b"
RUNS_DIR = ROOT / "runs"
QUEUE_DB_NAME = "resume_pool.sqlite3"
IDLE_POLL_SECONDS = 2.0
REQUEUE_PRIORITY_NVIDIA = 100
TASK_TIMEOUT_SECONDS = max(120, int(os.getenv("YT_RESUME_TASK_TIMEOUT_SECONDS", "330") or 330))

# Global state for Groq-to-Nvidia prioritization
active_groq_lock = threading.Lock()
active_groq_workers = 0

def log(msg: str) -> None:
    print(msg, flush=True)


def load_available_accounts(provider: str, model_name: str) -> list[dict]:
    rows = coordinator_status_accounts(provider=provider, model_name=model_name)
    items: list[dict] = []
    for row in rows:
        if int(row.get("is_active") or 0) != 1:
            continue
        if str(row.get("state") or "idle").strip().lower() != "idle":
            continue
        items.append(
            {
                "id": int(row["provider_account_id"]),
                "provider": str(row["provider"]),
                "account_name": str(row["account_name"]),
            }
        )
    items.sort(key=lambda item: int(item["id"]))
    return items


def load_missing_summary_tasks(video_ids: set[str] | None = None, limit: int = 0) -> list[dict]:
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    try:
        if video_ids:
            placeholders = ",".join("?" for _ in video_ids)
            query = f"""
                SELECT v.video_id, c.channel_name
                FROM videos v
                JOIN channels c ON c.id = v.channel_id
                WHERE v.transcript_downloaded = 1
                  AND COALESCE(v.summary_file_path, '') = ''
                  AND COALESCE(v.is_short, 0) = 0 AND COALESCE(v.is_member_only, 0) = 0
                  AND v.video_id IN ({placeholders})
                ORDER BY c.channel_name ASC, v.id DESC
            """
            params = list(video_ids)
        else:
            query = """
                SELECT v.video_id, c.channel_name
                FROM videos v
                JOIN channels c ON c.id = v.channel_id
                WHERE v.transcript_downloaded = 1
                  AND COALESCE(v.summary_file_path, '') = ''
                  AND COALESCE(v.is_short, 0) = 0 AND COALESCE(v.is_member_only, 0) = 0
                ORDER BY c.channel_name ASC, v.id DESC
            """
            params = []
            
        if limit > 0:
            query += " LIMIT ?"
            params.append(limit)
            
        rows = con.execute(query, params).fetchall()
    finally:
        con.close()
    return [{"video_id": str(r["video_id"]), "channel_name": str(r["channel_name"])} for r in rows]


def load_tasks_csv(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return [
            {
                "video_id": str(row.get("video_id") or "").strip(),
                "channel_name": str(row.get("channel_name") or "").strip(),
            }
            for row in csv.DictReader(f)
            if str(row.get("video_id") or "").strip()
        ]


def write_tasks_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["video_id", "channel_name"])
        writer.writeheader()
        writer.writerows(rows)


def init_queue_db(queue_db: Path, tasks: list[dict], *, preferred_provider: str = "groq") -> None:
    con = sqlite3.connect(str(queue_db), timeout=30)
    try:
        with con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS queue_tasks (
                    video_id TEXT PRIMARY KEY,
                    channel_name TEXT NOT NULL DEFAULT '',
                    state TEXT NOT NULL DEFAULT 'pending',
                    preferred_provider TEXT NOT NULL DEFAULT '',
                    priority INTEGER NOT NULL DEFAULT 0,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    assigned_provider TEXT NOT NULL DEFAULT '',
                    assigned_account_id INTEGER NOT NULL DEFAULT 0,
                    last_status TEXT NOT NULL DEFAULT '',
                    note TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            con.execute("DELETE FROM queue_tasks")
            con.executemany(
                """
                INSERT INTO queue_tasks (
                    video_id, channel_name, state, preferred_provider, priority, attempts,
                    assigned_provider, assigned_account_id, last_status, note
                ) VALUES (?, ?, 'pending', ?, 0, 0, '', 0, '', '')
                """,
                [
                    (
                        str(row["video_id"]),
                        str(row.get("channel_name") or ""),
                        preferred_provider,
                    )
                    for row in tasks
                ],
            )
    finally:
        con.close()


def flip_pending_tasks_provider(queue_db: Path, provider: str) -> None:
    con = sqlite3.connect(str(queue_db))
    try:
        with con:
            con.execute(
                "UPDATE queue_tasks SET preferred_provider = ? WHERE state = 'pending'",
                (provider,),
            )
    finally:
        con.close()


def queue_counts(queue_db: Path) -> dict[str, int]:
    con = sqlite3.connect(str(queue_db), timeout=30)
    try:
        rows = con.execute(
            """
            SELECT state, COUNT(*) AS n
            FROM queue_tasks
            GROUP BY state
            """
        ).fetchall()
        data = {str(state): int(count) for state, count in rows}
        return {
            "pending": int(data.get("pending", 0)),
            "in_progress": int(data.get("in_progress", 0)),
            "done": int(data.get("done", 0)),
            "failed": int(data.get("failed", 0)),
        }
    finally:
        con.close()


def claim_next_task(queue_db: Path, provider: str, account_id: int) -> dict | None:
    con = sqlite3.connect(str(queue_db), timeout=30)
    con.row_factory = sqlite3.Row
    try:
        con.execute("BEGIN IMMEDIATE")
        row = con.execute(
            """
            SELECT video_id, channel_name, attempts, preferred_provider, priority
            FROM queue_tasks
            WHERE state = 'pending'
              AND (preferred_provider = '' OR preferred_provider = ?)
            ORDER BY
                CASE WHEN preferred_provider = ? THEN 0 ELSE 1 END,
                priority DESC,
                attempts ASC,
                video_id ASC
            LIMIT 1
            """,
            (provider, provider),
        ).fetchone()
        if row is None:
            con.rollback()
            return None
        con.execute(
            """
            UPDATE queue_tasks
            SET state = 'in_progress',
                assigned_provider = ?,
                assigned_account_id = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE video_id = ?
            """,
            (provider, int(account_id), str(row["video_id"])),
        )
        con.commit()
        return dict(row)
    finally:
        con.close()


def mark_done(queue_db: Path, video_id: str, *, provider: str, account_id: int, status: str, note: str) -> None:
    con = sqlite3.connect(str(queue_db), timeout=30)
    try:
        with con:
            con.execute(
                """
                UPDATE queue_tasks
                SET state = 'done',
                    assigned_provider = ?,
                    assigned_account_id = ?,
                    last_status = ?,
                    note = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE video_id = ?
                """,
                (provider, int(account_id), status[:100], note[:1000], video_id),
            )
    finally:
        con.close()


def requeue_task(
    queue_db: Path,
    video_id: str,
    *,
    provider: str,
    account_id: int,
    status: str,
    note: str,
    preferred_provider: str = "",
    priority: int = 0,
) -> None:
    con = sqlite3.connect(str(queue_db), timeout=30)
    try:
        with con:
            con.execute(
                """
                UPDATE queue_tasks
                SET state = 'pending',
                    preferred_provider = ?,
                    priority = ?,
                    attempts = attempts + 1,
                    assigned_provider = ?,
                    assigned_account_id = ?,
                    last_status = ?,
                    note = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE video_id = ?
                """,
                (
                    preferred_provider[:50],
                    int(priority),
                    provider,
                    int(account_id),
                    status[:100],
                    note[:1000],
                    video_id,
                ),
            )
    finally:
        con.close()


def mark_failed(queue_db: Path, video_id: str, *, provider: str, account_id: int, status: str, note: str) -> None:
    con = sqlite3.connect(str(queue_db), timeout=30)
    try:
        with con:
            con.execute(
                """
                UPDATE queue_tasks
                SET state = 'failed',
                    attempts = attempts + 1,
                    assigned_provider = ?,
                    assigned_account_id = ?,
                    last_status = ?,
                    note = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE video_id = ?
                """,
                (provider, int(account_id), status[:100], note[:1000], video_id),
            )
    finally:
        con.close()


def parse_worker_report(report_csv: Path) -> dict:
    rows = list(csv.DictReader(report_csv.open("r", encoding="utf-8", newline=""))) if report_csv.exists() else []
    if not rows:
        return {"status": "failed", "note": "worker produced no report"}
    row = rows[-1]
    return {
        "status": str(row.get("status") or "").strip() or "failed",
        "note": str(row.get("note") or "").strip(),
        "video_id": str(row.get("video_id") or "").strip(),
    }


def run_single_task(
    *,
    run_dir: Path,
    provider: str,
    account_id: int,
    model_name: str,
    task: dict,
    worker_dir: Path,
) -> dict:
    video_id = str(task["video_id"])
    task_csv = worker_dir / f"task_{provider}_{account_id}_{video_id}.csv"
    report_csv = worker_dir / f"report_{provider}_{account_id}_{video_id}.csv"
    log_path = worker_dir / f"run_{provider}_{account_id}.log"
    write_tasks_csv(task_csv, [{"video_id": video_id, "channel_name": str(task.get("channel_name") or "")}])
    env = os.environ.copy()
    env["YT_PROVIDER_COORDINATOR_URL"] = str(
        os.getenv("YT_PROVIDER_COORDINATOR_URL", "http://8.215.77.132:8788")
    ).strip()
    cmd = [
        PYTHON_BIN,
        WORKER_SCRIPT,
        "--db", DB,
        "--provider", provider,
        "--model", model_name,
        "--tasks-csv", str(task_csv),
        "--report-csv", str(report_csv),
        "--provider-account-id", str(account_id),
        "--limit", "1",
        "--max-attempts", "6",
    ]
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"\n=== {time.strftime('%Y-%m-%d %H:%M:%S')} | video_id={video_id} ===\n")
        handle.flush()
        try:
            result = subprocess.run(
                cmd,
                cwd=str(ROOT),
                env=env,
                stdout=handle,
                stderr=subprocess.STDOUT,
                check=False,
                timeout=TASK_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired:
            handle.write(
                f"\n[TIMEOUT] worker exceeded {TASK_TIMEOUT_SECONDS}s for video_id={video_id}\n"
            )
            handle.flush()
            return {
                "status": "retry",
                "note": f"worker_timeout>{TASK_TIMEOUT_SECONDS}s",
                "video_id": video_id,
                "returncode": 124,
                "report_csv": str(report_csv),
            }
    payload = parse_worker_report(report_csv)
    payload["returncode"] = int(result.returncode)
    payload["report_csv"] = str(report_csv)
    return payload


def worker_loop(
    *,
    queue_db: Path,
    run_dir: Path,
    provider: str,
    account_id: int,
    account_name: str,
    model_name: str,
    worker_dir: Path,
) -> None:
    global active_groq_workers
    if provider == "groq":
        with active_groq_lock:
            active_groq_workers += 1

    log(f"started {provider}:{account_id} pid=pool account='{account_name}'")
    try:
        while True:
            task = claim_next_task(queue_db, provider, account_id)
            if task is None:
                counts = queue_counts(queue_db)
                if counts["pending"] == 0 and counts["in_progress"] == 0:
                    log(f"finished {provider}:{account_id} no remaining tasks")
                    return
                time.sleep(IDLE_POLL_SECONDS)
                continue

            video_id = str(task["video_id"])
            log(f"claimed {provider}:{account_id} video_id={video_id}")
            payload = run_single_task(
                run_dir=run_dir,
                provider=provider,
                account_id=account_id,
                model_name=model_name,
                task=task,
                worker_dir=worker_dir,
            )
            status = str(payload.get("status") or "failed")
            note = str(payload.get("note") or "")

            if status == "ok":
                mark_done(queue_db, video_id, provider=provider, account_id=account_id, status=status, note=note)
                continue

            if provider == "groq" and status in {"blocked_provider_quota", "retry", "failed", "disabled_auth"}:
                requeue_task(
                    queue_db,
                    video_id,
                    provider=provider,
                    account_id=account_id,
                    status=status,
                    note=note,
                    preferred_provider="nvidia",
                    priority=REQUEUE_PRIORITY_NVIDIA,
                )
                log(f"requeued for nvidia video_id={video_id} from groq:{account_id} status={status}")
                if status in {"blocked_provider_quota", "disabled_auth"}:
                    log(f"stopping groq worker {account_id} after status={status}")
                    return
                continue

            if provider == "nvidia" and status == "retry":
                requeue_task(
                    queue_db,
                    video_id,
                    provider=provider,
                    account_id=account_id,
                    status=status,
                    note=note,
                    preferred_provider="nvidia",
                    priority=REQUEUE_PRIORITY_NVIDIA,
                )
                continue

            mark_failed(queue_db, video_id, provider=provider, account_id=account_id, status=status, note=note)

    finally:
        if provider == "groq":
            with active_groq_lock:
                active_groq_workers -= 1
                if active_groq_workers == 0:
                    log("All Groq workers finished. Flipping remaining tasks to Nvidia.")
                    con = sqlite3.connect(str(queue_db))
                    try:
                        with con:
                            con.execute(
                                "UPDATE queue_tasks SET preferred_provider = 'nvidia' WHERE preferred_provider = 'groq' AND state = 'pending'"
                            )
                    except Exception as e:
                        log(f"Failed to flip tasks: {e}")
                    finally:
                        con.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks-csv", default="", help="CSV video_id spesifik untuk resume.")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--run-dir", default="")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument(
        "--max-workers",
        type=int,
        default=0,
        help="Batas jumlah akun/worker yang dipakai dari pool coordinator (0 = default internal).",
    )
    parser.add_argument(
        "--nvidia-only",
        action="store_true",
        help="Disable Groq completely and run the resume queue with Nvidia accounts only.",
    )
    parser.add_argument(
        "--reuse-queue-db",
        action="store_true",
        help="Reuse an existing queue DB instead of rebuilding it from scratch.",
    )
    args = parser.parse_args()

    model_name = str(args.model or DEFAULT_MODEL).strip()
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    run_dir = Path(args.run_dir) if args.run_dir else RUNS_DIR / f"resume_queue_{timestamp}"
    worker_dir = run_dir / "workers"
    worker_dir.mkdir(parents=True, exist_ok=True)

    groq_accounts = load_available_accounts("groq", model_name)
    nvidia_accounts = load_available_accounts("nvidia", model_name)
    if args.nvidia_only:
        log("NVIDIA-only resume mode enabled; Groq accounts will not be used.")
        accounts = nvidia_accounts
    else:
        accounts = groq_accounts + nvidia_accounts
    if not accounts:
        raise SystemExit(f"No available accounts for model {model_name}")
    max_workers = int(args.max_workers or 0)
    if max_workers > 0:
        accounts = accounts[:max_workers]
        log(f"Limiting resume workers to {len(accounts)} account(s) via --max-workers")

    limit_val = int(args.limit or 0)
    if args.tasks_csv:
        task_seed = load_tasks_csv(Path(args.tasks_csv))
        task_ids = {str(row["video_id"]) for row in task_seed}
        tasks = load_missing_summary_tasks(task_ids, limit=limit_val) if task_ids else []
    else:
        tasks = load_missing_summary_tasks(limit=limit_val)

    queue_db = run_dir / QUEUE_DB_NAME
    if args.reuse_queue_db and not queue_db.exists():
        raise SystemExit(f"Queue DB not found for reuse: {queue_db}")
    if not tasks:
        write_tasks_csv(run_dir / "all_tasks.csv", tasks)
        (run_dir / "meta.txt").write_text(
            "\n".join(
                [
                    f"started_at={time.strftime('%Y-%m-%dT%H:%M:%S')}",
                    f"model={model_name}",
                    "pool_mode=shared_queue",
                    f"groq_accounts={[a['id'] for a in groq_accounts]}",
                    f"nvidia_accounts={[a['id'] for a in nvidia_accounts]}",
                    "task_count=0",
                    f"queue_db={queue_db}",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        log(f"run_dir={run_dir}")
        log("queue_counts={'pending': 0, 'in_progress': 0, 'done': 0, 'failed': 0}")
        return 0

    if not args.reuse_queue_db:
        init_queue_db(queue_db, tasks, preferred_provider="nvidia" if args.nvidia_only else "groq")
        write_tasks_csv(run_dir / "all_tasks.csv", tasks)
    else:
        write_tasks_csv(run_dir / "all_tasks.csv", tasks)

    # Automatic takeover if no Groq accounts are available at start.
    if tasks and (args.nvidia_only or not groq_accounts):
        if args.nvidia_only:
            log("Flipping all pending tasks to Nvidia because nvidia-only mode is active.")
        else:
            log("No Groq accounts identified. Automatically flipping all tasks to Nvidia.")
        try:
            flip_pending_tasks_provider(queue_db, "nvidia")
        except Exception as e:
            log(f"Failed to flip tasks: {e}")

    (run_dir / "meta.txt").write_text(
        "\n".join(
            [
                f"started_at={time.strftime('%Y-%m-%dT%H:%M:%S')}",
                f"model={model_name}",
                f"pool_mode=shared_queue",
                f"provider_plan={'nvidia_only' if args.nvidia_only else 'shared'}",
                f"groq_accounts={[a['id'] for a in groq_accounts]}",
                f"nvidia_accounts={[a['id'] for a in nvidia_accounts]}",
                f"task_count={len(tasks)}",
                f"queue_db={queue_db}",
            ]
        ) + "\n",
        encoding="utf-8",
    )

    threads: list[threading.Thread] = []
    # Throttle workers to keep DB stable.
    max_workers = max_workers if max_workers > 0 else 12
    for i, account in enumerate(accounts):
        if i >= max_workers:
            log(f"Throttled: Skipping account {account['id']} ({account['provider']}) to maintain DB stability.")
            continue
        thread = threading.Thread(
            target=worker_loop,
            kwargs={
                "queue_db": queue_db,
                "run_dir": run_dir,
                "provider": str(account["provider"]),
                "account_id": int(account["id"]),
                "account_name": str(account["account_name"]),
                "model_name": model_name,
                "worker_dir": worker_dir,
            },
            daemon=False,
        )
        thread.start()
        threads.append(thread)
        time.sleep(0.2)

    for thread in threads:
        thread.join()

    counts = queue_counts(queue_db)
    log(f"run_dir={run_dir}")
    log(f"queue_counts={counts}")
    return 0 if counts["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
