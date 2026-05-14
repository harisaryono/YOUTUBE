#!/usr/bin/env python3
"""
Format Transcripts Watchdog

Goal:
- Every N seconds (default 1800), print a compact status snapshot.
- If no formatter is running and there are still pending transcripts, start the next batch (limit=1000 by default).

This is designed to run as a long-lived process (tmux/screen/systemd or a persistent exec session).
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from local_services import coordinator_status_accounts


REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_DB = str(REPO_ROOT / "youtube_transcripts.db")
DEFAULT_UPLOADS = str(REPO_ROOT / "uploads")
DEFAULT_POLL_SECONDS = 60
DEFAULT_REPORT_SECONDS = 1800
DEFAULT_BATCH_LIMIT = 1000
DEFAULT_WORKERS = 3


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def count_pending(db_path: str) -> int:
    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        cur.execute(
            """
            SELECT COUNT(*)
            FROM videos v
            WHERE v.transcript_downloaded = 1
              AND COALESCE(v.transcript_language, '') != 'no_subtitle'
              AND COALESCE(v.transcript_file_path, '') != ''
              AND (v.transcript_formatted_path IS NULL OR v.transcript_formatted_path = '')
            """
        )
        row = cur.fetchone()
        return int(row[0] or 0) if row else 0
    finally:
        con.close()


def pgrep_formatter() -> list[int]:
    # Match any running formatter pool process.
    try:
        out = subprocess.check_output(
            ["pgrep", "-f", "python.*format_transcripts_pool\\.py"],
            text=True,
        ).strip()
    except subprocess.CalledProcessError:
        return []
    pids: list[int] = []
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            pid = int(line)
        except ValueError:
            continue
        # Filter out wrapper shells that happen to have the pattern in their argv.
        try:
            comm = Path(f"/proc/{pid}/comm").read_text(encoding="utf-8", errors="replace").strip().lower()
        except Exception:
            comm = ""
        if comm and "python" not in comm:
            continue
        pids.append(pid)
    return sorted(set(pids))


def coordinator_nvidia_in_use() -> Optional[int]:
    try:
        rows = coordinator_status_accounts(provider="nvidia", include_inactive=True)
    except Exception:
        return None
    in_use = 0
    for a in rows:
        # Coordinator status payload uses runtime fields.
        state = str(a.get("state") or "").strip().lower()
        lease_token = str(a.get("lease_token") or "").strip()
        if state == "in_use" and lease_token:
            in_use += 1
    return in_use


def latest_pool_log(runs_dir: Path, batch_limit: int, workers: int) -> Optional[Path]:
    pattern = f"format_pool_*_limit{batch_limit}_w{workers}.log"
    matches = sorted(runs_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return matches[0] if matches else None


def tail_text(path: Path, n: int = 5) -> str:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return ""
    return "\n".join(lines[-n:])


def extract_last_progress(path: Path) -> str:
    try:
        raw = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""
    # Progress lines are printed with carriage returns.
    raw = raw.replace("\r", "\n")
    lines = [ln.strip() for ln in raw.split("\n") if "Progress:" in ln]
    return lines[-1] if lines else ""


def start_next_batch(
    *,
    db_path: str,
    uploads_dir: str,
    workers: int,
    batch_limit: int,
    runs_dir: Path,
) -> subprocess.Popen:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = runs_dir / f"format_pool_{ts}_limit{batch_limit}_w{workers}.log"
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    venv_dir = Path(
        os.environ.get("YOUTUBE_VENV_DIR")
        or os.environ.get("EXTERNAL_VENV_DIR")
        or "/media/harry/DATA120B/venv_youtube"
    )
    cmd = [
        str(venv_dir / "bin" / "python"),
        "-u",
        str(REPO_ROOT / "format_transcripts_pool.py"),
        "--db",
        db_path,
        "--uploads",
        uploads_dir,
        "--workers",
        str(workers),
        "--limit",
        str(batch_limit),
    ]
    log_fh = open(log_path, "ab", buffering=0)
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(REPO_ROOT),
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            env=env,
            close_fds=True,
            start_new_session=True,
        )
    finally:
        # The child already inherited the fd; close our copy to avoid leaking fds per batch.
        try:
            log_fh.close()
        except Exception:
            pass
    return proc


def main() -> int:
    ap = argparse.ArgumentParser(description="Watchdog: monitor transcript formatter and run next batches.")
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--uploads", default=DEFAULT_UPLOADS)
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    ap.add_argument("--batch-limit", type=int, default=DEFAULT_BATCH_LIMIT)
    ap.add_argument("--poll", type=int, default=DEFAULT_POLL_SECONDS, help="Polling cadence in seconds (default: 60)")
    ap.add_argument("--report", type=int, default=DEFAULT_REPORT_SECONDS, help="Status report cadence in seconds (default: 1800)")
    args = ap.parse_args()

    runs_dir = REPO_ROOT / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    status_log = runs_dir / "format_pool_watchdog.log"
    stop_file = runs_dir / "format_pool_watchdog.stop"

    child: Optional[subprocess.Popen] = None
    last_report_at = 0.0
    last_state = ""

    def log(msg: str) -> None:
        line = f"[{now_utc_iso()}] {msg}\n"
        sys.stdout.write(line)
        sys.stdout.flush()
        with open(status_log, "a", encoding="utf-8") as fh:
            fh.write(line)

    log(
        f"watchdog started db={args.db} uploads={args.uploads} workers={args.workers} "
        f"batch_limit={args.batch_limit} poll={args.poll}s report={args.report}s"
    )
    log(f"stop with: touch {stop_file}")

    while True:
        if stop_file.exists():
            log("stop file detected; exiting")
            return 0

        # Reconcile: if there's already a formatter running (maybe started manually), just monitor.
        running_pids = pgrep_formatter()
        if child is not None and child.poll() is not None:
            child = None

        if running_pids:
            pending = count_pending(args.db)
            nvidia_in_use = coordinator_nvidia_in_use()
            last_log = latest_pool_log(runs_dir, args.batch_limit, args.workers)
            progress = extract_last_progress(last_log) if last_log else ""
            state = f"running:{','.join(str(p) for p in running_pids)}"
            now = time.time()
            if state != last_state or (now - last_report_at) >= float(args.report):
                last_report_at = now
                last_state = state
                log(
                    f"formatter running pids={running_pids} pending={pending} "
                    f"nvidia_in_use={(nvidia_in_use if nvidia_in_use is not None else 'unknown')}"
                )
                if last_log and progress:
                    log(f"latest_log={last_log.name} {progress}")
            time.sleep(max(5, int(args.poll)))
            continue

        pending = count_pending(args.db)
        nvidia_in_use = coordinator_nvidia_in_use()
        state = "idle"
        now = time.time()
        if state != last_state or (now - last_report_at) >= float(args.report):
            last_report_at = now
            last_state = state
            log(
                f"no formatter running pending={pending} "
                f"nvidia_in_use={(nvidia_in_use if nvidia_in_use is not None else 'unknown')}"
            )
        if pending <= 0:
            time.sleep(max(5, int(args.poll)))
            continue

        log("starting next batch...")
        try:
            child = start_next_batch(
                db_path=args.db,
                uploads_dir=args.uploads,
                workers=int(args.workers),
                batch_limit=int(args.batch_limit),
                runs_dir=runs_dir,
            )
            log(f"batch started pid={child.pid}")
        except Exception as exc:
            log(f"failed to start batch: {exc!r}")
            time.sleep(max(5, int(args.poll)))
            continue

        # Don't spam; give it time to begin producing output.
        time.sleep(10)


if __name__ == "__main__":
    raise SystemExit(main())
