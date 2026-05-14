#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sqlite3
import subprocess
import sys
import signal
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from . import db


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main() -> None:
    stop_requested = False
    child: Optional[subprocess.Popen] = None

    def _on_stop(signum: int, _frame: object) -> None:
        nonlocal stop_requested, child
        stop_requested = True
        try:
            if child and child.poll() is None:
                child.terminate()
        except Exception:
            pass

    # Install handlers ASAP to reduce the chance a SIGTERM kills the runner
    # before it can update the DB status.
    signal.signal(signal.SIGTERM, _on_stop)
    signal.signal(signal.SIGINT, _on_stop)

    ap = argparse.ArgumentParser()
    ap.add_argument("--job-id", required=True)
    ap.add_argument("--db", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--script", required=True)
    ap.add_argument("--channel-url", required=True)
    ap.add_argument("--log-path")
    ap.add_argument("--update", action="store_true")
    ap.add_argument("--stop-at-known", action="store_true")
    ap.add_argument("--stop-at-known-after", type=int, default=25)
    ap.add_argument("--stop-at-known-min-scan", type=int, default=200)
    ap.add_argument("--pending-only", action="store_true")
    ap.add_argument("--video-id", default="")
    args = ap.parse_args()

    job_id = args.job_id
    db_path = Path(args.db)
    out_root = Path(args.out)
    script_path = Path(args.script)
    channel_url = args.channel_url

    log_path: Optional[Path] = Path(args.log_path).resolve() if args.log_path else None
    if not log_path:
        with db.session(db_path) as con:
            job = con.execute("SELECT id, log_path FROM jobs WHERE id=?", (job_id,)).fetchone()
            if not job:
                raise SystemExit(f"job not found: {job_id}")
            log_path = Path(job["log_path"]).resolve() if job["log_path"] else None

    if not log_path:
        raise SystemExit("missing log_path")
    log_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        str(script_path),
        channel_url,
        "--db",
        str(db_path),
        "--out",
        str(out_root),
    ]
    if args.update:
        cmd.append("--update")
    if args.stop_at_known:
        cmd += [
            "--stop-at-known",
            "--stop-at-known-after",
            str(int(args.stop_at_known_after)),
            "--stop-at-known-min-scan",
            str(int(args.stop_at_known_min_scan)),
        ]
    if args.pending_only:
        cmd.append("--pending-only")
    if (args.video_id or "").strip():
        cmd += ["--video-id", (args.video_id or "").strip()]

    rc: Optional[int] = None
    err_msg: Optional[str] = None

    with log_path.open("a", encoding="utf-8") as logf:
        logf.write(f"[{now_iso()}] $ {' '.join(cmd)}\n")
        logf.flush()
        if stop_requested:
            logf.write(f"[{now_iso()}] [runner] stop requested before starting job\n")
            logf.flush()
        try:
            child = subprocess.Popen(
                cmd,
                cwd=str(script_path.resolve().parent),
                stdout=logf,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            while True:
                try:
                    rc = child.wait()
                    break
                except InterruptedError:
                    continue
        except Exception as ex:
            err_msg = str(ex)
            logf.write(f"[{now_iso()}] [runner error] {err_msg}\n")
            rc = 1

    if stop_requested:
        status = "stopped"
        err_msg = err_msg or "stopped by user"
    else:
        status = "done" if rc == 0 else "error"
    with db.session(db_path) as con:
        with con:
            con.execute(
                "UPDATE jobs SET status=?, returncode=?, finished_at=?, error_msg=? WHERE id=?",
                (status, rc, now_iso(), err_msg, job_id),
            )

    if status != "done":
        if log_path:
            try:
                with log_path.open("a", encoding="utf-8") as logf:
                    logf.write(f"[{now_iso()}] [queue] halted after {status}; not starting next job\n")
            except Exception:
                pass
        return

    next_job_id: Optional[str] = None
    # Start the next queued job (global single-job queue), only after success.
    try:
        from . import jobs as jobs_mod

        next_job_id = jobs_mod.start_next_queued_job(db_path)
    except Exception as ex:
        if log_path:
            try:
                with log_path.open("a", encoding="utf-8") as logf:
                    logf.write(f"[{now_iso()}] [queue error] {ex}\n")
            except Exception:
                pass
    else:
        if log_path and next_job_id:
            try:
                with log_path.open("a", encoding="utf-8") as logf:
                    logf.write(f"[{now_iso()}] [queue] started next job {next_job_id}\n")
            except Exception:
                pass

if __name__ == "__main__":
    main()
