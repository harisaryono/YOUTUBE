#!/usr/bin/env python3
"""Persistent job tracker for admin-visible background jobs."""

from __future__ import annotations

import argparse
import json
import secrets
from datetime import datetime
from pathlib import Path

from database_optimized import OptimizedDatabase


REPO_ROOT = Path(__file__).resolve().parent
DB_PATH = REPO_ROOT / "youtube_transcripts.db"
BASE_DIR = REPO_ROOT / "uploads"


def _db() -> OptimizedDatabase:
    return OptimizedDatabase(str(DB_PATH), str(BASE_DIR))


def _now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _default_job_id(job_type: str) -> str:
    return f"{job_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{secrets.token_hex(3)}"


def start_job(args: argparse.Namespace) -> str:
    job_id = args.job_id or _default_job_id(args.job_type)
    db = _db()
    try:
        db.upsert_admin_job(
            job_id,
            args.job_type,
            args.status,
            source=args.source,
            pid=args.pid,
            command=args.command,
            log_path=args.log_path,
            run_dir=args.run_dir,
            target_channel_id=args.target_channel_id,
            target_video_id=args.target_video_id,
            started_at=args.started_at or _now_ts(),
        )
    finally:
        db.close()
    return job_id


def finish_job(args: argparse.Namespace) -> None:
    db = _db()
    try:
        db.upsert_admin_job(
            args.job_id,
            args.job_type,
            args.status,
            source=args.source,
            pid=args.pid,
            command=args.command,
            log_path=args.log_path,
            run_dir=args.run_dir,
            target_channel_id=args.target_channel_id,
            target_video_id=args.target_video_id,
            exit_code=args.exit_code,
            error_message=args.error_message,
            started_at=args.started_at,
            finished_at=args.finished_at or _now_ts(),
        )
    finally:
        db.close()


def list_jobs(args: argparse.Namespace) -> None:
    db = _db()
    try:
        jobs = db.list_admin_jobs(limit=args.limit, status=args.status)
    finally:
        db.close()
    print(json.dumps(jobs, ensure_ascii=False, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Track background jobs in youtube_transcripts.db")
    subparsers = parser.add_subparsers(dest="action", required=True)

    common = {
        "job_id": {"help": "Stable job id", "default": ""},
        "job_type": {"help": "Job type label"},
        "status": {"help": "Job status", "default": "running"},
        "source": {"help": "Job source label", "default": "wrapper"},
        "pid": {"help": "Worker PID", "type": int},
        "command": {"help": "Command string", "default": ""},
        "log_path": {"help": "Log file path", "default": ""},
        "run_dir": {"help": "Run directory", "default": ""},
        "target_channel_id": {"help": "Target channel id", "default": ""},
        "target_video_id": {"help": "Target video id", "default": ""},
    }

    start = subparsers.add_parser("start", help="Insert or update a running job")
    for name, kwargs in common.items():
        start.add_argument(f"--{name.replace('_', '-')}", **kwargs)
    start.add_argument("--started-at", dest="started_at", default="")

    finish = subparsers.add_parser("finish", help="Mark a job as finished")
    for name, kwargs in common.items():
        finish.add_argument(f"--{name.replace('_', '-')}", **kwargs)
    finish.add_argument("--exit-code", dest="exit_code", type=int)
    finish.add_argument("--error-message", dest="error_message", default="")
    finish.add_argument("--finished-at", dest="finished_at", default="")

    listing = subparsers.add_parser("list", help="Print recent jobs")
    listing.add_argument("--limit", type=int, default=20)
    listing.add_argument("--status", default="")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.action == "start":
        args.job_id = args.job_id or ""
        args.command = args.command or ""
        args.log_path = args.log_path or ""
        args.run_dir = args.run_dir or ""
        args.target_channel_id = args.target_channel_id or ""
        args.target_video_id = args.target_video_id or ""
        args.started_at = getattr(args, "started_at", "") or ""
        job_id = start_job(args)
        print(job_id)
        return 0

    if args.action == "finish":
        args.job_id = args.job_id or ""
        args.command = args.command or ""
        args.log_path = args.log_path or ""
        args.run_dir = args.run_dir or ""
        args.target_channel_id = args.target_channel_id or ""
        args.target_video_id = args.target_video_id or ""
        args.started_at = getattr(args, "started_at", "") or ""
        args.error_message = args.error_message or ""
        args.finished_at = args.finished_at or ""
        finish_job(args)
        return 0

    if args.action == "list":
        args.status = args.status or None
        list_jobs(args)
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
