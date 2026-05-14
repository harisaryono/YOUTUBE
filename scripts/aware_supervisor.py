#!/usr/bin/env python3
"""Supervisor sadar-state untuk discovery, transcript, audio warmup, ASR, resume, dan format.

Ini bukan scheduler paralel agresif. Tujuannya adalah:
- membaca backlog dari SQLite,
- menjalankan stage yang relevan,
- dan menjaga setiap stage tetap kecil, terkontrol, dan resumable.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = REPO_ROOT / "youtube_transcripts.db"
DEFAULT_RUN_DIR = REPO_ROOT / "runs" / f"supervisor_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    return con


def _resolve_channel_id(con: sqlite3.Connection, value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    row = con.execute(
        """
        SELECT channel_id
        FROM channels
        WHERE channel_id = ?
           OR channel_id = ?
           OR channel_name = ?
        ORDER BY id ASC
        LIMIT 1
        """,
        (text, text.lstrip("@"), text),
    ).fetchone()
    if row:
        return str(row["channel_id"] or "").strip()
    return ""


def _count_backlog(con: sqlite3.Connection, channel_id: str = "") -> dict[str, int]:
    params: list[object] = []
    channel_clause = ""
    if channel_id:
        channel_clause = """
          AND (c.channel_id = ? OR c.channel_id = ?)
        """
        params.extend([channel_id, channel_id.lstrip("@")])
    row = con.execute(
        f"""
        SELECT
            COALESCE(SUM(CASE
                WHEN COALESCE(v.is_short, 0) = 0
                 AND COALESCE(v.is_member_only, 0) = 0
                 AND COALESCE(v.transcript_downloaded, 0) = 0
                 AND (COALESCE(v.transcript_language, '') = '' OR v.transcript_language <> 'no_subtitle')
                 AND (v.transcript_retry_after IS NULL OR datetime(v.transcript_retry_after) <= datetime('now'))
                THEN 1 ELSE 0 END), 0) AS transcript_pending,
            COALESCE(SUM(CASE
                WHEN COALESCE(v.is_short, 0) = 0
                 AND COALESCE(v.is_member_only, 0) = 0
                 AND COALESCE(v.transcript_downloaded, 0) = 0
                 AND COALESCE(v.transcript_language, '') = 'no_subtitle'
                 AND (v.transcript_retry_after IS NULL OR datetime(v.transcript_retry_after) <= datetime('now'))
                THEN 1 ELSE 0 END), 0) AS audio_pending,
            COALESCE(SUM(CASE
                WHEN COALESCE(v.is_short, 0) = 0
                 AND COALESCE(v.is_member_only, 0) = 0
                 AND COALESCE(v.transcript_downloaded, 0) = 0
                 AND COALESCE(v.transcript_language, '') = 'no_subtitle'
                 AND (v.transcript_retry_after IS NULL OR datetime(v.transcript_retry_after) <= datetime('now'))
                THEN 1 ELSE 0 END), 0) AS asr_pending,
            COALESCE(SUM(CASE
                WHEN COALESCE(v.is_short, 0) = 0
                 AND COALESCE(v.is_member_only, 0) = 0
                 AND COALESCE(v.transcript_downloaded, 0) = 1
                 AND COALESCE(v.summary_file_path, '') = ''
                THEN 1 ELSE 0 END), 0) AS resume_pending,
            COALESCE(SUM(CASE
                WHEN COALESCE(v.is_short, 0) = 0
                 AND COALESCE(v.is_member_only, 0) = 0
                 AND COALESCE(v.transcript_downloaded, 0) = 1
                 AND COALESCE(v.transcript_formatted_path, '') = ''
                THEN 1 ELSE 0 END), 0) AS format_pending,
            COUNT(*) AS total_videos
        FROM videos v
        JOIN channels c ON c.id = v.channel_id
        WHERE 1 = 1
        {channel_clause}
        """,
        params,
    ).fetchone()
    return {
        "transcript_pending": int(row["transcript_pending"] or 0),
        "audio_pending": int(row["audio_pending"] or 0),
        "asr_pending": int(row["asr_pending"] or 0),
        "resume_pending": int(row["resume_pending"] or 0),
        "format_pending": int(row["format_pending"] or 0),
        "total_videos": int(row["total_videos"] or 0),
    }


def _run_command(label: str, cmd: list[str], *, dry_run: bool) -> int:
    pretty = " ".join(shlex.quote(part) for part in cmd)
    print(f"[{label}] {pretty}")
    sys.stdout.flush()
    if dry_run:
        return 0
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT))
    if proc.returncode != 0:
        print(f"[{label}] exit={proc.returncode}")
    return int(proc.returncode)


def _build_pipeline_base_args() -> list[str]:
    return [str(REPO_ROOT / "scripts" / "run_pipeline.sh")]


def _build_wrapper(name: str) -> list[str]:
    return [str(REPO_ROOT / "scripts" / f"{name}.sh")]


def _write_state(run_dir: Path, state: dict) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "state.json").write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Awareness supervisor untuk discovery/transcript/audio/asr/resume/format")
    parser.add_argument("--run-dir", default=str(DEFAULT_RUN_DIR), help="Direktori run supervisor")
    parser.add_argument("--once", action="store_true", help="Jalankan satu siklus lalu berhenti")
    parser.add_argument("--interval-seconds", type=int, default=600, help="Jeda antar siklus jika mode loop")
    parser.add_argument("--discover-channel-limit", type=int, default=5, help="Jumlah channel per siklus discovery")
    parser.add_argument("--discover-recent-per-channel", type=int, default=50, help="Window video terbaru per channel saat discovery")
    parser.add_argument("--transcript-limit", type=int, default=100, help="Limit item transcript per siklus")
    parser.add_argument("--audio-limit", type=int, default=100, help="Limit item audio warmup per siklus")
    parser.add_argument("--asr-limit", type=int, default=100, help="Limit item ASR per siklus")
    parser.add_argument("--resume-limit", type=int, default=100, help="Limit item resume per siklus")
    parser.add_argument("--format-limit", type=int, default=100, help="Limit item format per siklus")
    parser.add_argument("--transcript-workers", type=int, default=10)
    parser.add_argument("--audio-workers", type=int, default=2)
    parser.add_argument("--asr-workers", type=int, default=2)
    parser.add_argument("--resume-workers", type=int, default=10)
    parser.add_argument("--format-workers", type=int, default=8)
    parser.add_argument("--channel-id", default="", help="Fokus satu channel")
    parser.add_argument("--channel-name", default="", help="Fokus satu channel lewat nama")
    parser.add_argument("--skip-discovery", action="store_true")
    parser.add_argument("--skip-transcript", action="store_true")
    parser.add_argument("--skip-audio", action="store_true")
    parser.add_argument("--skip-asr", action="store_true")
    parser.add_argument("--skip-resume", action="store_true")
    parser.add_argument("--skip-format", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="Cetak command tanpa menjalankannya")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    run_dir = Path(args.run_dir).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    state_path = run_dir / "state.json"

    con = _connect()
    try:
        resolved_channel_id = _resolve_channel_id(con, args.channel_id or args.channel_name)
        initial_counts = _count_backlog(con, resolved_channel_id)
    finally:
        con.close()

    print("=============================================")
    print("YouTube aware supervisor")
    print(f"Run dir: {run_dir}")
    print(f"DB: {DB_PATH}")
    print(f"Channel: {resolved_channel_id or args.channel_name or '<all>'}")
    print("=============================================")
    print(
        json.dumps(
            {
                "counts": initial_counts,
                "channel_id": resolved_channel_id,
                "channel_name": args.channel_name,
            },
            ensure_ascii=False,
            indent=2,
        )
    )

    cycles = 0
    while True:
        cycles += 1
        cycle_state: dict[str, object] = {
            "cycle": cycles,
            "ts": datetime.now().isoformat(timespec="seconds"),
            "channel_id": resolved_channel_id,
            "channel_name": args.channel_name,
        }
        con = _connect()
        try:
            counts = _count_backlog(con, resolved_channel_id)
        finally:
            con.close()
        cycle_state["counts"] = counts
        _write_state(run_dir, cycle_state)

        print("=============================================")
        print(f"Cycle {cycles}")
        print(json.dumps(counts, ensure_ascii=False, indent=2))
        print("=============================================")

        ran_any = False

        if not args.skip_discovery:
            cmd = _build_pipeline_base_args() + [
                "--discovery-only",
                "--discover-auto",
                "--all-channels",
                "--discover-channel-limit",
                str(max(1, int(args.discover_channel_limit))),
                "--discover-recent-per-channel",
                str(max(1, int(args.discover_recent_per_channel))),
            ]
            if resolved_channel_id:
                cmd = _build_pipeline_base_args() + [
                    "--discovery-only",
                    "--discover-auto",
                    "--channel-id",
                    resolved_channel_id,
                    "--discover-recent-per-channel",
                    str(max(1, int(args.discover_recent_per_channel))),
                ]
            _run_command("DISCOVERY", cmd, dry_run=args.dry_run)
            ran_any = True

        if not args.skip_transcript and counts["transcript_pending"] > 0:
            cmd = _build_wrapper("transcript") + [
                "--run-dir",
                str(run_dir / "transcript"),
                "--workers",
                str(max(1, int(args.transcript_workers))),
                "--limit",
                str(max(0, int(args.transcript_limit))),
                "--rate-limit-safe",
            ]
            if resolved_channel_id:
                cmd += ["--channel-id", resolved_channel_id]
            _run_command("TRANSCRIPT", cmd, dry_run=args.dry_run)
            ran_any = True

        if not args.skip_audio and counts["audio_pending"] > 0:
            cmd = _build_wrapper("audio") + [
                "--run-dir",
                str(run_dir / "audio"),
                "--video-workers",
                str(max(1, int(args.audio_workers))),
                "--limit",
                str(max(0, int(args.audio_limit))),
            ]
            if resolved_channel_id:
                cmd += ["--channel-id", resolved_channel_id]
            _run_command("AUDIO", cmd, dry_run=args.dry_run)
            ran_any = True

        if not args.skip_asr and counts["asr_pending"] > 0:
            cmd = _build_wrapper("asr") + [
                "--run-dir",
                str(run_dir / "asr"),
                "--video-workers",
                str(max(1, int(args.asr_workers))),
                "--limit",
                str(max(0, int(args.asr_limit))),
                "--require-cached-audio",
            ]
            if resolved_channel_id:
                cmd += ["--channel-id", resolved_channel_id]
            _run_command("ASR", cmd, dry_run=args.dry_run)
            ran_any = True

        if not args.skip_resume and counts["resume_pending"] > 0:
            cmd = _build_wrapper("resume") + [
                "--run-dir",
                str(run_dir / "resume"),
                "--max-workers",
                str(max(1, int(args.resume_workers))),
                "--nvidia-only",
            ]
            if resolved_channel_id:
                cmd += ["--channel-id", resolved_channel_id]
            if args.resume_limit and int(args.resume_limit) > 0:
                cmd += ["--limit", str(int(args.resume_limit))]
            _run_command("RESUME", cmd, dry_run=args.dry_run)
            ran_any = True

        if not args.skip_format and counts["format_pending"] > 0:
            cmd = _build_wrapper("format") + [
                "--run-dir",
                str(run_dir / "format"),
                "--workers",
                str(max(1, int(args.format_workers))),
                "--provider-plan",
                "nvidia_only",
            ]
            if resolved_channel_id:
                cmd += ["--channel-id", resolved_channel_id]
            if args.format_limit and int(args.format_limit) > 0:
                cmd += ["--limit", str(int(args.format_limit))]
            _run_command("FORMAT", cmd, dry_run=args.dry_run)
            ran_any = True

        _write_state(
            run_dir,
            {
                "cycle": cycles,
                "ts": datetime.now().isoformat(timespec="seconds"),
                "counts": counts,
                "channel_id": resolved_channel_id,
                "channel_name": args.channel_name,
                "last_run": bool(ran_any),
            },
        )

        if args.once:
            break
        if not ran_any:
            print(f"Tidak ada stage yang perlu dijalankan. Tidur {args.interval_seconds}s.")
        time.sleep(max(1, int(args.interval_seconds)))

    print("Supervisor selesai.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
