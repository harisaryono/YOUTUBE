"""
Storage lifecycle guard for orchestrator.

Keeps pipeline from silently growing storage:
- blocks audio_download when uploads/audio exceeds configured cap
- blocks audio_download when downloaded-audio ASR backlog is too high
- can block all stages when runs/ or orchestrator DB exceeds budget
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .config import DEFAULT_CONFIG_PATH, load_config

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BYTES_PER_GB = 1024 ** 3
DEFAULT_ORCHESTRATOR_DB = PROJECT_ROOT / "db" / "orchestrator.db"
DEFAULT_YOUTUBE_DBS = [PROJECT_ROOT / "db" / "youtube_transcripts.db", PROJECT_ROOT / "youtube_transcripts.db"]


@dataclass
class LifecycleDecision:
    verdict: str = "RUN"
    reason_code: str = "LIFECYCLE_OK"
    reason: str = "Lifecycle budgets OK"
    recommendation: str = ""
    cooldown_seconds: int = 0


def _resolve_path(value: str | Path) -> Path:
    path = Path(str(value)).expanduser()
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    return path


def _human_size(value: int) -> str:
    size = float(max(0, int(value)))
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{int(size)} {unit}" if unit == "B" else f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"


def _dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        try:
            return int(path.stat().st_size)
        except OSError:
            return 0
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                file_path = Path(root) / name
                if file_path.is_file():
                    total += int(file_path.stat().st_size)
            except OSError:
                continue
    return total


def _sqlite_size(path: Path) -> int:
    total = 0
    for item in (path, Path(str(path) + "-wal"), Path(str(path) + "-shm")):
        try:
            if item.exists() and item.is_file():
                total += int(item.stat().st_size)
        except OSError:
            continue
    return total


def _usage(path: str | Path, max_gb: float, warn_gb: float | None = None, *, sqlite_file: bool = False) -> dict[str, Any]:
    resolved = _resolve_path(path)
    size = _sqlite_size(resolved) if sqlite_file else _dir_size(resolved)
    max_bytes = int(float(max_gb or 0) * BYTES_PER_GB)
    warn_value = float(warn_gb if warn_gb is not None else max(float(max_gb or 0) * 0.8, 0))
    warn_bytes = int(warn_value * BYTES_PER_GB)
    return {
        "path": str(resolved),
        "exists": resolved.exists(),
        "bytes": size,
        "size": _human_size(size),
        "max_gb": float(max_gb or 0),
        "over_limit": bool(max_bytes > 0 and size >= max_bytes),
        "warn_gb": warn_value,
        "over_warning": bool(warn_bytes > 0 and size >= warn_bytes),
    }


def _youtube_db_path(config: dict[str, Any]) -> Path:
    configured = str((config.get("database", {}) or {}).get("youtube_db_path", "")).strip()
    if configured:
        return _resolve_path(configured)
    for candidate in DEFAULT_YOUTUBE_DBS:
        if candidate.exists():
            return candidate
    return DEFAULT_YOUTUBE_DBS[0]


def _asr_downloaded_audio_backlog(config: dict[str, Any]) -> int:
    db_path = _youtube_db_path(config)
    if not db_path.exists():
        return 0
    conn = None
    try:
        conn = sqlite3.connect(str(db_path))
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM videos v
            JOIN video_audio_assets a ON a.video_id = v.video_id
            WHERE COALESCE(v.is_short, 0) = 0
              AND COALESCE(v.is_member_only, 0) = 0
              AND COALESCE(v.transcript_downloaded, 0) = 0
              AND v.transcript_language = 'no_subtitle'
              AND COALESCE(a.status, '') = 'downloaded'
              AND COALESCE(a.audio_file_path, '') != ''
            """
        ).fetchone()
        return int(row[0] if row else 0)
    except Exception:
        return 0
    finally:
        if conn is not None:
            conn.close()


def build_lifecycle_status(config: dict[str, Any]) -> dict[str, Any]:
    lifecycle = config.get("lifecycle", {}) or {}
    audio_cfg = config.get("audio_download", {}) or {}
    archive_cfg = config.get("log_archive", {}) or {}

    audio_max = float(audio_cfg.get("max_audio_dir_gb", lifecycle.get("max_audio_dir_gb", 5)) or 5)
    audio_warn = float(audio_cfg.get("warn_audio_dir_gb", lifecycle.get("warn_audio_dir_gb", audio_max * 0.8)) or audio_max * 0.8)
    runs_max = float(((archive_cfg.get("disk_guard", {}) or {}).get("max_runs_dir_gb", lifecycle.get("max_runs_dir_gb", 5))) or 5)
    logs_max = float(lifecycle.get("max_logs_dir_gb", 2) or 2)
    orch_max = float(lifecycle.get("max_orchestrator_db_gb", 1) or 1)

    status: dict[str, Any] = {
        "enabled": bool(lifecycle.get("enabled", True) is not False),
        "audio": _usage(audio_cfg.get("audio_dir", "uploads/audio"), audio_max, audio_warn),
        "runs": _usage("runs", runs_max, float(lifecycle.get("warn_runs_dir_gb", runs_max * 0.8) or runs_max * 0.8)),
        "logs": _usage("logs", logs_max, float(lifecycle.get("warn_logs_dir_gb", logs_max * 0.8) or logs_max * 0.8)),
        "orchestrator_db": _usage(
            lifecycle.get("orchestrator_db_path", DEFAULT_ORCHESTRATOR_DB),
            orch_max,
            float(lifecycle.get("warn_orchestrator_db_gb", orch_max * 0.8) or orch_max * 0.8),
            sqlite_file=True,
        ),
        "asr_downloaded_audio_backlog": _asr_downloaded_audio_backlog(config),
        "limits": {
            "max_asr_downloaded_audio_backlog": int(lifecycle.get("max_asr_downloaded_audio_backlog", 200) or 200),
            "block_audio_download_on_asr_backlog": bool(lifecycle.get("block_audio_download_on_asr_backlog", True) is not False),
            "block_on_runs_over_limit": bool(lifecycle.get("block_on_runs_over_limit", True) is not False),
            "block_on_logs_over_limit": bool(lifecycle.get("block_on_logs_over_limit", False) is True),
            "block_on_orchestrator_db_over_limit": bool(lifecycle.get("block_on_orchestrator_db_over_limit", True) is not False),
        },
    }
    blockers: list[str] = []
    warnings: list[str] = []
    for key in ("audio", "runs", "logs", "orchestrator_db"):
        item = status[key]
        if item["over_limit"]:
            blockers.append(f"{key} over limit: {item['size']} >= {item['max_gb']} GB")
        elif item["over_warning"]:
            warnings.append(f"{key} near limit: {item['size']} >= {item['warn_gb']} GB")
    backlog = int(status["asr_downloaded_audio_backlog"])
    max_backlog = int(status["limits"]["max_asr_downloaded_audio_backlog"])
    if max_backlog > 0 and backlog >= max_backlog:
        blockers.append(f"ASR downloaded-audio backlog high: {backlog} >= {max_backlog}")
    status["warnings"] = warnings
    status["blockers"] = blockers
    status["ok"] = not blockers
    return status


def decision_for_stage(stage: str, config: dict[str, Any]) -> LifecycleDecision:
    stage = str(stage or "").strip().lower()
    status = build_lifecycle_status(config)
    if not status.get("enabled", True):
        return LifecycleDecision("RUN", "LIFECYCLE_DISABLED", "Lifecycle guard disabled")

    if stage == "audio_download":
        audio = status["audio"]
        if audio["over_limit"]:
            return LifecycleDecision(
                "WAIT",
                "DEFER_AUDIO_CACHE_OVER_LIMIT",
                f"Audio cache over limit: {audio['size']} >= {audio['max_gb']} GB",
                "Run ASR first or clean consumed audio before downloading more audio.",
                1800,
            )
        limits = status["limits"]
        backlog = int(status["asr_downloaded_audio_backlog"])
        max_backlog = int(limits["max_asr_downloaded_audio_backlog"])
        if limits["block_audio_download_on_asr_backlog"] and max_backlog > 0 and backlog >= max_backlog:
            return LifecycleDecision(
                "WAIT",
                "DEFER_ASR_BACKLOG_TOO_HIGH",
                f"ASR downloaded-audio backlog high: {backlog} >= {max_backlog}",
                "Let ASR consume existing downloaded audio before adding more audio.",
                1800,
            )

    if status["limits"]["block_on_runs_over_limit"] and status["runs"]["over_limit"]:
        return LifecycleDecision("WAIT", "DEFER_RUNS_DIR_OVER_LIMIT", f"runs/ over limit: {status['runs']['size']}", "Run archive/janitor before launching more jobs.", 1800)
    if status["limits"]["block_on_logs_over_limit"] and status["logs"]["over_limit"]:
        return LifecycleDecision("WAIT", "DEFER_LOGS_DIR_OVER_LIMIT", f"logs/ over limit: {status['logs']['size']}", "Compact/archive logs before launching more jobs.", 1800)
    if status["limits"]["block_on_orchestrator_db_over_limit"] and status["orchestrator_db"]["over_limit"]:
        return LifecycleDecision("WAIT", "DEFER_ORCHESTRATOR_DB_OVER_LIMIT", f"orchestrator DB over limit: {status['orchestrator_db']['size']}", "Trim events/snapshots and run checkpoint/vacuum during downtime.", 1800)
    return LifecycleDecision()


def _print_json(payload: Any) -> None:
    print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Storage budget and lifecycle guard")
    parser.add_argument("--config", default=None)
    sub = parser.add_subparsers(dest="command", required=True)
    status_cmd = sub.add_parser("status")
    status_cmd.set_defaults(kind="status")
    decision_cmd = sub.add_parser("decision")
    decision_cmd.add_argument("--stage", required=True)
    decision_cmd.set_defaults(kind="decision")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = load_config(Path(args.config) if args.config else DEFAULT_CONFIG_PATH)
    if args.kind == "status":
        _print_json(build_lifecycle_status(config))
        return 0
    decision = decision_for_stage(args.stage, config)
    _print_json(asdict(decision))
    return 0 if decision.verdict == "RUN" else 75


if __name__ == "__main__":
    raise SystemExit(main())
