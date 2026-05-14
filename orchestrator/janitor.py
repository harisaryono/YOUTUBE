"""
Janitor — lightweight maintenance for orchestrator run artifacts and stale state.
"""

from __future__ import annotations

import shutil
import sqlite3
import time
from pathlib import Path
from typing import Any

from .state import OrchestratorState


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RUNS_DIR = PROJECT_ROOT / "runs" / "orchestrator"
REPORTS_DIR = RUNS_DIR / "reports"
LOGS_DIR = PROJECT_ROOT / "logs"
YOUTUBE_DB_PATH = PROJECT_ROOT / "db" / "youtube_transcripts.db"


def _to_seconds(days: int) -> int:
    return max(int(days), 0) * 86400


def _file_mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except FileNotFoundError:
        return 0.0


def _cleanup_files_by_age(directory: Path, older_than_seconds: int, *, preserve_patterns: tuple[str, ...] = ()) -> int:
    if not directory.exists():
        return 0
    cutoff = time.time() - max(older_than_seconds, 0)
    removed = 0
    for path in directory.glob("*"):
        if any(path.match(pattern) for pattern in preserve_patterns):
            continue
        if _file_mtime(path) < cutoff:
            try:
                if path.is_dir():
                    shutil.rmtree(path, ignore_errors=True)
                else:
                    path.unlink(missing_ok=True)
                removed += 1
            except Exception:
                continue
    return removed


def _referenced_audio_paths() -> set[str]:
    if not YOUTUBE_DB_PATH.exists():
        return set()
    try:
        conn = sqlite3.connect(str(YOUTUBE_DB_PATH))
        rows = conn.execute(
            """SELECT DISTINCT audio_file_path
               FROM video_audio_assets
               WHERE COALESCE(audio_file_path, '') != ''
                 AND COALESCE(status, '') = 'downloaded'"""
        ).fetchall()
        conn.close()
    except Exception:
        return set()
    result: set[str] = set()
    for row in rows:
        raw = str(row[0] or "").strip()
        if raw:
            result.add(str(Path(raw).resolve()))
    return result


def cleanup_audio_orphans(audio_dir: Path, older_than_seconds: int) -> int:
    """Remove audio files that are not referenced in DB and older than threshold."""
    if not audio_dir.exists():
        return 0
    referenced = _referenced_audio_paths()
    cutoff = time.time() - max(older_than_seconds, 0)
    removed = 0
    for path in audio_dir.rglob("*"):
        if not path.is_file():
            continue
        try:
            resolved = str(path.resolve())
        except Exception:
            continue
        if resolved in referenced:
            continue
        if _file_mtime(path) >= cutoff:
            continue
        try:
            path.unlink(missing_ok=True)
            removed += 1
        except Exception:
            continue
    return removed


def run_janitor(config: dict[str, Any], state: OrchestratorState) -> dict[str, Any]:
    """Run a maintenance pass and return cleanup counts."""
    janitor_cfg = config.get("janitor", {})
    if not janitor_cfg.get("enabled", True):
        return {"success": True, "skipped": True, "reason": "janitor disabled"}

    events_days = int(janitor_cfg.get("keep_events_days", 30) or 30)
    logs_days = int(janitor_cfg.get("keep_logs_days", 14) or 14)
    runs_days = int(janitor_cfg.get("keep_run_dirs_days", 7) or 7)
    reports_days = int(janitor_cfg.get("keep_reports_days", 14) or 14)
    cleanup_audio = bool(janitor_cfg.get("cleanup_audio_orphans", True))

    result = {
        "success": True,
        "events_deleted": 0,
        "log_files_deleted": 0,
        "run_dirs_deleted": 0,
        "report_files_deleted": 0,
        "audio_orphans_deleted": 0,
    }

    try:
        result["events_deleted"] = state.cleanup_old_events(days=events_days)
    except Exception as exc:
        state.add_event(
            event_type="janitor",
            message=f"Janitor failed deleting old events: {exc}",
            severity="warning",
        )
        result["success"] = False

    try:
        if LOGS_DIR.exists():
            result["log_files_deleted"] = _cleanup_files_by_age(LOGS_DIR, _to_seconds(logs_days))
    except Exception as exc:
        state.add_event(
            event_type="janitor",
            message=f"Janitor failed cleaning logs: {exc}",
            severity="warning",
        )
        result["success"] = False

    try:
        if RUNS_DIR.exists():
            cutoff = time.time() - _to_seconds(runs_days)
            for path in RUNS_DIR.iterdir():
                if path.name == "reports":
                    continue
                if _file_mtime(path) < cutoff:
                    if path.is_dir():
                        shutil.rmtree(path, ignore_errors=True)
                    else:
                        path.unlink(missing_ok=True)
                    result["run_dirs_deleted"] += 1
    except Exception as exc:
        state.add_event(
            event_type="janitor",
            message=f"Janitor failed cleaning run dirs: {exc}",
            severity="warning",
        )
        result["success"] = False

    try:
        if REPORTS_DIR.exists():
            cutoff = time.time() - _to_seconds(reports_days)
            for path in REPORTS_DIR.glob("report_*.json"):
                if _file_mtime(path) < cutoff:
                    path.unlink(missing_ok=True)
                    result["report_files_deleted"] += 1
    except Exception as exc:
        state.add_event(
            event_type="janitor",
            message=f"Janitor failed cleaning reports: {exc}",
            severity="warning",
        )
        result["success"] = False

    try:
        if cleanup_audio:
            audio_dir_cfg = str(config.get("audio_download", {}).get("audio_dir", "uploads/audio") or "uploads/audio")
            audio_dir = Path(audio_dir_cfg)
            if not audio_dir.is_absolute():
                audio_dir = PROJECT_ROOT / audio_dir
            result["audio_orphans_deleted"] = cleanup_audio_orphans(audio_dir, _to_seconds(runs_days))
    except Exception as exc:
        state.add_event(
            event_type="janitor",
            message=f"Janitor failed cleaning audio orphans: {exc}",
            severity="warning",
        )
        result["success"] = False

    state.set("janitor_last_run_at", str(int(time.time())))
    state.add_event(
        event_type="janitor",
        message=(
            "Janitor completed: "
            f"events={result['events_deleted']}, "
            f"logs={result['log_files_deleted']}, "
            f"run_dirs={result['run_dirs_deleted']}, "
            f"reports={result['report_files_deleted']}, "
            f"audio_orphans={result['audio_orphans_deleted']}"
        ),
        severity="info" if result["success"] else "warning",
        payload=result,
    )
    return result


def janitor_due(config: dict[str, Any], state: OrchestratorState) -> bool:
    """Return True when janitor should run again."""
    janitor_cfg = config.get("janitor", {})
    if not janitor_cfg.get("enabled", True):
        return False
    interval_minutes = int(janitor_cfg.get("interval_minutes", 60) or 60)
    last_run = state.get_int("janitor_last_run_at", 0)
    if last_run <= 0:
        return True
    return (int(time.time()) - last_run) >= (interval_minutes * 60)
