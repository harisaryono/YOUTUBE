"""
Janitor — lightweight maintenance for orchestrator artifacts and stale state.

Stage 19 changes:
- Raw run directories are no longer deleted blindly by age.
- Daily log archive is run first when due.
- Raw logs/run dirs are compressed/deleted only through log_archive retention,
  which requires a .log_archive.json marker.
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


def _cleanup_archive_files(config: dict[str, Any]) -> dict[str, int]:
    """Clean old compact archive outputs; raw run dirs are handled by log_archive."""
    archive_cfg = config.get("log_archive", {}) or {}
    archive_dir_raw = str(archive_cfg.get("archive_dir", "logs/archive") or "logs/archive")
    archive_dir = Path(archive_dir_raw)
    if not archive_dir.is_absolute():
        archive_dir = PROJECT_ROOT / archive_dir
    retention = archive_cfg.get("archive_retention", {}) or {}
    md_days = int(retention.get("markdown_days", archive_cfg.get("keep_daily_markdown_days", 180)) or 180)
    jsonl_days = int(retention.get("jsonl_days", archive_cfg.get("keep_jsonl_days", 365)) or 365)
    incidents_days = int(retention.get("incidents_json_days", archive_cfg.get("keep_incidents_json_days", 365)) or 365)
    result = {"archive_markdown_deleted": 0, "archive_jsonl_deleted": 0, "archive_incidents_deleted": 0}
    if not archive_dir.exists():
        return result
    result["archive_markdown_deleted"] = _cleanup_files_by_age(archive_dir, _to_seconds(md_days), preserve_patterns=("reports",))
    # The generic markdown cleanup above also sees json/jsonl files if they are older.
    # Run targeted cleanup for formats with longer retention after that pass.
    for pattern, days, key in (
        ("*.jsonl", jsonl_days, "archive_jsonl_deleted"),
        ("*-incidents.json", incidents_days, "archive_incidents_deleted"),
    ):
        cutoff = time.time() - _to_seconds(days)
        for path in archive_dir.glob(pattern):
            if _file_mtime(path) < cutoff:
                try:
                    path.unlink(missing_ok=True)
                    result[key] += 1
                except Exception:
                    continue
    return result


def run_janitor(config: dict[str, Any], state: OrchestratorState) -> dict[str, Any]:
    """Run a maintenance pass and return cleanup/archive counts."""
    janitor_cfg = config.get("janitor", {})
    if not janitor_cfg.get("enabled", True):
        return {"success": True, "skipped": True, "reason": "janitor disabled"}

    events_days = int(janitor_cfg.get("keep_events_days", 30) or 30)
    reports_days = int(janitor_cfg.get("keep_reports_days", 14) or 14)
    cleanup_audio = bool(janitor_cfg.get("cleanup_audio_orphans", True))

    result: dict[str, Any] = {
        "success": True,
        "events_deleted": 0,
        "log_files_deleted": 0,
        "run_dirs_deleted": 0,
        "report_files_deleted": 0,
        "audio_orphans_deleted": 0,
        "daily_archive": {},
        "archive_prune": {},
        "archive_files_deleted": {},
        "log_compact": {},
        "chunk_clean": {},
    }

    # Build compact daily archive first, then prune only raw logs that have been
    # marked archived. This prevents accidental deletion of unarchived evidence.
    try:
        from .log_archive import log_archive_due, prune_archived_raw_logs, run_daily_archive

        if log_archive_due(config, state):
            result["daily_archive"] = run_daily_archive(config, state, prune=False)
        result["archive_prune"] = prune_archived_raw_logs(config, state)
        result["log_files_deleted"] = int(result["archive_prune"].get("compressed", 0) or 0)
        result["run_dirs_deleted"] = int(result["archive_prune"].get("run_dirs_deleted", 0) or 0)
    except Exception as exc:
        state.add_event(
            event_type="janitor",
            message=f"Janitor failed running log archive/prune: {exc}",
            severity="warning",
        )
        result["success"] = False

    if bool(janitor_cfg.get("auto_compact_logs", True)):
        try:
            from .log_compact import compact_raw_logs

            compact_hours = float(janitor_cfg.get("compact_older_than_hours", 0.5) or 0.5)
            result["log_compact"] = compact_raw_logs(
                config,
                state,
                older_than_hours=compact_hours,
                include_unarchived=True,
                min_size_kb=16,
            )
        except Exception as exc:
            state.add_event(
                event_type="janitor",
                message=f"Janitor failed running log compact: {exc}",
                severity="warning",
            )
            result["success"] = False

    if bool(janitor_cfg.get("auto_clean_chunks", True)):
        try:
            from .asr_chunk_cleaner import clean_transcribed_chunks

            chunk_hours = float(janitor_cfg.get("chunk_clean_older_than_hours", 2) or 2)
            result["chunk_clean"] = clean_transcribed_chunks(older_than_hours=chunk_hours)
        except Exception as exc:
            state.add_event(
                event_type="janitor",
                message=f"Janitor failed running chunk cleaner: {exc}",
                severity="warning",
            )
            result["success"] = False

    try:
        result["events_deleted"] = state.cleanup_old_events(days=events_days)
    except Exception as exc:
        state.add_event(event_type="janitor", message=f"Janitor failed deleting old events: {exc}", severity="warning")
        result["success"] = False

    try:
        if REPORTS_DIR.exists():
            cutoff = time.time() - _to_seconds(reports_days)
            for path in REPORTS_DIR.glob("report_*.json"):
                if _file_mtime(path) < cutoff:
                    path.unlink(missing_ok=True)
                    result["report_files_deleted"] += 1
    except Exception as exc:
        state.add_event(event_type="janitor", message=f"Janitor failed cleaning reports: {exc}", severity="warning")
        result["success"] = False

    try:
        result["archive_files_deleted"] = _cleanup_archive_files(config)
    except Exception as exc:
        state.add_event(event_type="janitor", message=f"Janitor failed cleaning archive files: {exc}", severity="warning")
        result["success"] = False

    try:
        if cleanup_audio:
            audio_dir_cfg = str(config.get("audio_download", {}).get("audio_dir", "uploads/audio") or "uploads/audio")
            audio_dir = Path(audio_dir_cfg)
            if not audio_dir.is_absolute():
                audio_dir = PROJECT_ROOT / audio_dir
            runs_days = int(janitor_cfg.get("keep_run_dirs_days", 7) or 7)
            result["audio_orphans_deleted"] = cleanup_audio_orphans(audio_dir, _to_seconds(runs_days))
    except Exception as exc:
        state.add_event(event_type="janitor", message=f"Janitor failed cleaning audio orphans: {exc}", severity="warning")
        result["success"] = False

    state.set("janitor_last_run_at", str(int(time.time())))

    compact_info = result.get("log_compact") or {}
    chunk_info = result.get("chunk_clean") or {}
    state.add_event(
        event_type="janitor",
        message=(
            "Janitor completed: "
            f"events={result['events_deleted']}, "
            f"compressed_logs={result['log_files_deleted']}, "
            f"compact_new={compact_info.get('compressed', 0)}, "
            f"run_dirs={result['run_dirs_deleted']}, "
            f"reports={result['report_files_deleted']}, "
            f"audio_orphans={result['audio_orphans_deleted']}, "
            f"chunks_deleted={chunk_info.get('deleted', 0)}"
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
