"""
Daily Log Archive — compact daily summaries and retention for raw run logs.

The archive is intentionally read-mostly and conservative:
- Build human-readable Markdown and machine-readable JSONL/JSON summaries.
- Copy report CSV files into the archive before raw run directories are pruned.
- Mark run directories as archived before compression/deletion is allowed.
- Keep raw logs for different lengths depending on outcome:
  success < normal failure < important incident.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import shutil
import sqlite3
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .config import DEFAULT_CONFIG_PATH, load_config
from .error_analyzer import classify_error
from .state import OrchestratorState


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RUNS_DIR = PROJECT_ROOT / "runs" / "orchestrator"
DEFAULT_ARCHIVE_DIR = PROJECT_ROOT / "logs" / "archive"

SUCCESS_STATUSES = {
    "ok",
    "success",
    "done",
    "skipped",
    "downloaded",
    "audio_downloaded",
    "audio_cached",
    "completed",
    "processed",
    "formatted",
}

IMPORTANT_INCIDENT_ERROR_TYPES = {
    "youtube_geo_blocked",
    "youtube_bot_detection",
    "youtube_ip_blocked",
    "youtube_429",
    "youtube_403",
    "youtube_signin_required",
    "coordinator_unavailable",
    "provider_quota_exceeded",
    "provider_auth_error",
    "provider_429",
    "nvidia_riva_degraded",
    "nvidia_riva_rpc_error",
    "asr_provider_unavailable",
    "memory_low",
    "disk_low",
}

IMPORTANT_EVENT_TYPES = {
    "error",
    "dispatch_failure",
    "timeout",
    "terminal_failure",
    "deferred",
    "control.quarantine_channel",
    "control.unquarantine_channel",
    "control.pause_stage",
    "control.pause_group",
    "control.resume_stage",
    "control.resume_group",
    "retry_drain",
    "safety.emergency_stop.enabled",
    "safety.emergency_stop.cleared",
    "safety.launch_blocked",
}


def _jakarta_today() -> date:
    return (datetime.now(timezone.utc) + timedelta(hours=7)).date()


def _date_str(value: str | date | None = None) -> str:
    if value is None or str(value).strip().lower() in {"", "today"}:
        return _jakarta_today().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    datetime.strptime(text, "%Y-%m-%d")
    return text


def _archive_cfg(config: dict[str, Any]) -> dict[str, Any]:
    cfg = dict(config.get("log_archive", {}) or {})
    cfg.setdefault("enabled", True)
    cfg.setdefault("archive_dir", "logs/archive")
    cfg.setdefault("timezone", "Asia/Jakarta")
    cfg.setdefault("run_hour", 23)
    cfg.setdefault("run_minute", 55)
    cfg.setdefault("copy_report_csv", True)
    cfg.setdefault("keep_daily_markdown_days", 180)
    cfg.setdefault("keep_jsonl_days", 365)
    cfg.setdefault("keep_incidents_json_days", 365)
    cfg.setdefault("raw_log_retention", {})
    cfg["raw_log_retention"].setdefault("success_days", 3)
    cfg["raw_log_retention"].setdefault("normal_failure_days", 14)
    cfg["raw_log_retention"].setdefault("incident_days", 30)
    cfg.setdefault("report_retention", {})
    cfg["report_retention"].setdefault("csv_days", 90)
    cfg.setdefault("compression", {})
    cfg["compression"].setdefault("enabled", True)
    cfg["compression"].setdefault("compress_after_days", 1)
    cfg["compression"].setdefault("delete_original_after_compress", True)
    cfg.setdefault("tail_capture", {})
    cfg["tail_capture"].setdefault("success_lines", 20)
    cfg["tail_capture"].setdefault("normal_failure_lines", 150)
    cfg["tail_capture"].setdefault("incident_lines", 300)
    cfg.setdefault("disk_guard", {})
    cfg["disk_guard"].setdefault("max_runs_dir_gb", 5)
    cfg["disk_guard"].setdefault("min_free_disk_gb", 3)
    cfg["disk_guard"].setdefault("emergency_delete_success_logs_first", True)
    return cfg


def _archive_dir(config: dict[str, Any]) -> Path:
    raw = str(_archive_cfg(config).get("archive_dir") or "logs/archive")
    path = Path(raw)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    path.mkdir(parents=True, exist_ok=True)
    return path


def _connect(state: OrchestratorState) -> sqlite3.Connection:
    # The state object intentionally centralizes DB path/connection setup.
    return state._connect()  # noqa: SLF001 - internal orchestration utility


def _safe_json(raw: str) -> dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _rows_for_date(state: OrchestratorState, table: str, column: str, target_date: str) -> list[dict[str, Any]]:
    conn = _connect(state)
    rows = conn.execute(
        f"SELECT * FROM {table} WHERE date({column}) = date(?) ORDER BY {column} ASC",
        (target_date,),
    ).fetchall()
    return [dict(row) for row in rows]


def _jobs_for_date(state: OrchestratorState, target_date: str) -> list[dict[str, Any]]:
    conn = _connect(state)
    rows = conn.execute(
        """
        SELECT *
        FROM orchestrator_active_jobs
        WHERE date(started_at) = date(?)
           OR date(COALESCE(finished_at, updated_at)) = date(?)
        ORDER BY started_at ASC
        """,
        (target_date, target_date),
    ).fetchall()
    return [dict(row) for row in rows]


def _report_candidates(run_dir: str | Path, stage: str) -> list[Path]:
    run_path = Path(run_dir)
    stage = str(stage or "").strip().lower()
    names_by_stage = {
        "transcript": ["recover_report.csv"],
        "audio_download": ["audio_download_report.csv", "report.csv"],
        "asr": ["recover_asr_report.csv", "report.csv"],
        "resume": ["report.csv", "resume_report.csv"],
        "format": ["report.csv", "format_report.csv"],
    }
    return [run_path / name for name in names_by_stage.get(stage, ["report.csv"]) if (run_path / name).exists()]


def _read_report_counts(report_path: Path) -> dict[str, Any]:
    status_counts: Counter[str] = Counter()
    error_counts: Counter[str] = Counter()
    sample_errors: dict[str, list[dict[str, str]]] = defaultdict(list)
    total = 0
    if not report_path.exists():
        return {"total": 0, "status_counts": {}, "error_counts": {}, "sample_errors": {}}
    try:
        with report_path.open(newline="", encoding="utf-8", errors="ignore") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                total += 1
                status = str(row.get("status") or "unknown").strip().lower() or "unknown"
                status_counts[status] += 1
                if status in SUCCESS_STATUSES:
                    continue
                error_text = str(row.get("error") or row.get("message") or row.get("error_text") or status).strip()
                classification = classify_error(error_text or status)
                error_counts[classification.error_type] += 1
                samples = sample_errors[classification.error_type]
                if len(samples) < 5:
                    samples.append(
                        {
                            "video_id": str(row.get("video_id") or row.get("id") or "").strip(),
                            "channel_id": str(row.get("channel_id") or "").strip(),
                            "message": error_text[:300],
                        }
                    )
    except Exception as exc:
        error_counts["report_parse_error"] += 1
        sample_errors["report_parse_error"].append({"message": str(exc)[:300]})
    return {
        "total": total,
        "status_counts": dict(status_counts),
        "error_counts": dict(error_counts),
        "sample_errors": dict(sample_errors),
    }


def _tail_lines(path: Path, limit: int) -> list[str]:
    if limit <= 0 or not path.exists() or not path.is_file():
        return []
    lines: list[str] = []
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                lines.append(line.rstrip("\n"))
                if len(lines) > limit:
                    lines = lines[-limit:]
    except Exception as exc:
        return [f"<failed to read tail: {exc}>"]
    return lines[-limit:]


def _job_error_types(job: dict[str, Any], reports: list[dict[str, Any]]) -> set[str]:
    result: set[str] = set()
    for report in reports:
        result.update(str(k) for k in (report.get("error_counts") or {}).keys())
    error_text = str(job.get("error_text") or "").strip()
    if error_text:
        result.add(classify_error(error_text, int(job.get("returncode") or 0)).error_type)
    return result


def _job_category(job: dict[str, Any], reports: list[dict[str, Any]]) -> str:
    status = str(job.get("status") or "").strip().lower()
    error_types = _job_error_types(job, reports)
    if error_types & IMPORTANT_INCIDENT_ERROR_TYPES:
        return "incident"
    if status == "completed" or int(job.get("returncode") or 0) == 0:
        if any((report.get("error_counts") or {}) for report in reports):
            return "normal_failure"
        return "success"
    if status in {"failed", "timeout", "cancelled"}:
        return "normal_failure"
    return "success"


def _tail_limit_for_category(config: dict[str, Any], category: str) -> int:
    tails = _archive_cfg(config).get("tail_capture", {}) or {}
    if category == "incident":
        return int(tails.get("incident_lines", 300) or 300)
    if category == "normal_failure":
        return int(tails.get("normal_failure_lines", 150) or 150)
    return int(tails.get("success_lines", 20) or 20)


def _copy_reports(config: dict[str, Any], target_date: str, job: dict[str, Any], report_paths: list[Path]) -> list[str]:
    cfg = _archive_cfg(config)
    if not cfg.get("copy_report_csv", True):
        return []
    copied: list[str] = []
    if not report_paths:
        return copied
    dest_dir = _archive_dir(config) / "reports" / target_date
    dest_dir.mkdir(parents=True, exist_ok=True)
    run_name = Path(str(job.get("run_dir") or "run")).name
    for report_path in report_paths:
        if not report_path.exists():
            continue
        dest = dest_dir / f"{run_name}__{report_path.name}"
        try:
            shutil.copy2(report_path, dest)
            copied.append(str(dest.relative_to(PROJECT_ROOT)))
        except Exception:
            continue
    return copied


def _mark_run_dir(job: dict[str, Any], *, category: str, target_date: str, archive_files: dict[str, str]) -> None:
    run_dir = Path(str(job.get("run_dir") or ""))
    if not run_dir.exists() or not run_dir.is_dir():
        return
    marker = run_dir / ".log_archive.json"
    payload = {
        "archived": True,
        "archived_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "archive_date": target_date,
        "category": category,
        "job_id": str(job.get("job_id") or ""),
        "stage": str(job.get("stage") or ""),
        "archive_files": archive_files,
    }
    try:
        marker.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def build_daily_archive_payload(config: dict[str, Any], state: OrchestratorState, target_date: str | date | None = None) -> dict[str, Any]:
    target = _date_str(target_date)
    jobs = _jobs_for_date(state, target)
    events = _rows_for_date(state, "orchestrator_events", "created_at", target)

    job_summaries: list[dict[str, Any]] = []
    stage_totals: dict[str, Counter[str]] = defaultdict(Counter)
    error_totals: Counter[str] = Counter()
    incident_groups: dict[tuple[str, str, str], dict[str, Any]] = {}

    for job in jobs:
        stage = str(job.get("stage") or "").strip().lower()
        run_dir = Path(str(job.get("run_dir") or ""))
        report_paths = _report_candidates(run_dir, stage)
        reports = [{"path": str(path), **_read_report_counts(path)} for path in report_paths]
        category = _job_category(job, reports)
        copied_reports = _copy_reports(config, target, job, report_paths)
        tail_path = Path(str(job.get("log_path") or run_dir / "stdout_stderr.log"))
        tail = _tail_lines(tail_path, _tail_limit_for_category(config, category)) if category != "success" else []

        status = str(job.get("status") or "unknown")
        stage_totals[stage][status] += 1
        for report in reports:
            for key, value in (report.get("status_counts") or {}).items():
                stage_totals[stage][f"report_status:{key}"] += int(value)
            for key, value in (report.get("error_counts") or {}).items():
                error_totals[key] += int(value)
                group_key = (stage, str(job.get("scope") or ""), key)
                item = incident_groups.setdefault(
                    group_key,
                    {
                        "stage": stage,
                        "scope": str(job.get("scope") or ""),
                        "error_type": key,
                        "count": 0,
                        "sample_jobs": [],
                        "sample_errors": [],
                        "category": "incident" if key in IMPORTANT_INCIDENT_ERROR_TYPES else "normal_failure",
                    },
                )
                item["count"] += int(value)
                if len(item["sample_jobs"]) < 5:
                    item["sample_jobs"].append(str(job.get("job_id") or ""))
                for sample in (report.get("sample_errors") or {}).get(key, []):
                    if len(item["sample_errors"]) < 5:
                        item["sample_errors"].append(sample)

        summary = {
            "job_id": str(job.get("job_id") or ""),
            "stage": stage,
            "scope": str(job.get("scope") or ""),
            "group_name": str(job.get("group_name") or ""),
            "status": status,
            "returncode": job.get("returncode"),
            "category": category,
            "started_at": str(job.get("started_at") or ""),
            "finished_at": str(job.get("finished_at") or ""),
            "run_dir": str(job.get("run_dir") or ""),
            "log_path": str(job.get("log_path") or ""),
            "reports": reports,
            "copied_reports": copied_reports,
            "tail": tail,
        }
        job_summaries.append(summary)

    important_events: list[dict[str, Any]] = []
    for event in events:
        payload = _safe_json(str(event.get("payload_json") or ""))
        event_type = str(event.get("event_type") or "")
        error_type = str(payload.get("error_type") or payload.get("reason_code") or "")
        if event_type in IMPORTANT_EVENT_TYPES or error_type in IMPORTANT_INCIDENT_ERROR_TYPES or str(event.get("severity") or "") in {"blocking", "warning"}:
            important_events.append({**event, "payload": payload})
            if error_type:
                group_key = (str(event.get("stage") or ""), str(event.get("scope") or ""), error_type)
                item = incident_groups.setdefault(
                    group_key,
                    {
                        "stage": str(event.get("stage") or ""),
                        "scope": str(event.get("scope") or ""),
                        "error_type": error_type,
                        "count": 0,
                        "sample_jobs": [],
                        "sample_errors": [],
                        "category": "incident" if error_type in IMPORTANT_INCIDENT_ERROR_TYPES else "normal_failure",
                    },
                )
                item["count"] += 1
                if len(item["sample_errors"]) < 5:
                    item["sample_errors"].append({"message": str(event.get("message") or "")[:300]})

    payload = {
        "date": target,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "jobs": job_summaries,
        "stage_totals": {stage: dict(counter) for stage, counter in stage_totals.items()},
        "error_totals": dict(error_totals),
        "important_events": important_events,
        "incidents": sorted(incident_groups.values(), key=lambda x: (x.get("category") != "incident", x.get("stage", ""), x.get("error_type", ""))),
        "summary": {
            "job_count": len(job_summaries),
            "completed_jobs": sum(1 for j in job_summaries if j["status"] == "completed"),
            "failed_jobs": sum(1 for j in job_summaries if j["status"] in {"failed", "timeout", "cancelled"}),
            "incident_jobs": sum(1 for j in job_summaries if j["category"] == "incident"),
            "important_event_count": len(important_events),
            "incident_group_count": len(incident_groups),
        },
    }
    return payload


def _format_counts(counts: dict[str, Any]) -> str:
    if not counts:
        return "- none"
    lines = []
    for key, value in sorted(counts.items(), key=lambda item: str(item[0])):
        lines.append(f"- {key}: {value}")
    return "\n".join(lines)


def render_markdown(payload: dict[str, Any]) -> str:
    lines: list[str] = []
    summary = payload.get("summary", {}) or {}
    lines.append(f"# Orchestrator Daily Archive — {payload.get('date', '')}")
    lines.append("")
    lines.append("## Executive Summary")
    lines.append("")
    lines.append(f"- Jobs: {summary.get('job_count', 0)}")
    lines.append(f"- Completed: {summary.get('completed_jobs', 0)}")
    lines.append(f"- Failed/timeout/cancelled: {summary.get('failed_jobs', 0)}")
    lines.append(f"- Incident jobs: {summary.get('incident_jobs', 0)}")
    lines.append(f"- Important events: {summary.get('important_event_count', 0)}")
    lines.append(f"- Incident groups: {summary.get('incident_group_count', 0)}")
    lines.append("")

    lines.append("## Stage Totals")
    lines.append("")
    for stage, counts in sorted((payload.get("stage_totals") or {}).items()):
        lines.append(f"### {stage}")
        lines.append("")
        lines.append(_format_counts(counts))
        lines.append("")

    lines.append("## Error Totals")
    lines.append("")
    lines.append(_format_counts(payload.get("error_totals") or {}))
    lines.append("")

    lines.append("## Incidents and Notable Problems")
    lines.append("")
    incidents = payload.get("incidents") or []
    if not incidents:
        lines.append("No incidents recorded.")
        lines.append("")
    for idx, incident in enumerate(incidents, start=1):
        lines.append(f"### Incident {idx} — {incident.get('error_type', 'unknown')}")
        lines.append("")
        lines.append(f"- Stage: {incident.get('stage', '')}")
        lines.append(f"- Scope: {incident.get('scope', '')}")
        lines.append(f"- Count: {incident.get('count', 0)}")
        lines.append(f"- Category: {incident.get('category', '')}")
        samples = incident.get("sample_errors") or []
        if samples:
            lines.append("- Samples:")
            for sample in samples[:5]:
                msg = str(sample.get("message") or "").replace("\n", " ")[:220]
                vid = str(sample.get("video_id") or "").strip()
                prefix = f"video={vid} " if vid else ""
                lines.append(f"  - {prefix}{msg}")
        lines.append("")

    lines.append("## Job Batches")
    lines.append("")
    for job in payload.get("jobs") or []:
        lines.append(f"### {job.get('job_id', '')}")
        lines.append("")
        lines.append(f"- Stage: {job.get('stage', '')}")
        lines.append(f"- Scope: {job.get('scope', '')}")
        lines.append(f"- Status: {job.get('status', '')}")
        lines.append(f"- Category: {job.get('category', '')}")
        lines.append(f"- Run dir: `{job.get('run_dir', '')}`")
        if job.get("copied_reports"):
            lines.append("- Archived reports:")
            for report in job.get("copied_reports") or []:
                lines.append(f"  - `{report}`")
        for report in job.get("reports") or []:
            lines.append(f"- Report: `{report.get('path', '')}`")
            status_counts = report.get("status_counts") or {}
            error_counts = report.get("error_counts") or {}
            if status_counts:
                lines.append("  - Status counts:")
                for key, value in sorted(status_counts.items()):
                    lines.append(f"    - {key}: {value}")
            if error_counts:
                lines.append("  - Error counts:")
                for key, value in sorted(error_counts.items()):
                    lines.append(f"    - {key}: {value}")
        tail = job.get("tail") or []
        if tail:
            lines.append("- Log tail:")
            lines.append("```text")
            lines.extend(str(line) for line in tail[-80:])
            lines.append("```")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_daily_archive(config: dict[str, Any], state: OrchestratorState, target_date: str | date | None = None) -> dict[str, Any]:
    target = _date_str(target_date)
    payload = build_daily_archive_payload(config, state, target)
    archive_dir = _archive_dir(config)
    md_path = archive_dir / f"{target}.md"
    jsonl_path = archive_dir / f"{target}.jsonl"
    incidents_path = archive_dir / f"{target}-incidents.json"
    payload_path = archive_dir / f"{target}.json"

    md_path.write_text(render_markdown(payload), encoding="utf-8")
    payload_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    incidents_path.write_text(json.dumps(payload.get("incidents") or [], indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    with jsonl_path.open("w", encoding="utf-8") as fh:
        fh.write(json.dumps({"type": "daily_summary", "date": target, **payload.get("summary", {})}, ensure_ascii=False) + "\n")
        for stage, counts in (payload.get("stage_totals") or {}).items():
            fh.write(json.dumps({"type": "stage_summary", "date": target, "stage": stage, "counts": counts}, ensure_ascii=False) + "\n")
        for incident in payload.get("incidents") or []:
            fh.write(json.dumps({"type": "incident", "date": target, **incident}, ensure_ascii=False) + "\n")
        for event in payload.get("important_events") or []:
            fh.write(json.dumps({"type": "event", "date": target, **event}, ensure_ascii=False, default=str) + "\n")

    archive_files = {
        "markdown": str(md_path.relative_to(PROJECT_ROOT)),
        "json": str(payload_path.relative_to(PROJECT_ROOT)),
        "jsonl": str(jsonl_path.relative_to(PROJECT_ROOT)),
        "incidents": str(incidents_path.relative_to(PROJECT_ROOT)),
    }
    for job in payload.get("jobs") or []:
        _mark_run_dir(job, category=str(job.get("category") or "success"), target_date=target, archive_files=archive_files)

    state.set(f"log_archive:date:{target}", json.dumps({"archive_files": archive_files, "summary": payload.get("summary", {})}, ensure_ascii=False))
    state.set("log_archive:last_run_date", target)
    state.set("log_archive:last_run_at", datetime.now(timezone.utc).isoformat(timespec="seconds"))
    state.add_event(
        event_type="log_archive",
        message=f"Daily log archive written for {target}: jobs={payload['summary']['job_count']} incidents={payload['summary']['incident_group_count']}",
        severity="info",
        payload={"date": target, "archive_files": archive_files, "summary": payload.get("summary", {})},
    )
    return {"success": True, "date": target, "archive_files": archive_files, "summary": payload.get("summary", {})}


def _load_marker(run_dir: Path) -> dict[str, Any]:
    marker = run_dir / ".log_archive.json"
    if not marker.exists():
        return {}
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _age_days(path: Path) -> float:
    try:
        return (time.time() - path.stat().st_mtime) / 86400
    except FileNotFoundError:
        return 0.0


def _compress_log(path: Path, *, delete_original: bool = True) -> bool:
    if not path.exists() or path.suffix == ".gz":
        return False
    gz_path = path.with_suffix(path.suffix + ".gz")
    if gz_path.exists():
        if delete_original:
            path.unlink(missing_ok=True)
        return False
    try:
        with path.open("rb") as src, gzip.open(gz_path, "wb", compresslevel=6) as dst:
            shutil.copyfileobj(src, dst)
        shutil.copystat(path, gz_path)
        if delete_original:
            path.unlink(missing_ok=True)
        return True
    except Exception:
        return False


def _retention_days_for_category(config: dict[str, Any], category: str) -> int:
    retention = _archive_cfg(config).get("raw_log_retention", {}) or {}
    if category == "incident":
        return int(retention.get("incident_days", 30) or 30)
    if category == "normal_failure":
        return int(retention.get("normal_failure_days", 14) or 14)
    return int(retention.get("success_days", 3) or 3)


def prune_archived_raw_logs(config: dict[str, Any], state: OrchestratorState, *, dry_run: bool = False) -> dict[str, Any]:
    cfg = _archive_cfg(config)
    compression = cfg.get("compression", {}) or {}
    compress_enabled = bool(compression.get("enabled", True))
    compress_after_days = float(compression.get("compress_after_days", 1) or 1)
    delete_original = bool(compression.get("delete_original_after_compress", True))

    result = {"success": True, "compressed": 0, "run_dirs_deleted": 0, "skipped_unarchived": 0, "scanned": 0, "dry_run": dry_run}
    if not RUNS_DIR.exists():
        return result

    for run_dir in RUNS_DIR.iterdir():
        if run_dir.name == "reports" or not run_dir.is_dir():
            continue
        result["scanned"] += 1
        marker = _load_marker(run_dir)
        if not marker.get("archived"):
            result["skipped_unarchived"] += 1
            continue
        category = str(marker.get("category") or "success")
        age = _age_days(run_dir)
        log_path = run_dir / "stdout_stderr.log"
        if compress_enabled and log_path.exists() and age >= compress_after_days:
            if dry_run:
                result["compressed"] += 1
            elif _compress_log(log_path, delete_original=delete_original):
                result["compressed"] += 1
        keep_days = _retention_days_for_category(config, category)
        if age >= keep_days:
            if dry_run:
                result["run_dirs_deleted"] += 1
            else:
                shutil.rmtree(run_dir, ignore_errors=True)
                result["run_dirs_deleted"] += 1

    state.add_event(
        event_type="log_archive.prune",
        message=f"Archived log prune completed: compressed={result['compressed']} deleted={result['run_dirs_deleted']} skipped_unarchived={result['skipped_unarchived']}",
        severity="info",
        payload=result,
    )
    return result


def log_archive_due(config: dict[str, Any], state: OrchestratorState) -> bool:
    cfg = _archive_cfg(config)
    if not cfg.get("enabled", True):
        return False
    now_jkt = datetime.now(timezone.utc) + timedelta(hours=7)
    run_hour = int(cfg.get("run_hour", 23) or 23)
    run_minute = int(cfg.get("run_minute", 55) or 55)
    if now_jkt.hour < run_hour or (now_jkt.hour == run_hour and now_jkt.minute < run_minute):
        return False
    today = now_jkt.date().isoformat()
    return state.get("log_archive:last_run_date", "") != today


def run_daily_archive(config: dict[str, Any], state: OrchestratorState, *, target_date: str | date | None = None, prune: bool = True, dry_run: bool = False) -> dict[str, Any]:
    if dry_run:
        payload = build_daily_archive_payload(config, state, target_date)
        return {"success": True, "dry_run": True, "date": payload.get("date"), "summary": payload.get("summary"), "incidents": payload.get("incidents", [])[:10]}
    archive_result = write_daily_archive(config, state, target_date)
    if prune:
        archive_result["prune"] = prune_archived_raw_logs(config, state)
    return archive_result


def _cmd_run(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config) if args.config else DEFAULT_CONFIG_PATH)
    state = OrchestratorState()
    try:
        result = run_daily_archive(config, state, target_date=args.date, prune=not args.no_prune, dry_run=bool(args.dry_run))
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        return 0 if result.get("success") else 1
    finally:
        state.close()


def _cmd_prune(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config) if args.config else DEFAULT_CONFIG_PATH)
    state = OrchestratorState()
    try:
        result = prune_archived_raw_logs(config, state, dry_run=bool(args.dry_run))
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        return 0 if result.get("success") else 1
    finally:
        state.close()


def _cmd_status(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config) if args.config else DEFAULT_CONFIG_PATH)
    state = OrchestratorState()
    try:
        payload = {
            "enabled": bool(_archive_cfg(config).get("enabled", True)),
            "last_run_date": state.get("log_archive:last_run_date", ""),
            "last_run_at": state.get("log_archive:last_run_at", ""),
            "due": log_archive_due(config, state),
            "archive_dir": str(_archive_dir(config)),
        }
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0
    finally:
        state.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Daily log archive and raw log retention")
    parser.add_argument("--config", default=None, help="Path to orchestrator.yaml")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("run", help="Build archive for a date and optionally prune archived raw logs")
    p.add_argument("--date", default="today", help="YYYY-MM-DD or today")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--no-prune", action="store_true")
    p.set_defaults(func=_cmd_run)

    p = sub.add_parser("prune", help="Compress/delete archived raw run logs according to retention")
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=_cmd_prune)

    p = sub.add_parser("status", help="Show archive status")
    p.set_defaults(func=_cmd_status)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
