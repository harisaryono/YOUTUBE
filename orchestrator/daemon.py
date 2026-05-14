"""
Orchestrator Daemon — Main loop that plans, checks safety, and dispatches jobs.
Modes:
  once  — Run one cycle and exit.
  run   — Run continuously with adaptive sleep.
"""

from __future__ import annotations

from collections import deque
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import load_config
from .state import OrchestratorState
from .safety import (
    check_system_health,
    check_provider_health,
    check_youtube_health,
    safety_gate_for_job,
)
from .planner import plan_jobs
from .dispatcher import launch_job
from .cooldown import clear_all_cooldowns, get_next_wakeup
from .reports import generate_report
from .reports import build_inventory_snapshot
from .preflight import run_preflight, format_preflight
from .janitor import run_janitor, janitor_due


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _effective_max_jobs(config: dict[str, Any], requested: int) -> int:
    if requested and requested > 0:
        return requested
    return int(config.get("orchestrator", {}).get("max_jobs_per_cycle", 7) or 7)


def _parallel_config(config: dict[str, Any]) -> dict[str, Any]:
    parallel = config.get("parallel", {}) or {}
    if parallel:
        return parallel
    # Backward-compatible fallback for older configs.
    groups = config.get("orchestrator", {}).get("parallel_groups", {}) or {}
    return {
        "enabled": True,
        "max_total_jobs": config.get("orchestrator", {}).get("max_parallel_jobs", 1),
        "groups": {
            "discovery": {"max_running": 1, "stages": ["discovery"]},
            "youtube": {"max_running": groups.get("youtube", 1), "stages": ["discovery", "transcript", "audio_download"]},
            "provider": {"max_running": groups.get("provider", 1), "stages": ["resume", "asr"]},
            "local": {"max_running": groups.get("local", 1), "stages": ["format", "janitor", "import_pending"]},
        },
        "stages": {
            "discovery": {"slots": 1},
            "transcript": {"slots": 1},
            "audio_download": {"slots": 1},
            "resume": {"slots": 1},
            "asr": {"slots": 1},
            "format": {"slots": 1},
            "janitor": {"slots": 1},
        },
    }


def _max_total_jobs(config: dict[str, Any]) -> int:
    parallel = _parallel_config(config)
    value = parallel.get("max_total_jobs")
    if value is None:
        value = config.get("orchestrator", {}).get("max_parallel_jobs", 1)
    try:
        return max(1, int(value or 1))
    except (TypeError, ValueError):
        return 1


def _stage_slots(config: dict[str, Any], stage: str) -> int:
    parallel = _parallel_config(config)
    stage_cfg = parallel.get("stages", {}).get(stage, {}) or {}
    try:
        return max(1, int(stage_cfg.get("slots", 1) or 1))
    except (TypeError, ValueError):
        return 1


def _stage_group_name(config: dict[str, Any], stage: str) -> str:
    stage = str(stage or "").strip().lower()
    parallel = _parallel_config(config)
    groups = parallel.get("groups", {}) or {}
    for group_name, group_cfg in groups.items():
        stages = {str(s).strip().lower() for s in (group_cfg.get("stages", []) or [])}
        if stage in stages:
            return str(group_name)
    if stage == "discovery":
        return "discovery"
    if stage in {"transcript", "audio_download"}:
        return "youtube"
    if stage in {"resume", "asr"}:
        return "provider"
    return "local"


def _group_limit(config: dict[str, Any], group_name: str) -> int:
    parallel = _parallel_config(config)
    group_cfg = parallel.get("groups", {}).get(group_name, {}) or {}
    try:
        return max(0, int(group_cfg.get("max_running", 1) or 1))
    except (TypeError, ValueError):
        return 1


def _running_slot_indexes(state: OrchestratorState, stage: str) -> set[int]:
    indexes: set[int] = set()
    for row in state.list_running_jobs():
        if str(row.get("stage", "")).strip() != stage:
            continue
        try:
            slot_index = int(row.get("slot_index") or 0)
        except (TypeError, ValueError):
            slot_index = 0
        if slot_index > 0:
            indexes.add(slot_index)
    return indexes


def _claim_stage_slot(
    config: dict[str, Any],
    state: OrchestratorState,
    stage: str,
) -> tuple[int, str] | None:
    slots = _stage_slots(config, stage)
    used = _running_slot_indexes(state, stage)
    for slot_index in range(1, slots + 1):
        if slot_index in used:
            continue
        lock_key = f"stage:{stage}:slot:{slot_index}"
        if state.acquire_lock(lock_key, owner=f"pending:{stage}:{slot_index}", ttl_seconds=7200):
            return slot_index, lock_key
    return None


def _active_jobs_snapshot(state: OrchestratorState) -> list[dict[str, Any]]:
    try:
        return state.list_running_jobs()
    except Exception:
        return []


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False


def _read_exit_code(run_dir: str | Path) -> int | None:
    path = Path(run_dir) / "exit_code.txt"
    if not path.exists():
        return None
    try:
        return int(path.read_text().strip())
    except Exception:
        return None


def _parse_sqlite_datetime(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _job_age_seconds(row: dict[str, Any]) -> int:
    started = _parse_sqlite_datetime(str(row.get("started_at") or ""))
    updated = _parse_sqlite_datetime(str(row.get("updated_at") or ""))
    stamp = started or updated
    if stamp is None:
        return 0
    now = datetime.now(timezone.utc)
    delta = now - stamp.astimezone(timezone.utc)
    return max(0, int(delta.total_seconds()))


def _format_duration(seconds: int) -> str:
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds}s"
    minutes, sec = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h{minutes:02d}m"
    days, hours = divmod(hours, 24)
    return f"{days}d{hours:02d}h"


def _shorten_text(value: str, width: int) -> str:
    text = str(value or "").strip()
    if len(text) <= width:
        return text
    if width <= 3:
        return text[:width]
    return text[: width - 3] + "..."


def _job_runtime_state(row: dict[str, Any]) -> str:
    pid = int(row.get("pid") or 0)
    if _pid_is_alive(pid):
        return "alive"
    return "stale"


def _timeout_key_for_stage(stage: str) -> str:
    stage = str(stage or "").strip().lower()
    mapping = {
        "discovery": "discovery_seconds",
        "transcript": "transcript_seconds",
        "audio_download": "audio_download_seconds",
        "resume": "resume_seconds",
        "asr": "asr_seconds",
        "format": "format_seconds",
    }
    return mapping.get(stage, "default_seconds")


def _cooldown_scopes_for_failure(stage: str, scope: str, classification: Any) -> list[str]:
    stage = str(stage or "").strip().lower()
    scope = str(scope or "").strip()
    error_type = str(getattr(classification, "error_type", "") or "").strip()

    scopes: list[str] = []
    if scope.startswith("channel:"):
        scopes.append(scope)

    severe_youtube_errors = {
        "youtube_bot_detection",
        "youtube_signin_required",
        "youtube_ip_blocked",
    }

    if error_type.startswith("youtube_"):
        if error_type in severe_youtube_errors:
            scopes.append("youtube")
        elif stage == "discovery":
            scopes.append("youtube:discovery")
        elif stage in {"transcript", "audio_download"}:
            scopes.append("youtube:content")
        else:
            scopes.append("youtube")
    elif getattr(classification, "suggested_scope", ""):
        scopes.append(str(classification.suggested_scope))
    elif not scopes:
        scopes.append(scope or "global")

    deduped: list[str] = []
    for item in scopes:
        if item and item not in deduped:
            deduped.append(item)
    return deduped


def _stage_timeout_seconds(config: dict[str, Any], stage: str) -> int:
    timeouts = config.get("timeouts", {}) or {}
    key = _timeout_key_for_stage(stage)
    value = timeouts.get(key, timeouts.get("default_seconds", 0))
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _job_timeout_remaining_seconds(config: dict[str, Any], row: dict[str, Any]) -> int:
    stage = str(row.get("stage") or "")
    timeout_seconds = _stage_timeout_seconds(config, stage)
    if timeout_seconds <= 0:
        return 0
    age_seconds = _job_age_seconds(row)
    return max(0, timeout_seconds - age_seconds)


def _job_timeout_seconds(config: dict[str, Any], row: dict[str, Any]) -> int:
    stage = str(row.get("stage") or "")
    return _stage_timeout_seconds(config, stage)


def _tail_file(path: Path, tail_lines: int) -> list[str]:
    if tail_lines <= 0:
        tail_lines = 100
    lines: deque[str] = deque(maxlen=tail_lines)
    with path.open("r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            lines.append(line.rstrip("\n"))
    return list(lines)


def _job_matches_filters(
    row: dict[str, Any],
    *,
    job_id: str | None = None,
    stage: str | None = None,
    group_name: str | None = None,
) -> bool:
    if job_id and str(row.get("job_id") or "") != job_id:
        return False
    if stage and str(row.get("stage") or "") != stage:
        return False
    if group_name and str(row.get("group_name") or "") != group_name:
        return False
    return True


def _select_running_jobs(
    state: OrchestratorState,
    *,
    job_id: str | None = None,
    stage: str | None = None,
    group_name: str | None = None,
) -> list[dict[str, Any]]:
    jobs = state.list_running_jobs()
    return [job for job in jobs if _job_matches_filters(job, job_id=job_id, stage=stage, group_name=group_name)]


def _select_jobs_for_logs(
    state: OrchestratorState,
    *,
    job_id: str,
) -> dict[str, Any] | None:
    return state.get_job(job_id)


def _print_active_jobs(config: dict[str, Any], jobs: list[dict[str, Any]]) -> None:
    headers = ["JOB ID", "STAGE", "GROUP", "SLOT", "PID", "AGE", "TIMEOUT", "REMAIN", "STATUS", "STATE", "RUN DIR"]
    rows: list[list[str]] = []
    for job in jobs:
        rows.append(
            [
                _shorten_text(str(job.get("job_id") or ""), 24),
                _shorten_text(str(job.get("stage") or ""), 12),
                _shorten_text(str(job.get("group_name") or ""), 10),
                str(job.get("slot_index") or 0),
                str(job.get("pid") or 0),
                _format_duration(_job_age_seconds(job)),
                _format_duration(_job_timeout_seconds(config, job)),
                _format_duration(_job_timeout_remaining_seconds(config, job)),
                _shorten_text(str(job.get("status") or ""), 10),
                _shorten_text(_job_runtime_state(job), 8),
                _shorten_text(str(job.get("run_dir") or ""), 42),
            ]
        )

    widths = [24, 12, 10, 4, 7, 8, 8, 8, 10, 8, 42]
    header_line = (
        f"{headers[0]:<{widths[0]}} "
        f"{headers[1]:<{widths[1]}} "
        f"{headers[2]:<{widths[2]}} "
        f"{headers[3]:>{widths[3]}} "
        f"{headers[4]:>{widths[4]}} "
        f"{headers[5]:>{widths[5]}} "
        f"{headers[6]:<{widths[6]}} "
        f"{headers[7]:<{widths[7]}} "
        f"{headers[8]:<{widths[8]}} "
        f"{headers[9]:<{widths[9]}} "
        f"{headers[10]:<{widths[10]}}"
    )
    print(header_line)
    print("-" * len(header_line))
    for row in rows:
        print(
            f"{row[0]:<{widths[0]}} "
            f"{row[1]:<{widths[1]}} "
            f"{row[2]:<{widths[2]}} "
            f"{row[3]:>{widths[3]}} "
            f"{row[4]:>{widths[4]}} "
            f"{row[5]:>{widths[5]}} "
            f"{row[6]:<{widths[6]}} "
            f"{row[7]:<{widths[7]}} "
            f"{row[8]:<{widths[8]}} "
            f"{row[9]:<{widths[9]}} "
            f"{row[10]:<{widths[10]}}"
        )


def _kill_process_group(pid: int, sig: signal.Signals) -> bool:
    if pid <= 0:
        return False
    try:
        os.killpg(pid, sig)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        try:
            os.kill(pid, sig)
            return True
        except Exception:
            return False
    except OSError:
        try:
            os.kill(pid, sig)
            return True
        except Exception:
            return False


def _cancel_job_row(
    state: OrchestratorState,
    row: dict[str, Any],
    *,
    force: bool = False,
    grace_seconds: int = 10,
) -> dict[str, Any]:
    job_id = str(row.get("job_id") or "")
    pid = int(row.get("pid") or 0)
    lock_key = str(row.get("lock_key") or "").strip()
    stage = str(row.get("stage") or "")
    scope = str(row.get("scope") or "")

    if pid <= 0:
        return {"job_id": job_id, "status": "skipped", "reason": "invalid pid"}
    if not _pid_is_alive(pid):
        reconciled = _reconcile_running_job(state, row)
        if reconciled.get("status") == "alive":
            return {"job_id": job_id, "status": "pending", "reason": "process still alive"}
        return {
            "job_id": job_id,
            "status": reconciled.get("status", "reconciled"),
            "reason": f"reconciled as {reconciled.get('status', 'unknown')}",
        }

    sent_term = _kill_process_group(pid, signal.SIGTERM)
    if not sent_term:
        return {"job_id": job_id, "status": "failed", "reason": "unable to send SIGTERM"}

    deadline = time.time() + max(1, int(grace_seconds))
    while time.time() < deadline:
        if not _pid_is_alive(pid):
            break
        time.sleep(1)

    if _pid_is_alive(pid):
        if force:
            _kill_process_group(pid, signal.SIGKILL)
            deadline = time.time() + 5
            while time.time() < deadline and _pid_is_alive(pid):
                time.sleep(1)
        else:
            return {
                "job_id": job_id,
                "status": "pending",
                "reason": "process still alive after SIGTERM",
            }

    if _pid_is_alive(pid):
        return {
            "job_id": job_id,
            "status": "pending",
            "reason": "process still alive after SIGKILL",
        }

    state.mark_active_job_finished(
        job_id=job_id,
        returncode=143 if not force else 137,
        status="cancelled",
        error_text="cancelled via orchestrator",
    )
    if lock_key:
        state.release_lock(lock_key)
    state.add_event(
        event_type="control",
        stage=stage,
        scope=scope,
        message=f"Cancelled job {job_id} (pid={pid})",
        severity="info",
        payload={"action": "cancel", "job_id": job_id, "pid": pid, "force": force},
    )
    return {"job_id": job_id, "status": "cancelled", "reason": "terminated"}


def _timeout_running_job(
    state: OrchestratorState,
    row: dict[str, Any],
    *,
    timeout_seconds: int,
) -> dict[str, Any]:
    job_id = str(row.get("job_id") or "")
    pid = int(row.get("pid") or 0)
    lock_key = str(row.get("lock_key") or "").strip()
    stage = str(row.get("stage") or "")
    scope = str(row.get("scope") or "")
    run_dir = str(row.get("run_dir") or "")
    log_path = str(row.get("log_path") or "")

    if pid <= 0:
        return {"job_id": job_id, "status": "skipped", "reason": "invalid pid"}
    if not _pid_is_alive(pid):
        return _reconcile_running_job(state, row)

    _kill_process_group(pid, signal.SIGTERM)
    deadline = time.time() + 5
    while time.time() < deadline and _pid_is_alive(pid):
        time.sleep(1)
    if _pid_is_alive(pid):
        _kill_process_group(pid, signal.SIGKILL)
        deadline = time.time() + 3
        while time.time() < deadline and _pid_is_alive(pid):
            time.sleep(1)

    if _pid_is_alive(pid):
        return {
            "job_id": job_id,
            "status": "pending",
            "reason": f"process still alive after timeout kill for {stage}",
        }

    state.mark_active_job_finished(
        job_id=job_id,
        returncode=124,
        status="timeout",
        error_text=f"timed out after {timeout_seconds}s",
    )
    if lock_key:
        state.release_lock(lock_key)
    state.add_event(
        event_type="timeout",
        stage=stage,
        scope=scope,
        message=f"Timed out job {job_id} after {timeout_seconds}s",
        severity="warning",
        payload={
            "job_id": job_id,
            "pid": pid,
            "run_dir": run_dir,
            "log_path": log_path,
            "timeout_seconds": timeout_seconds,
        },
    )
    state.add_event(
        event_type="control",
        stage=stage,
        scope=scope,
        message=f"Timeout terminated job {job_id} (pid={pid})",
        severity="info",
        payload={"action": "timeout", "job_id": job_id, "pid": pid, "timeout_seconds": timeout_seconds},
    )
    return {"job_id": job_id, "status": "timeout", "reason": "terminated after timeout"}


def _reconcile_running_job(state: OrchestratorState, row: dict[str, Any]) -> dict[str, Any]:
    job_id = str(row.get("job_id") or "")
    pid = int(row.get("pid") or 0)
    lock_key = str(row.get("lock_key") or "").strip()
    stage = str(row.get("stage") or "")
    scope = str(row.get("scope") or "")
    run_dir = str(row.get("run_dir") or "")
    log_path = str(row.get("log_path") or "")

    if pid > 0 and _pid_is_alive(pid):
        return {"job_id": job_id, "status": "alive"}

    exit_code = _read_exit_code(run_dir)
    if exit_code is None:
        exit_code = 1
    status = "completed" if exit_code == 0 else "failed"
    error_text = ""
    if exit_code != 0 and log_path:
        try:
            error_text = Path(log_path).read_text(errors="ignore")[-4000:]
        except Exception:
            error_text = "failed to read log"

    state.mark_active_job_finished(
        job_id=job_id,
        returncode=exit_code,
        status=status,
        error_text=error_text,
    )
    if lock_key:
        state.release_lock(lock_key)
    state.add_event(
        event_type="reconcile",
        stage=stage,
        scope=scope,
        message=f"Reconciled job {job_id} as {status} (exit={exit_code})",
        severity="info" if exit_code == 0 else "warning",
        payload={
            "job_id": job_id,
            "pid": pid,
            "run_dir": run_dir,
            "log_path": log_path,
            "returncode": exit_code,
            "status": status,
        },
    )
    return {"job_id": job_id, "status": status, "returncode": exit_code}


def _waitpid_returncode(pid: int) -> tuple[bool, int | None]:
    """Try to reap a child without blocking. Returns (finished, returncode)."""
    try:
        waited_pid, status = os.waitpid(pid, os.WNOHANG)
    except ChildProcessError:
        return False, None
    except OSError:
        return False, None

    if waited_pid == 0:
        return False, None

    if os.WIFEXITED(status):
        return True, os.WEXITSTATUS(status)
    if os.WIFSIGNALED(status):
        return True, 128 + int(os.WTERMSIG(status))
    return True, 1


def poll_active_jobs(
    config: dict[str, Any],
    state: OrchestratorState,
    *,
    enforce_timeouts: bool = True,
) -> dict[str, int]:
    """Poll active jobs and finalize the ones that already exited."""
    result = {"finished": 0, "failed": 0, "still_running": 0}
    active_jobs = _active_jobs_snapshot(state)

    for row in active_jobs:
        job_id = str(row.get("job_id") or "")
        stage = str(row.get("stage") or "")
        scope = str(row.get("scope") or "")
        pid = int(row.get("pid") or 0)
        lock_key = str(row.get("lock_key") or "").strip()
        run_dir = str(row.get("run_dir") or "")
        log_path = str(row.get("log_path") or "")
        slot_index = int(row.get("slot_index") or 0)

        timeout_seconds = _stage_timeout_seconds(config, stage)
        if enforce_timeouts and timeout_seconds > 0:
            age_seconds = _job_age_seconds(row)
            if age_seconds >= timeout_seconds:
                result_row = _timeout_running_job(
                    state,
                    row,
                    timeout_seconds=timeout_seconds,
                )
                if result_row.get("status") == "pending":
                    result["still_running"] += 1
                    continue
                if result_row.get("status") == "completed":
                    result["finished"] += 1
                else:
                    result["failed"] += 1
                continue

        finished, exit_code = _waitpid_returncode(pid)
        if not finished:
            if _pid_is_alive(pid):
                result["still_running"] += 1
                continue
            exit_code = _read_exit_code(run_dir)
            if exit_code is None:
                exit_code = 1

        status = "completed" if exit_code == 0 else "failed"
        error_text = ""
        if exit_code != 0:
            try:
                log_text = Path(log_path).read_text(errors="ignore")
                error_text = log_text[-4000:]
            except Exception:
                error_text = "failed to read log"

        state.mark_active_job_finished(
            job_id=job_id,
            returncode=exit_code,
            status=status,
            error_text=error_text,
        )
        if lock_key:
            state.release_lock(lock_key)

        if exit_code == 0:
            state.add_event(
                event_type="dispatch_success",
                stage=stage,
                scope=scope,
                message=f"{stage} slot {slot_index} completed successfully",
                severity="info",
                payload={"job_id": job_id, "pid": pid, "slot_index": slot_index},
            )
            _maybe_update_adaptive_batch(config, state, stage, success=True, blocked=False)
            result["finished"] += 1
            continue

        from .error_analyzer import classify_error

        classification = classify_error(error_text or f"exit code {exit_code}", exit_code)
        if classification.cooldown_seconds > 0:
            for cooldown_scope in _cooldown_scopes_for_failure(stage, scope, classification):
                state.set_cooldown(
                    scope=cooldown_scope,
                    reason=classification.description,
                    duration_seconds=classification.cooldown_seconds,
                    severity=classification.severity,
                    recommendation=classification.recommendation,
                )
        state.add_event(
            event_type="dispatch_failure",
            stage=stage,
            scope=scope,
            message=f"{stage} slot {slot_index} failed: {classification.error_type}",
            severity=classification.severity,
            recommendation=classification.recommendation,
            reason_code=classification.error_type,
            payload={
                "job_id": job_id,
                "pid": pid,
                "returncode": exit_code,
                "slot_index": slot_index,
                "error_text": error_text[-1000:],
            },
        )
        _maybe_update_adaptive_batch(config, state, stage, success=False, blocked=classification.cooldown_seconds > 0)
        result["failed"] += 1

    return result


def _short_sleep_seconds(config: dict[str, Any]) -> int:
    orchestrator_cfg = config.get("orchestrator", {})
    loop_cfg = config.get("loop", {})
    return int(
        orchestrator_cfg.get(
            "short_sleep_seconds",
            loop_cfg.get("min_sleep_seconds", 5),
        )
        or 5
    )


def _idle_sleep_seconds(config: dict[str, Any]) -> int:
    orchestrator_cfg = config.get("orchestrator", {})
    loop_cfg = config.get("loop", {})
    return int(
        orchestrator_cfg.get(
            "idle_sleep_seconds",
            loop_cfg.get("idle_sleep_seconds", 900),
        )
        or 900
    )


def _error_sleep_seconds(config: dict[str, Any]) -> int:
    loop_cfg = config.get("loop", {})
    return int(loop_cfg.get("error_sleep_seconds", 1800) or 1800)


def _maybe_update_adaptive_batch(
    config: dict[str, Any],
    state: OrchestratorState,
    stage: str,
    success: bool,
    blocked: bool,
) -> None:
    adaptive_cfg = config.get("adaptive", {}).get(stage, {})
    if not adaptive_cfg.get("enabled", False):
        return
    state.record_stage_batch_outcome(
        stage,
        success=success,
        blocked=blocked,
        min_batch=adaptive_cfg.get("min_batch", 1),
        max_batch=adaptive_cfg.get("max_batch", 1),
        step=adaptive_cfg.get("step", 1),
        increase_after_success_batches=adaptive_cfg.get("increase_after_success_batches", 3),
        decrease_on_block=adaptive_cfg.get("decrease_on_block", True),
    )


def _target_to_pause_key(target: str) -> str:
    value = str(target or "").strip()
    if not value:
        return "scope:all"
    if value.startswith("pause:"):
        return value.replace("pause:", "", 1)
    if value in {"youtube", "provider", "all"}:
        return f"scope:{value}"
    if value.startswith(("stage:", "scope:")):
        return value
    return f"stage:{value}"


def _run_preflight_or_exit(
    config: dict[str, Any],
    state: OrchestratorState,
    require_coordinator: bool = False,
) -> None:
    result = run_preflight(config, require_coordinator=require_coordinator, state=state)
    state.add_event(
        event_type="preflight",
        message="Preflight passed" if result.ok else "Preflight failed",
        severity="info" if result.ok else "blocking",
        payload={
            "ok": result.ok,
            "checks": result.checks,
            "warnings": result.warnings,
            "errors": result.errors,
        },
    )
    if not result.ok:
        print(format_preflight(result))
        raise SystemExit(1)


def _maybe_run_janitor(config: dict[str, Any], state: OrchestratorState) -> None:
    """Run janitor when the configured interval has elapsed."""
    if not janitor_due(config, state):
        return
    run_janitor(config, state)


def run_once(
    config: dict[str, Any],
    state: OrchestratorState,
    max_jobs: int = 5,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run one launch-only orchestrator cycle."""
    start_time = time.time()
    max_jobs = _effective_max_jobs(config, max_jobs)
    cycle_result: dict[str, Any] = {
        "jobs_planned": 0,
        "jobs_dispatched": 0,
        "jobs_succeeded": 0,
        "jobs_failed": 0,
        "jobs_deferred": 0,
        "active_jobs": 0,
        "duration_seconds": 0,
        "disk_free_gb": 0,
        "mem_available_mb": 0,
        "dry_run": dry_run,
    }

    cleared_cd = clear_all_cooldowns(state)
    if cleared_cd > 0:
        state.add_event(event_type="cleanup", message=f"Cleared {cleared_cd} expired cooldown(s)", severity="info")
    cleared_locks = state.clear_expired_locks()
    if cleared_locks > 0:
        state.add_event(event_type="cleanup", message=f"Cleared {cleared_locks} expired lock(s)", severity="info")
    cleared_stale_locks = state.clear_stale_pid_locks()
    if cleared_stale_locks > 0:
        state.add_event(event_type="cleanup", message=f"Cleared {cleared_stale_locks} stale pid lock(s)", severity="info")

    poll_result = poll_active_jobs(config, state, enforce_timeouts=not dry_run)
    cycle_result["jobs_succeeded"] += poll_result["finished"]
    cycle_result["jobs_failed"] += poll_result["failed"]
    cycle_result["active_jobs"] = state.count_running_total()

    sys_health = check_system_health(config)
    provider_health = check_provider_health(config, state)
    youtube_health = check_youtube_health(config, state)

    cycle_result["disk_free_gb"] = sys_health.disk_free_gb
    cycle_result["mem_available_mb"] = sys_health.mem_available_mb

    if not sys_health.ok:
        for err in sys_health.errors:
            state.add_event(event_type="health", message=f"System: {err}", severity="blocking")
    if not provider_health.ok:
        for err in provider_health.errors:
            state.add_event(event_type="health", message=f"Provider: {err}", severity="warning")
    if not youtube_health.ok:
        for err in youtube_health.errors:
            state.add_event(event_type="health", message=f"YouTube: {err}", severity="blocking")

    jobs = plan_jobs(config, state, max_jobs=max_jobs)
    cycle_result["jobs_planned"] = len(jobs)

    if not jobs and cycle_result["active_jobs"] == 0:
        state.add_event(event_type="plan", message="No jobs to run", severity="info")
        cycle_result["duration_seconds"] = time.time() - start_time
        generate_report(config, state, cycle_result)
        return cycle_result

    max_total = _max_total_jobs(config)

    for job in jobs:
        stage = str(job.get("stage", ""))
        decision = safety_gate_for_job(job, config, sys_health, provider_health, youtube_health, state)

        if decision.verdict == "WAIT":
            cycle_result["jobs_deferred"] += 1
            state.add_event(
                event_type="deferred",
                message=f"{decision.reason_code or 'DEFER'}: {stage} - {decision.reason}",
                stage=stage,
                scope=job.get("scope", ""),
                severity="info",
                recommendation=decision.recommendation,
                reason_code=decision.reason_code,
            )
            if decision.cooldown_seconds > 0 and job.get("scope"):
                state.set_cooldown(
                    scope=str(job["scope"]),
                    reason=decision.reason,
                    duration_seconds=decision.cooldown_seconds,
                    recommendation=decision.recommendation,
                )
            continue

        if decision.verdict == "SKIP_PERMANENT":
            state.add_event(
                event_type="skipped",
                message=f"Job skipped permanently ({stage}): {decision.reason}",
                stage=stage,
                scope=job.get("scope", ""),
                severity="info",
                recommendation=decision.recommendation,
            )
            continue

        if decision.verdict == "REPORT":
            state.add_event(
                event_type="report",
                message=f"Job reported ({stage}): {decision.reason}",
                stage=stage,
                scope=job.get("scope", ""),
                severity="warning",
                recommendation=decision.recommendation,
            )
            continue

        if state.count_running_total() >= max_total:
            cycle_result["jobs_deferred"] += 1
            state.add_event(
                event_type="deferred",
                message=f"{stage} deferred: max_total_jobs reached",
                stage=stage,
                scope=job.get("scope", ""),
                severity="info",
                reason_code="DEFER_MAX_TOTAL_JOBS",
            )
            continue

        group_name = _stage_group_name(config, stage)
        if state.count_running_by_group(group_name) >= _group_limit(config, group_name):
            cycle_result["jobs_deferred"] += 1
            state.add_event(
                event_type="deferred",
                message=f"{stage} deferred: group {group_name} limit reached",
                stage=stage,
                scope=job.get("scope", ""),
                severity="info",
                reason_code="DEFER_PARALLEL_GROUP_LIMIT",
            )
            continue

        slot_claim = _claim_stage_slot(config, state, stage)
        if slot_claim is None:
            cycle_result["jobs_deferred"] += 1
            state.add_event(
                event_type="deferred",
                message=f"{stage} deferred: no free slot available",
                stage=stage,
                scope=job.get("scope", ""),
                severity="info",
                reason_code="DEFER_STAGE_SLOTS_FULL",
            )
            continue

        slot_index, lock_key = slot_claim

        if dry_run:
            print(f"  [DRY-RUN] Would launch: {job.get('description', stage)}")
            state.release_lock(lock_key)
            cycle_result["jobs_dispatched"] += 1
            continue

        try:
            result = launch_job(
                job,
                config,
                state,
                slot_index=slot_index,
                lock_key=lock_key,
            )
        except Exception as e:
            state.release_lock(lock_key)
            cycle_result["jobs_failed"] += 1
            state.add_event(
                event_type="error",
                message=f"Launch failed for {stage}: {e}",
                stage=stage,
                scope=job.get("scope", ""),
                severity="warning",
                reason_code="LAUNCH_FAILED",
            )
            continue

        if result.get("launched"):
            cycle_result["jobs_dispatched"] += 1
        else:
            state.release_lock(lock_key)
            cycle_result["jobs_failed"] += 1
            state.add_event(
                event_type="error",
                message=f"Launch failed for {stage}: {result.get('error', 'unknown error')}",
                stage=stage,
                scope=job.get("scope", ""),
                severity="warning",
                reason_code="LAUNCH_FAILED",
            )

    cycle_result["active_jobs"] = state.count_running_total()
    cycle_result["duration_seconds"] = time.time() - start_time
    generate_report(config, state, cycle_result)
    return cycle_result


def run_loop(
    config: dict[str, Any],
    state: OrchestratorState,
    max_jobs: int = 5,
    dry_run: bool = False,
) -> None:
    """
    Run orchestrator continuously with adaptive sleep.
    """
    state.add_event(
        event_type="startup",
        message=f"Orchestrator started (profile: {config.get('profile', 'safe')})",
        severity="info",
    )

    while True:
        cycle_start = time.time()
        max_jobs = _effective_max_jobs(config, max_jobs)

        try:
            result = run_once(config, state, max_jobs=max_jobs, dry_run=dry_run)
            _maybe_run_janitor(config, state)
        except KeyboardInterrupt:
            state.add_event(
                event_type="shutdown",
                message="Orchestrator stopped by user (Ctrl+C)",
                severity="info",
            )
            break
        except Exception as e:
            state.add_event(
                event_type="error",
                message=f"Cycle failed: {e}",
                severity="blocking",
            )
            time.sleep(_error_sleep_seconds(config))
            continue

        # Adaptive sleep
        jobs_dispatched = result.get("jobs_dispatched", 0)
        jobs_failed = result.get("jobs_failed", 0)
        jobs_planned = result.get("jobs_planned", 0)
        jobs_deferred = result.get("jobs_deferred", 0)
        active_jobs = result.get("active_jobs", 0)

        if dry_run:
            # Dry-run: check again soon.
            sleep_seconds = _short_sleep_seconds(config)
        elif active_jobs > 0 or jobs_dispatched > 0 or jobs_failed > 0:
            # Keep moving while there is still runnable work.
            # Hard blocks are handled through cooldown state, not by long sleeps here.
            sleep_seconds = _short_sleep_seconds(config)
        elif jobs_planned == 0:
            # No work — wait for next cooldown or idle
            next_wakeup = get_next_wakeup(state)
            if next_wakeup > 0:
                sleep_seconds = min(next_wakeup, _idle_sleep_seconds(config))
            else:
                sleep_seconds = _idle_sleep_seconds(config)
        else:
            # Jobs were planned but mostly/fully deferred.
            # In aggressive mode, check again soon unless a real cooldown says otherwise.
            next_wakeup = get_next_wakeup(state)
            if next_wakeup > 0:
                sleep_seconds = min(next_wakeup, _idle_sleep_seconds(config))
            else:
                sleep_seconds = _short_sleep_seconds(config)

        # Ensure minimum sleep
        sleep_seconds = max(sleep_seconds, _short_sleep_seconds(config))

        cycle_duration = time.time() - cycle_start
        actual_sleep = max(1, sleep_seconds - cycle_duration)

        state.add_event(
            event_type="sleep",
            message=f"Sleeping {int(actual_sleep)}s (cycle took {cycle_duration:.1f}s, "
                    f"planned={jobs_planned}, dispatched={jobs_dispatched}, "
                    f"deferred={jobs_deferred}, failed={jobs_failed})",
            severity="info",
        )

        time.sleep(actual_sleep)


def main() -> None:
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="YouTube Orchestrator Daemon")
    parser.add_argument(
        "mode",
        nargs="?",
        default="once",
        choices=[
            "once",
            "run",
            "status",
            "active",
            "logs",
            "cancel",
            "reconcile",
            "explain",
            "report",
            "preflight",
            "pause",
            "resume",
            "janitor",
        ],
        help="Operation mode (default: once)",
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to orchestrator.yaml (default: orchestrator.yaml in project root)",
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=0,
        help="Maximum jobs per cycle (0 = use config orchestrator.max_jobs_per_cycle)",
    )
    parser.add_argument(
        "--profile",
        default=None,
        choices=["safe", "normal", "fast"],
        help="Override profile from config",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would run without dispatching anything",
    )
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        help="Skip startup preflight checks for once/run modes",
    )
    parser.add_argument(
        "--require-coordinator",
        action="store_true",
        help="Treat coordinator availability as a hard preflight requirement",
    )
    parser.add_argument(
        "--target",
        default=None,
        help="Pause/resume target, e.g. youtube, transcript, audio_download, resume",
    )
    parser.add_argument(
        "--reason",
        default="",
        help="Pause reason when using pause mode",
    )
    parser.add_argument(
        "--job-id",
        default="",
        help="Target job id for logs/cancel",
    )
    parser.add_argument(
        "--stage",
        default="",
        help="Filter jobs by stage for cancel",
    )
    parser.add_argument(
        "--group",
        default="",
        help="Filter jobs by group for cancel",
    )
    parser.add_argument(
        "--tail",
        type=int,
        default=100,
        help="Number of log lines to show for logs mode",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force cancel with SIGKILL after the grace period",
    )
    parser.add_argument(
        "--grace-seconds",
        type=int,
        default=10,
        help="Grace period before forcing cancellation",
    )

    args = parser.parse_args()

    # Load config
    config = load_config(args.config)
    if args.profile:
        config["profile"] = args.profile

    # Init state
    state = OrchestratorState()

    if args.mode in {"once", "run", "preflight"} and not args.skip_preflight:
        preflight = run_preflight(config, require_coordinator=args.require_coordinator, state=state)
        state.add_event(
            event_type="preflight",
            message="Preflight passed" if preflight.ok else "Preflight failed",
            severity="info" if preflight.ok else "blocking",
            payload={
                "ok": preflight.ok,
                "checks": preflight.checks,
                "warnings": preflight.warnings,
                "errors": preflight.errors,
            },
        )
        if args.mode == "preflight":
            print(format_preflight(preflight))
            state.close()
            raise SystemExit(0 if preflight.ok else 1)
        if not preflight.ok:
            print(format_preflight(preflight))
            state.close()
            raise SystemExit(1)

    if args.mode == "once":
        result = run_once(config, state, max_jobs=args.max_jobs, dry_run=args.dry_run)
        _maybe_run_janitor(config, state)
        print(f"Cycle complete: {result.get('jobs_dispatched', 0)} dispatched, "
              f"{result.get('jobs_succeeded', 0)} succeeded, "
              f"{result.get('jobs_failed', 0)} failed, "
              f"{result.get('jobs_deferred', 0)} deferred "
              f"({result.get('duration_seconds', 0):.1f}s)")

    elif args.mode == "run":
        print(f"Orchestrator starting (profile: {config.get('profile', 'safe')})")
        if args.dry_run:
            print("⚠️  DRY-RUN MODE — no jobs will actually run")
        print("Press Ctrl+C to stop.")
        run_loop(config, state, max_jobs=args.max_jobs, dry_run=args.dry_run)

    elif args.mode == "active":
        jobs = state.list_running_jobs()
        alive = sum(1 for job in jobs if _job_runtime_state(job) == "alive")
        stale = len(jobs) - alive
        print("Active jobs")
        print(f"Running: {len(jobs)}")
        print(f"Alive: {alive}")
        print(f"Stale: {stale}")
        if jobs:
            print("")
            _print_active_jobs(config, jobs)
        else:
            print("No running jobs.")

    elif args.mode == "logs":
        job_id = args.job_id.strip()
        if not job_id:
            print("Missing --job-id for logs mode", file=sys.stderr)
            raise SystemExit(2)
        job = _select_jobs_for_logs(state, job_id=job_id)
        if job is None:
            print(f"Job not found: {job_id}", file=sys.stderr)
            raise SystemExit(1)
        log_path_raw = str(job.get("log_path") or "").strip()
        if not log_path_raw:
            print(f"Job {job_id} does not have a log path recorded", file=sys.stderr)
            raise SystemExit(1)
        log_path = Path(log_path_raw).expanduser()
        print(f"Job ID: {job_id}")
        print(f"Stage: {job.get('stage', '')}")
        print(f"Status: {job.get('status', '')}")
        print(f"Log path: {log_path}")
        print("")
        if not log_path.exists():
            print("Log file not found.", file=sys.stderr)
            raise SystemExit(1)
        for line in _tail_file(log_path, int(args.tail or 100)):
            print(line)

    elif args.mode == "cancel":
        job_id = args.job_id.strip() or None
        stage = args.stage.strip() or None
        group_name = args.group.strip() or None
        jobs = _select_running_jobs(
            state,
            job_id=job_id,
            stage=stage,
            group_name=group_name,
        )
        if not jobs:
            print("No matching running jobs.")
            raise SystemExit(1)
        results = []
        for job in jobs:
            results.append(
                _cancel_job_row(
                    state,
                    job,
                    force=bool(args.force),
                    grace_seconds=int(args.grace_seconds or 10),
                )
            )
        cancelled = sum(1 for item in results if item.get("status") == "cancelled")
        pending = sum(1 for item in results if item.get("status") == "pending")
        stale = sum(1 for item in results if item.get("status") == "stale")
        failed = sum(1 for item in results if item.get("status") == "failed")
        print("Cancel summary")
        print(f"Matched: {len(results)}")
        print(f"Cancelled: {cancelled}")
        print(f"Pending: {pending}")
        print(f"Stale: {stale}")
        print(f"Failed: {failed}")
        for item in results:
            print(
                f"- {item.get('job_id', '?')}: {item.get('status', '?')}"
                + (f" ({item.get('reason', '')})" if item.get("reason") else "")
            )
        if pending or failed:
            raise SystemExit(1)

    elif args.mode == "reconcile":
        cleared_locks = state.clear_expired_locks()
        cleared_stale_locks = state.clear_stale_pid_locks()
        jobs = state.list_running_jobs()
        reconciled = 0
        completed = 0
        failed = 0
        alive = 0
        for job in jobs:
            result = _reconcile_running_job(state, job)
            if result.get("status") == "alive":
                alive += 1
                continue
            reconciled += 1
            if result.get("status") == "completed":
                completed += 1
            else:
                failed += 1
        print("Reconcile summary")
        print(f"Running: {len(jobs)}")
        print(f"Alive: {alive}")
        print(f"Reconciled: {reconciled}")
        print(f"Completed: {completed}")
        print(f"Failed: {failed}")
        print(f"Expired locks cleared: {cleared_locks}")
        print(f"Stale pid locks cleared: {cleared_stale_locks}")

    elif args.mode == "pause":
        target = _target_to_pause_key(args.target or "all")
        reason = args.reason.strip() or f"Paused via orchestrator daemon ({target})"
        state.set_pause(target, reason)
        state.add_event(
            event_type="control",
            message=f"Paused {target}: {reason}",
            severity="info",
            payload={"action": "pause", "target": target, "reason": reason},
        )
        print(f"Paused {target}")

    elif args.mode == "resume":
        target = _target_to_pause_key(args.target or "all")
        state.clear_pause(target)
        state.add_event(
            event_type="control",
            message=f"Resumed {target}",
            severity="info",
            payload={"action": "resume", "target": target},
        )
        print(f"Resumed {target}")

    elif args.mode == "janitor":
        result = run_janitor(config, state)
        print(
            "Janitor complete: "
            f"success={result.get('success', False)} "
            f"events={result.get('events_deleted', 0)} "
            f"logs={result.get('log_files_deleted', 0)} "
            f"run_dirs={result.get('run_dirs_deleted', 0)} "
            f"reports={result.get('report_files_deleted', 0)} "
            f"audio_orphans={result.get('audio_orphans_deleted', 0)}"
        )

    elif args.mode == "status":
        from .reports import get_latest_report
        report = get_latest_report()
        if report:
            print(f"Last cycle: {report.get('generated_at', '?')}")
            print(f"Profile: {report.get('profile', '?')}")
            print(f"Pending: {report.get('pending_work', {})}")
            print(f"Cooldowns: {report.get('cooldowns', {}).get('active_count', 0)} active")
            print(f"Active jobs: {report.get('inventory', {}).get('active_jobs', {}).get('active_count', 0)}")
        else:
            print("No report yet. Run 'orchestrator once' first.")

    elif args.mode == "explain":
        inventory = build_inventory_snapshot(config, state, None)
        print("Orchestrator explain")
        print(f"Mode: {inventory.get('mode', config.get('orchestrator', {}).get('mode', 'work_conserving'))}")
        print(f"Work remaining: {inventory.get('work_remaining', {})}")
        print(f"Blocked: {inventory.get('blocked', {})}")
        print(f"Active locks: {inventory.get('locks', {}).get('active_count', 0)}")
        print(f"Active jobs: {inventory.get('active_jobs', {}).get('active_count', 0)}")
        print(f"Active cooldowns: {inventory.get('cooldowns', {}).get('active_count', 0)}")
        print(f"Timeouts: {config.get('timeouts', {})}")
        system_info = inventory.get("system", {})
        print(f"Disk free: {system_info.get('disk_free_gb', 0):.1f} GB")
        print(f"Memory available: {system_info.get('mem_available_mb', 0):.0f} MB")
        pauses = {str(p.get("pause_key", "")).strip(): str(p.get("value", "")).strip() for p in inventory.get("pauses", [])}
        work_remaining = inventory.get("work_remaining", {})
        blocked = inventory.get("blocked", {})
        youtube_blocked = bool(blocked.get("youtube"))
        provider_blocked = bool(blocked.get("provider"))
        disk_low = float(system_info.get("disk_free_gb", 0) or 0) < float(config.get("system", {}).get("min_free_disk_gb", 5) or 5)
        mem_low = float(system_info.get("mem_available_mb", 0) or 0)
        stage_defs = [
            ("Import Pending", "import_pending", "local"),
            ("Transcript", "transcript", "youtube"),
            ("Audio Download", "audio_download", "youtube"),
            ("ASR", "asr", "provider"),
            ("Resume", "resume", "provider"),
            ("Format", "format", "local"),
            ("Discovery", "discovery", "youtube"),
        ]
        stage_decisions: list[tuple[str, str]] = []
        active_total = int(inventory.get("active_jobs", {}).get("active_count", 0) or 0)
        max_total_jobs = _max_total_jobs(config)
        for label, stage_key, dep in stage_defs:
            work_count = int(work_remaining.get(stage_key, 0) or 0)
            if pauses.get(f"pause:stage:{stage_key}") or pauses.get("pause:scope:all") or (dep == "youtube" and pauses.get("pause:scope:youtube")) or (dep == "provider" and pauses.get("pause:scope:provider")):
                stage_decisions.append((label, "PAUSED"))
                continue
            if work_count <= 0:
                stage_decisions.append((label, "NO_WORK"))
                continue
            if disk_low:
                stage_decisions.append((label, "WAIT_DISK"))
                continue
            if stage_key == "asr" and mem_low < float(config.get("system", {}).get("min_memory_mb_asr", 2500) or 2500):
                stage_decisions.append((label, "WAIT_MEMORY"))
                continue
            if stage_key == "resume" and mem_low < float(config.get("system", {}).get("min_memory_mb_resume", 1500) or 1500):
                stage_decisions.append((label, "WAIT_MEMORY"))
                continue
            if stage_key == "format" and mem_low < float(config.get("system", {}).get("min_memory_mb_format", 1200) or 1200):
                stage_decisions.append((label, "WAIT_MEMORY"))
                continue
            if dep == "youtube" and youtube_blocked:
                stage_decisions.append((label, "WAIT_YOUTUBE_COOLDOWN"))
                continue
            if dep == "provider" and provider_blocked:
                stage_decisions.append((label, "WAIT_PROVIDER_COOLDOWN"))
                continue
            stage_group = _stage_group_name(config, stage_key)
            if active_total >= max_total_jobs:
                stage_decisions.append((label, "WAIT_MAX_TOTAL_JOBS"))
                continue
            if state.count_running_by_group(stage_group) >= _group_limit(config, stage_group):
                stage_decisions.append((label, "WAIT_GROUP_SLOT"))
                continue
            if state.count_running_by_stage(stage_key) >= _stage_slots(config, stage_key):
                stage_decisions.append((label, "WAIT_STAGE_SLOT"))
                continue
            stage_decisions.append((label, "RUN"))
        print("Stage decisions:")
        for stage_name, decision_text in stage_decisions:
            print(f"  - {stage_name}: {decision_text}")
        reasons = inventory.get("defer_reasons", {})
        if reasons:
            print("Defer reasons:")
            for code, count in sorted(reasons.items(), key=lambda kv: (-kv[1], kv[0])):
                print(f"  - {code}: {count}")
        else:
            print("Defer reasons: none")

    elif args.mode == "report":
        from .reports import get_latest_report
        report = get_latest_report()
        if report:
            print(json.dumps(report, indent=2, default=str))
        else:
            print("No report yet.")

    state.close()


if __name__ == "__main__":
    main()
