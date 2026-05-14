"""
Orchestrator Daemon — Main loop that plans, checks safety, and dispatches jobs.
Modes:
  once  — Run one cycle and exit.
  run   — Run continuously with adaptive sleep.
"""

from __future__ import annotations

import json
import os
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
from .dispatcher import dispatch_job
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
    """
    Run one orchestrator cycle:
    1. Clear expired cooldowns and locks
    2. Check system health
    3. Plan jobs (batch model: 1 job = 1 stage)
    4. Safety gate each job
    5. Acquire stage lock before dispatch
    6. Dispatch safe jobs
    7. Generate report
    """
    start_time = time.time()
    max_jobs = _effective_max_jobs(config, max_jobs)
    cycle_result: dict[str, Any] = {
        "jobs_planned": 0,
        "jobs_dispatched": 0,
        "jobs_succeeded": 0,
        "jobs_failed": 0,
        "jobs_deferred": 0,
        "duration_seconds": 0,
        "disk_free_gb": 0,
        "mem_available_mb": 0,
        "dry_run": dry_run,
    }

    # 1. Clear expired cooldowns and locks
    cleared_cd = clear_all_cooldowns(state)
    if cleared_cd > 0:
        state.add_event(
            event_type="cleanup",
            message=f"Cleared {cleared_cd} expired cooldown(s)",
            severity="info",
        )
    cleared_locks = state.clear_expired_locks()
    if cleared_locks > 0:
        state.add_event(
            event_type="cleanup",
            message=f"Cleared {cleared_locks} expired lock(s)",
            severity="info",
        )
    cleared_stale_locks = state.clear_stale_pid_locks()
    if cleared_stale_locks > 0:
        state.add_event(
            event_type="cleanup",
            message=f"Cleared {cleared_stale_locks} stale pid lock(s)",
            severity="info",
        )

    # 2. Check health
    sys_health = check_system_health(config)
    provider_health = check_provider_health(config, state)
    youtube_health = check_youtube_health(config, state)

    cycle_result["disk_free_gb"] = sys_health.disk_free_gb
    cycle_result["mem_available_mb"] = sys_health.mem_available_mb

    # Log health issues
    if not sys_health.ok:
        for err in sys_health.errors:
            state.add_event(
                event_type="health",
                message=f"System: {err}",
                severity="blocking",
            )
    if not provider_health.ok:
        for err in provider_health.errors:
            state.add_event(
                event_type="health",
                message=f"Provider: {err}",
                severity="warning",
            )
    if not youtube_health.ok:
        for err in youtube_health.errors:
            state.add_event(
                event_type="health",
                message=f"YouTube: {err}",
                severity="blocking",
            )

    # 3. Plan jobs (batch model)
    jobs = plan_jobs(config, state, max_jobs=max_jobs)
    cycle_result["jobs_planned"] = len(jobs)

    if not jobs:
        state.add_event(
            event_type="plan",
            message="No jobs to run",
            severity="info",
        )
        cycle_result["duration_seconds"] = time.time() - start_time
        generate_report(config, state, cycle_result)
        return cycle_result

    # 4. Safety gate + lock + dispatch
    for job in jobs:
        decision = safety_gate_for_job(
            job, config, sys_health, provider_health, youtube_health, state
        )

        if decision.verdict == "RUN":
            stage = job.get("stage", "")
            lock_key = f"stage:{stage}"

            if dry_run:
                print(f"  [DRY-RUN] Would dispatch: {job.get('description', stage)}")
                cycle_result["jobs_dispatched"] += 1
                continue

            # Acquire stage lock and always release it, even if dispatch fails.
            if not state.acquire_lock(lock_key, ttl_seconds=7200):
                state.add_event(
                    event_type="deferred",
                    message=f"{stage} lock could not be acquired, deferring",
                    stage=stage,
                    scope=job.get("scope", ""),
                    severity="info",
                    reason_code="DEFER_STAGE_LOCKED",
                )
                cycle_result["jobs_deferred"] += 1
                continue

            cycle_result["jobs_dispatched"] += 1
            result: dict[str, Any] = {
                "success": False,
                "error": "dispatch did not run",
                "returncode": 1,
            }
            try:
                result = dispatch_job(job, config, state)
            except Exception as e:
                result = {
                    "success": False,
                    "error": f"dispatch exception: {e}",
                    "returncode": 1,
                }
            finally:
                state.release_lock(lock_key)

            if result.get("success"):
                cycle_result["jobs_succeeded"] += 1
                _maybe_update_adaptive_batch(config, state, stage, success=True, blocked=False)
            else:
                cycle_result["jobs_failed"] += 1
                # Apply cooldown based on error
                error_msg = result.get("error", result.get("stderr", ""))
                blocked_failure = False
                if error_msg:
                    from .error_analyzer import classify_error

                    classification = classify_error(error_msg, result.get("returncode", 0))
                    blocked_failure = classification.cooldown_seconds > 0
                    if classification.cooldown_seconds > 0:
                        # Use suggested_scope from classification, fallback to job scope
                        scope = classification.suggested_scope or job.get("scope", "global")
                        state.set_cooldown(
                            scope=scope,
                            reason=classification.description,
                            duration_seconds=classification.cooldown_seconds,
                            severity=classification.severity,
                            recommendation=classification.recommendation,
                        )
                    state.add_event(
                        event_type="error",
                        message=f"[{classification.error_type}] {classification.description}: {error_msg[:200]}",
                        stage=stage,
                        scope=job.get("scope", ""),
                        severity=classification.severity,
                        recommendation=classification.recommendation,
                        reason_code=classification.error_type,
                        payload={
                            "returncode": result.get("returncode", 0),
                            "error_type": classification.error_type,
                            "cooldown_seconds": classification.cooldown_seconds,
                        },
                    )
                _maybe_update_adaptive_batch(config, state, stage, success=False, blocked=blocked_failure)

        elif decision.verdict == "WAIT":
            cycle_result["jobs_deferred"] += 1
            state.add_event(
                event_type="deferred",
                message=f"{decision.reason_code or 'DEFER'}: {job.get('stage', '?')} - {decision.reason}",
                stage=job.get("stage", ""),
                scope=job.get("scope", ""),
                severity="info",
                recommendation=decision.recommendation,
                reason_code=decision.reason_code,
            )
            if decision.cooldown_seconds > 0 and job.get("scope"):
                state.set_cooldown(
                    scope=job["scope"],
                    reason=decision.reason,
                    duration_seconds=decision.cooldown_seconds,
                    recommendation=decision.recommendation,
                )

        elif decision.verdict == "SKIP_PERMANENT":
            state.add_event(
                event_type="skipped",
                message=f"Job skipped permanently ({job.get('stage', '?')}): {decision.reason}",
                stage=job.get("stage", ""),
                scope=job.get("scope", ""),
                severity="info",
                recommendation=decision.recommendation,
            )

        elif decision.verdict == "REPORT":
            state.add_event(
                event_type="report",
                message=f"Job reported ({job.get('stage', '?')}): {decision.reason}",
                stage=job.get("stage", ""),
                scope=job.get("scope", ""),
                severity="warning",
                recommendation=decision.recommendation,
            )

    # 5. Generate report
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

        if dry_run:
            # Dry-run: check again soon.
            sleep_seconds = _short_sleep_seconds(config)
        elif jobs_dispatched > 0 or jobs_failed > 0:
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
        choices=["once", "run", "status", "explain", "report", "preflight", "pause", "resume", "janitor"],
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
        else:
            print("No report yet. Run 'orchestrator once' first.")

    elif args.mode == "explain":
        inventory = build_inventory_snapshot(config, state, None)
        print("Orchestrator explain")
        print(f"Mode: {inventory.get('mode', config.get('orchestrator', {}).get('mode', 'work_conserving'))}")
        print(f"Work remaining: {inventory.get('work_remaining', {})}")
        print(f"Blocked: {inventory.get('blocked', {})}")
        print(f"Active locks: {inventory.get('locks', {}).get('active_count', 0)}")
        print(f"Active cooldowns: {inventory.get('cooldowns', {}).get('active_count', 0)}")
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
        stage_decisions = [
            ("Import Pending", "RUN" if work_remaining.get("import_pending", 0) else "NO_WORK"),
            (
                "Transcript",
                "PAUSED"
                if pauses.get("pause:stage:transcript") or pauses.get("pause:scope:youtube") or pauses.get("pause:scope:all")
                else "WAIT_YOUTUBE_COOLDOWN"
                if youtube_blocked and work_remaining.get("transcript", 0)
                else ("RUN" if work_remaining.get("transcript", 0) else "NO_WORK"),
            ),
            (
                "Audio Download",
                "PAUSED"
                if pauses.get("pause:stage:audio_download") or pauses.get("pause:scope:youtube") or pauses.get("pause:scope:all")
                else "WAIT_YOUTUBE_COOLDOWN"
                if youtube_blocked and work_remaining.get("audio_download", 0)
                else ("RUN" if work_remaining.get("audio_download", 0) else "NO_WORK"),
            ),
            (
                "ASR",
                "PAUSED"
                if pauses.get("pause:stage:asr") or pauses.get("pause:scope:provider") or pauses.get("pause:scope:all")
                else "WAIT_MEMORY"
                if mem_low < float(config.get("system", {}).get("min_memory_mb_asr", 2500) or 2500)
                else ("RUN" if work_remaining.get("asr", 0) else "NO_WORK"),
            ),
            (
                "Resume",
                "PAUSED"
                if pauses.get("pause:stage:resume") or pauses.get("pause:scope:provider") or pauses.get("pause:scope:all")
                else "WAIT_PROVIDER_COOLDOWN"
                if provider_blocked and work_remaining.get("resume", 0)
                else "WAIT_MEMORY"
                if mem_low < float(config.get("system", {}).get("min_memory_mb_resume", 1500) or 1500)
                else ("RUN" if work_remaining.get("resume", 0) else "NO_WORK"),
            ),
            (
                "Format",
                "PAUSED"
                if pauses.get("pause:stage:format") or pauses.get("pause:scope:provider") or pauses.get("pause:scope:all")
                else "WAIT_PROVIDER_COOLDOWN"
                if provider_blocked and work_remaining.get("format", 0)
                else "WAIT_MEMORY"
                if mem_low < float(config.get("system", {}).get("min_memory_mb_format", 1200) or 1200)
                else ("RUN" if work_remaining.get("format", 0) else "NO_WORK"),
            ),
            (
                "Discovery",
                "PAUSED"
                if pauses.get("pause:stage:discovery") or pauses.get("pause:scope:youtube") or pauses.get("pause:scope:all")
                else "WAIT_YOUTUBE_COOLDOWN"
                if youtube_blocked and work_remaining.get("discovery", 0)
                else ("RUN" if work_remaining.get("discovery", 0) else "NO_WORK"),
            ),
        ]
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
