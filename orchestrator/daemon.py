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


PROJECT_ROOT = Path(__file__).resolve().parent.parent


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
                )
                cycle_result["jobs_deferred"] += 1
                continue

            cycle_result["jobs_dispatched"] += 1
            try:
                result = dispatch_job(job, config, state)
            finally:
                state.release_lock(lock_key)

            if result.get("success"):
                cycle_result["jobs_succeeded"] += 1
            else:
                cycle_result["jobs_failed"] += 1
                # Apply cooldown based on error
                error_msg = result.get("error", result.get("stderr", ""))
                if error_msg:
                    from .error_analyzer import classify_error
                    classification = classify_error(error_msg, result.get("returncode", 0))
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

        elif decision.verdict == "WAIT":
            cycle_result["jobs_deferred"] += 1
            state.add_event(
                event_type="deferred",
                message=f"Job deferred ({job.get('stage', '?')}): {decision.reason}",
                stage=job.get("stage", ""),
                scope=job.get("scope", ""),
                severity="info",
                recommendation=decision.recommendation,
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

        try:
            result = run_once(config, state, max_jobs=max_jobs, dry_run=dry_run)
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
            time.sleep(60)
            continue

        # Adaptive sleep
        jobs_dispatched = result.get("jobs_dispatched", 0)
        jobs_failed = result.get("jobs_failed", 0)
        jobs_planned = result.get("jobs_planned", 0)
        jobs_deferred = result.get("jobs_deferred", 0)

        if dry_run:
            # Dry-run: check again soon
            sleep_seconds = config.get("loop", {}).get("min_sleep_seconds", 60)
        elif jobs_dispatched > 0:
            # Work was done — check again soon
            sleep_seconds = config.get("loop", {}).get("min_sleep_seconds", 60)
        elif jobs_failed > 0:
            # Errors — wait longer
            sleep_seconds = config.get("loop", {}).get("error_sleep_seconds", 1800)
        elif jobs_planned == 0:
            # No work — wait for next cooldown or idle
            next_wakeup = get_next_wakeup(state)
            if next_wakeup > 0:
                sleep_seconds = min(next_wakeup, config.get("loop", {}).get("idle_sleep_seconds", 900))
            else:
                sleep_seconds = config.get("loop", {}).get("idle_sleep_seconds", 900)
        else:
            # Jobs planned but all deferred — moderate wait
            sleep_seconds = config.get("loop", {}).get("error_sleep_seconds", 1800)

        # Ensure minimum sleep
        sleep_seconds = max(sleep_seconds, config.get("loop", {}).get("min_sleep_seconds", 60))

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
        choices=["once", "run", "status", "report"],
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
        default=5,
        help="Maximum jobs per cycle (default: 5)",
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

    args = parser.parse_args()

    # Load config
    config = load_config(args.config)
    if args.profile:
        config["profile"] = args.profile

    # Init state
    state = OrchestratorState()

    if args.mode == "once":
        result = run_once(config, state, max_jobs=args.max_jobs, dry_run=args.dry_run)
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
