"""
Retry queue executor and inspection helpers.

This module keeps retry-queue handling explicit so operators can inspect,
dry-run, and drain retry candidates without inventing a second control plane.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .config import DEFAULT_CONFIG_PATH, load_config
from .dispatcher import launch_job
from .planner import build_retry_queue_job
from .policies import policy_blockers_for_job
from .safety import ensure_launch_allowed
from .state import OrchestratorState


def _parallel_config(config: dict[str, Any]) -> dict[str, Any]:
    parallel = config.get("parallel", {}) or {}
    if parallel:
        return parallel
    groups = config.get("orchestrator", {}).get("parallel_groups", {}) or {}
    return {
        "enabled": True,
        "max_total_jobs": config.get("orchestrator", {}).get("max_parallel_jobs", 1),
        "groups": {
            "discovery": {"max_running": 1, "stages": ["discovery"]},
            "youtube": {"max_running": groups.get("youtube", 2), "stages": ["transcript"]},
            "youtube_download": {"max_running": groups.get("youtube_download", 1), "stages": ["audio_download"]},
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
            "import_pending": {"slots": 1},
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


def _group_limit(config: dict[str, Any], group_name: str) -> int:
    parallel = _parallel_config(config)
    group_cfg = parallel.get("groups", {}).get(group_name, {}) or {}
    try:
        return max(0, int(group_cfg.get("max_running", 1) or 1))
    except (TypeError, ValueError):
        return 1


def _parallel_group_for_stage(stage: str) -> str:
    stage = str(stage or "").strip().lower()
    if stage == "discovery":
        return "discovery"
    if stage == "transcript":
        return "youtube"
    if stage == "audio_download":
        return "youtube_download"
    if stage in {"resume", "asr"}:
        return "provider"
    return "local"


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


def _format_duration(seconds: int) -> str:
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds}s"
    minutes, sec = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m{sec:02d}s"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h{minutes:02d}m"
    days, hours = divmod(hours, 24)
    return f"{days}d{hours:02d}h"


def _summarize_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_job_id": str(item.get("source_job_id") or "").strip(),
        "stage": str(item.get("stage") or "").strip(),
        "scope": str(item.get("scope") or "").strip(),
        "reason": str(item.get("reason") or "").strip(),
        "requested_by": str(item.get("requested_by") or "").strip(),
        "requested_at": str(item.get("requested_at") or "").strip(),
        "status": str(item.get("status") or "").strip(),
        "attempts": int(item.get("attempts") or 0),
        "max_attempts": int(item.get("max_attempts") or 0),
        "claimed_by": str(item.get("claimed_by") or "").strip(),
        "claimed_at": str(item.get("claimed_at") or "").strip(),
        "error_text": str(item.get("error_text") or "").strip(),
    }


def build_retry_queue_summary(
    state: OrchestratorState,
    *,
    pending_limit: int = 200,
) -> dict[str, Any]:
    pending_count = state.count_retry_queue("pending")
    running_count = state.count_retry_queue("running")
    completed_count = state.count_retry_queue("completed")
    failed_count = state.count_retry_queue("failed")
    claimed_count = state.count_retry_queue("claimed")
    preview_limit = max(1, min(int(pending_limit or 200), 1000))
    pending_items = state.list_retry_queue(status="pending", limit=preview_limit)

    blocked_preview: list[dict[str, Any]] = []
    blocked_count = 0
    oldest_pending = _summarize_item(pending_items[0]) if pending_items else {}
    oldest_pending_age = 0
    if pending_items:
        try:
            from datetime import datetime, timezone

            raw = str(pending_items[0].get("requested_at") or "").strip()
            if raw:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                oldest_pending_age = max(0, int((datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds()))
        except Exception:
            oldest_pending_age = 0

    for item in pending_items:
        job = build_retry_queue_job(item)
        if not job:
            continue
        blockers = policy_blockers_for_job(
            state,
            stage=str(job.get("stage") or ""),
            scope=str(job.get("scope") or ""),
        )
        if not blockers:
            continue
        blocked_count += 1
        if len(blocked_preview) < 5:
            blocked_preview.append(
                _summarize_item(item)
                | {
                    "policy_blockers": blockers,
                }
            )

    total = pending_count + claimed_count + running_count + completed_count + failed_count
    return {
        "pending": pending_count,
        "claimed": claimed_count,
        "running": running_count,
        "completed": completed_count,
        "failed": failed_count,
        "blocked_pending": blocked_count,
        "total": total,
        "oldest_pending": oldest_pending,
        "oldest_pending_age_seconds": oldest_pending_age,
        "pending_preview": [_summarize_item(item) for item in pending_items[:10]],
        "blocked_preview": blocked_preview,
    }


def list_retry_queue_items(
    state: OrchestratorState,
    *,
    status: str | None = None,
    stage: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    items = state.list_retry_queue(status=status, stage=stage, limit=limit)
    result: list[dict[str, Any]] = []
    for item in items:
        job = build_retry_queue_job(item)
        blockers = []
        if job:
            blockers = policy_blockers_for_job(
                state,
                stage=str(job.get("stage") or ""),
                scope=str(job.get("scope") or ""),
            )
        result.append(
            _summarize_item(item)
            | {
                "job": job or {},
                "policy_blockers": blockers,
            }
        )
    return result


def drain_retry_queue(
    config: dict[str, Any],
    state: OrchestratorState,
    *,
    limit: int | None = None,
    dry_run: bool = True,
    actor: str = "cli",
) -> dict[str, Any]:
    retry_cfg = config.get("retry_queue", {}) or {}
    effective_limit = limit if limit is not None else int(retry_cfg.get("max_per_cycle", 3) or 3)
    limit = max(1, int(effective_limit))
    candidates = state.list_retry_queue(status="pending", limit=max(limit * 4, limit, 20))
    result: dict[str, Any] = {
        "ok": True,
        "dry_run": bool(dry_run),
        "requested_limit": limit,
        "candidate_count": len(candidates),
        "claimed": 0,
        "launched": 0,
        "blocked": 0,
        "deferred": 0,
        "skipped": 0,
        "items": [],
    }

    # Safety guard: block real drain if emergency stop is active, allow dry-run
    if not dry_run:
        drain_allowed, drain_blockers = ensure_launch_allowed(config, state, action="retry_queue_drain")
        if not drain_allowed:
            state.record_event(
                event_type="safety.drain_blocked",
                message=f"Retry queue drain blocked: {'; '.join(drain_blockers)}",
                stage="control",
                scope="retry_queue",
                payload={"blockers": drain_blockers},
                recommendation="Clear emergency stop before draining retry queue.",
            )
            result["blocked"] = len(candidates)
            for item in candidates:
                result["items"].append(
                    _summarize_item(item)
                    | {
                        "status": "blocked",
                        "policy_blockers": [{"type": "safety", "message": block} for block in drain_blockers],
                    }
                )
            return result

    max_total = _max_total_jobs(config)
    for item in candidates:
        if result["launched"] >= limit:
            break
        source_job_id = str(item.get("source_job_id") or "").strip()
        job = build_retry_queue_job(item)
        if not job:
            result["skipped"] += 1
            result["items"].append(_summarize_item(item) | {"status": "invalid"})
            continue

        stage = str(job.get("stage") or "").strip()
        scope = str(job.get("scope") or "").strip()
        blockers = policy_blockers_for_job(state, stage=stage, scope=scope)
        if blockers:
            result["blocked"] += 1
            result["items"].append(
                _summarize_item(item)
                | {
                    "status": "blocked",
                    "policy_blockers": blockers,
                }
            )
            continue

        if state.count_running_total() >= max_total:
            result["deferred"] += 1
            result["items"].append(_summarize_item(item) | {"status": "deferred:max_total_jobs"})
            continue

        group_name = _parallel_group_for_stage(stage)
        if state.count_running_by_group(group_name) >= _group_limit(config, group_name):
            result["deferred"] += 1
            result["items"].append(_summarize_item(item) | {"status": f"deferred:group:{group_name}"})
            continue

        slot_claim = _claim_stage_slot(config, state, stage)
        if slot_claim is None:
            result["deferred"] += 1
            result["items"].append(_summarize_item(item) | {"status": "deferred:no_slot"})
            continue

        slot_index, lock_key = slot_claim
        if dry_run:
            state.release_lock(lock_key)
            result["items"].append(
                _summarize_item(item)
                | {
                    "status": "would_launch",
                    "slot_index": slot_index,
                    "lock_key": lock_key,
                }
            )
            continue

        if not state.claim_retry_queue_item(source_job_id, claimed_by=actor):
            state.release_lock(lock_key)
            result["skipped"] += 1
            result["items"].append(_summarize_item(item) | {"status": "skipped:claim_lost"})
            continue

        result["claimed"] += 1
        try:
            launch_result = launch_job(
                job,
                config,
                state,
                slot_index=slot_index,
                lock_key=lock_key,
            )
        except Exception as exc:
            state.release_lock(lock_key)
            state.release_retry_queue_item(source_job_id, status="pending", error_text=str(exc))
            result["skipped"] += 1
            result["items"].append(
                _summarize_item(item)
                | {
                    "status": "launch_error",
                    "error": str(exc),
                }
            )
            continue

        if launch_result.get("launched"):
            try:
                state.mark_retry_queue_running(source_job_id, launched_job_id=str(launch_result.get("job_id") or ""))
            except Exception:
                pass
            result["launched"] += 1
            result["items"].append(
                _summarize_item(item)
                | {
                    "status": "launched",
                    "job_id": str(launch_result.get("job_id") or ""),
                    "pid": int(launch_result.get("pid") or 0),
                    "slot_index": slot_index,
                    "lock_key": lock_key,
                }
            )
            continue

        state.release_lock(lock_key)
        state.release_retry_queue_item(
            source_job_id,
            status="pending",
            error_text=str(launch_result.get("reason") or launch_result.get("error") or "launch deferred"),
        )
        if launch_result.get("deferred"):
            result["deferred"] += 1
            result["items"].append(
                _summarize_item(item)
                | {
                    "status": "deferred",
                    "reason": str(launch_result.get("reason") or ""),
                    "reason_code": str(launch_result.get("reason_code") or ""),
                    "policy_blockers": launch_result.get("policy_blockers", []),
                }
            )
        else:
            result["skipped"] += 1
            result["items"].append(
                _summarize_item(item)
                | {
                    "status": "launch_failed",
                    "error": str(launch_result.get("error") or "unknown error"),
                }
            )

    state.record_event(
        event_type="control.retry_queue_drain",
        message=(
            f"Retry queue drain requested limit={limit} dry_run={dry_run} "
            f"claimed={result['claimed']} launched={result['launched']} blocked={result['blocked']} "
            f"deferred={result['deferred']} skipped={result['skipped']}"
        ),
        stage="control",
        scope="retry_queue",
        payload=result,
        recommendation="Dry-run default; actual drain stays bounded by policy and slot availability",
    )
    return result


def _render_stats_text(result: dict[str, Any]) -> str:
    lines = [
        "RETRY QUEUE",
        "",
        f"Pending: {result.get('pending', 0)}",
        f"Claimed: {result.get('claimed', 0)}",
        f"Running: {result.get('running', 0)}",
        f"Completed: {result.get('completed', 0)}",
        f"Failed: {result.get('failed', 0)}",
        f"Blocked pending: {result.get('blocked_pending', 0)}",
    ]
    oldest = result.get("oldest_pending") or {}
    if oldest:
        lines.append("")
        lines.append(
            f"Oldest pending: {oldest.get('source_job_id', '')} "
            f"({oldest.get('stage', '')} / {oldest.get('scope', '')}) "
            f"requested {oldest.get('requested_at', '')}"
        )
        if result.get("oldest_pending_age_seconds", 0):
            lines.append(f"Age: {_format_duration(int(result.get('oldest_pending_age_seconds') or 0))}")
    blocked_preview = result.get("blocked_preview") or []
    if blocked_preview:
        lines.append("")
        lines.append("Blocked preview:")
        for item in blocked_preview[:5]:
            blockers = item.get("policy_blockers", [])
            blocker_text = "; ".join(str(b.get("message") or b) for b in blockers)
            lines.append(
                f"  - {item.get('source_job_id', '')} "
                f"({item.get('stage', '')} / {item.get('scope', '')}): {blocker_text}"
            )
    return "\n".join(lines)


def _render_list_text(items: list[dict[str, Any]]) -> str:
    if not items:
        return "Retry queue empty."
    lines = ["RETRY QUEUE ITEMS", ""]
    for item in items:
        lines.append(
            f"- {item.get('source_job_id', '')} | {item.get('stage', '')} | {item.get('scope', '')} "
            f"| {item.get('status', '')} | attempts {item.get('attempts', 0)}/{item.get('max_attempts', 0)}"
        )
        if item.get("claimed_by") or item.get("claimed_at"):
            lines.append(
                f"  claimed_by={item.get('claimed_by', '')} claimed_at={item.get('claimed_at', '')}"
            )
        if item.get("policy_blockers"):
            for blocker in item.get("policy_blockers", []):
                lines.append(f"  blocker: {blocker.get('message') or blocker}")
    return "\n".join(lines)


def _print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Retry queue executor")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", default=None, help="Path to orchestrator.yaml")
    common.add_argument("--json", action="store_true", help="Emit machine-readable JSON output")
    subparsers = parser.add_subparsers(dest="command", required=True)

    p = subparsers.add_parser("stats", help="Show retry queue summary", parents=[common])
    p.add_argument("--pending-limit", type=int, default=200)

    p = subparsers.add_parser("list", help="List retry queue items", parents=[common])
    p.add_argument("--status", default="pending")
    p.add_argument("--stage", default="")
    p.add_argument("--limit", type=int, default=50)

    p = subparsers.add_parser("drain", help="Drain retry queue items through the launcher", parents=[common])
    p.add_argument("--limit", type=int, default=3)
    p.add_argument("--dry-run", action="store_true", default=True)
    p.add_argument("--no-dry-run", dest="dry_run", action="store_false")
    p.add_argument("--actor", default="cli")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    config = load_config(Path(args.config) if args.config else DEFAULT_CONFIG_PATH)
    state = OrchestratorState()
    try:
        if args.command == "stats":
            result = build_retry_queue_summary(state, pending_limit=max(1, int(args.pending_limit or 200)))
        elif args.command == "list":
            result = {
                "items": list_retry_queue_items(
                    state,
                    status=args.status or None,
                    stage=args.stage or None,
                    limit=max(1, int(args.limit or 50)),
                )
            }
        elif args.command == "drain":
            result = drain_retry_queue(
                config,
                state,
                limit=max(1, int(args.limit or 3)),
                dry_run=bool(args.dry_run),
                actor=str(args.actor or "cli"),
            )
        else:
            raise SystemExit(f"Unsupported command: {args.command}")

        if args.json:
            _print_json(result)
        elif args.command == "stats":
            print(_render_stats_text(result))
        elif args.command == "list":
            print(_render_list_text(result.get("items", [])))
        else:
            print(_render_list_text(result.get("items", [])))
            if result.get("dry_run"):
                print(f"\nDry-run: would launch {len([item for item in result.get('items', []) if item.get('status') == 'would_launch'])} item(s)")
            else:
                print(
                    f"\nLaunched {result.get('launched', 0)} item(s), "
                    f"blocked {result.get('blocked', 0)}, deferred {result.get('deferred', 0)}, skipped {result.get('skipped', 0)}"
                )
        return 0
    finally:
        state.close()


if __name__ == "__main__":
    raise SystemExit(main())
