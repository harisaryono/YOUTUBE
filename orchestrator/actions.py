"""
Orchestrator safe control actions.

This module centralizes pause/resume, quarantine, and retry-candidate logic so
CLI and admin UI can share the same control-plane semantics.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from .state import OrchestratorState
from .policies import policy_blockers_for_job, quarantine_stages_from_payload


KNOWN_GROUPS = {"discovery", "youtube", "youtube_download", "provider", "local"}
KNOWN_STAGES = {"discovery", "transcript", "audio_download", "resume", "asr", "format", "janitor", "import_pending"}


@dataclass
class ActionResult:
    ok: bool
    action: str
    message: str
    data: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


def _emit_event(
    state: OrchestratorState,
    *,
    event_type: str,
    message: str,
    stage: str = "",
    scope: str = "",
    severity: str = "info",
    recommendation: str = "",
    payload: dict[str, Any] | None = None,
) -> None:
    state.record_event(
        event_type=event_type,
        message=message,
        stage=stage,
        scope=scope,
        severity=severity,
        recommendation=recommendation,
        payload=payload or {},
    )


def _normalized_pause_key(target: str) -> str:
    value = str(target or "").strip().lower()
    if not value:
        return "scope:all"
    if value.startswith("pause:"):
        value = value.replace("pause:", "", 1)
    if value in {"all", "scope:all"}:
        return "scope:all"
    if value.startswith(("stage:", "group:", "scope:")):
        return value
    if value in KNOWN_GROUPS:
        return f"scope:{value}"
    if value in KNOWN_STAGES:
        return f"stage:{value}"
    return f"scope:{value}"


def _pause_kind_from_key(key: str) -> str:
    value = str(key or "").strip()
    if ":" not in value:
        return "scope"
    return value.split(":", 1)[0]


def _pause_name_from_key(key: str) -> str:
    value = str(key or "").strip()
    if ":" not in value:
        return value
    return value.split(":", 1)[1]


def _parse_stage_args(values: list[str] | None) -> list[str]:
    if not values:
        return []
    payload = {"stages": values}
    return sorted(quarantine_stages_from_payload(payload))


def _infer_quarantine_stages_from_reason(reason: str) -> list[str]:
    payload = {"reason": reason or ""}
    return sorted(quarantine_stages_from_payload(payload))


def pause_target(state: OrchestratorState, target: str, minutes: int, reason: str, *, actor: str = "cli") -> ActionResult:
    pause_key = _normalized_pause_key(target)
    reason = str(reason or "").strip() or f"Paused via orchestrator ({pause_key})"
    until = (datetime.now(timezone.utc) + timedelta(minutes=max(1, int(minutes or 1)))).isoformat(timespec="seconds")
    state.set_pause_details(
        pause_key,
        reason,
        until=until,
        actor=actor,
        metadata={"target": pause_key},
    )
    kind = _pause_kind_from_key(pause_key)
    name = _pause_name_from_key(pause_key)
    event_type = f"control.pause_{kind}"
    _emit_event(
        state,
        event_type=event_type,
        message=f"Paused {pause_key}: {reason}",
        stage=name if kind == "stage" else kind,
        scope=pause_key,
        payload={"action": "pause", "pause_key": pause_key, "reason": reason, "actor": actor, "until": until, "minutes": int(minutes or 1)},
    )
    return ActionResult(ok=True, action="pause", message=f"Paused {pause_key} for {minutes} minutes", data={"pause_key": pause_key, "reason": reason, "until": until})


def resume_target(state: OrchestratorState, target: str, *, actor: str = "cli") -> ActionResult:
    pause_key = _normalized_pause_key(target)
    state.clear_pause(pause_key)
    kind = _pause_kind_from_key(pause_key)
    name = _pause_name_from_key(pause_key)
    event_type = f"control.resume_{kind}"
    _emit_event(
        state,
        event_type=event_type,
        message=f"Resumed {pause_key}",
        stage=name if kind == "stage" else kind,
        scope=pause_key,
        payload={"action": "resume", "pause_key": pause_key, "actor": actor},
    )
    return ActionResult(ok=True, action="resume", message=f"Resumed {pause_key}", data={"pause_key": pause_key})


def pause_stage(state: OrchestratorState, stage: str, minutes: int, reason: str, *, actor: str = "cli") -> ActionResult:
    stage = str(stage or "").strip()
    if not stage:
        return ActionResult(ok=False, action="pause-stage", message="Stage kosong", data={})
    result = pause_target(state, f"stage:{stage}", minutes, reason, actor=actor)
    result.action = "pause-stage"
    return result


def resume_stage(state: OrchestratorState, stage: str, *, actor: str = "cli") -> ActionResult:
    stage = str(stage or "").strip()
    if not stage:
        return ActionResult(ok=False, action="resume-stage", message="Stage kosong", data={})
    result = resume_target(state, f"stage:{stage}", actor=actor)
    result.action = "resume-stage"
    return result


def pause_group(state: OrchestratorState, group: str, minutes: int, reason: str, *, actor: str = "cli") -> ActionResult:
    group = str(group or "").strip()
    if not group:
        return ActionResult(ok=False, action="pause-group", message="Group kosong", data={})
    if group not in KNOWN_GROUPS:
        return ActionResult(ok=False, action="pause-group", message=f"Group tidak dikenal: {group}", data={"group": group})
    result = pause_target(state, f"group:{group}", minutes, reason, actor=actor)
    result.action = "pause-group"
    return result


def resume_group(state: OrchestratorState, group: str, *, actor: str = "cli") -> ActionResult:
    group = str(group or "").strip()
    if not group:
        return ActionResult(ok=False, action="resume-group", message="Group kosong", data={})
    result = resume_target(state, f"group:{group}", actor=actor)
    result.action = "resume-group"
    return result


def quarantine_channel(
    state: OrchestratorState,
    channel_id: str,
    reason: str,
    *,
    actor: str = "cli",
    stages: list[str] | None = None,
) -> ActionResult:
    channel_id = str(channel_id or "").strip()
    if not channel_id:
        return ActionResult(ok=False, action="quarantine-channel", message="Channel ID kosong", data={})
    reason = str(reason or "").strip() or f"Quarantined via orchestrator ({channel_id})"
    scoped_stages = sorted(set(stages or []))
    if not scoped_stages:
        scoped_stages = _infer_quarantine_stages_from_reason(reason)
    metadata: dict[str, Any] = {}
    if scoped_stages:
        metadata["stages"] = scoped_stages
        metadata["scope_mode"] = "stage_scoped"
    else:
        metadata["scope_mode"] = "channel_global"
    state.quarantine_channel(channel_id, reason, actor=actor, metadata=metadata)
    state.clear_cooldown(f"channel:{channel_id}")
    _emit_event(
        state,
        event_type="control.quarantine_channel",
        message=f"Quarantined channel {channel_id}: {reason}",
        stage="control",
        scope=f"channel:{channel_id}",
        payload={"action": "quarantine", "channel_id": channel_id, "reason": reason, "actor": actor, **metadata},
    )
    stage_suffix = f" for stage(s): {', '.join(scoped_stages)}" if scoped_stages else " globally"
    return ActionResult(
        ok=True,
        action="quarantine-channel",
        message=f"Quarantined channel {channel_id}{stage_suffix}",
        data={"channel_id": channel_id, "reason": reason, **metadata},
    )


def unquarantine_channel(state: OrchestratorState, channel_id: str, *, actor: str = "cli") -> ActionResult:
    channel_id = str(channel_id or "").strip()
    if not channel_id:
        return ActionResult(ok=False, action="unquarantine-channel", message="Channel ID kosong", data={})
    state.unquarantine_channel(channel_id, actor=actor)
    _emit_event(
        state,
        event_type="control.unquarantine_channel",
        message=f"Unquarantined channel {channel_id}",
        stage="control",
        scope=f"channel:{channel_id}",
        payload={"action": "unquarantine", "channel_id": channel_id, "actor": actor},
    )
    return ActionResult(ok=True, action="unquarantine-channel", message=f"Unquarantined channel {channel_id}", data={"channel_id": channel_id})


def retry_failed(state: OrchestratorState, *, stage: str = "", limit: int = 20, dry_run: bool = True, actor: str = "cli") -> ActionResult:
    stage = str(stage or "").strip().lower()
    limit = max(1, int(limit or 1))
    failed_jobs = state.list_jobs(status="failed", stage=stage or None)
    timed_out_jobs = state.list_jobs(status="timeout", stage=stage or None)
    combined: dict[str, dict[str, Any]] = {}
    for row in failed_jobs + timed_out_jobs:
        job_id = str(row.get("job_id") or "").strip()
        if job_id:
            combined[job_id] = dict(row)
    candidates = list(combined.values())
    candidates.sort(key=lambda row: str(row.get("started_at") or ""))
    candidates = candidates[:limit]

    eligible: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    for row in candidates:
        stage_name = str(row.get("stage") or "").strip().lower()
        scope = str(row.get("scope") or "").strip()
        blockers = policy_blockers_for_job(state, stage=stage_name, scope=scope)
        row = dict(row)
        row["policy_blockers"] = blockers
        if blockers:
            blocked.append(row)
        else:
            eligible.append(row)

    eligible = eligible[:limit]
    queued: list[dict[str, Any]] = []
    if not dry_run:
        for row in eligible:
            queued_item = state.enqueue_retry_queue_item(row, requested_by=actor, reason=f"retry_failed:{stage or 'all'}", max_attempts=3)
            queued.append(queued_item)

    payload = {
        "stage": stage,
        "limit": limit,
        "dry_run": dry_run,
        "actor": actor,
        "candidate_count": len(candidates),
        "eligible_count": len(eligible),
        "blocked_count": len(blocked),
        "queued_count": len(queued),
        "candidates": candidates,
        "eligible": eligible,
        "blocked": blocked,
        "queued": queued,
    }
    _emit_event(
        state,
        event_type="control.retry_failed",
        message=f"Retry failed requested for stage={stage or 'all'} limit={limit} dry_run={dry_run} eligible={len(eligible)} blocked={len(blocked)} queued={len(queued)}",
        stage=stage or "control",
        scope=f"stage:{stage}" if stage else "scope:all",
        payload=payload,
        recommendation="Dry-run only; actual requeue uses retry queue and daemon planner",
    )
    message = f"Dry-run retry candidates: {len(candidates)} job(s)" if dry_run else f"Queued {len(queued)} retry job(s)"
    if not dry_run and blocked:
        message += f" ({len(blocked)} blocked by policy)"
    return ActionResult(ok=True, action="retry-failed", message=message, data=payload, warnings=[] if dry_run else ([f"{len(blocked)} candidate(s) blocked by policy and not queued"] if blocked else []))


def _print_result(result: ActionResult) -> None:
    print(result.message)
    if result.warnings:
        print("Warnings:")
        for warning in result.warnings:
            print(f"- {warning}")
    if result.data:
        print("Data:")
        print(json.dumps(result.data, indent=2, ensure_ascii=False, default=str))


def _emit_json(result: ActionResult) -> None:
    print(json.dumps(asdict(result), indent=2, ensure_ascii=False, default=str))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Orchestrator safe control actions")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common_flags(p: argparse.ArgumentParser) -> None:
        p.add_argument("--json", action="store_true", help="Emit machine-readable JSON output")
        p.add_argument("--actor", default="cli", help="Actor label for audit events")

    p = subparsers.add_parser("pause-stage", help="Pause a stage for a number of minutes")
    p.add_argument("stage")
    p.add_argument("--minutes", type=int, default=60)
    p.add_argument("--reason", default="")
    add_common_flags(p)

    p = subparsers.add_parser("resume-stage", help="Resume a paused stage")
    p.add_argument("stage")
    add_common_flags(p)

    p = subparsers.add_parser("pause-group", help="Pause a control group")
    p.add_argument("group")
    p.add_argument("--minutes", type=int, default=60)
    p.add_argument("--reason", default="")
    add_common_flags(p)

    p = subparsers.add_parser("resume-group", help="Resume a control group")
    p.add_argument("group")
    add_common_flags(p)

    p = subparsers.add_parser("retry-failed", help="List or enqueue retry candidates")
    p.add_argument("--stage", default="")
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--dry-run", action="store_true", default=True)
    p.add_argument("--no-dry-run", dest="dry_run", action="store_false")
    add_common_flags(p)

    p = subparsers.add_parser("quarantine-channel", help="Quarantine a channel")
    p.add_argument("channel_id")
    p.add_argument("--reason", default="")
    p.add_argument("--stage", action="append", choices=sorted(KNOWN_STAGES), help="Limit quarantine to a stage; can be repeated")
    p.add_argument("--stages", default="", help="Comma-separated stage list for scoped quarantine")
    add_common_flags(p)

    p = subparsers.add_parser("unquarantine-channel", help="Release a channel from quarantine")
    p.add_argument("channel_id")
    add_common_flags(p)
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    state = OrchestratorState()
    try:
        if args.command == "pause-stage":
            result = pause_stage(state, args.stage, args.minutes, args.reason, actor=args.actor)
        elif args.command == "resume-stage":
            result = resume_stage(state, args.stage, actor=args.actor)
        elif args.command == "pause-group":
            result = pause_group(state, args.group, args.minutes, args.reason, actor=args.actor)
        elif args.command == "resume-group":
            result = resume_group(state, args.group, actor=args.actor)
        elif args.command == "retry-failed":
            result = retry_failed(state, stage=args.stage, limit=args.limit, dry_run=bool(args.dry_run), actor=args.actor)
        elif args.command == "quarantine-channel":
            stage_values = list(args.stage or [])
            if str(args.stages or "").strip():
                stage_values.extend([item.strip() for item in str(args.stages).split(",") if item.strip()])
            result = quarantine_channel(state, args.channel_id, args.reason, actor=args.actor, stages=_parse_stage_args(stage_values))
        elif args.command == "unquarantine-channel":
            result = unquarantine_channel(state, args.channel_id, actor=args.actor)
        else:
            raise SystemExit(f"Unsupported command: {args.command}")

        if args.json:
            _emit_json(result)
        else:
            _print_result(result)
        raise SystemExit(0 if result.ok else 1)
    finally:
        state.close()


if __name__ == "__main__":
    main()
