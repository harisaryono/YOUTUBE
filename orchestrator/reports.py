"""
Reports — Generate Markdown and JSON reports for user/admin.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .state import OrchestratorState
from . import cooldown as cd
from . import planner
from . import db_queries
from .safety import check_system_health


PROJECT_ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR = PROJECT_ROOT / "runs" / "orchestrator" / "reports"


def _ensure_reports_dir() -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    return REPORTS_DIR


def build_inventory_snapshot(
    config: dict[str, Any],
    state: OrchestratorState,
    cycle_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a compact work inventory snapshot for status/explain/reporting."""
    counts = planner.get_summary_counts(config, state)
    active_cooldowns = state.list_active_cooldowns()
    active_locks = state.list_active_locks()
    active_jobs = state.list_running_jobs()
    recent_events = state.get_recent_events(limit=50)
    pauses = state.list_pauses()
    quarantined_channels = state.list_quarantined_channels()
    retry_queue = {
        "pending": state.count_retry_queue("pending"),
        "running": state.count_retry_queue("running"),
        "completed": state.count_retry_queue("completed"),
        "failed": state.count_retry_queue("failed"),
    }
    retry_queue["total"] = sum(int(retry_queue.get(key, 0) or 0) for key in ("pending", "running", "completed", "failed"))
    sys_health = None
    try:
        sys_health = check_system_health(config)
    except Exception:
        sys_health = None

    blocked = {
        "youtube": any(cd_entry["scope"] == "youtube" for cd_entry in active_cooldowns),
        "youtube_discovery": any(cd_entry["scope"] == "youtube:discovery" for cd_entry in active_cooldowns),
        "youtube_content": any(cd_entry["scope"] == "youtube:content" for cd_entry in active_cooldowns),
        "provider": any(cd_entry["scope"].startswith("provider:") for cd_entry in active_cooldowns),
        "channel": any(cd_entry["scope"].startswith("channel:") for cd_entry in active_cooldowns),
        "pauses": bool(pauses),
        "quarantine": bool(quarantined_channels),
    }

    work_remaining = {
        "import_pending": counts.get("pending_imports", 0),
        "transcript": counts.get("videos_need_transcript", 0),
        "audio_download": counts.get("videos_need_audio_download", 0),
        "asr": counts.get("videos_need_asr", 0),
        "resume": counts.get("videos_need_resume", 0),
        "format": counts.get("videos_need_format", 0),
        "discovery": db_queries.count_channels_need_discovery(config, state),
        "discovery_full_history": counts.get("channels_need_discovery_full_history", 0),
        "discovery_latest_only": counts.get("channels_need_discovery_latest_only", 0),
    }

    system = {
        "disk_free_gb": (cycle_result or {}).get("disk_free_gb", 0),
        "mem_available_mb": (cycle_result or {}).get("mem_available_mb", 0),
    }
    if sys_health is not None:
        system["disk_free_gb"] = getattr(sys_health, "disk_free_gb", system["disk_free_gb"])
        system["mem_available_mb"] = getattr(sys_health, "mem_available_mb", system["mem_available_mb"])

    defer_reasons: dict[str, int] = {}
    for event in recent_events:
        if (event.get("event_type") or "") != "deferred":
            continue
        try:
            payload = json.loads(event.get("payload_json") or "{}")
        except Exception:
            payload = {}
        reason_code = str(payload.get("reason_code") or "").strip()
        if not reason_code:
            msg = str(event.get("message") or "")
            if ":" in msg:
                reason_code = msg.split(":", 1)[0].strip()
        if reason_code:
            defer_reasons[reason_code] = defer_reasons.get(reason_code, 0) + 1

    return {
        "mode": config.get("orchestrator", {}).get("mode", "work_conserving"),
        "timeouts": config.get("timeouts", {}),
        "work_remaining": work_remaining,
        "blocked": blocked,
        "system": system,
        "cooldowns": {
            "active_count": len(active_cooldowns),
            "details": active_cooldowns,
        },
        "locks": {
            "active_count": len(active_locks),
            "details": active_locks,
        },
        "active_jobs": {
            "active_count": len(active_jobs),
            "details": active_jobs,
        },
        "pauses": pauses,
        "quarantined_channels": quarantined_channels,
        "retry_queue": retry_queue,
        "control_actions": [
            event for event in recent_events
            if str(event.get("event_type") or "").startswith("control")
        ],
        "defer_reasons": defer_reasons,
        "cycle_result": cycle_result or {},
    }


def generate_report(
    config: dict[str, Any],
    state: OrchestratorState,
    cycle_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Generate a full orchestrator report.
    Returns dict with report data, and writes markdown + JSON to disk.
    """
    now = datetime.now(timezone.utc)

    # Gather data
    counts = planner.get_summary_counts(config, state)
    active_cooldowns = state.list_active_cooldowns()
    recent_events = state.get_recent_events(limit=20)
    blocked_providers = cd.get_blocked_providers(state)
    blocked_channels = cd.get_blocked_channels(state)
    next_wakeup = cd.get_next_wakeup(state)
    active_locks = state.list_active_locks()
    active_jobs = state.list_running_jobs()
    inventory = build_inventory_snapshot(config, state, cycle_result)

    # Build suggestions
    suggestions = _build_suggestions(config, state, active_cooldowns, cycle_result)

    # Build report
    report: dict[str, Any] = {
        "generated_at": now.isoformat(timespec="seconds"),
        "profile": config.get("profile", "safe"),
        "system": {
            "disk_free_gb": cycle_result.get("disk_free_gb", 0) if cycle_result else 0,
            "mem_available_mb": cycle_result.get("mem_available_mb", 0) if cycle_result else 0,
        },
        "cooldowns": {
            "active_count": len(active_cooldowns),
            "next_wakeup_seconds": next_wakeup,
            "blocked_providers": blocked_providers,
            "blocked_channels": blocked_channels,
            "details": active_cooldowns,
        },
        "locks": {
            "active_count": len(active_locks),
            "details": active_locks,
        },
        "active_jobs": {
            "active_count": len(active_jobs),
            "details": active_jobs,
        },
        "inventory": inventory,
        "pending_work": counts,
        "recent_events": recent_events[:10],
        "cycle_result": cycle_result,
        "suggestions": suggestions,
    }

    try:
        state.record_inventory_snapshot(inventory | {
            "generated_at": now.isoformat(timespec="seconds"),
            "profile": config.get("profile", "safe"),
        })
    except Exception:
        pass

    # Write files
    reports_dir = _ensure_reports_dir()

    # JSON
    json_path = reports_dir / "latest.json"
    json_path.write_text(json.dumps(report, indent=2, default=str))

    # Markdown
    md_path = reports_dir / "latest.md"
    md_path.write_text(_render_markdown(report, config))

    # Also write timestamped copy
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    json_archive = reports_dir / f"report_{timestamp}.json"
    json_archive.write_text(json.dumps(report, indent=2, default=str))

    return report


def _build_suggestions(
    config: dict[str, Any],
    state: OrchestratorState,
    active_cooldowns: list[dict[str, Any]],
    cycle_result: dict[str, Any] | None,
) -> list[str]:
    """Build actionable suggestions based on current state."""
    suggestions: list[str] = []

    # Check YouTube cooldowns
    for cd_entry in active_cooldowns:
        scope = cd_entry["scope"]
        reason = cd_entry["reason"]
        if scope == "youtube":
            if "429" in reason or "rate" in reason.lower():
                suggestions.append("Reduce transcript workers to 1-2, enable --rate-limit-safe")
            elif "bot" in reason.lower() or "captcha" in reason.lower():
                suggestions.append("Stop all YouTube activity for 12-24h, use different IP/proxy")
            elif "403" in reason or "forbidden" in reason.lower():
                suggestions.append("Check cookies/proxy/IP, wait before retry")
        elif scope == "youtube:discovery":
            suggestions.append("Discovery is rate-limited; keep transcript/audio running and retry discovery later")
        elif scope == "youtube:content":
            suggestions.append("Transcript/audio is rate-limited; keep discovery running and let content cooldown expire")
        elif scope.startswith("provider:"):
            provider = scope.replace("provider:", "")
            if "quota" in reason.lower():
                suggestions.append(f"Provider {provider} quota exceeded — wait for reset or switch provider")
            elif "auth" in reason.lower():
                suggestions.append(f"Provider {provider} auth error — check API key")
        elif scope == "coordinator":
            suggestions.append("Check coordinator service, restart if needed")

    # Check memory
    if cycle_result:
        mem_mb = cycle_result.get("mem_available_mb", 0)
        if mem_mb < 2000:
            suggestions.append("Memory low — reduce workers or wait for other jobs to finish")

    # Check disk
    if cycle_result:
        disk_gb = cycle_result.get("disk_free_gb", 0)
        if disk_gb < 10:
            suggestions.append("Disk space low — clean up runs/uploads/logs/cache")

    # Check idle hours for format
    # In aggressive mode, idle hours are advisory only, so do not block the queue
    # or add a strong recommendation here.

    # Check if format/resume are deferred due to lease
    if cycle_result and cycle_result.get("jobs_deferred", 0) > 0:
        suggestions.append("Check orchestrator events for deferred job reasons")

    if not suggestions:
        suggestions.append("System healthy — no action needed")

    return suggestions


def _render_markdown(report: dict[str, Any], config: dict[str, Any]) -> str:
    """Render report as Markdown."""
    lines: list[str] = []
    lines.append("# Orchestrator Report")
    lines.append("")
    lines.append(f"**Generated:** {report['generated_at']}")
    lines.append(f"**Profile:** {report['profile']}")
    lines.append("")

    # System health
    sys_info = report.get("system", {})
    lines.append("## System Health")
    lines.append("")
    lines.append(f"- Disk free: {sys_info.get('disk_free_gb', '?'):.1f} GB")
    lines.append(f"- Memory available: {sys_info.get('mem_available_mb', '?'):.0f} MB")
    lines.append("")

    # Cooldowns
    cd_info = report.get("cooldowns", {})
    lines.append("## Cooldowns")
    lines.append("")
    lines.append(f"- Active cooldowns: {cd_info.get('active_count', 0)}")
    if cd_info.get("next_wakeup_seconds", 0) > 0:
        next_min = cd_info["next_wakeup_seconds"] // 60
        next_sec = cd_info["next_wakeup_seconds"] % 60
        lines.append(f"- Next wakeup: ~{next_min}m {next_sec}s")
    lines.append("")

    if cd_info.get("blocked_providers"):
        lines.append("### Blocked Providers")
        for p in cd_info["blocked_providers"]:
            lines.append(f"- `{p}`")
        lines.append("")

    if cd_info.get("blocked_channels"):
        lines.append("### Blocked Channels")
        for ch in cd_info["blocked_channels"]:
            lines.append(f"- `{ch['channel_id']}`: {ch['reason']} (until {ch['until']})")
        lines.append("")

    if cd_info.get("details"):
        lines.append("### All Active Cooldowns")
        lines.append("")
        lines.append("| Scope | Reason | Until | Severity |")
        lines.append("|-------|--------|-------|----------|")
        for cd_entry in cd_info["details"]:
            lines.append(
                f"| {cd_entry['scope']} | {cd_entry['reason']} "
                f"| {cd_entry['cooldown_until']} | {cd_entry['severity']} |"
            )
        lines.append("")

    # Active locks
    lock_info = report.get("locks", {})
    if lock_info.get("active_count", 0) > 0:
        lines.append("## Active Stage Locks")
        lines.append("")
        lines.append("| Lock Key | Owner | Expires At |")
        lines.append("|----------|-------|------------|")
        for lock in lock_info.get("details", []):
            lines.append(
                f"| {lock['lock_key']} | {lock['owner']} | {lock['expires_at']} |"
            )
        lines.append("")

    active_jobs_info = report.get("inventory", {}).get("active_jobs", {})
    if active_jobs_info.get("active_count", 0) > 0:
        lines.append("## Active Jobs")
        lines.append("")
        lines.append("| Job ID | Stage | Scope | PID | Slot | Status |")
        lines.append("|--------|-------|-------|-----|------|--------|")
        for job in active_jobs_info.get("details", [])[:20]:
            lines.append(
                f"| {job.get('job_id', '?')} | {job.get('stage', '?')} | {job.get('scope', '')} "
                f"| {job.get('pid', '?')} | {job.get('slot_index', '?')} | {job.get('status', '?')} |"
            )
        lines.append("")

    # Pending work
    counts = report.get("pending_work", {})
    lines.append("## Pending Work")
    lines.append("")
    lines.append("| Category | Count |")
    lines.append("|----------|-------|")
    for key, val in sorted(counts.items()):
        lines.append(f"| {key} | {val} |")
    lines.append("")

    # Inventory
    inv = report.get("inventory", {})
    if inv:
        lines.append("## Work Inventory")
        lines.append("")
        lines.append(f"- Mode: `{inv.get('mode', '?')}`")
        lines.append(f"- Blocked: {inv.get('blocked', {})}")
        sys_info = inv.get("system", {})
        lines.append(f"- Disk free: {sys_info.get('disk_free_gb', 0):.1f} GB")
        lines.append(f"- Memory available: {sys_info.get('mem_available_mb', 0):.0f} MB")
        lines.append(f"- Active cooldowns: {inv.get('cooldowns', {}).get('active_count', 0)}")
        lines.append(f"- Active locks: {inv.get('locks', {}).get('active_count', 0)}")
        if inv.get("retry_queue"):
            rq = inv["retry_queue"]
            lines.append(
                "- Retry queue: "
                f"pending={rq.get('pending', 0)}, running={rq.get('running', 0)}, "
                f"completed={rq.get('completed', 0)}, failed={rq.get('failed', 0)}"
            )
        if inv.get("pauses"):
            lines.append("- Pauses:")
            for pause in inv["pauses"]:
                lines.append(f"  - `{pause.get('pause_key', pause.get('key', ''))}`: {pause.get('value', '')}")
        lines.append("")
        lines.append("### Remaining Work")
        for key, val in sorted(inv.get("work_remaining", {}).items()):
            lines.append(f"- {key}: {val}")
        lines.append("")
        if inv.get("defer_reasons"):
            lines.append("### Deferred Reasons")
            for code, val in sorted(inv["defer_reasons"].items(), key=lambda kv: (-kv[1], kv[0])):
                lines.append(f"- {code}: {val}")
            lines.append("")

    # Stage status
    lines.append("## Stage Status")
    lines.append("")
    stages = {
        "Discovery": ("discovery", "youtube"),
        "Transcript": ("transcript", "youtube"),
        "Audio Download": ("audio_download", "youtube"),
        "Resume": ("resume", "provider"),
        "Format": ("format", "provider"),
        "ASR": ("asr", "system"),
    }
    blocked_providers = cd_info.get("blocked_providers", [])
    youtube_blocked = any(cd_entry["scope"] == "youtube" for cd_entry in cd_info.get("details", []))
    pause_keys = {str(p.get("pause_key", "")).strip() for p in inv.get("pauses", [])}
    sys_info = inv.get("system", {})
    mem_mb = float(sys_info.get("mem_available_mb", 0) or 0)
    disk_gb = float(sys_info.get("disk_free_gb", 0) or 0)
    for stage, (stage_key, dep) in stages.items():
        enabled = config.get(stage_key, {}).get("enabled", True)
        stage_paused = (
            f"stage:{stage_key}" in pause_keys
            or "scope:all" in pause_keys
            or (dep == "youtube" and "scope:youtube" in pause_keys)
            or (dep == "provider" and "scope:provider" in pause_keys)
        )
        if not enabled:
            lines.append(f"- **{stage}**: disabled")
        elif stage_paused:
            lines.append(f"- **{stage}**: paused")
        elif disk_gb < float(config.get("system", {}).get("min_free_disk_gb", 5) or 5):
            lines.append(f"- **{stage}**: blocked (disk low)")
        elif stage_key in ("resume", "format") and mem_mb < float(config.get("system", {}).get(f"min_memory_mb_{stage_key}", 1200) or 1200):
            lines.append(f"- **{stage}**: limited (memory low)")
        elif stage_key == "asr" and mem_mb < float(config.get("system", {}).get("min_memory_mb_asr", 2500) or 2500):
            lines.append(f"- **{stage}**: limited (memory low)")
        elif dep == "youtube" and youtube_blocked:
            lines.append(f"- **{stage}**: blocked (YouTube cooldown)")
        elif dep == "provider" and blocked_providers:
            lines.append(f"- **{stage}**: limited (provider cooldown)")
        else:
            lines.append(f"- **{stage}**: ready")
    lines.append("")

    # Suggestions
    suggestions = report.get("suggestions", [])
    if suggestions:
        lines.append("## Suggestions")
        lines.append("")
        for s in suggestions:
            lines.append(f"- {s}")
        lines.append("")

    # Recent events
    events = report.get("recent_events", [])
    if events:
        lines.append("## Recent Events")
        lines.append("")
        lines.append("| Time | Type | Severity | Message |")
        lines.append("|------|------|----------|---------|")
        for ev in events[:10]:
            lines.append(
                f"| {ev.get('created_at', '?')} | {ev.get('event_type', '?')} "
                f"| {ev.get('severity', '?')} | {ev.get('message', '')[:80]} |"
            )
        lines.append("")

    # Cycle result
    cycle = report.get("cycle_result")
    if cycle:
        lines.append("## Last Cycle")
        lines.append("")
        lines.append(f"- Jobs planned: {cycle.get('jobs_planned', 0)}")
        lines.append(f"- Jobs dispatched: {cycle.get('jobs_dispatched', 0)}")
        lines.append(f"- Jobs succeeded: {cycle.get('jobs_succeeded', 0)}")
        lines.append(f"- Jobs failed: {cycle.get('jobs_failed', 0)}")
        lines.append(f"- Jobs deferred: {cycle.get('jobs_deferred', 0)}")
        lines.append(f"- Duration: {cycle.get('duration_seconds', 0)}s")
        if cycle.get("dry_run"):
            lines.append("- **⚠️ Dry-run mode** — no jobs actually ran")
        lines.append("")

    return "\n".join(lines)


def get_latest_report() -> dict[str, Any] | None:
    """Read the latest JSON report."""
    json_path = REPORTS_DIR / "latest.json"
    if json_path.exists():
        try:
            return json.loads(json_path.read_text())
        except (json.JSONDecodeError, Exception):
            return None
    return None
