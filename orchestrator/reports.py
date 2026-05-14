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


PROJECT_ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR = PROJECT_ROOT / "runs" / "orchestrator" / "reports"


def _ensure_reports_dir() -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    return REPORTS_DIR


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
    counts = planner.get_summary_counts(state)
    active_cooldowns = state.list_active_cooldowns()
    recent_events = state.get_recent_events(limit=20)
    blocked_providers = cd.get_blocked_providers(state)
    blocked_channels = cd.get_blocked_channels(state)
    next_wakeup = cd.get_next_wakeup(state)
    active_locks = state.list_active_locks()

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
        "pending_work": counts,
        "recent_events": recent_events[:10],
        "cycle_result": cycle_result,
        "suggestions": suggestions,
    }

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

    # Pending work
    counts = report.get("pending_work", {})
    lines.append("## Pending Work")
    lines.append("")
    lines.append("| Category | Count |")
    lines.append("|----------|-------|")
    for key, val in sorted(counts.items()):
        lines.append(f"| {key} | {val} |")
    lines.append("")

    # Stage status
    lines.append("## Stage Status")
    lines.append("")
    stages = {
        "Discovery": "youtube",
        "Transcript": "youtube",
        "Resume": "provider",
        "Format": "provider",
        "ASR": "system",
    }
    blocked_providers = cd_info.get("blocked_providers", [])
    youtube_blocked = any(cd_entry["scope"] == "youtube" for cd_entry in cd_info.get("details", []))
    for stage, dep in stages.items():
        stage_key = stage.lower()
        enabled = config.get(stage_key, {}).get("enabled", True)
        if not enabled:
            lines.append(f"- **{stage}**: disabled")
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
