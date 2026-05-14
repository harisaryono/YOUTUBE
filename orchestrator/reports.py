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
        "pending_work": counts,
        "recent_events": recent_events[:10],
        "cycle_result": cycle_result,
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
        lines.append(f"- Next wakeup: ~{next_min} minutes")
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
        lines.append(f"- Duration: {cycle.get('duration_seconds', 0)}s")
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
