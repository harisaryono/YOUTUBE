"""
Orchestrator doctor CLI.

This command reports daemon health, slot usage, backlog, cooldowns, and recent
failures in a compact operational view.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import DEFAULT_CONFIG_PATH, load_config
from .reports import build_inventory_snapshot
from .state import OrchestratorState


PROJECT_ROOT = Path(__file__).resolve().parent.parent
PID_FILE = Path("/tmp/orchestrator_daemon.pid")


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


def _cooldown_remaining_seconds(item: dict[str, Any]) -> int:
    raw = str(item.get("cooldown_until") or "").strip()
    if not raw:
        return 0
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return 0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    delta = parsed.astimezone(timezone.utc) - datetime.now(timezone.utc)
    return max(0, int(delta.total_seconds()))


def _daemon_status() -> dict[str, Any]:
    pid = 0
    running = False
    if PID_FILE.exists():
        try:
            pid = int(PID_FILE.read_text().strip())
        except Exception:
            pid = 0
    if pid > 0:
        try:
            os.kill(pid, 0)
            running = True
        except Exception:
            running = False
    return {
        "running": running,
        "pid": pid if pid > 0 else None,
        "pid_file": str(PID_FILE),
    }


def _recent_failure_summary(state: OrchestratorState, limit: int = 50) -> list[dict[str, Any]]:
    rows = state.get_recent_events(limit=limit)
    failures: list[dict[str, Any]] = []
    for row in rows:
        event_type = str(row.get("event_type") or "")
        if event_type not in {"dispatch_failure", "timeout", "error"}:
            continue
        payload = {}
        try:
            payload = json.loads(str(row.get("payload_json") or "{}"))
        except Exception:
            payload = {}
        reason_code = str(payload.get("reason_code") or row.get("reason_code") or "").strip()
        if not reason_code:
            reason_code = str(row.get("message") or "").split(":", 1)[0].strip() or event_type
        failures.append(
            {
                "event_type": event_type,
                "stage": str(row.get("stage") or "daemon"),
                "scope": str(row.get("scope") or ""),
                "reason_code": reason_code,
                "severity": str(row.get("severity") or ""),
                "message": str(row.get("message") or ""),
                "created_at": str(row.get("created_at") or ""),
            }
        )
    return failures


def _recent_control_actions(state: OrchestratorState, limit: int = 20) -> list[dict[str, Any]]:
    rows = state.get_recent_events(limit=max(limit, 50))
    actions: list[dict[str, Any]] = []
    for row in rows:
        event_type = str(row.get("event_type") or "")
        if not event_type.startswith("control"):
            continue
        actions.append(dict(row))
    return actions[:limit]


def _cycle_failure_summary(recent_events: list[dict[str, Any]]) -> dict[str, Any]:
    cycle_events = []
    for row in recent_events:
        event_type = str(row.get("event_type") or "")
        message = str(row.get("message") or "")
        if event_type == "error" and "cycle failed" in message.lower():
            cycle_events.append(dict(row))
    if not cycle_events:
        return {"count": 0, "latest": None, "latest_message": "", "repeated": False, "events": []}
    latest = cycle_events[0]
    latest_created = str(latest.get("created_at") or "")
    latest_message = str(latest.get("message") or "")
    return {
        "count": len(cycle_events),
        "latest": latest_created,
        "latest_message": latest_message,
        "repeated": len(cycle_events) > 1,
        "events": cycle_events,
    }


def _count_by_key(items: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        value = str(item.get(key) or "").strip()
        if not value:
            continue
        counts[value] = counts.get(value, 0) + 1
    return counts


def _build_recommendations(report: dict[str, Any]) -> list[str]:
    recs: list[str] = []
    cooldowns = report.get("cooldowns", {}).get("details", [])
    cooldown_scopes = {str(item.get("scope") or "") for item in cooldowns if item}
    failures = report.get("recent_failures", [])
    failure_codes = [str(item.get("reason_code") or "").lower() for item in failures]
    failure_stages = [str(item.get("stage") or "").lower() for item in failures]
    backlog = report.get("backlog", {})
    policy_blockers = report.get("policy_blockers", [])
    retry_queue = report.get("retry_queue", {})

    if "youtube" in cooldown_scopes:
        recs.append("YouTube global cooldown aktif; tahan transcript/audio_download sampai cooldown selesai.")
    elif "youtube:content" in cooldown_scopes and "youtube:discovery" not in cooldown_scopes:
        recs.append("Discovery masih aman; content cooldown hanya menahan transcript/audio_download.")

    if "provider" in cooldown_scopes or any(scope.startswith("provider:") for scope in cooldown_scopes):
        recs.append("Provider sedang cooldown; prioritaskan discovery dan local jobs.")

    if backlog.get("transcript", 0) > backlog.get("resume", 0):
        recs.append("Transcript backlog dominan; jaga transcript workers tetap rendah bila YouTube mulai error.")

    if any("bot" in code or "captcha" in code for code in failure_codes):
        recs.append("Ada indikasi bot/captcha; pause YouTube content sementara.")
    elif any("429" in code or "rate" in code for code in failure_codes):
        recs.append("Ada rate limit; keep discovery running dan kecilkan worker transcript/audio_download.")
    elif any("timeout" in code for code in failure_codes):
        recs.append("Ada timeout; cek job aktif dan pertimbangkan cancel/reconcile pada job paling tua.")

    if any(stage == "daemon" for stage in failure_stages):
        recs.append("Ada error daemon terakhir; cek logs orchestrator dan jalankan explain/reconcile.")

    if policy_blockers:
        recs.append(f"Ada {len(policy_blockers)} policy blocker aktif; cek pause/quarantine sebelum retry.")

    if int(retry_queue.get("pending", 0) or 0) > 0:
        recs.append(f"Ada {int(retry_queue.get('pending', 0) or 0)} retry queue pending; daemon akan memprosesnya di cycle berikutnya.")

    if not recs:
        recs.append("Tidak ada anomali besar yang terdeteksi.")

    return recs


def build_doctor_report(
    config: dict[str, Any],
    *,
    recent_events_limit: int = 20,
    show_errors: bool = False,
) -> dict[str, Any]:
    state = OrchestratorState()
    try:
        inventory = build_inventory_snapshot(config, state, None)
        active_jobs = inventory.get("active_jobs", {}).get("details", [])
        cooldowns = inventory.get("cooldowns", {}).get("details", [])
        pauses = inventory.get("pauses", [])
        quarantined_channels = inventory.get("quarantined_channels", [])
        retry_queue = inventory.get("retry_queue", {})
        recent_events = state.get_recent_events(limit=max(50, recent_events_limit))
        failures = _recent_failure_summary(state, limit=50)
        control_actions = _recent_control_actions(state, limit=recent_events_limit)
        cycle_failures = _cycle_failure_summary(recent_events)

        parallel = config.get("parallel", {}) or {}
        groups = parallel.get("groups", {}) or {}
        stage_defs = parallel.get("stages", {}) or {}

        group_usage: list[dict[str, Any]] = []
        for group_name, group_cfg in groups.items():
            stages = [str(stage) for stage in (group_cfg.get("stages", []) or [])]
            group_usage.append(
                {
                    "group": group_name,
                    "running": state.count_running_by_group(group_name),
                    "max_running": int(group_cfg.get("max_running", 0) or 0),
                    "stages": stages,
                }
            )

        stage_usage: list[dict[str, Any]] = []
        work_remaining = inventory.get("work_remaining", {}) or {}
        for stage_name, stage_cfg in stage_defs.items():
            stage_usage.append(
                {
                    "stage": stage_name,
                    "running": state.count_running_by_stage(stage_name),
                    "slots": int(stage_cfg.get("slots", 0) or 0),
                    "work_remaining": int(work_remaining.get(stage_name, 0) or 0),
                }
            )

        stage_usage.sort(key=lambda item: (item["stage"] != "discovery", item["stage"]))
        group_usage.sort(key=lambda item: item["group"])

        return {
            "ok": True,
            "daemon": _daemon_status(),
            "mode": inventory.get("mode", config.get("orchestrator", {}).get("mode", "work_conserving")),
            "system": inventory.get("system", {}),
            "backlog": work_remaining,
            "blocked": inventory.get("blocked", {}),
            "pauses": pauses,
            "quarantined_channels": quarantined_channels,
            "retry_queue": retry_queue,
            "policy_blockers": [
                f"{str(item.get('pause_key') or item.get('scope') or '')} paused: {str(item.get('reason') or '').strip()}"
                for item in pauses
            ]
            + [
                f"channel {str(item.get('channel_id') or '')} quarantined: {str(item.get('reason') or '').strip()}"
                for item in quarantined_channels
            ],
            "cooldowns": {
                "active_count": len(cooldowns),
                "details": [
                    item | {"remaining_seconds": _cooldown_remaining_seconds(item)}
                    for item in cooldowns
                ],
            },
            "active_jobs": {
                "active_count": len(active_jobs),
                "details": active_jobs,
            },
            "group_usage": group_usage,
            "stage_usage": stage_usage,
            "recent_failures": failures[:10],
            "recent_failure_counts": _count_by_key(failures, "reason_code"),
            "recent_events": recent_events[:recent_events_limit],
            "recent_control_actions": control_actions,
            "cycle_failures": cycle_failures,
            "show_errors": bool(show_errors),
            "recommendations": _build_recommendations(
                {
                    "cooldowns": {"details": cooldowns},
                    "recent_failures": failures,
                    "backlog": work_remaining,
                    "policy_blockers": [
                        f"{str(item.get('pause_key') or item.get('scope') or '')} paused: {str(item.get('reason') or '').strip()}"
                        for item in pauses
                    ]
                    + [
                        f"channel {str(item.get('channel_id') or '')} quarantined: {str(item.get('reason') or '').strip()}"
                        for item in quarantined_channels
                    ],
                    "retry_queue": retry_queue,
                }
            ),
        }
    finally:
        state.close()


def render_doctor_text(report: dict[str, Any]) -> str:
    lines: list[str] = []
    daemon = report.get("daemon", {})
    system = report.get("system", {})
    cooldowns = report.get("cooldowns", {}).get("details", [])
    backlog = report.get("backlog", {})
    group_usage = report.get("group_usage", [])
    stage_usage = report.get("stage_usage", [])
    failures = report.get("recent_failures", [])
    pauses = report.get("pauses", [])
    quarantined_channels = report.get("quarantined_channels", [])
    retry_queue = report.get("retry_queue", {})
    control_actions = report.get("recent_control_actions", [])
    cycle_failures = report.get("cycle_failures", {})
    policy_blockers = report.get("policy_blockers", [])

    lines.append("ORCHESTRATOR DOCTOR")
    lines.append("")
    lines.append(
        f"Daemon: {'running' if daemon.get('running') else 'stopped'}"
        + (f" (PID {daemon.get('pid')})" if daemon.get("pid") else "")
    )
    lines.append(f"Active jobs: {report.get('active_jobs', {}).get('active_count', 0)}")
    lines.append(
        f"System: disk {system.get('disk_free_gb', 0):.1f} GB, "
        f"memory {system.get('mem_available_mb', 0):.0f} MB"
    )
    lines.append("")
    lines.append("Group usage:")
    for item in group_usage:
        lines.append(
            f"  - {item['group']}: {item['running']}/{item['max_running']} "
            f"running ({', '.join(item['stages'])})"
        )
    lines.append("")
    lines.append("Stage usage:")
    for item in stage_usage:
        lines.append(
            f"  - {item['stage']}: running {item['running']}, "
            f"slots {item['slots']}, backlog {item['work_remaining']}"
        )
    lines.append("")
    lines.append("Cooldowns:")
    if cooldowns:
        for item in cooldowns:
            remaining = _format_duration(int(item.get("remaining_seconds", 0) or 0))
            lines.append(f"  - {item.get('scope', '')}: {remaining} remaining - {item.get('reason', '')}")
    else:
        lines.append("  - none")
    lines.append("")
    lines.append("Pauses:")
    if pauses:
        for item in pauses:
            key = str(item.get("pause_key") or item.get("scope") or "").strip()
            reason = str(item.get("reason") or "").strip()
            until = str(item.get("until") or "").strip()
            extra = f" until {until}" if until else ""
            lines.append(f"  - {key}{extra}: {reason}")
    else:
        lines.append("  - none")
    lines.append("")
    lines.append("Quarantined channels:")
    if quarantined_channels:
        for item in quarantined_channels:
            lines.append(
                f"  - {item.get('channel_id', '')}: {item.get('reason', '')}"
            )
    else:
        lines.append("  - none")
    lines.append("")
    lines.append("Backlog:")
    for key in sorted(backlog):
        lines.append(f"  - {key}: {backlog[key]}")
    lines.append("")
    lines.append("Recent failures:")
    if failures:
        for item in failures[:5]:
            lines.append(f"  - {item['stage']}: {item['reason_code']} ({item['severity']})")
    else:
        lines.append("  - none")
    lines.append("")
    lines.append("Policy blockers:")
    if policy_blockers:
        for item in policy_blockers:
            lines.append(f"  - {item}")
    else:
        lines.append("  - none")
    lines.append("")
    lines.append("Retry queue:")
    if retry_queue:
        lines.append(
            f"  - pending: {retry_queue.get('pending', 0)}, running: {retry_queue.get('running', 0)}, "
            f"completed: {retry_queue.get('completed', 0)}, failed: {retry_queue.get('failed', 0)}"
        )
    else:
        lines.append("  - none")
    lines.append("")
    lines.append("Recent control actions:")
    if control_actions:
        for item in control_actions[:5]:
            lines.append(
                f"  - {item.get('event_type', '')}: {item.get('message', '')}"
            )
    else:
        lines.append("  - none")
    lines.append("")
    lines.append("Cycle failures:")
    if cycle_failures.get("count", 0):
        lines.append(
            f"  - count: {cycle_failures.get('count', 0)}"
            + (f", latest: {cycle_failures.get('latest', '')}" if cycle_failures.get("latest") else "")
        )
        if cycle_failures.get("latest_message"):
            lines.append(f"  - latest_message: {cycle_failures.get('latest_message', '')}")
    else:
        lines.append("  - none")
    if report.get("show_errors"):
        lines.append("")
        lines.append("Recent events:")
        for item in report.get("recent_events", [])[:10]:
            lines.append(
                f"  - {item.get('created_at', '')} {item.get('event_type', '')}: {item.get('message', '')}"
            )
    lines.append("")
    lines.append("Recommendations:")
    for rec in report.get("recommendations", []):
        lines.append(f"  - {rec}")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Orchestrator doctor report")
    parser.add_argument(
        "--config",
        default=None,
        help="Path to orchestrator.yaml (default: orchestrator.yaml in project root)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON output",
    )
    parser.add_argument(
        "--recent-events",
        type=int,
        default=20,
        help="Number of recent events to include in the report",
    )
    parser.add_argument(
        "--show-errors",
        action="store_true",
        help="Include recent error events in the text output",
    )
    args = parser.parse_args()

    config = load_config(Path(args.config) if args.config else DEFAULT_CONFIG_PATH)
    report = build_doctor_report(
        config,
        recent_events_limit=max(1, int(args.recent_events or 20)),
        show_errors=bool(args.show_errors),
    )
    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print(render_doctor_text(report))


if __name__ == "__main__":
    main()
