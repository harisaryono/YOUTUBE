"""
Error Analyzer — Classify errors from logs, reports, and exit codes.
"""

from __future__ import annotations

import re
from typing import Any

from .state import OrchestratorState


# Error classification patterns
ERROR_PATTERNS: list[tuple[str, str, str, int]] = [
    # YouTube errors
    (r"(?i)(429|too\s*many\s*requests|rate\s*limit)", "youtube_429", "YouTube rate limited", 7200),
    (r"(?i)(403|forbidden|access\s*denied)", "youtube_403", "YouTube access denied", 3600),
    (r"(?i)(bot\s*detect|unusual\s*traffic|captcha)", "youtube_bot_detection", "YouTube bot detection", 86400),
    (r"(?i)(sign\s*in|login\s*required|cookie)", "youtube_signin_required", "YouTube sign-in required", 21600),
    (r"(?i)(no\s*subtitle|subtitles\s*disabled)", "no_subtitle", "Video has no subtitles", 0),
    (r"(?i)(member\s*only|membership)", "member_only", "Member-only video", 0),
    (r"(?i)(video\s*unavailable|not\s*found|404)", "video_unavailable", "Video unavailable", 0),
    (r"(?i)(channel\s*unavailable|channel\s*not\s*found)", "channel_unavailable", "Channel unavailable", 86400),
    (r"(?i)(ip\s*blocked|blocked\s*ip)", "youtube_ip_blocked", "YouTube IP blocked", 43200),

    # Provider errors
    (r"(?i)(429|rate\s*limit|too\s*many\s*requests).*provider", "provider_429", "Provider rate limited", 3600),
    (r"(?i)(quota|tokens\s*per\s*day|tpd)", "provider_quota_exceeded", "Provider quota exceeded", 86400),
    (r"(?i)(context\s*length|context\s*too\s*large|max\s*context)", "provider_context_too_large", "Context too large", 0),
    (r"(?i)(auth|api\s*key|unauthorized|401)", "provider_auth_error", "Provider auth error", 86400),
    (r"(?i)(timeout|timed\s*out)", "provider_timeout", "Provider timeout", 300),

    # System errors
    (r"(?i)(memory|oom|out\s*of\s*memory)", "memory_low", "Out of memory", 900),
    (r"(?i)(disk|space|no\s*space)", "disk_low", "Disk full", 1800),
    (r"(?i)(coordinator|connection\s*refused).*8788", "coordinator_unavailable", "Coordinator unavailable", 300),
]


class ErrorClassification:
    """Result of error classification."""

    def __init__(
        self,
        error_type: str = "unknown_error",
        description: str = "Unknown error",
        cooldown_seconds: int = 300,
        severity: str = "warning",
        recommendation: str = "",
    ):
        self.error_type = error_type
        self.description = description
        self.cooldown_seconds = cooldown_seconds
        self.severity = severity
        self.recommendation = recommendation


# Recommendation map
RECOMMENDATIONS: dict[str, str] = {
    "youtube_429": "Reduce workers, enable --rate-limit-safe, increase inter-video delay",
    "youtube_403": "Check cookies/proxy/IP, wait before retry",
    "youtube_bot_detection": "Stop all YouTube activity for 12-24h, use different IP/proxy",
    "youtube_signin_required": "Refresh cookies, check cookie file, use different proxy",
    "no_subtitle": "Mark as no_subtitle in DB, skip permanently",
    "member_only": "Mark as member_only in DB, skip permanently",
    "video_unavailable": "Mark as unavailable in DB, skip permanently",
    "channel_unavailable": "Disable channel scan, skip permanently",
    "youtube_ip_blocked": "Change IP/proxy, wait 12h before retry",
    "provider_429": "Reduce workers, switch to different provider",
    "provider_quota_exceeded": "Wait for quota reset or use different provider",
    "provider_context_too_large": "Reduce chunk size, split transcript",
    "provider_auth_error": "Check API key, rotate if needed",
    "provider_timeout": "Retry with longer timeout, reduce batch size",
    "memory_low": "Reduce workers, wait for other jobs to finish",
    "disk_low": "Clean up runs/uploads/logs/cache",
    "coordinator_unavailable": "Check coordinator service, restart if needed",
}


def classify_error(
    log_line: str,
    exit_code: int = 0,
) -> ErrorClassification:
    """
    Classify a single error from a log line or exit code.
    Returns an ErrorClassification with type, severity, and recommended cooldown.
    """
    # Check exit code first
    if exit_code != 0:
        if exit_code == 1:
            return ErrorClassification(
                "general_error", f"Exit code {exit_code}", 60, "warning",
                "Check logs for details"
            )
        if exit_code == 137:
            return ErrorClassification(
                "memory_low", "Process killed (OOM)", 900, "blocking",
                RECOMMENDATIONS["memory_low"]
            )
        if exit_code in (139, 134, 6):
            return ErrorClassification(
                "process_crash", f"Process crashed (signal {exit_code - 128})", 300, "warning",
                "Check for bugs or memory issues"
            )

    # Check patterns
    for pattern, error_type, description, cooldown in ERROR_PATTERNS:
        if re.search(pattern, log_line):
            severity = "blocking" if cooldown >= 3600 else "warning"
            if cooldown == 0:
                severity = "info"
            return ErrorClassification(
                error_type, description, cooldown, severity,
                RECOMMENDATIONS.get(error_type, "")
            )

    return ErrorClassification(
        "unknown_error", f"Unclassified: {log_line[:100]}", 300, "warning",
        "Check logs manually"
    )


def analyze_report_csv(
    report_path: str,
    state: OrchestratorState,
) -> list[dict[str, Any]]:
    """
    Analyze a report CSV from a run and set cooldowns accordingly.
    Returns list of events created.
    """
    import csv
    events: list[dict[str, Any]] = []

    try:
        with open(report_path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                status = (row.get("status") or "").strip().lower()
                error_msg = (row.get("error") or row.get("message") or "").strip()
                video_id = (row.get("video_id") or row.get("id") or "").strip()
                channel_id = (row.get("channel_id") or "").strip()

                if status in ("ok", "success", "done", "skipped"):
                    continue

                classification = classify_error(error_msg)

                # Determine scope
                scope = "global"
                if classification.error_type in ("no_subtitle", "member_only", "video_unavailable"):
                    scope = f"video:{video_id}" if video_id else "global"
                elif classification.error_type in ("channel_unavailable",):
                    scope = f"channel:{channel_id}" if channel_id else "global"
                elif classification.error_type.startswith("youtube_"):
                    scope = "youtube"
                elif classification.error_type.startswith("provider_"):
                    # Try to extract provider name
                    provider_match = re.search(r"(?i)(nvidia|groq|cerebras|openrouter|z\.ai)", error_msg)
                    provider = provider_match.group(1).lower() if provider_match else "unknown"
                    scope = f"provider:{provider}"

                # Set cooldown if needed
                if classification.cooldown_seconds > 0:
                    state.set_cooldown(
                        scope=scope,
                        reason=classification.description,
                        duration_seconds=classification.cooldown_seconds,
                        severity=classification.severity,
                        recommendation=classification.recommendation,
                    )

                # Record event
                event_id = state.add_event(
                    event_type="error",
                    message=f"[{classification.error_type}] {classification.description}: {error_msg[:200]}",
                    stage=row.get("stage", ""),
                    scope=scope,
                    severity=classification.severity,
                    recommendation=classification.recommendation,
                    payload={
                        "video_id": video_id,
                        "channel_id": channel_id,
                        "error_type": classification.error_type,
                        "cooldown_seconds": classification.cooldown_seconds,
                    },
                )
                events.append({
                    "event_id": event_id,
                    "scope": scope,
                    "error_type": classification.error_type,
                    "cooldown_seconds": classification.cooldown_seconds,
                })

    except FileNotFoundError:
        pass
    except Exception as e:
        state.add_event(
            event_type="error",
            message=f"Failed to analyze report CSV: {e}",
            severity="warning",
        )

    return events
