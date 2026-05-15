"""
Error Analyzer — Classify errors from logs, reports, and exit codes.

Stage 17:
- Terminal/video-scoped YouTube failures are governed by terminal_failures.py.
- Terminal failures are recorded as events but do not create global/channel cooldowns.
- Report payloads include terminal failure policy metadata for planner/dashboard use.
"""

from __future__ import annotations

import re
from typing import Any

from .state import OrchestratorState
from .terminal_failures import is_terminal_failure, terminal_failure_policy


# Error classification patterns.
# Order matters: precise terminal/video-scoped failures must stay above broad
# 403/access-denied/video-unavailable patterns.
ERROR_PATTERNS: list[tuple[str, str, str, int]] = [
    # YouTube terminal / video-scoped errors that should not create global cooldowns.
    (
        r"(?i)(not\s*made\s*this\s*video\s*available\s*in\s*your\s*country|"
        r"this\s*video\s*is\s*not\s*available\s*in\s*your\s*country|"
        r"video\s*is\s*not\s*available\s*in\s*your\s*country|"
        r"not\s*available\s*in\s*your\s*country|"
        r"not\s*available\s*in\s*your\s*region|"
        r"available\s*only\s*in|"
        r"only\s*available\s*in|"
        r"country\s*restricted|"
        r"region\s*restricted|"
        r"region\s*locked|"
        r"geo\s*blocked|"
        r"geo-blocked)",
        "youtube_geo_blocked",
        "YouTube geo/region restricted",
        0,
    ),
    (
        r"(?i)(members[-\s]?only|member\s+only|membership|"
        r"available\s+to\s+(this\s+channel'?s\s+)?members|"
        r"join\s+this\s+channel\s+to\s+get\s+access)",
        "member_only",
        "Member-only video",
        0,
    ),
    (
        r"(?i)(private\s+video|video\s+is\s+private|"
        r"sign\s+in\s+if\s+you'?ve\s+been\s+granted\s+access)",
        "private_video",
        "Private video",
        0,
    ),
    (
        r"(?i)(confirm\s+your\s+age|age[-\s]?restricted|age\s+restriction|"
        r"inappropriate\s+for\s+some\s+users)",
        "age_restricted",
        "Age-restricted video",
        0,
    ),
    (
        r"(?i)(copyright|blocked\s+on\s+copyright\s+grounds|contains\s+content\s+from|"
        r"has\s+been\s+blocked\s+by\s+.*copyright)",
        "copyright_blocked",
        "Video blocked by copyright/rights restriction",
        0,
    ),
    (
        r"(?i)(premiere\s+will\s+begin|premieres\s+in|"
        r"live\s+stream\s+recording\s+is\s+not\s+available|"
        r"this\s+live\s+event\s+will\s+begin|waiting\s+for\s+.*live|"
        r"not\s+yet\s+available)",
        "not_ready_yet",
        "Video/live/premiere not ready yet",
        0,
    ),
    (
        r"(?i)(requested\s+format\s+is\s+not\s+available|requested\s+format\s+is\s+unavailable|"
        r"this\s+video\s+format\s+is\s+unavailable|format\s+is\s+unavailable|"
        r"no\s+video\s+formats\s+found|no\s+formats\s+available|format\s+not\s+available|"
        r"format\s+unavailable)",
        "format_unavailable",
        "Requested YouTube format is unavailable",
        0,
    ),
    (
        r"(?i)(no\s*subtitle|subtitles\s*disabled|transcript\s+is\s+disabled|"
        r"no\s+transcripts\s+were\s+found)",
        "no_subtitle",
        "Video has no subtitles",
        0,
    ),
    (
        r"(?i)(video\s*unavailable|video\s+is\s+unavailable|not\s*found|404|"
        r"removed\s+by\s+the\s+uploader|has\s+been\s+removed)",
        "video_unavailable",
        "Video unavailable",
        0,
    ),
    (r"(?i)(channel\s*unavailable|channel\s*not\s*found)", "channel_unavailable", "Channel unavailable", 0),

    # YouTube errors that represent environment/IP/request pressure and may need cooldown.
    (
        r"(?i)(bot\s*detect|unusual\s*traffic|captcha|"
        r"sign\s+in\s+to\s+confirm\s+you.?re\s+not\s+a\s+bot)",
        "youtube_bot_detection",
        "YouTube bot detection",
        86400,
    ),
    (
        r"(?i)(ip\s*blocked|blocked\s*ip|blocked\s+requests\s+from\s+your\s+ip|"
        r"your\s+ip\s+has\s+been\s+blocked|ipblocked|requestblocked)",
        "youtube_ip_blocked",
        "YouTube IP blocked",
        43200,
    ),
    (r"(?i)(429|too\s*many\s*requests|rate\s*limit)", "youtube_429", "YouTube rate limited", 7200),
    (r"(?i)(403|forbidden|access\s*denied)", "youtube_403", "YouTube access denied", 3600),
    (r"(?i)(sign\s*in|login\s*required|cookie)", "youtube_signin_required", "YouTube sign-in required", 21600),

    # Provider errors
    (r"(?i)(429|rate\s*limit|too\s*many\s*requests).*provider", "provider_429", "Provider rate limited", 3600),
    (r"(?i)(quota|tokens\s*per\s*day|tpd)", "provider_quota_exceeded", "Provider quota exceeded", 86400),
    (r"(?i)(context\s*length|context\s*too\s*large|max\s*context)", "provider_context_too_large", "Context too large", 0),
    (r"(?i)(auth|api\s*key|unauthorized|401)", "provider_auth_error", "Provider auth error", 86400),
    (r"(?i)(timeout|timed\s*out)", "provider_timeout", "Provider timeout", 300),
    (r"(?i)(DEGRADED\s+function\s+cannot\s+be\s+invoked|StatusCode\.INVALID_ARGUMENT.*DEGRADED|nvidia.*riva.*degraded)", "nvidia_riva_degraded", "NVIDIA Riva service degraded", 1800),
    (r"(?i)(StatusCode\.\w+.*rpc\s*error|_MultiThreadedRendezvous.*StatusCode)", "nvidia_riva_rpc_error", "NVIDIA Riva RPC error", 600),
    (
        r"(?i)(no\s*active\s*asr\s*provider\s*capacity\s*available\s*from\s*coordinator|"
        r"no\s*asr\s*lease\s*available|"
        r"tidak\s*ada\s*lease\s*coordinator\s*yang\s*tersedia|"
        r"tidak\s*ada\s*lease\s*coordinator\s*untuk\s*provider\s*asr)",
        "asr_provider_unavailable",
        "ASR provider capacity unavailable",
        300,
    ),
    (
        r"(?i)(no\s*accounts\s*for\s+[a-z0-9_./-]+/[a-z0-9_./-]+|"
        r"no\s*lease\s*available\s*for\s+[a-z0-9_./-]+/[a-z0-9_./-]+|"
        r"lease\s*tidak\s*tersedia|"
        r"lease\s*belum\s*tersedia)",
        "lease_unavailable",
        "Requested lease unavailable",
        0,
    ),

    # System errors
    (r"(?i)(memory|oom|out\s*of\s*memory)", "memory_low", "Out of memory", 900),
    (r"(?i)(disk|space|no\s*space)", "disk_low", "Disk full", 1800),
    (r"(?i)(coordinator\s+tidak\s+bisa\s*dihubungi|coordinator|connection\s*refused).*8788", "coordinator_unavailable", "Coordinator unavailable", 300),
    (r"(?i)(coordinator\s+tidak\s+bisa\s*dihubungi|coordinator\s*unreachable|unable\s*to\s*connect\s*to\s*coordinator)", "coordinator_unavailable", "Coordinator unavailable", 300),
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
        suggested_scope: str = "",
    ):
        self.error_type = error_type
        self.description = description
        self.cooldown_seconds = cooldown_seconds
        self.severity = severity
        self.recommendation = recommendation
        self.suggested_scope = suggested_scope


# Recommendation map
RECOMMENDATIONS: dict[str, str] = {
    "youtube_geo_blocked": "Do not cooldown globally; retry only with a proxy/IP from an allowed region",
    "member_only": "Mark as member_only in DB, skip permanently unless authorized access is intended",
    "private_video": "Mark as private/unavailable; do not global cooldown",
    "age_restricted": "Use valid cookies/account if permitted; do not global cooldown",
    "copyright_blocked": "Mark copyright_blocked; do not global cooldown",
    "not_ready_yet": "Retry this video later; do not global cooldown",
    "format_unavailable": "Change yt-dlp format selector; do not global cooldown",
    "no_subtitle": "Mark as no_subtitle in DB, skip subtitle retry and route to ASR if needed",
    "video_unavailable": "Mark as unavailable in DB, skip permanently",
    "channel_unavailable": "Disable channel scan, skip permanently",
    "youtube_429": "Reduce workers, enable --rate-limit-safe, increase inter-video delay",
    "youtube_403": "Check cookies/proxy/IP, wait before retry",
    "youtube_bot_detection": "Stop all YouTube activity for 12-24h, use different IP/proxy",
    "youtube_signin_required": "Refresh cookies, check cookie file, use different proxy",
    "youtube_ip_blocked": "Change IP/proxy, wait 12h before retry",
    "provider_429": "Reduce workers, switch to different provider",
    "provider_quota_exceeded": "Wait for quota reset or use different provider",
    "provider_context_too_large": "Reduce chunk size, split transcript",
    "provider_auth_error": "Check API key, rotate if needed",
    "provider_timeout": "Retry with longer timeout, reduce batch size",
    "nvidia_riva_degraded": "NVIDIA Riva service is degraded; wait for recovery or switch provider",
    "nvidia_riva_rpc_error": "NVIDIA Riva RPC error; check Riva endpoint or retry later",
    "asr_provider_unavailable": "Wait for ASR provider lease to become available",
    "lease_unavailable": "Wait for lease availability or check coordinator account pool",
    "memory_low": "Reduce workers, wait for other jobs to finish",
    "disk_low": "Clean up runs/uploads/logs/cache",
    "coordinator_unavailable": "Check coordinator service, restart if needed",
}


SUCCESS_STATUSES = {
    "ok",
    "success",
    "done",
    "skipped",
    "downloaded",
    "audio_downloaded",
    "audio_cached",
    "completed",
    "processed",
    "formatted",
}


def _cooldown_scopes_for_row(stage: str, scope: str, classification: ErrorClassification) -> list[str]:
    stage = str(stage or "").strip().lower()
    scope = str(scope or "").strip()
    error_type = str(classification.error_type or "").strip()

    # Terminal states are audit/routing signals, not pressure signals.
    if is_terminal_failure(error_type):
        return []

    scopes: list[str] = []
    stage_scope = f"stage:{stage}" if stage else ""
    channel_scope = scope if scope.startswith("channel:") else ""
    stage_local_scopes = {"resume", "format", "asr"}
    youtube_scoped_stages = {"transcript", "audio_download"}

    # Keep non-YouTube stages isolated from the channel scope that happened to
    # produce the job. A resume/ASR failure should only block the stage itself
    # unless the error is explicitly provider- or YouTube-scoped.
    if stage in stage_local_scopes:
        if stage_scope:
            scopes.append(stage_scope)
    elif channel_scope:
        scopes.append(channel_scope)

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
        elif stage in youtube_scoped_stages:
            scopes.append("youtube:content")
        else:
            scopes.append("youtube")
    elif classification.suggested_scope:
        suggested_scope = str(classification.suggested_scope).strip()
        if stage in stage_local_scopes and suggested_scope in {"coordinator", "provider"}:
            if stage_scope:
                scopes.append(stage_scope)
        else:
            scopes.append(suggested_scope)
    elif not scopes:
        if stage_scope:
            scopes.append(stage_scope)
        else:
            scopes.append(scope or "global")

    deduped: list[str] = []
    for item in scopes:
        if item and item not in deduped:
            deduped.append(item)
    return deduped


def _suggested_scope_for_error(error_type: str, log_line: str) -> str:
    if is_terminal_failure(error_type):
        policy = terminal_failure_policy(error_type)
        return str(policy.get("scope") or "video")
    if error_type.startswith("youtube_"):
        return "youtube"
    if error_type.startswith("provider_"):
        provider_match = re.search(r"(?i)(nvidia|groq|cerebras|openrouter|z\.ai)", log_line)
        provider = provider_match.group(1).lower() if provider_match else "unknown"
        return f"provider:{provider}"
    if error_type == "memory_low":
        return "stage:llm"
    if error_type == "disk_low":
        return "global"
    if error_type in {"nvidia_riva_degraded", "nvidia_riva_rpc_error"}:
        return "provider:nvidia_riva"
    if error_type == "asr_provider_unavailable":
        return "stage:asr"
    if error_type == "lease_unavailable":
        return "provider"
    if error_type == "coordinator_unavailable":
        return "coordinator"
    return ""


def classify_error(log_line: str, exit_code: int = 0) -> ErrorClassification:
    """
    Classify a single error from a log line or exit code.
    Returns an ErrorClassification with type, severity, and recommended cooldown.
    """
    text = str(log_line or "")

    # Check patterns first (more specific than exit code)
    for pattern, error_type, description, cooldown in ERROR_PATTERNS:
        if re.search(pattern, text):
            severity = "blocking" if cooldown >= 3600 else "warning"
            if cooldown == 0:
                severity = "info"
            return ErrorClassification(
                error_type,
                description,
                cooldown,
                severity,
                RECOMMENDATIONS.get(error_type, ""),
                suggested_scope=_suggested_scope_for_error(error_type, text),
            )

    # Fallback to exit code if no pattern matched
    if exit_code != 0:
        if exit_code == 1:
            return ErrorClassification(
                "general_error", f"Exit code {exit_code}", 60, "warning",
                "Check logs for details",
            )
        if exit_code == 137:
            return ErrorClassification(
                "memory_low", "Process killed (OOM)", 900, "blocking",
                RECOMMENDATIONS["memory_low"],
                suggested_scope="stage:llm",
            )
        if exit_code in (139, 134, 6):
            return ErrorClassification(
                "process_crash", f"Process crashed (signal {exit_code - 128})", 300, "warning",
                "Check for bugs or memory issues",
            )

    return ErrorClassification(
        "unknown_error", f"Unclassified: {text[:100]}", 300, "warning",
        "Check logs manually",
    )


def _video_scope(video_id: str) -> str:
    video_id = str(video_id or "").strip()
    return f"video:{video_id}" if video_id else "global"


def analyze_report_csv(report_path: str, state: OrchestratorState) -> list[dict[str, Any]]:
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
                error_msg = (row.get("error") or row.get("message") or row.get("error_text") or "").strip()
                video_id = (row.get("video_id") or row.get("id") or "").strip()
                channel_id = (row.get("channel_id") or "").strip()

                if status in SUCCESS_STATUSES:
                    continue

                if not error_msg and not status:
                    continue

                classification = classify_error(error_msg or status)
                terminal = is_terminal_failure(classification.error_type)
                terminal_policy = terminal_failure_policy(classification.error_type) if terminal else {}

                # Determine scope
                scope = "global"
                if terminal:
                    if str(terminal_policy.get("scope") or "") == "channel":
                        scope = f"channel:{channel_id}" if channel_id else "global"
                    else:
                        scope = _video_scope(video_id)
                elif classification.error_type.startswith("provider_"):
                    provider_match = re.search(r"(?i)(nvidia|groq|cerebras|openrouter|z\.ai)", error_msg)
                    provider = provider_match.group(1).lower() if provider_match else "unknown"
                    scope = f"provider:{provider}"

                # Set cooldown if needed
                if classification.cooldown_seconds > 0:
                    for cooldown_scope in _cooldown_scopes_for_row(row.get("stage", ""), scope, classification):
                        state.set_cooldown(
                            scope=cooldown_scope,
                            reason=classification.description,
                            duration_seconds=classification.cooldown_seconds,
                            severity=classification.severity,
                            recommendation=classification.recommendation,
                        )

                # Record event
                message_detail = error_msg or status
                event_id = state.add_event(
                    event_type="terminal_failure" if terminal else "error",
                    message=f"[{classification.error_type}] {classification.description}: {message_detail[:200]}",
                    stage=row.get("stage", ""),
                    scope=scope,
                    severity=classification.severity,
                    recommendation=classification.recommendation,
                    payload={
                        "video_id": video_id,
                        "channel_id": channel_id,
                        "error_type": classification.error_type,
                        "cooldown_seconds": classification.cooldown_seconds,
                        "suggested_scope": classification.suggested_scope,
                        "terminal": terminal,
                        "retry_strategy": str(terminal_policy.get("retry_strategy") or ""),
                        "route_to_asr": bool(terminal_policy.get("route_to_asr", False)),
                        "retryable": bool(terminal_policy.get("retryable", False)),
                        "normal_retry": bool(terminal_policy.get("normal_retry", False)),
                        "target_stage": str(terminal_policy.get("target_stage") or ""),
                    },
                )
                events.append({
                    "event_id": event_id,
                    "scope": scope,
                    "error_type": classification.error_type,
                    "cooldown_seconds": classification.cooldown_seconds,
                    "terminal": terminal,
                    "retry_strategy": str(terminal_policy.get("retry_strategy") or ""),
                    "route_to_asr": bool(terminal_policy.get("route_to_asr", False)),
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
