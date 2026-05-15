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
    "youtube_429": "Reduce workers, enable --rate-limit-safe, increase inter-video delay",
    "youtube_403": "Check cookies/proxy/IP, wait before retry",
    "youtube_bot_detection": "Stop all YouTube activity for 12-24h, use different IP/proxy",
    "youtube_signin_required": "Refresh cookies, check cookie file, use different proxy",
    "no_subtitle": "Mark as no_subtitle in DB, skip permanently",
    "member_only": "Mark as member_only in DB, skip permanently",
    "youtube_geo_blocked": "Use a proxy/IP from an allowed region or skip this video for now",
    "video_unavailable": "Mark as unavailable in DB, skip permanently",
    "channel_unavailable": "Disable channel scan, skip permanently",
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


def classify_error(
    log_line: str,
    exit_code: int = 0,
) -> ErrorClassification:
    """
    Classify a single error from a log line or exit code.
    Returns an ErrorClassification with type, severity, and recommended cooldown.
    """
    # Check patterns first (more specific than exit code)
    for pattern, error_type, description, cooldown in ERROR_PATTERNS:
        if re.search(pattern, log_line):
            severity = "blocking" if cooldown >= 3600 else "warning"
            if cooldown == 0:
                severity = "info"
            if error_type == "youtube_geo_blocked":
                severity = "warning"
            # Determine suggested scope based on error type
            suggested_scope = ""
            if error_type == "youtube_geo_blocked":
                suggested_scope = "youtube:content"
            if error_type.startswith("youtube_"):
                suggested_scope = suggested_scope or "youtube"
            elif error_type.startswith("provider_"):
                # Try to extract provider name from log line
                provider_match = re.search(r"(?i)(nvidia|groq|cerebras|openrouter|z\.ai)", log_line)
                provider = provider_match.group(1).lower() if provider_match else "unknown"
                suggested_scope = f"provider:{provider}"
            elif error_type == "memory_low":
                suggested_scope = "stage:llm"
            elif error_type == "disk_low":
                suggested_scope = "global"
            elif error_type == "nvidia_riva_degraded":
                suggested_scope = "provider:nvidia_riva"
            elif error_type == "nvidia_riva_rpc_error":
                suggested_scope = "provider:nvidia_riva"
            elif error_type == "asr_provider_unavailable":
                suggested_scope = "stage:asr"
            elif error_type == "lease_unavailable":
                suggested_scope = "provider"
            elif error_type == "coordinator_unavailable":
                suggested_scope = "coordinator"
            return ErrorClassification(
                error_type, description, cooldown, severity,
                RECOMMENDATIONS.get(error_type, ""),
                suggested_scope=suggested_scope,
            )

    # Fallback to exit code if no pattern matched
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

                if status in SUCCESS_STATUSES:
                    continue

                if not error_msg:
                    continue

                classification = classify_error(error_msg)

                # Determine scope
                scope = "global"
                if classification.error_type in ("no_subtitle", "member_only", "video_unavailable"):
                    scope = f"video:{video_id}" if video_id else "global"
                elif classification.error_type in ("channel_unavailable",):
                    scope = f"channel:{channel_id}" if channel_id else "global"
                elif classification.error_type.startswith("provider_"):
                    # Try to extract provider name
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
