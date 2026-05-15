"""
Cooldown Manager — Set, check, and clear cooldowns per scope.
"""

from __future__ import annotations

from typing import Any

from .state import OrchestratorState


# Standard cooldown durations by error type (seconds)
STANDARD_COOLDOWNS: dict[str, int] = {
    "youtube_429": 7200,
    "youtube_403": 3600,
    "youtube_bot_detection": 86400,
    "youtube_signin_required": 21600,
    "youtube_ip_blocked": 43200,
    "youtube_geo_blocked": 0,
    "channel_unavailable": 86400,
    "provider_429": 3600,
    "provider_quota_exceeded": 86400,
    "provider_auth_error": 86400,
    "memory_low": 900,
    "disk_low": 1800,
    "coordinator_unavailable": 300,
}


def apply_cooldown(
    state: OrchestratorState,
    scope: str,
    error_type: str,
    custom_duration: int | None = None,
    severity: str = "normal",
    recommendation: str = "",
) -> None:
    """
    Apply a cooldown for a given scope and error type.
    Uses standard duration unless custom_duration is provided.
    """
    duration = custom_duration if custom_duration is not None else STANDARD_COOLDOWNS.get(error_type, 300)
    state.set_cooldown(scope, error_type, duration, severity, recommendation)


def is_scope_blocked(
    state: OrchestratorState,
    scope: str,
) -> bool:
    """Check if a scope is currently under cooldown."""
    return state.is_cooldown_active(scope)


def get_blocked_scopes(
    state: OrchestratorState,
    prefix: str | None = None,
) -> list[dict[str, Any]]:
    """
    Get all active cooldowns, optionally filtered by scope prefix.
    Example: prefix="provider:" returns all provider cooldowns.
    """
    cooldowns = state.list_active_cooldowns()
    if prefix:
        return [cd for cd in cooldowns if cd["scope"].startswith(prefix)]
    return cooldowns


def get_blocked_providers(state: OrchestratorState) -> list[str]:
    """Get list of provider names that are currently blocked."""
    providers = []
    for cd in state.list_active_cooldowns():
        if cd["scope"].startswith("provider:"):
            provider_name = cd["scope"].replace("provider:", "")
            providers.append(provider_name)
    return providers


def get_blocked_channels(state: OrchestratorState) -> list[dict[str, Any]]:
    """Get list of channels that are currently blocked."""
    channels = []
    for cd in state.list_active_cooldowns():
        if cd["scope"].startswith("channel:"):
            channels.append({
                "channel_id": cd["scope"].replace("channel:", ""),
                "reason": cd["reason"],
                "until": cd["cooldown_until"],
            })
    return channels


def clear_all_cooldowns(state: OrchestratorState) -> int:
    """Clear all expired cooldowns. Returns count removed."""
    return state.clear_expired_cooldowns()


def clear_scope_cooldown(state: OrchestratorState, scope: str) -> None:
    """Clear cooldown for a specific scope."""
    state.clear_cooldown(scope)


def get_next_wakeup(state: OrchestratorState) -> int:
    """
    Get the number of seconds until the next cooldown expires.
    Returns 0 if no cooldowns are active.
    """
    cooldowns = state.list_active_cooldowns()
    if not cooldowns:
        return 0

    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)

    min_seconds = float("inf")
    for cd in cooldowns:
        try:
            until = datetime.fromisoformat(cd["cooldown_until"])
            if until.tzinfo is None:
                until = until.replace(tzinfo=timezone.utc)
            seconds = (until - now).total_seconds()
            if 0 < seconds < min_seconds:
                min_seconds = seconds
        except (ValueError, TypeError):
            continue

    return int(min_seconds) if min_seconds != float("inf") else 0
