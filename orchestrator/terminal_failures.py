"""
Terminal Failure Registry — centralized policy for video-scoped YouTube failures.

Terminal failures are conditions attached to a video/channel/access requirement,
not a signal that the whole YouTube pipeline, channel, or IP should be cooled down.
They should be recorded for audit and routed with the correct retry strategy.
"""

from __future__ import annotations

from typing import Any


TERMINAL_FAILURES: dict[str, dict[str, Any]] = {
    "youtube_geo_blocked": {
        "scope": "video",
        "retryable": True,
        "retry_strategy": "matching_region_proxy",
        "global_cooldown": False,
        "route_to_asr": False,
        "normal_retry": False,
        "target_stage": "transcript",
        "description": "Video is available only from specific regions/countries.",
    },
    "member_only": {
        "scope": "video",
        "retryable": False,
        "retry_strategy": "authorized_account_only",
        "global_cooldown": False,
        "route_to_asr": False,
        "normal_retry": False,
        "target_stage": "",
        "description": "Video is restricted to channel members.",
    },
    "private_video": {
        "scope": "video",
        "retryable": False,
        "retry_strategy": "skip",
        "global_cooldown": False,
        "route_to_asr": False,
        "normal_retry": False,
        "target_stage": "",
        "description": "Video is private or requires explicit grant.",
    },
    "age_restricted": {
        "scope": "video",
        "retryable": True,
        "retry_strategy": "valid_cookies_required",
        "global_cooldown": False,
        "route_to_asr": False,
        "normal_retry": False,
        "target_stage": "transcript",
        "description": "Video requires a valid age-eligible account/cookies.",
    },
    "copyright_blocked": {
        "scope": "video",
        "retryable": False,
        "retry_strategy": "skip",
        "global_cooldown": False,
        "route_to_asr": False,
        "normal_retry": False,
        "target_stage": "",
        "description": "Video is blocked by copyright/rights restrictions.",
    },
    "not_ready_yet": {
        "scope": "video",
        "retryable": True,
        "retry_strategy": "retry_after_delay",
        "global_cooldown": False,
        "route_to_asr": False,
        "normal_retry": True,
        "target_stage": "transcript",
        "retry_after_hours": 12,
        "description": "Premiere/live/recording is not available yet.",
    },
    "format_unavailable": {
        "scope": "video",
        "retryable": True,
        "retry_strategy": "change_format_selector",
        "global_cooldown": False,
        "route_to_asr": False,
        "normal_retry": False,
        "target_stage": "audio_download",
        "description": "The requested yt-dlp format is not available.",
    },
    "no_subtitle": {
        "scope": "video",
        "retryable": False,
        "retry_strategy": "route_to_asr",
        "global_cooldown": False,
        "route_to_asr": True,
        "normal_retry": False,
        "target_stage": "audio_download",
        "description": "Video has no usable YouTube subtitle and should route to ASR if audio is available.",
    },
    "video_unavailable": {
        "scope": "video",
        "retryable": False,
        "retry_strategy": "skip",
        "global_cooldown": False,
        "route_to_asr": False,
        "normal_retry": False,
        "target_stage": "",
        "description": "Video is unavailable, removed, deleted, or not found.",
    },
    "channel_unavailable": {
        "scope": "channel",
        "retryable": False,
        "retry_strategy": "skip_channel",
        "global_cooldown": False,
        "route_to_asr": False,
        "normal_retry": False,
        "target_stage": "",
        "description": "Channel is unavailable or not found.",
    },
}


def is_terminal_failure(error_type: str) -> bool:
    return str(error_type or "").strip() in TERMINAL_FAILURES


def terminal_failure_policy(error_type: str) -> dict[str, Any]:
    policy = TERMINAL_FAILURES.get(str(error_type or "").strip(), {})
    return dict(policy) if isinstance(policy, dict) else {}


def retry_strategy_for(error_type: str) -> str:
    return str(terminal_failure_policy(error_type).get("retry_strategy") or "").strip()


def route_to_asr(error_type: str) -> bool:
    return bool(terminal_failure_policy(error_type).get("route_to_asr", False))


def normal_retry_allowed(error_type: str) -> bool:
    policy = terminal_failure_policy(error_type)
    if not policy:
        return True
    return bool(policy.get("normal_retry", False))
