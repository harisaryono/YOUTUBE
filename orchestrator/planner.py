"""
Planner — Find and prioritize batch jobs across all stages.
1 job = 1 batch per stage per cycle (not 1 job per video).
"""

from __future__ import annotations

from typing import Any

from .state import OrchestratorState
from . import db_queries


# Priority order for job types (lower = higher priority).
# Safe/local stages are scheduled first so the daemon keeps moving even when
# YouTube-dependent stages are cooling down or blocked.
JOB_PRIORITY = {
    "import_pending": 0,
    "resume": 1,
    "format": 2,
    "asr": 3,
    "transcript": 4,
    "audio_download": 5,
    "discovery": 6,
}


def _limit_value(value: Any, default: int) -> int:
    try:
        limit = int(value)
    except (TypeError, ValueError):
        return default
    return max(limit, 0)


def _batch_description(stage: str, limit: int, noun: str) -> str:
    if limit <= 0:
        return f"{stage.capitalize()} all pending {noun}"
    return f"{stage.capitalize()} up to {limit} pending {noun}"


def _priority(stage: str) -> int:
    return int(JOB_PRIORITY.get(stage, 99))


def plan_jobs(
    config: dict[str, Any],
    state: OrchestratorState,
    max_jobs: int = 10,
) -> list[dict[str, Any]]:
    """
    Find and prioritize batch jobs across all stages.
    Returns at most `max_jobs` batch jobs, sorted by priority.
    Each job represents one batch of work for one stage.
    """
    jobs: list[dict[str, Any]] = []

    # 1. Import pending updates (always first)
    pending_count = db_queries.count_pending_imports(state)
    if pending_count > 0:
        jobs.append({
            "stage": "import_pending",
            "scope": "global",
            "priority": 0,
            "limit": 100,
            "description": f"Import {pending_count} pending update(s)",
            "count": pending_count,
        })

    # 2. Resume — max 1 batch per cycle
    if config.get("resume", {}).get("enabled", True):
        resume_count = db_queries.count_videos_need_resume(config, state)
        if resume_count > 0:
            batch_limit = _limit_value(config.get("resume", {}).get("batch_limit", 0), 0)
            jobs.append({
                "stage": "resume",
                "scope": "provider",
                "priority": _priority("resume"),
                "limit": batch_limit,
                "description": f"{_batch_description('resume', batch_limit, 'videos')} ({resume_count} total pending)",
                "count": resume_count,
            })

    # 3. Format — max 1 batch per cycle
    if config.get("format", {}).get("enabled", True):
        format_count = db_queries.count_videos_need_format(config, state)
        if format_count > 0:
            batch_limit = _limit_value(config.get("format", {}).get("batch_limit", 500), 500)
            jobs.append({
                "stage": "format",
                "scope": "global",
                "priority": _priority("format"),
                "limit": batch_limit,
                "description": f"{_batch_description('format', batch_limit, 'videos')} ({format_count} total pending)",
                "count": format_count,
            })

    # 4. ASR — local-audio processing only
    if config.get("asr", {}).get("enabled", False):
        asr_count = db_queries.count_videos_need_asr(config, state)
        if asr_count > 0:
            batch_limit = _limit_value(config.get("asr", {}).get("batch_limit", 20), 20)
            jobs.append({
                "stage": "asr",
                "scope": "local:asr",
                "priority": _priority("asr"),
                "limit": batch_limit,
                "description": f"{_batch_description('asr', batch_limit, 'local audio files')} ({asr_count} total pending)",
                "count": asr_count,
            })

    # 5. Transcript — max 1 batch per cycle
    transcript_count = db_queries.count_videos_need_transcript(config, state)
    if transcript_count > 0:
        batch_limit = _limit_value(config.get("youtube", {}).get("batch_limit", 100), 100)
        jobs.append({
            "stage": "transcript",
            "scope": "youtube",
            "priority": _priority("transcript"),
            "limit": batch_limit,
            "description": f"{_batch_description('transcript', batch_limit, 'videos')} ({transcript_count} total pending)",
            "count": transcript_count,
        })

    # 6. Audio download — fetch local audio for no_subtitle videos
    if config.get("audio_download", {}).get("enabled", True):
        audio_count = db_queries.count_videos_need_audio_download(config, state)
        if audio_count > 0:
            batch_limit = _limit_value(config.get("audio_download", {}).get("batch_limit", 50), 50)
            jobs.append({
                "stage": "audio_download",
                "scope": "youtube",
                "priority": _priority("audio_download"),
                "limit": batch_limit,
                "description": f"{_batch_description('audio download', batch_limit, 'videos')} ({audio_count} total pending)",
                "count": audio_count,
            })

    # 7. Discovery — max 1 batch per cycle, pick 1 real channel
    channels = db_queries.find_channels_need_discovery(config, state, limit=1)
    if channels:
        ch = channels[0]
        discovery_count = db_queries.count_channels_need_discovery(config, state)
        jobs.append({
            "stage": "discovery",
            "scope": f"channel:{ch['channel_id']}",
            "channel_id": ch["channel_id"],
            "limit": 1,
            "priority": _priority("discovery"),
            "description": f"Discover {ch.get('channel_name', ch['channel_id'])} ({discovery_count} total pending)",
            "count": discovery_count,
        })

    # Sort by priority (lower = higher priority)
    jobs.sort(key=lambda j: (j.get("priority", 99), j.get("stage", "")))

    # Limit to max_jobs
    return jobs[:max_jobs]


def get_summary_counts(state: OrchestratorState) -> dict[str, int]:
    """Get summary counts of pending work."""
    counts = db_queries.get_job_counts()
    counts["pending_imports"] = db_queries.count_pending_imports(state)
    return counts
