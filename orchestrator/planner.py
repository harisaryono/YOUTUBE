"""
Planner — Find and prioritize batch jobs across all stages.
1 job = 1 batch per stage per cycle (not 1 job per video).
"""

from __future__ import annotations

from typing import Any

from .state import OrchestratorState
from . import db_queries


# Priority order for job types (lower = higher priority)
JOB_PRIORITY = [
    "import_pending",   # 0 — highest
    "discovery",        # 1
    "transcript",       # 2
    "audio_download",   # 3
    "asr",              # 4
    "resume",           # 5
    "format",           # 6
]


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

    # 2. Discovery — max 1 batch per cycle, pick 1 real channel
    channels = db_queries.find_channels_need_discovery(config, state, limit=1)
    if channels:
        ch = channels[0]
        discovery_count = db_queries.count_channels_need_discovery(config, state)
        jobs.append({
            "stage": "discovery",
            "scope": f"channel:{ch['channel_id']}",
            "channel_id": ch["channel_id"],
            "limit": 1,
            "priority": 1,
            "description": f"Discover {ch.get('channel_name', ch['channel_id'])} ({discovery_count} total pending)",
            "count": discovery_count,
        })

    # 3. Transcript — max 1 batch per cycle
    transcript_count = db_queries.count_videos_need_transcript(config, state)
    if transcript_count > 0:
        batch_limit = _limit_value(config.get("youtube", {}).get("batch_limit", 100), 100)
        jobs.append({
            "stage": "transcript",
            "scope": "youtube",
            "priority": 2,
            "limit": batch_limit,
            "description": f"{_batch_description('transcript', batch_limit, 'videos')} ({transcript_count} total pending)",
            "count": transcript_count,
        })

    # 4. Audio download — fetch local audio for no_subtitle videos
    if config.get("audio_download", {}).get("enabled", True):
        audio_count = db_queries.count_videos_need_audio_download(config, state)
        if audio_count > 0:
            batch_limit = _limit_value(config.get("audio_download", {}).get("batch_limit", 50), 50)
            jobs.append({
                "stage": "audio_download",
                "scope": "youtube",
                "priority": 3,
                "limit": batch_limit,
                "description": f"{_batch_description('audio download', batch_limit, 'videos')} ({audio_count} total pending)",
                "count": audio_count,
            })

    # 5. ASR — local-audio processing only
    if config.get("asr", {}).get("enabled", False):
        asr_count = db_queries.count_videos_need_asr(config, state)
        if asr_count > 0:
            batch_limit = _limit_value(config.get("asr", {}).get("batch_limit", 20), 20)
            jobs.append({
                "stage": "asr",
                "scope": "local:asr",
                "priority": 4,
                "limit": batch_limit,
                "description": f"{_batch_description('asr', batch_limit, 'local audio files')} ({asr_count} total pending)",
                "count": asr_count,
            })

    # 6. Resume — max 1 batch per cycle
    if config.get("resume", {}).get("enabled", True):
        resume_count = db_queries.count_videos_need_resume(config, state)
        if resume_count > 0:
            batch_limit = _limit_value(config.get("resume", {}).get("batch_limit", 0), 0)
            jobs.append({
                "stage": "resume",
                "scope": "provider",
                "priority": 5,
                "limit": batch_limit,
                "description": f"{_batch_description('resume', batch_limit, 'videos')} ({resume_count} total pending)",
                "count": resume_count,
            })

    # 7. Format — max 1 batch per cycle
    if config.get("format", {}).get("enabled", True):
        format_count = db_queries.count_videos_need_format(config, state)
        if format_count > 0:
            batch_limit = _limit_value(config.get("format", {}).get("batch_limit", 500), 500)
            jobs.append({
                "stage": "format",
                "scope": "global",
                "priority": 6,
                "limit": batch_limit,
                "description": f"{_batch_description('format', batch_limit, 'videos')} ({format_count} total pending)",
                "count": format_count,
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
