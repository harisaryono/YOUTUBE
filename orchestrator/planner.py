"""
Planner — Find and prioritize jobs that need to be done.
"""

from __future__ import annotations

from typing import Any

from .state import OrchestratorState
from . import db_queries


# Priority order for job types
JOB_PRIORITY = [
    "import_pending",   # 0 — highest
    "discovery",        # 1
    "transcript",       # 2
    "resume",           # 3
    "format",           # 4
    "asr",              # 5 — lowest
]


def plan_jobs(
    config: dict[str, Any],
    state: OrchestratorState,
    max_jobs: int = 10,
) -> list[dict[str, Any]]:
    """
    Find and prioritize jobs across all stages.
    Returns a list of job dicts sorted by priority.
    """
    jobs: list[dict[str, Any]] = []

    # 1. Import pending updates (always first)
    pending_count = db_queries.count_pending_imports(state)
    if pending_count > 0:
        jobs.append({
            "stage": "import_pending",
            "scope": "global",
            "priority": 0,
            "description": f"Import {pending_count} pending update(s)",
            "count": pending_count,
        })

    # 2. Discovery
    discovery_jobs = db_queries.find_channels_need_discovery(
        config, state, limit=5
    )
    for j in discovery_jobs:
        j["priority"] = 1
        j["description"] = f"Discover channel {j.get('channel_name', j.get('channel_id', '?'))}"
    jobs.extend(discovery_jobs)

    # 3. Transcript
    transcript_jobs = db_queries.find_videos_need_transcript(
        config, state, limit=20
    )
    for j in transcript_jobs:
        j["priority"] = 2
        j["description"] = f"Transcript {j.get('video_id', '?')} — {j.get('title', '')[:60]}"
    jobs.extend(transcript_jobs)

    # 4. Resume
    if config.get("resume", {}).get("enabled", True):
        resume_jobs = db_queries.find_videos_need_resume(
            config, state, limit=30
        )
        for j in resume_jobs:
            j["priority"] = 3
            j["description"] = f"Resume {j.get('video_id', '?')} — {j.get('title', '')[:60]}"
        jobs.extend(resume_jobs)

    # 5. Format
    if config.get("format", {}).get("enabled", True):
        format_jobs = db_queries.find_videos_need_format(
            config, state, limit=30
        )
        for j in format_jobs:
            j["priority"] = 4
            j["description"] = f"Format {j.get('video_id', '?')} — {j.get('title', '')[:60]}"
        jobs.extend(format_jobs)

    # 6. ASR
    if config.get("asr", {}).get("enabled", False):
        asr_jobs = db_queries.find_videos_need_asr(
            config, state, limit=5
        )
        for j in asr_jobs:
            j["priority"] = 5
            j["description"] = f"ASR {j.get('video_id', '?')} — {j.get('title', '')[:60]}"
        jobs.extend(asr_jobs)

    # Sort by priority (lower = higher priority)
    jobs.sort(key=lambda j: (j.get("priority", 99), j.get("id", 0)))

    # Limit
    return jobs[:max_jobs]


def get_summary_counts(state: OrchestratorState) -> dict[str, int]:
    """Get summary counts of pending work."""
    counts = db_queries.get_job_counts()
    counts["pending_imports"] = db_queries.count_pending_imports(state)
    return counts
