"""
Planner — Find and prioritize batch jobs across all stages.
1 job = 1 batch per stage per cycle (not 1 job per video).
"""

from __future__ import annotations

from typing import Any

from .state import OrchestratorState
from . import db_queries


# Priority order for job types (lower = higher priority).
# Base mode is work-conserving: run any available safe work first.
# When YouTube backlog grows, discovery is promoted so the pipeline keeps feeding.
JOB_PRIORITY_AVAILABLE_WORK_FIRST = {
    "import_pending": 0,
    "transcript": 1,
    "audio_download": 2,
    "asr": 3,
    "format": 4,
    "resume": 5,
    "discovery": 6,
}

JOB_PRIORITY_YOUTUBE_HEAVY = {
    "import_pending": 0,
    "transcript": 1,
    "audio_download": 2,
    "discovery": 3,
    "asr": 4,
    "format": 5,
    "resume": 6,
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


def _adaptive_priority(stage: str, youtube_pressure: int, boost_threshold: int) -> int:
    if youtube_pressure >= boost_threshold:
        return int(JOB_PRIORITY_YOUTUBE_HEAVY.get(stage, 99))
    return int(JOB_PRIORITY_AVAILABLE_WORK_FIRST.get(stage, 99))


def _parallel_config(config: dict[str, Any]) -> dict[str, Any]:
    return config.get("parallel", {}) or {}


def _stage_slots(config: dict[str, Any], stage: str) -> int:
    parallel = _parallel_config(config)
    stage_cfg = parallel.get("stages", {}).get(stage, {}) or {}
    try:
        return max(1, int(stage_cfg.get("slots", 1) or 1))
    except (TypeError, ValueError):
        return 1


def _query_limit_for_stage(stage: str, batch_limit: int, slots: int) -> int:
    # Overfetch a little so we can split rows into per-channel jobs.
    if stage in {"discovery", "import_pending"}:
        return max(1, batch_limit)
    return max(batch_limit, batch_limit * max(1, slots) * 4)


def _group_rows_by_channel(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for row in rows:
        channel_id = str(row.get("channel_identifier") or row.get("channel_id") or "").strip()
        if not channel_id:
            channel_id = str(row.get("scope") or row.get("video_id") or "").strip()
        if not channel_id:
            channel_id = f"row:{row.get('id', '')}"
        if channel_id not in grouped:
            grouped[channel_id] = {
                "channel_id": channel_id,
                "channel_name": str(row.get("channel_name") or channel_id),
                "rows": [],
            }
            order.append(channel_id)
        grouped[channel_id]["rows"].append(row)
    return [grouped[key] for key in order]


def _append_scoped_jobs(
    jobs: list[dict[str, Any]],
    *,
    stage: str,
    scope_prefix: str,
    rows: list[dict[str, Any]],
    limit: int,
    priority: int,
    description_label: str,
    max_jobs: int,
) -> None:
    grouped = _group_rows_by_channel(rows)
    for group in grouped[:max_jobs]:
        channel_id = str(group.get("channel_id") or "").strip()
        if not channel_id:
            continue
        channel_name = str(group.get("channel_name") or channel_id).strip()
        count = len(group.get("rows", []))
        jobs.append({
            "stage": stage,
            "scope": f"{scope_prefix}:{channel_id}",
            "channel_identifier": channel_id,
            "channel_id": channel_id,
            "priority": priority,
            "limit": limit,
            "description": f"{description_label} for {channel_name} ({count} pending)",
            "count": count,
        })


def _adaptive_batch_limit(
    stage: str,
    default_limit: int,
    config: dict[str, Any],
    state: OrchestratorState,
) -> int:
    adaptive_cfg = config.get("adaptive", {}).get(stage, {})
    if not adaptive_cfg.get("enabled", False):
        return default_limit

    min_batch = _limit_value(adaptive_cfg.get("min_batch", default_limit), default_limit or 1)
    max_batch = _limit_value(adaptive_cfg.get("max_batch", default_limit), default_limit or 1)
    if max_batch < min_batch:
        max_batch = min_batch

    current = state.get_stage_batch_limit(stage, default_limit)
    current = max(min_batch, min(current, max_batch))
    return current


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
    boost_threshold = int(config.get("orchestrator", {}).get("youtube_backlog_boost_threshold", 500) or 500)

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

    # Precompute backlog counts so the planner can adjust priorities adaptively.
    resume_count = db_queries.count_videos_need_resume(config, state)
    format_count = db_queries.count_videos_need_format(config, state)
    asr_count = db_queries.count_videos_need_asr(config, state)
    transcript_count = db_queries.count_videos_need_transcript(config, state)
    audio_count = db_queries.count_videos_need_audio_download(config, state)
    youtube_pressure = transcript_count + audio_count

    # 2. Resume — split by channel so multiple lanes can work without colliding.
    if config.get("resume", {}).get("enabled", True) and resume_count > 0:
        batch_limit = _limit_value(config.get("resume", {}).get("batch_limit", 0), 0)
        stage_slots = _stage_slots(config, "resume")
        fetch_limit = _query_limit_for_stage("resume", batch_limit or 100, stage_slots)
        rows = db_queries.find_videos_need_resume(config, state, limit=fetch_limit)
        _append_scoped_jobs(
            jobs,
            stage="resume",
            scope_prefix="channel",
            rows=rows,
            limit=batch_limit,
            priority=_adaptive_priority("resume", youtube_pressure, boost_threshold),
            description_label="Resume batch",
            max_jobs=stage_slots,
        )

    # 3. Format — split by channel so multiple lanes can work without colliding.
    if config.get("format", {}).get("enabled", True) and format_count > 0:
        batch_limit = _limit_value(config.get("format", {}).get("batch_limit", 500), 500)
        stage_slots = _stage_slots(config, "format")
        fetch_limit = _query_limit_for_stage("format", batch_limit, stage_slots)
        rows = db_queries.find_videos_need_format(config, state, limit=fetch_limit)
        _append_scoped_jobs(
            jobs,
            stage="format",
            scope_prefix="channel",
            rows=rows,
            limit=batch_limit,
            priority=_adaptive_priority("format", youtube_pressure, boost_threshold),
            description_label="Format batch",
            max_jobs=stage_slots,
        )

    # 4. ASR — split by channel so local audio lanes can overlap safely.
    if config.get("asr", {}).get("enabled", False) and asr_count > 0:
        batch_limit = _limit_value(config.get("asr", {}).get("batch_limit", 20), 20)
        stage_slots = _stage_slots(config, "asr")
        fetch_limit = _query_limit_for_stage("asr", batch_limit, stage_slots)
        rows = db_queries.find_videos_need_asr(config, state, limit=fetch_limit)
        _append_scoped_jobs(
            jobs,
            stage="asr",
            scope_prefix="channel",
            rows=rows,
            limit=batch_limit,
            priority=_adaptive_priority("asr", youtube_pressure, boost_threshold),
            description_label="ASR batch",
            max_jobs=stage_slots,
        )

    # 5. Transcript — split by channel so YouTube lanes can overlap safely.
    if transcript_count > 0:
        batch_limit = _limit_value(config.get("youtube", {}).get("batch_limit", 100), 100)
        batch_limit = _adaptive_batch_limit("transcript", batch_limit, config, state)
        stage_slots = _stage_slots(config, "transcript")
        fetch_limit = _query_limit_for_stage("transcript", batch_limit, stage_slots)
        rows = db_queries.find_videos_need_transcript(config, state, limit=fetch_limit)
        _append_scoped_jobs(
            jobs,
            stage="transcript",
            scope_prefix="channel",
            rows=rows,
            limit=batch_limit,
            priority=_adaptive_priority("transcript", youtube_pressure, boost_threshold),
            description_label="Transcript batch",
            max_jobs=stage_slots,
        )

    # 6. Audio download — fetch local audio for no_subtitle videos, split by channel.
    if config.get("audio_download", {}).get("enabled", True) and audio_count > 0:
        batch_limit = _limit_value(config.get("audio_download", {}).get("batch_limit", 50), 50)
        batch_limit = _adaptive_batch_limit("audio_download", batch_limit, config, state)
        stage_slots = _stage_slots(config, "audio_download")
        fetch_limit = _query_limit_for_stage("audio_download", batch_limit, stage_slots)
        rows = db_queries.find_videos_need_audio_download(config, state, limit=fetch_limit)
        _append_scoped_jobs(
            jobs,
            stage="audio_download",
            scope_prefix="channel",
            rows=rows,
            limit=batch_limit,
            priority=_adaptive_priority("audio_download", youtube_pressure, boost_threshold),
            description_label="Audio download batch",
            max_jobs=stage_slots,
        )

    # 7. Discovery — split by channel, one lane per discovered target.
    discovery_slots = _stage_slots(config, "discovery")
    channels = db_queries.find_channels_need_discovery(config, state, limit=max(1, discovery_slots * 4))
    if channels:
        discovery_count = db_queries.count_channels_need_discovery(config, state)
        for ch in channels[:discovery_slots]:
            jobs.append({
                "stage": "discovery",
                "scope": f"channel:{ch['channel_id']}",
                "channel_id": ch["channel_id"],
                "channel_identifier": ch["channel_id"],
                "limit": 1,
                "priority": _adaptive_priority("discovery", youtube_pressure, boost_threshold),
                "description": f"Discover {ch.get('channel_name', ch['channel_id'])} ({discovery_count} total pending)",
                "count": discovery_count,
            })

    # Sort by priority (lower = higher priority)
    jobs.sort(key=lambda j: (j.get("priority", 99), j.get("stage", "")))

    # Limit to max_jobs
    return jobs[:max_jobs]


def get_summary_counts(config: dict[str, Any], state: OrchestratorState) -> dict[str, int]:
    """Get summary counts of pending work."""
    counts = db_queries.get_job_counts()
    counts["pending_imports"] = db_queries.count_pending_imports(state)
    counts["channels_need_discovery"] = db_queries.count_channels_need_discovery(config, state)
    return counts
