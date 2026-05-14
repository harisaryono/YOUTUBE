"""
Database Queries — Find work items from the YouTube database.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .state import OrchestratorState
from .video_claims import active_video_claim_clause, ensure_video_processing_columns


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = PROJECT_ROOT / "db" / "youtube_transcripts.db"


def _connect(db_path: str | Path | None = None) -> sqlite3.Connection:
    path = Path(db_path) if db_path else DEFAULT_DB_PATH
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _ensure_channel_runtime_discovery_columns(conn: sqlite3.Connection) -> None:
    rows = conn.execute("PRAGMA table_info(channel_runtime_state)").fetchall()
    existing = {str(row["name"]) for row in rows}
    if "last_discovery_scope" not in existing:
        conn.execute("ALTER TABLE channel_runtime_state ADD COLUMN last_discovery_scope TEXT NOT NULL DEFAULT ''")
    if "full_history_scanned_at" not in existing:
        conn.execute("ALTER TABLE channel_runtime_state ADD COLUMN full_history_scanned_at TEXT")


def _active_channel_cooldown_ids(state: OrchestratorState) -> set[str]:
    active: set[str] = set()
    for cd in state.list_active_cooldowns():
        scope = str(cd.get("scope") or "").strip()
        if not scope.startswith("channel:"):
            continue
        channel_id = scope.split("channel:", 1)[1].strip()
        if channel_id:
            active.add(channel_id)
    return active


def _ensure_video_claim_columns(conn: sqlite3.Connection) -> None:
    ensure_video_processing_columns(conn)


def count_pending_imports(state: OrchestratorState) -> int:
    """Count pending updates in pending_updates directory."""
    pending_dir = PROJECT_ROOT / "pending_updates"
    if not pending_dir.exists():
        return 0
    return len(list(pending_dir.glob("*.csv"))) + len(list(pending_dir.glob("*.json")))


def count_channels_need_discovery(
    config: dict[str, Any],
    state: OrchestratorState,
    db_path: str | Path | None = None,
) -> int:
    """Count channels that need discovery."""
    return len(find_channels_need_discovery(config, state, limit=1000000, db_path=db_path))


def count_channels_need_full_history_discovery(
    config: dict[str, Any],
    state: OrchestratorState,
    db_path: str | Path | None = None,
) -> int:
    """Count channels that have never completed a full-history discovery scan."""
    conn = _connect(db_path)
    _ensure_channel_runtime_discovery_columns(conn)
    active_cooldowns = _active_channel_cooldown_ids(state)
    rows = conn.execute(
        """SELECT c.id, c.channel_id, c.channel_name, c.channel_url,
                  COALESCE(crs.scan_enabled, 1) as scan_enabled,
                  COALESCE(crs.skip_reason, '') as skip_reason,
                  c.updated_at as last_discovery_at,
                  COALESCE(crs.last_discovery_scope, '') as last_discovery_scope,
                  COALESCE(crs.full_history_scanned_at, '') as full_history_scanned_at
           FROM channels c
           LEFT JOIN channel_runtime_state crs ON c.channel_id = crs.channel_id
           WHERE COALESCE(crs.scan_enabled, 1) = 1
             AND (crs.skip_reason IS NULL OR crs.skip_reason = '')
             AND COALESCE(crs.full_history_scanned_at, '') = ''"""
    ).fetchall()
    conn.close()
    return sum(1 for row in rows if str(row["channel_id"]) not in active_cooldowns)


def count_channels_need_latest_discovery(
    config: dict[str, Any],
    state: OrchestratorState,
    db_path: str | Path | None = None,
) -> int:
    """Count channels that already had a full-history scan but are stale again."""
    conn = _connect(db_path)
    _ensure_channel_runtime_discovery_columns(conn)
    interval_hours = config.get("youtube", {}).get("discovery_interval_hours", 24)
    active_cooldowns = _active_channel_cooldown_ids(state)
    rows = conn.execute(
        """SELECT c.id, c.channel_id, c.channel_name, c.channel_url,
                  COALESCE(crs.scan_enabled, 1) as scan_enabled,
                  COALESCE(crs.skip_reason, '') as skip_reason,
                  c.updated_at as last_discovery_at,
                  COALESCE(crs.last_discovery_scope, '') as last_discovery_scope,
                  COALESCE(crs.full_history_scanned_at, '') as full_history_scanned_at
           FROM channels c
           LEFT JOIN channel_runtime_state crs ON c.channel_id = crs.channel_id
           WHERE COALESCE(crs.scan_enabled, 1) = 1
             AND (crs.skip_reason IS NULL OR crs.skip_reason = '')
             AND COALESCE(crs.full_history_scanned_at, '') != ''
             AND (
                 c.updated_at IS NULL
                 OR c.updated_at < datetime('now', '-' || ? || ' hours')
             )""",
        (interval_hours,),
    ).fetchall()
    conn.close()
    return sum(1 for row in rows if str(row["channel_id"]) not in active_cooldowns)


def count_videos_need_transcript(
    config: dict[str, Any],
    state: OrchestratorState,
    db_path: str | Path | None = None,
) -> int:
    """Count videos that need transcript download."""
    conn = _connect(db_path)
    _ensure_video_claim_columns(conn)
    row = conn.execute(
        f"""SELECT COUNT(*) as cnt FROM videos v
           WHERE v.transcript_downloaded = 0
             AND (v.transcript_language IS NULL OR v.transcript_language != 'no_subtitle')
             AND COALESCE(v.is_member_only, 0) = 0
             AND COALESCE(v.is_short, 0) = 0
             AND (v.transcript_retry_after IS NULL OR v.transcript_retry_after <= datetime('now'))
             AND {active_video_claim_clause('v')}"""
    ).fetchone()
    conn.close()
    return row["cnt"] if row else 0


def count_videos_need_audio_download(
    config: dict[str, Any],
    state: OrchestratorState,
    db_path: str | Path | None = None,
) -> int:
    """Count videos that need local audio download for ASR."""
    max_duration = config.get("audio_download", {}).get("max_duration_minutes", 60) * 60
    conn = _connect(db_path)
    _ensure_video_claim_columns(conn)
    row = conn.execute(
        """
        SELECT COUNT(*) as cnt
        FROM videos v
        LEFT JOIN video_audio_assets a ON a.video_id = v.video_id
        WHERE COALESCE(v.transcript_downloaded, 0) = 0
          AND v.transcript_language = 'no_subtitle'
          AND COALESCE(v.is_member_only, 0) = 0
          AND COALESCE(v.is_short, 0) = 0
          AND (v.duration IS NULL OR v.duration <= ?)
          AND (
              a.video_id IS NULL
              OR COALESCE(a.status, '') IN ('pending', 'failed')
          )
          AND (
              a.retry_after IS NULL
              OR a.retry_after <= datetime('now')
          )
          AND {active_video_claim_clause('v')}
        """,
        (max_duration,),
    ).fetchone()
    conn.close()
    return row["cnt"] if row else 0


def count_videos_need_resume(
    config: dict[str, Any],
    state: OrchestratorState,
    db_path: str | Path | None = None,
) -> int:
    """Count videos that have transcript but no resume."""
    conn = _connect(db_path)
    _ensure_video_claim_columns(conn)
    row = conn.execute(
        f"""SELECT COUNT(*) as cnt FROM videos v
           WHERE transcript_downloaded = 1
             AND (summary_file_path IS NULL OR summary_file_path = '')
             AND (summary_text IS NULL OR summary_text = '')
             AND {active_video_claim_clause('v')}"""
    ).fetchone()
    conn.close()
    return row["cnt"] if row else 0


def count_videos_need_format(
    config: dict[str, Any],
    state: OrchestratorState,
    db_path: str | Path | None = None,
) -> int:
    """Count videos that have transcript but no formatted version."""
    conn = _connect(db_path)
    _ensure_video_claim_columns(conn)
    row = conn.execute(
        f"""SELECT COUNT(*) as cnt FROM videos v
           WHERE transcript_downloaded = 1
             AND (transcript_formatted_path IS NULL OR transcript_formatted_path = '')
             AND {active_video_claim_clause('v')}"""
    ).fetchone()
    conn.close()
    return row["cnt"] if row else 0


def count_videos_need_asr(
    config: dict[str, Any],
    state: OrchestratorState,
    db_path: str | Path | None = None,
) -> int:
    """Count videos with no_subtitle that could use ASR."""
    max_duration = config.get("asr", {}).get("max_duration_minutes", 60) * 60
    conn = _connect(db_path)
    _ensure_video_claim_columns(conn)
    row = conn.execute(
        f"""SELECT COUNT(*) as cnt FROM videos v
           WHERE transcript_language = 'no_subtitle'
             AND COALESCE(is_member_only, 0) = 0
             AND COALESCE(is_short, 0) = 0
             AND (duration IS NULL OR duration <= ?)
             AND {active_video_claim_clause('v')}""",
        (max_duration,),
    ).fetchone()
    conn.close()
    return row["cnt"] if row else 0


def find_channels_need_discovery(
    config: dict[str, Any],
    state: OrchestratorState,
    limit: int = 5,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """
    Find channels that need discovery.
    Priority: never-discovered > stale > recently done.
    """
    conn = _connect(db_path)
    _ensure_channel_runtime_discovery_columns(conn)
    interval_hours = config.get("youtube", {}).get("discovery_interval_hours", 24)
    active_cooldowns = _active_channel_cooldown_ids(state)

    rows = conn.execute(
        """SELECT c.id, c.channel_id, c.channel_name, c.channel_url,
                  COALESCE(crs.scan_enabled, 1) as scan_enabled,
                  COALESCE(crs.skip_reason, '') as skip_reason,
                  c.updated_at as last_discovery_at,
                  COALESCE(crs.last_discovery_scope, '') as last_discovery_scope,
                  COALESCE(crs.full_history_scanned_at, '') as full_history_scanned_at
           FROM channels c
           LEFT JOIN channel_runtime_state crs ON c.channel_id = crs.channel_id
           WHERE COALESCE(crs.scan_enabled, 1) = 1
             AND (crs.skip_reason IS NULL OR crs.skip_reason = '')
             AND (
                 COALESCE(crs.full_history_scanned_at, '') = ''
                 OR c.updated_at IS NULL
                 OR c.updated_at < datetime('now', '-' || ? || ' hours')
             )
           ORDER BY
               CASE WHEN COALESCE(crs.full_history_scanned_at, '') = '' THEN 0 ELSE 1 END,
               CASE WHEN c.updated_at IS NULL THEN 0 ELSE 1 END,
               c.updated_at ASC
           LIMIT ?""",
        (interval_hours, limit),
    ).fetchall()

    result = []
    for row in rows:
        d = dict(row)
        if str(d.get("channel_id") or "") in active_cooldowns:
            continue
        d["stage"] = "discovery"
        d["scope"] = f"channel:{d['channel_id']}"
        d["scan_mode"] = "full_history" if not str(d.get("full_history_scanned_at") or "").strip() else "latest_only"
        d["priority"] = 1 if d["scan_mode"] == "full_history" else 6
        result.append(d)

    conn.close()
    return result


def find_videos_need_transcript(
    config: dict[str, Any],
    state: OrchestratorState,
    limit: int = 20,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """
    Find videos that need transcript download.
    Excludes: already downloaded, no_subtitle, member_only, shorts.
    """
    conn = _connect(db_path)
    _ensure_video_claim_columns(conn)

    rows = conn.execute(
        f"""SELECT v.id, v.video_id, v.title, v.channel_id,
                  c.channel_id as channel_identifier,
                  c.channel_name,
                  COALESCE(v.duration, 0) as duration,
                  COALESCE(v.transcript_retry_count, 0) as retry_count
           FROM videos v
           JOIN channels c ON v.channel_id = c.id
           WHERE v.transcript_downloaded = 0
             AND (v.transcript_language IS NULL OR v.transcript_language != 'no_subtitle')
             AND COALESCE(v.is_member_only, 0) = 0
             AND COALESCE(v.is_short, 0) = 0
             AND (v.transcript_retry_after IS NULL OR v.transcript_retry_after <= datetime('now'))
             AND {active_video_claim_clause('v')}
           ORDER BY
               COALESCE(v.transcript_retry_count, 0) ASC,
               v.upload_date DESC NULLS LAST,
               v.id DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()

    result = []
    for row in rows:
        d = dict(row)
        d["stage"] = "transcript"
        d["scope"] = f"channel:{d['channel_identifier']}"
        result.append(d)

    conn.close()
    return result


def find_videos_need_resume(
    config: dict[str, Any],
    state: OrchestratorState,
    limit: int = 30,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """
    Find videos that have transcript but no resume.
    """
    conn = _connect(db_path)
    _ensure_video_claim_columns(conn)

    rows = conn.execute(
        f"""SELECT v.id, v.video_id, v.title, v.channel_id,
                  c.channel_id as channel_identifier,
                  c.channel_name,
                  COALESCE(v.word_count, 0) as word_count
           FROM videos v
           JOIN channels c ON v.channel_id = c.id
           WHERE v.transcript_downloaded = 1
             AND (v.summary_file_path IS NULL OR v.summary_file_path = '')
             AND (v.summary_text IS NULL OR v.summary_text = '')
             AND {active_video_claim_clause('v')}
           ORDER BY v.word_count DESC, v.id DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()

    result = []
    for row in rows:
        d = dict(row)
        d["stage"] = "resume"
        d["scope"] = "global"
        result.append(d)

    conn.close()
    return result


def find_videos_need_audio_download(
    config: dict[str, Any],
    state: OrchestratorState,
    limit: int = 20,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Find videos that need local audio download for ASR."""
    max_duration = config.get("audio_download", {}).get("max_duration_minutes", 60) * 60
    conn = _connect(db_path)
    _ensure_video_claim_columns(conn)

    rows = conn.execute(
        f"""
        SELECT v.id, v.video_id, v.title, v.channel_id,
               c.channel_id as channel_identifier,
               c.channel_name,
               COALESCE(v.duration, 0) as duration,
               COALESCE(a.status, 'pending') as audio_status,
               COALESCE(a.audio_file_path, '') as audio_file_path
        FROM videos v
        JOIN channels c ON v.channel_id = c.id
        LEFT JOIN video_audio_assets a ON a.video_id = v.video_id
        WHERE COALESCE(v.transcript_downloaded, 0) = 0
          AND v.transcript_language = 'no_subtitle'
          AND COALESCE(v.is_member_only, 0) = 0
          AND COALESCE(v.is_short, 0) = 0
          AND (v.duration IS NULL OR v.duration <= ?)
          AND (
              a.video_id IS NULL
              OR COALESCE(a.status, '') IN ('pending', 'failed')
          )
          AND (
              a.retry_after IS NULL
              OR a.retry_after <= datetime('now')
          )
          AND {active_video_claim_clause('v')}
        ORDER BY
            COALESCE(a.retry_count, 0) ASC,
            COALESCE(v.duration, 0) ASC,
            v.id DESC
        LIMIT ?
        """,
        (max_duration, limit),
    ).fetchall()

    result = []
    for row in rows:
        d = dict(row)
        d["stage"] = "audio_download"
        d["scope"] = "youtube"
        result.append(d)

    conn.close()
    return result


def find_videos_need_format(
    config: dict[str, Any],
    state: OrchestratorState,
    limit: int = 30,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """
    Find videos that have transcript but no formatted version.
    """
    conn = _connect(db_path)
    _ensure_video_claim_columns(conn)

    rows = conn.execute(
        f"""SELECT v.id, v.video_id, v.title, v.channel_id,
                  c.channel_id as channel_identifier,
                  c.channel_name,
                  COALESCE(v.word_count, 0) as word_count
           FROM videos v
           JOIN channels c ON v.channel_id = c.id
           WHERE v.transcript_downloaded = 1
             AND (v.transcript_formatted_path IS NULL OR v.transcript_formatted_path = '')
             AND {active_video_claim_clause('v')}
           ORDER BY v.word_count DESC, v.id DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()

    result = []
    for row in rows:
        d = dict(row)
        d["stage"] = "format"
        d["scope"] = "global"
        result.append(d)

    conn.close()
    return result


def find_videos_need_asr(
    config: dict[str, Any],
    state: OrchestratorState,
    limit: int = 5,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """
    Find videos with no_subtitle that could use ASR.
    Guarded by planner — only called when ASR is enabled in config.
    """
    max_duration = config.get("asr", {}).get("max_duration_minutes", 60) * 60

    conn = _connect(db_path)
    _ensure_video_claim_columns(conn)

    rows = conn.execute(
        f"""SELECT v.id, v.video_id, v.title, v.channel_id,
                  c.channel_id as channel_identifier,
                  c.channel_name,
                  COALESCE(v.duration, 0) as duration,
                  COALESCE(a.audio_file_path, '') as audio_file_path,
                  COALESCE(a.audio_format, '') as audio_format,
                  COALESCE(a.status, '') as audio_status
           FROM videos v
           JOIN channels c ON v.channel_id = c.id
           JOIN video_audio_assets a ON a.video_id = v.video_id
           WHERE v.transcript_downloaded = 0
             AND v.transcript_language = 'no_subtitle'
             AND COALESCE(v.is_member_only, 0) = 0
             AND COALESCE(v.is_short, 0) = 0
             AND (v.duration IS NULL OR v.duration <= ?)
             AND COALESCE(a.status, '') = 'downloaded'
             AND COALESCE(a.audio_file_path, '') != ''
             AND {active_video_claim_clause('v')}
           ORDER BY COALESCE(a.updated_at, v.created_at) ASC, v.id DESC
           LIMIT ?""",
        (max_duration, limit),
    ).fetchall()

    result = []
    for row in rows:
        d = dict(row)
        d["stage"] = "asr"
        d["scope"] = f"video:{d['video_id']}"
        result.append(d)

    conn.close()
    return result


def get_job_counts(db_path: str | Path | None = None) -> dict[str, int]:
    """Get counts of pending work by category."""
    conn = _connect(db_path)
    _ensure_video_claim_columns(conn)
    counts: dict[str, int] = {}

    # Channels total
    row = conn.execute(
        """SELECT COUNT(*) as cnt FROM channels c
           LEFT JOIN channel_runtime_state crs ON c.channel_id = crs.channel_id
           WHERE COALESCE(crs.scan_enabled, 1) = 1
             AND (crs.skip_reason IS NULL OR crs.skip_reason = '')"""
    ).fetchone()
    counts["channels_total"] = row["cnt"] if row else 0

    # Videos needing transcript
    row = conn.execute(
        f"""SELECT COUNT(*) as cnt FROM videos v
           WHERE transcript_downloaded = 0
             AND (transcript_language IS NULL OR transcript_language != 'no_subtitle')
             AND COALESCE(is_member_only, 0) = 0
             AND COALESCE(is_short, 0) = 0
             AND {active_video_claim_clause('v')}"""
    ).fetchone()
    counts["videos_need_transcript"] = row["cnt"] if row else 0

    # Videos needing audio download for ASR
    row = conn.execute(
        f"""
        SELECT COUNT(*) as cnt
        FROM videos v
        LEFT JOIN video_audio_assets a ON a.video_id = v.video_id
        WHERE COALESCE(v.transcript_downloaded, 0) = 0
          AND v.transcript_language = 'no_subtitle'
          AND COALESCE(v.is_member_only, 0) = 0
          AND COALESCE(v.is_short, 0) = 0
          AND (
              a.video_id IS NULL
              OR COALESCE(a.status, '') IN ('pending', 'failed')
          )
          AND (
              a.retry_after IS NULL
              OR a.retry_after <= datetime('now')
          )
          AND {active_video_claim_clause('v')}
        """
    ).fetchone()
    counts["videos_need_audio_download"] = row["cnt"] if row else 0

    # Videos needing resume
    row = conn.execute(
        f"""SELECT COUNT(*) as cnt FROM videos v
           WHERE transcript_downloaded = 1
             AND (summary_file_path IS NULL OR summary_file_path = '')
             AND (summary_text IS NULL OR summary_text = '')
             AND {active_video_claim_clause('v')}"""
    ).fetchone()
    counts["videos_need_resume"] = row["cnt"] if row else 0

    # Videos needing format
    row = conn.execute(
        f"""SELECT COUNT(*) as cnt FROM videos v
           WHERE transcript_downloaded = 1
             AND (transcript_formatted_path IS NULL OR transcript_formatted_path = '')
             AND {active_video_claim_clause('v')}"""
    ).fetchone()
    counts["videos_need_format"] = row["cnt"] if row else 0

    # Videos needing ASR
    row = conn.execute(
        f"""SELECT COUNT(*) as cnt FROM videos v
           JOIN video_audio_assets a ON a.video_id = v.video_id
           WHERE v.transcript_downloaded = 0
             AND v.transcript_language = 'no_subtitle'
             AND COALESCE(v.is_member_only, 0) = 0
             AND COALESCE(v.is_short, 0) = 0
             AND COALESCE(a.status, '') = 'downloaded'
             AND COALESCE(a.audio_file_path, '') != ''
             AND {active_video_claim_clause('v')}"""
    ).fetchone()
    counts["videos_need_asr"] = row["cnt"] if row else 0

    conn.close()
    return counts
