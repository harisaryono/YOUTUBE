"""
Video processing claim helpers.

Used by stage workers to claim rows in `videos` before they are handed to
parallel workers. Claims are time-limited so a crashed job can expire and be
reclaimed later.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


DEFAULT_CLAIM_TTL_SECONDS = 4 * 60 * 60


def ensure_video_processing_columns(conn: sqlite3.Connection) -> None:
    rows = conn.execute("PRAGMA table_info(videos)").fetchall()
    existing = {str(row["name"]) for row in rows}
    if "processing_stage" not in existing:
        conn.execute("ALTER TABLE videos ADD COLUMN processing_stage TEXT NOT NULL DEFAULT ''")
    if "processing_owner" not in existing:
        conn.execute("ALTER TABLE videos ADD COLUMN processing_owner TEXT NOT NULL DEFAULT ''")
    if "processing_until" not in existing:
        conn.execute("ALTER TABLE videos ADD COLUMN processing_until TEXT")


def _videos_has_column(conn: sqlite3.Connection, column_name: str) -> bool:
    rows = conn.execute("PRAGMA table_info(videos)").fetchall()
    existing = {str(row["name"]) for row in rows}
    return str(column_name or "").strip() in existing


def active_video_claim_clause(alias: str = "v") -> str:
    alias = str(alias or "v").strip() or "v"
    return (
        f"(COALESCE({alias}.processing_owner, '') = '' "
        f"OR COALESCE({alias}.processing_until, '') = '' "
        f"OR {alias}.processing_until <= datetime('now'))"
    )


def claim_rows_by_query(
    conn: sqlite3.Connection,
    *,
    select_sql: str,
    params: list[Any] | tuple[Any, ...] | None = None,
    owner: str,
    stage: str,
    ttl_seconds: int = DEFAULT_CLAIM_TTL_SECONDS,
) -> list[dict[str, Any]]:
    """
    Atomically select and claim a batch of videos for a processing stage.

    The SELECT must include an `id` column from `videos`.
    """
    ensure_video_processing_columns(conn)
    owner = str(owner or "").strip()
    stage = str(stage or "").strip()
    if not owner:
        raise ValueError("owner is required for claim_rows_by_query()")
    ttl = max(60, int(ttl_seconds or 0))
    params_list = list(params or [])
    cursor = conn.cursor()
    has_updated_at = _videos_has_column(conn, "updated_at")
    try:
        conn.commit()
    except Exception:
        pass
    cursor.execute("BEGIN IMMEDIATE")
    try:
        rows = cursor.execute(select_sql, params_list).fetchall()
        dict_rows = [dict(row) for row in rows]
        ids = [int(row["id"]) for row in dict_rows if row.get("id") is not None]
        if ids:
            placeholders = ",".join("?" for _ in ids)
            update_sql = """
                UPDATE videos
                SET processing_stage = ?,
                    processing_owner = ?,
                    processing_until = datetime('now', '+' || ? || ' seconds')
            """
            if has_updated_at:
                update_sql += ", updated_at = CURRENT_TIMESTAMP"
            update_sql += f"""
                WHERE id IN ({placeholders})
            """
            cursor.execute(
                update_sql,
                [stage, owner, ttl, *ids],
            )
        conn.commit()
        return dict_rows
    except Exception:
        conn.rollback()
        raise


def release_claims(
    conn: sqlite3.Connection,
    *,
    owner: str,
    stage: str | None = None,
    video_ids: list[str] | None = None,
) -> int:
    """
    Release claimed videos owned by `owner`.
    Returns number of rows cleared.
    """
    ensure_video_processing_columns(conn)
    owner = str(owner or "").strip()
    if not owner:
        return 0
    stage = str(stage or "").strip()
    ids = [str(video_id).strip() for video_id in (video_ids or []) if str(video_id).strip()]
    where = ["processing_owner = ?"]
    params: list[Any] = [owner]
    if stage:
        where.append("processing_stage = ?")
        params.append(stage)
    if ids:
        placeholders = ",".join("?" for _ in ids)
        where.append(f"video_id IN ({placeholders})")
        params.extend(ids)
    has_updated_at = _videos_has_column(conn, "updated_at")
    update_sql = """
        UPDATE videos
        SET processing_stage = '',
            processing_owner = '',
            processing_until = NULL
    """
    if has_updated_at:
        update_sql += ", updated_at = CURRENT_TIMESTAMP"
    update_sql += f"""
        WHERE {' AND '.join(where)}
    """
    cursor = conn.execute(
        update_sql,
        params,
    )
    conn.commit()
    return int(cursor.rowcount or 0)
