#!/usr/bin/env python3
"""Backfill and reconcile member-only state in the videos table.

Rules:
- If metadata indicates member-only, persist `is_member_only = 1`.
- If `is_member_only = 1` but metadata lacks member-only markers, add them.
- Preserve existing metadata fields and only add the missing markers.

This is intentionally aggressive so future audits do not drift between
the database flag and the metadata payload.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from database_optimized import OptimizedDatabase


def _infer_member_only(metadata_raw: str | None) -> bool:
    if not metadata_raw:
        return False
    try:
        metadata = json.loads(metadata_raw)
    except Exception:
        return False
    flags = metadata.get("flags") or {}
    return bool(
        metadata.get("member_only")
        or metadata.get("is_member_only")
        or flags.get("member_only")
        or metadata.get("upload_date_reason") == "member_only"
        or flags.get("upload_date_reason") == "member_only"
    )


def _normalize_metadata(metadata_raw: str | None, force_member_only: bool) -> str | None:
    metadata: dict | None = None
    if metadata_raw:
        try:
            parsed = json.loads(metadata_raw)
            if isinstance(parsed, dict):
                metadata = parsed
        except Exception:
            metadata = None

    if not force_member_only:
        return metadata_raw

    if metadata is None:
        metadata = {}

    flags = metadata.get("flags")
    if not isinstance(flags, dict):
        flags = {}
    flags["member_only"] = True
    metadata["flags"] = flags
    metadata["member_only"] = True
    metadata["is_member_only"] = True
    if not metadata.get("upload_date_reason"):
        metadata["upload_date_reason"] = "member_only"

    return json.dumps(metadata, ensure_ascii=False)


def repair_member_only_state(db_path: Path, dry_run: bool = False) -> dict[str, int]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    db = OptimizedDatabase(str(db_path), "uploads")
    rows = conn.execute(
        "SELECT id, video_id, is_member_only, metadata FROM videos"
    ).fetchall()

    updates: list[tuple[str, int, int, str]] = []
    remaining_missing = 0
    remaining_extra = 0
    for row in rows:
        metadata_raw = row["metadata"] or db.get_metadata_content(str(row["video_id"]))
        inferred = _infer_member_only(metadata_raw)
        stored = bool(row["is_member_only"])
        should_force = inferred or stored
        new_is_member_only = 1 if should_force else 0
        new_metadata = _normalize_metadata(metadata_raw, should_force)
        if int(stored) != new_is_member_only or new_metadata != metadata_raw:
            updates.append((new_metadata or "", new_is_member_only, row["id"], row["video_id"]))
        if inferred and not stored:
            remaining_missing += 1
        if stored and not inferred:
            remaining_extra += 1

    if not dry_run and updates:
        conn.executemany(
            """
            UPDATE videos
            SET metadata = ?,
                is_member_only = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            [(metadata, is_member_only, row_id) for metadata, is_member_only, row_id, _ in updates],
        )
        conn.commit()
        for metadata, _, _, video_id in updates:
            if metadata:
                try:
                    db.save_metadata_content(video_id, metadata)
                except Exception:
                    pass

    conn.close()
    db.close()
    return {
        "rows": len(rows),
        "updated": len(updates),
        "remaining_missing": remaining_missing,
        "remaining_extra": remaining_extra,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill member-only state in SQLite videos table.")
    parser.add_argument("--db-path", default="youtube_transcripts.db", help="Path to SQLite DB")
    parser.add_argument("--dry-run", action="store_true", help="Show counts without writing")
    args = parser.parse_args()

    result = repair_member_only_state(Path(args.db_path), dry_run=args.dry_run)
    mode = "dry-run" if args.dry_run else "applied"
    print(f"[{mode}] rows={result['rows']} updated={result['updated']} remaining_missing={result['remaining_missing']} remaining_extra={result['remaining_extra']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
