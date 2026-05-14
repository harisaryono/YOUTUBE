#!/usr/bin/env python3
"""
Centralized Importer for JSON Updates (Batch Import Pattern).
Finalizes metadata into youtube_transcripts.db and content into youtube_transcripts_blobs.db.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database_optimized import OptimizedDatabase

PENDING_DIR = Path("pending_updates")
DB_PATH = Path("youtube_transcripts.db")
BACKUP_DIR = Path("backups")
UPLOADS_DIR = "uploads"
THUMBNAIL_CACHE_DIR = Path("tmp") / "thumbnail_cache"
THUMBNAIL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
THUMBNAIL_VARIANTS = (
    ("maxresdefault", "maxresdefault.jpg"),
    ("hqdefault", "hqdefault.jpg"),
    ("default", "default.jpg"),
)


def _merge_metadata(existing_json: str | None, new_metadata: dict) -> str | None:
    merged: dict = {}
    if existing_json:
        try:
            existing = json.loads(existing_json)
            if isinstance(existing, dict):
                merged.update(existing)
        except Exception:
            pass
    if isinstance(new_metadata, dict):
        merged.update(new_metadata)
    return json.dumps(merged, ensure_ascii=False) if merged else None


def _upsert_discovery(db: OptimizedDatabase, cursor: sqlite3.Cursor, data: dict) -> None:
    video_id = str(data.get("video_id") or "").strip()
    if not video_id:
        raise ValueError("Missing video_id for discovery update")

    channel_db_id = data.get("channel_db_id")
    if channel_db_id is None:
        channel_db_id = data.get("channel_id")
    try:
        channel_db_id = int(channel_db_id)
    except Exception as exc:
        raise ValueError(f"Invalid channel_db_id for {video_id}: {channel_db_id}") from exc

    channel_exists = cursor.execute(
        "SELECT 1 FROM channels WHERE id = ? LIMIT 1",
        (channel_db_id,),
    ).fetchone()
    if not channel_exists:
        raise ValueError(f"Channel id {channel_db_id} for {video_id} not found in channels table")

    existing_metadata = db.get_metadata_content(video_id)
    merged_metadata = _merge_metadata(existing_metadata, data.get("metadata") or {})

    title = str(data.get("title") or "Unknown")
    duration = int(data.get("duration") or 0)
    upload_date = str(data.get("upload_date") or "")
    view_count = int(data.get("view_count") or 0)
    video_url = str(data.get("video_url") or f"https://www.youtube.com/watch?v={video_id}")
    thumbnail_url = str(data.get("thumbnail_url") or "")
    is_short = int(data.get("is_short") or 0)

    cursor.execute(
        """
        INSERT INTO videos (
            video_id, channel_id, title, description, duration, upload_date,
            view_count, like_count, comment_count, video_url, thumbnail_url,
            transcript_file_path, summary_file_path, transcript_downloaded,
            transcript_language, word_count, line_count, is_short,
            is_member_only, transcript_text, summary_text, metadata,
            transcript_formatted_path
        ) VALUES (?, ?, ?, NULL, ?, ?, ?, 0, 0, ?, ?, '', '', 0, '', 0, 0, ?, 0, '', '', NULL, '')
        ON CONFLICT(video_id) DO UPDATE SET
            channel_id = excluded.channel_id,
            title = CASE
                WHEN COALESCE(excluded.title, '') <> '' AND excluded.title <> 'Unknown'
                    THEN excluded.title
                ELSE videos.title
            END,
            duration = CASE
                WHEN COALESCE(excluded.duration, 0) > 0 THEN excluded.duration
                ELSE videos.duration
            END,
            upload_date = CASE
                WHEN COALESCE(excluded.upload_date, '') <> '' THEN excluded.upload_date
                ELSE videos.upload_date
            END,
            view_count = CASE
                WHEN COALESCE(excluded.view_count, 0) > 0 THEN excluded.view_count
                ELSE videos.view_count
            END,
            video_url = CASE
                WHEN COALESCE(excluded.video_url, '') <> '' THEN excluded.video_url
                ELSE videos.video_url
            END,
            thumbnail_url = CASE
                WHEN COALESCE(excluded.thumbnail_url, '') <> '' THEN excluded.thumbnail_url
                ELSE videos.thumbnail_url
            END,
            is_short = CASE
                WHEN COALESCE(excluded.is_short, 0) = 1 THEN 1
                ELSE videos.is_short
            END,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            video_id,
            channel_db_id,
            title,
            duration,
            upload_date,
            view_count,
            video_url,
            thumbnail_url,
            is_short,
        ),
    )
    if merged_metadata:
        try:
            db.save_metadata_content(video_id, merged_metadata)
        except Exception:
            pass


def _thumbnail_cache_path(video_id: str, variant: str) -> Path:
    return THUMBNAIL_CACHE_DIR / f"{video_id}_{variant}.jpg"


def _store_thumbnail_cache(video_id: str, variant: str, content: bytes) -> None:
    final_path = _thumbnail_cache_path(video_id, variant)
    tmp_path = final_path.with_suffix('.tmp')
    try:
        tmp_path.write_bytes(content)
        tmp_path.replace(final_path)
    except Exception:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass


def _warm_thumbnail_cache(video_id: str) -> None:
    for variant, filename in THUMBNAIL_VARIANTS:
        cache_path = _thumbnail_cache_path(video_id, variant)
        if cache_path.exists() and cache_path.is_file() and cache_path.stat().st_size > 0:
            return

    for variant, filename in THUMBNAIL_VARIANTS:
        url = f"https://i.ytimg.com/vi/{video_id}/{filename}"
        try:
            resp = requests.get(url, timeout=8)
            if resp.status_code == 200 and resp.content:
                _store_thumbnail_cache(video_id, variant, resp.content)
                print(f"  [THUMB] Cached thumbnail for {video_id} ({variant})")
                return
        except Exception:
            continue


def import_updates():
    if not PENDING_DIR.exists():
        print(f"Directory {PENDING_DIR} not found.")
        return 0

    json_files = sorted(list(PENDING_DIR.glob("update_*.json")))
    if not json_files:
        print("No pending updates to process.")
        return 0

    print(f"🚀 Starting Batch Import: found {len(json_files)} files.")

    db = OptimizedDatabase(str(DB_PATH), UPLOADS_DIR)

    counts = {"transcript": 0, "resume": 0, "formatted": 0, "no_subtitle": 0, "blocked": 0, "discovery": 0, "failed": 0}
    processed_files = []

    for json_file in json_files:
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            video_id = data.get("video_id")
            update_type = data.get("type")
            status = data.get("status", "ok")

            # Backward compatibility for old resume format
            if not update_type and data.get("summary_file_path"):
                update_type = "resume"
                data["file_path"] = data.get("summary_file_path")

            if not video_id or not update_type:
                print(f"  [SKIP] {json_file.name}: Missing essential fields (video_id/type)")
                counts["failed"] += 1
                continue

            # Discovery updates only touch the main DB metadata layer.
            if update_type == "discovery":
                if status != "ok":
                    print(f"  [SKIP] {json_file.name}: discovery status={status}")
                    counts["failed"] += 1
                    continue
                with db._get_cursor() as cursor:
                    _upsert_discovery(db, cursor, data)
                try:
                    _warm_thumbnail_cache(str(video_id))
                except Exception as thumb_err:
                    print(f"  [WARN] Failed to warm thumbnail cache for {video_id}: {thumb_err}")
                try:
                    db._bump_stats_cache_version()
                except Exception:
                    pass
                counts["discovery"] += 1
                processed_files.append(json_file)
                print(f"  [OK] Processed discovery for {video_id}")
                continue

            # 1. Handle Content Sync to Blob Storage
            content = data.get("content")
            file_path = data.get("file_path")

            # Fallback: read from disk if content is missing but file_path exists
            if not content and file_path and status == "ok":
                try:
                    p = Path(file_path)
                    if p.exists():
                        content = p.read_text(encoding="utf-8")
                except Exception:
                    pass

            if status == "ok" and content:
                try:
                    if update_type == "transcript":
                        db.save_transcript_content(video_id, content)
                    elif update_type == "resume":
                        db.save_summary_content(video_id, content)
                    elif update_type == "formatted":
                        db.save_formatted_content(video_id, content)
                    print(f"  [BLOB] Synced {update_type} for {video_id}")

                    # Cleanup physical file if sync succeeded
                    if file_path:
                        try:
                            fp = Path(file_path)
                            if fp.exists() and fp.is_file():
                                fp.unlink()
                                print(f"  [CLEAN] Deleted physical {update_type} file: {fp.name}")
                        except Exception as e_clean:
                            print(f"  [WARN] Failed to delete physical file {file_path}: {e_clean}")

                except Exception as e_blob:
                    print(f"  [WARN] Failed to sync blob for {video_id}: {e_blob}")

            # 2. Update Main Database Metadata
            with db._get_cursor() as cursor:
                if update_type == "transcript":
                    if status in {"no_subtitle", "blocked"}:
                        retry_reason = str(data.get("note") or data.get("reason") or "").strip()
                        if status == "blocked":
                            retry_reason = retry_reason or "blocked_member_only"
                        cursor.execute(
                            """
                            UPDATE videos
                            SET transcript_language = 'no_subtitle',
                                transcript_downloaded = 0,
                                transcript_file_path = '',
                                summary_file_path = '',
                                transcript_retry_reason = ?,
                                transcript_retry_after = NULL,
                                word_count = 0,
                                line_count = 0,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE video_id = ?
                            """,
                            (retry_reason, video_id),
                        )
                        if status == "blocked":
                            counts["blocked"] += 1
                        else:
                            counts["no_subtitle"] += 1
                    else:
                        metadata = data.get("metadata", {})
                        if isinstance(metadata, dict) and metadata:
                            try:
                                db.save_metadata_content(video_id, json.dumps(metadata, ensure_ascii=False))
                            except Exception:
                                pass
                        cursor.execute(
                            """
                            UPDATE videos
                            SET transcript_file_path = ?,
                                transcript_downloaded = 1,
                                transcript_language = ?,
                                word_count = ?,
                                line_count = ?,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE video_id = ?
                            """,
                            (
                                data.get("file_path"),
                                metadata.get("language"),
                                metadata.get("word_count", 0),
                                metadata.get("line_count", 0),
                                video_id,
                            ),
                        )
                        counts["transcript"] += 1
                    try:
                        db._bump_stats_cache_version()
                    except Exception:
                        pass

                elif update_type == "resume":
                    cursor.execute(
                        """
                        UPDATE videos
                        SET summary_file_path = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE video_id = ?
                        """,
                        (data.get("file_path"), video_id),
                    )
                    counts["resume"] += 1
                    try:
                        db._bump_stats_cache_version()
                    except Exception:
                        pass

                elif update_type == "formatted":
                    file_path = data.get("file_path")
                    cursor.execute(
                        """
                        UPDATE videos
                        SET transcript_formatted_path = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE video_id = ?
                        """,
                        (file_path, video_id),
                    )
                    counts["formatted"] += 1
                    try:
                        db._bump_stats_cache_version()
                    except Exception:
                        pass

            processed_files.append(json_file)
            print(f"  [OK] Processed {update_type} for {video_id}")

        except Exception as e:
            print(f"  [ERROR] Failed to process {json_file.name}: {e}")
            counts["failed"] += 1

    # Cleanup: Delete processed JSON files
    if processed_files:
        for f in processed_files:
            try:
                f.unlink()
                print(f"  [CLEAN] Deleted update file: {f.name}")
            except Exception as e_del:
                print(f"  [WARN] Failed to delete {f.name}: {e_del}")

    print("\n" + "=" * 50)
    print("IMPORT SUMMARY")
    print(f"  - Transcripts: {counts['transcript']}")
    print(f"  - Resumes:     {counts['resume']}")
    print(f"  - Formatted:   {counts['formatted']}")
    print(f"  - No Subtitle: {counts['no_subtitle']}")
    print(f"  - Blocked:     {counts['blocked']}")
    print(f"  - Discovery:   {counts['discovery']}")
    print(f"  - Failed:      {counts['failed']}")
    print("=" * 50)

    return len(processed_files)


if __name__ == "__main__":
    import_updates()
