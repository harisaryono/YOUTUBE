#!/usr/bin/env python3
"""
Repair channel sources that were ingested from a handle root page instead of
the uploads listing.

The script scans for channels with suspicious pseudo rows such as:
- "<channel> - Videos"
- "<channel> - Shorts"
- rows whose video_id matches the channel slug/handle

When --apply is provided, the script:
1. Backs up the current channel + videos state.
2. Re-fetches the channel from the "/videos" source.
3. Upserts the fresh channel/video rows.
4. Deletes stale pseudo rows that are no longer present in the fresh source.
5. Normalizes the channel URL to the "/videos" source.

The repair is idempotent and safe to re-run.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

from partial_py.youtube_transcript_complete import YouTubeTranscriptComplete


REPO_DIR = Path(__file__).resolve().parent
DB_PATH = REPO_DIR / "youtube_transcripts.db"
RUNS_DIR = REPO_DIR / "runs"


@dataclass
class SuspiciousChannel:
    channel_db_id: int
    channel_id: str
    channel_name: str
    channel_url: str
    video_count: int
    db_videos: int
    pseudo_titles: int
    id_match: int
    sample_title: str | None


def normalize_videos_url(channel_url: str) -> str:
    base = str(channel_url or "").strip().rstrip("/")
    if not base:
        return base
    if base.endswith("/videos"):
        return base
    return f"{base}/videos"


def backup_channel_state(
    con: sqlite3.Connection,
    channel_db_id: int,
    backup_path: Path,
    fetched_source_url: str,
    fetched_video_count: int,
    fetched_video_ids: list[str],
) -> dict:
    con.row_factory = sqlite3.Row
    channel_row = con.execute("SELECT * FROM channels WHERE id = ?", (channel_db_id,)).fetchone()
    if channel_row is None:
        raise SystemExit(f"channel id {channel_db_id} not found")

    video_rows = con.execute(
        "SELECT * FROM videos WHERE channel_id = ? ORDER BY id ASC",
        (channel_db_id,),
    ).fetchall()

    payload = {
        "channel_before": dict(channel_row),
        "videos_before": [dict(row) for row in video_rows],
        "fetched_source_url": fetched_source_url,
        "fetched_video_count": fetched_video_count,
        "fetched_video_ids": fetched_video_ids,
        "backup_created_at": datetime.now().isoformat(timespec="seconds"),
    }
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    backup_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload


def fetch_channel_payload(helper: YouTubeTranscriptComplete, source_url: str) -> dict:
    info = helper.get_channel_info(source_url)
    videos = info.get("videos") or []
    cleaned_videos = []
    for video in videos:
        video_id = str(video.get("video_id") or "").strip()
        if not video_id:
            continue
        cleaned_videos.append(
            {
                "video_id": video_id,
                "title": str(video.get("title") or "Unknown").strip() or "Unknown",
                "url": str(video.get("url") or f"https://www.youtube.com/watch?v={video_id}").strip(),
                "duration": int(video.get("duration") or 0),
                "upload_date": str(video.get("upload_date") or "").strip(),
                "view_count": int(video.get("view_count") or 0),
                "thumbnail": str(video.get("thumbnail") or f"https://i.ytimg.com/vi/{video_id}/maxresdefault.jpg").strip(),
            }
        )

    channel_id = str(info.get("channel_id") or "").strip()
    channel_name = str(info.get("channel_name") or "").strip()
    return {
        "channel_id": channel_id,
        "channel_name": channel_name,
        "channel_url": source_url,
        "subscriber_count": int(info.get("subscriber_count") or 0),
        "video_count": int(info.get("video_count") or len(cleaned_videos)),
        "videos": cleaned_videos,
    }


def detect_suspicious_channels(con: sqlite3.Connection) -> list[SuspiciousChannel]:
    con.row_factory = sqlite3.Row
    rows = con.execute(
        """
        SELECT
            c.id AS channel_db_id,
            c.channel_id,
            c.channel_name,
            c.channel_url,
            c.video_count,
            COUNT(v.id) AS db_videos,
            SUM(CASE WHEN v.title LIKE '% - Videos' OR v.title LIKE '% - Shorts' THEN 1 ELSE 0 END) AS pseudo_titles,
            SUM(CASE WHEN v.video_id = REPLACE(c.channel_id, '@', '') THEN 1 ELSE 0 END) AS id_match,
            MIN(v.title) AS sample_title
        FROM channels c
        LEFT JOIN videos v ON v.channel_id = c.id
        GROUP BY c.id
        HAVING pseudo_titles > 0 OR id_match > 0
        ORDER BY db_videos ASC, pseudo_titles DESC, id_match DESC, c.id ASC
        """
    ).fetchall()

    suspicious: list[SuspiciousChannel] = []
    for row in rows:
        suspicious.append(
            SuspiciousChannel(
                channel_db_id=int(row["channel_db_id"]),
                channel_id=str(row["channel_id"] or ""),
                channel_name=str(row["channel_name"] or ""),
                channel_url=str(row["channel_url"] or ""),
                video_count=int(row["video_count"] or 0),
                db_videos=int(row["db_videos"] or 0),
                pseudo_titles=int(row["pseudo_titles"] or 0),
                id_match=int(row["id_match"] or 0),
                sample_title=row["sample_title"],
            )
        )
    return suspicious


def repair_channel(
    helper: YouTubeTranscriptComplete,
    db_path: Path,
    channel: SuspiciousChannel,
    run_dir: Path,
) -> dict:
    source_url = normalize_videos_url(channel.channel_url)
    fetched = fetch_channel_payload(helper, source_url)
    fetched_ids = [row["video_id"] for row in fetched["videos"]]

    if not fetched_ids:
        raise RuntimeError(f"No videos extracted from {source_url}")

    backup_path = run_dir / "backups" / f"{channel.channel_db_id:05d}_{channel.channel_id.lstrip('@').replace('/', '_')}.json"
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout = 60000")
    backup_channel_state(
        con,
        channel.channel_db_id,
        backup_path,
        source_url,
        fetched["video_count"],
        fetched_ids,
    )
    con.close()

    helper.db.conn.execute("PRAGMA busy_timeout = 60000")
    helper.save_channel_to_database(fetched)

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout = 60000")
    placeholders = ",".join("?" for _ in fetched_ids)
    params = [channel.channel_db_id, *fetched_ids]

    # Remove stale rows that are not present in the refreshed /videos source.
    con.execute(
        f"""
        DELETE FROM transcripts
        WHERE video_id IN (
            SELECT id FROM videos WHERE channel_id = ? AND video_id NOT IN ({placeholders})
        )
        """,
        params,
    )
    con.execute(
        f"""
        DELETE FROM summaries
        WHERE video_id IN (
            SELECT id FROM videos WHERE channel_id = ? AND video_id NOT IN ({placeholders})
        )
        """,
        params,
    )
    con.execute(
        f"""
        DELETE FROM download_queue
        WHERE video_id IN (
            SELECT id FROM videos WHERE channel_id = ? AND video_id NOT IN ({placeholders})
        )
        """,
        params,
    )
    con.execute(
        f"DELETE FROM videos WHERE channel_id = ? AND video_id NOT IN ({placeholders})",
        params,
    )

    # Refresh channel metadata and normalize the source URL.
    con.execute(
        """
        UPDATE channels
        SET channel_url = ?,
            channel_name = ?,
            subscriber_count = ?,
            video_count = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (
            source_url,
            fetched["channel_name"] or channel.channel_name,
            fetched["subscriber_count"],
            fetched["video_count"],
            channel.channel_db_id,
        ),
    )

    # Update fresh rows so title/duration/url/thumbnail reflect the latest fetch.
    for video in fetched["videos"]:
        con.execute(
            """
            UPDATE videos
            SET title = ?,
                duration = ?,
                upload_date = ?,
                view_count = ?,
                video_url = ?,
                thumbnail_url = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE channel_id = ? AND video_id = ?
            """,
            (
                video["title"],
                video["duration"],
                video["upload_date"],
                video["view_count"],
                video["url"],
                video["thumbnail"],
                channel.channel_db_id,
                video["video_id"],
            ),
        )

    con.commit()

    channel_after = con.execute(
        "SELECT id, channel_id, channel_name, channel_url, video_count FROM channels WHERE id = ?",
        (channel.channel_db_id,),
    ).fetchone()
    videos_after = con.execute(
        "SELECT video_id, title, video_url, duration, upload_date FROM videos WHERE channel_id = ? ORDER BY id ASC",
        (channel.channel_db_id,),
    ).fetchall()

    return {
        "channel_db_id": channel.channel_db_id,
        "channel_id": channel.channel_id,
        "channel_name": channel.channel_name,
        "status": "repaired",
        "backup_path": str(backup_path),
        "source_url": source_url,
        "channel_after": dict(channel_after) if channel_after else None,
        "video_count_after": len(videos_after),
        "sample_video_ids": [row["video_id"] for row in videos_after[:5]],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(DB_PATH))
    parser.add_argument("--channel-id", default="", help="Target satu channel_id tertentu.")
    parser.add_argument("--apply", action="store_true", help="Benar-benar lakukan repair.")
    parser.add_argument("--limit", type=int, default=0, help="Batasi jumlah channel suspicious yang diproses.")
    parser.add_argument("--run-dir", default="", help="Direktori output report.")
    args = parser.parse_args()

    run_dir = Path(args.run_dir) if args.run_dir else RUNS_DIR / f"repair_channel_sources_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    run_dir.mkdir(parents=True, exist_ok=True)
    report_path = run_dir / "report.json"

    helper = YouTubeTranscriptComplete(db_path=str(args.db))
    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout = 60000")
    try:
        suspicious = detect_suspicious_channels(con)
        if args.channel_id:
            channel_id_filter = args.channel_id.strip()
            suspicious = [
                row for row in suspicious
                if row.channel_id == channel_id_filter or row.channel_id.lstrip("@") == channel_id_filter.lstrip("@")
            ]
        if args.limit > 0:
            suspicious = suspicious[: args.limit]

        report = {
            "mode": "apply" if args.apply else "scan",
            "count": len(suspicious),
            "items": [],
        }

        for channel in suspicious:
            item = {
                "channel_db_id": channel.channel_db_id,
                "channel_id": channel.channel_id,
                "channel_name": channel.channel_name,
                "channel_url": channel.channel_url,
                "video_count": channel.video_count,
                "db_videos": channel.db_videos,
                "pseudo_titles": channel.pseudo_titles,
                "id_match": channel.id_match,
                "sample_title": channel.sample_title,
            }
            if not args.apply:
                item["status"] = "suspicious"
                report["items"].append(item)
                continue

            try:
                result = repair_channel(helper, Path(args.db), channel, run_dir)
                item.update(result)
            except Exception as exc:
                item["status"] = "error"
                item["error"] = str(exc)
            report["items"].append(item)

        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        print(json.dumps(report, indent=2, ensure_ascii=False))
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
