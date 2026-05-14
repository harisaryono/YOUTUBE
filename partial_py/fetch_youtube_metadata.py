#!/usr/bin/env python3
"""
YouTube API Metadata Fetcher

Fetch video metadata from YouTube Data API v3:
- Duration
- View count
- Like count
- Comment count
- Description
- Thumbnail URL

This version calls the API directly over HTTPS, so it does not require
`google-api-python-client`.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
from pathlib import Path
from typing import Any

from database_optimized import OptimizedDatabase
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(*args, **kwargs):
        return False

from local_services import youtube_api_key_pool

load_dotenv()


class QuotaExhaustedError(Exception):
    pass


class YouTubeApiRequestError(Exception):
    def __init__(self, status: int, message: str, payload: dict[str, Any] | None = None):
        super().__init__(message)
        self.status = status
        self.payload = payload or {}


class YouTubeMetadataFetcher:
    """Fetcher untuk metadata video YouTube via API."""

    API_BASE_URL = "https://www.googleapis.com/youtube/v3"

    def __init__(self, db_path: str, api_key: str, api_key_name: str = ""):
        self.db_path = Path(db_path)
        self.api_key = api_key
        self.api_key_name = api_key_name or "ytapi"
        self.conn: sqlite3.Connection | None = None

        self.stats = {
            "videos_processed": 0,
            "videos_updated": 0,
            "videos_not_found": 0,
            "api_errors": 0,
            "quota_used": 0,
        }

    def connect(self):
        """Initialize database connection dan validasi API key."""
        if not str(self.api_key or "").strip():
            raise ValueError("YouTube API key is empty")

        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")

        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        print(f"✅ Using YouTube API key: {self.api_key_name}")
        print(f"✅ Connected to database: {self.db_path}")

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
        print("✅ Database connection closed")

    def _bump_stats_version(self):
        try:
            db = OptimizedDatabase(str(self.db_path), "uploads")
            db._bump_stats_cache_version()
            db.close()
        except Exception:
            pass

    @staticmethod
    def parse_duration(duration_iso: str) -> int:
        """Parse ISO 8601 duration to seconds."""
        if not duration_iso:
            return 0

        match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration_iso)
        if not match:
            return 0

        hours = int(match.group(1) or 0)
        minutes = int(match.group(2) or 0)
        seconds = int(match.group(3) or 0)
        return hours * 3600 + minutes * 60 + seconds

    def _api_get(self, resource: str, **params: object) -> dict[str, Any]:
        query = {key: value for key, value in params.items() if value not in (None, "", [])}
        query["key"] = self.api_key
        url = f"{self.API_BASE_URL}/{resource}?{urllib_parse.urlencode(query, doseq=True)}"
        req = urllib_request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "yt-channel-metadata-fetcher/1.0",
            },
        )

        try:
            with urllib_request.urlopen(req, timeout=30) as resp:
                body = resp.read().decode("utf-8")
        except urllib_error.HTTPError as exc:
            raw_body = exc.read().decode("utf-8", errors="replace")
            payload = self._safe_json(raw_body)
            message = self._extract_api_error_message(payload) or raw_body or str(exc)
            if exc.code == 403 and self._is_quota_error(payload):
                raise QuotaExhaustedError(f"quota exceeded on {self.api_key_name}: {message}") from exc
            raise YouTubeApiRequestError(exc.code, message, payload) from exc
        except urllib_error.URLError as exc:
            raise RuntimeError(f"Network error while calling YouTube API: {exc}") from exc

        payload = self._safe_json(body)
        if "error" in payload:
            message = self._extract_api_error_message(payload) or "Unknown YouTube API error"
            if self._is_quota_error(payload):
                raise QuotaExhaustedError(f"quota exceeded on {self.api_key_name}: {message}")
            raise YouTubeApiRequestError(400, message, payload)
        return payload

    @staticmethod
    def _safe_json(raw: str) -> dict[str, Any]:
        try:
            data = json.loads(raw or "{}")
            if isinstance(data, dict):
                return data
        except Exception:
            pass
        return {}

    @staticmethod
    def _extract_api_error_message(payload: dict[str, Any]) -> str:
        error_obj = payload.get("error")
        if not isinstance(error_obj, dict):
            return ""
        details = error_obj.get("errors")
        if isinstance(details, list) and details:
            first = details[0]
            if isinstance(first, dict) and first.get("message"):
                return str(first["message"])
        message = error_obj.get("message")
        return str(message or "")

    @staticmethod
    def _is_quota_error(payload: dict[str, Any]) -> bool:
        error_obj = payload.get("error")
        if not isinstance(error_obj, dict):
            return False
        reasons: set[str] = set()
        details = error_obj.get("errors")
        if isinstance(details, list):
            for entry in details:
                if isinstance(entry, dict) and entry.get("reason"):
                    reasons.add(str(entry["reason"]))
        return bool(reasons & {"quotaExceeded", "dailyLimitExceeded", "rateLimitExceeded"})

    def _load_target_videos(
        self,
        *,
        limit: int | None = None,
        channel_id: str = "",
        channel_name: str = "",
    ) -> list[sqlite3.Row]:
        if not self.conn:
            raise RuntimeError("database connection is not initialized")

        params: list[object] = []
        query = """
            SELECT v.video_id, v.duration, v.view_count
            FROM videos v
            LEFT JOIN channels c ON c.id = v.channel_id
            WHERE ((v.duration = 0 OR v.duration IS NULL)
               OR (v.view_count = 0 OR v.view_count IS NULL))
        """
        if channel_id:
            query += " AND (c.channel_id = ? OR c.channel_id = ?)"
            params.extend([channel_id, channel_id.lstrip("@")])
        if channel_name:
            query += " AND c.channel_name = ?"
            params.append(channel_name)
        query += " ORDER BY v.created_at DESC"
        if limit:
            query += " LIMIT ?"
            params.append(limit)
        return self.conn.execute(query, params).fetchall()

    def fetch_videos_batch(self, video_ids: list[str]) -> list[dict[str, Any]]:
        """Fetch metadata for batch of videos (max 50 per request)."""
        if not video_ids:
            return []

        batch = video_ids[:50]
        payload = self._api_get(
            "videos",
            part="snippet,contentDetails,statistics",
            id=",".join(batch),
            maxResults=len(batch),
        )
        self.stats["quota_used"] += len(batch)
        items = payload.get("items", [])
        return items if isinstance(items, list) else []

    def fetch_metadata(
        self,
        *,
        batch_size: int = 50,
        limit: int | None = None,
        channel_id: str = "",
        channel_name: str = "",
    ):
        """Fetch metadata for videos without duration/view_count."""
        print("\n" + "=" * 60)
        print("📺 FETCH YOUTUBE METADATA")
        print("=" * 60)

        videos = self._load_target_videos(limit=limit, channel_id=channel_id, channel_name=channel_name)

        if channel_id:
            print(f"🎯 Channel filter: {channel_id}")
        elif channel_name:
            print(f"🎯 Channel name filter: {channel_name}")

        print(f"📊 Found {len(videos)} videos needing metadata")
        print(f"📉 Estimated quota usage: {len(videos)} units")
        print()

        batch: list[str] = []
        processed = 0

        for video in videos:
            batch.append(video["video_id"])

            if len(batch) >= batch_size or processed == len(videos) - 1:
                print(f"⏳ Processing batch {processed // batch_size + 1}... ({len(batch)} videos)")
                items = self.fetch_videos_batch(batch)
                for item in items:
                    self._update_video_metadata(item)
                if self.conn:
                    self.conn.commit()
                batch = []

            processed += 1

            if processed and processed % 100 == 0:
                print(f"   📊 Processed: {processed}/{len(videos)} videos")
                print(f"   📉 Quota used: {self.stats['quota_used']} units")
                print(f"   ✅ Updated: {self.stats['videos_updated']} videos")

        print("\n📈 Fetch Summary:")
        print(f"   Videos processed: {self.stats['videos_processed']}")
        print(f"   Videos updated: {self.stats['videos_updated']}")
        print(f"   Videos not found: {self.stats['videos_not_found']}")
        print(f"   API errors: {self.stats['api_errors']}")
        print(f"   Quota used: {self.stats['quota_used']} units")

    def _update_video_metadata(self, item: dict[str, Any]):
        """Update single video metadata."""
        if not self.conn:
            raise RuntimeError("database connection is not initialized")

        video_id = item["id"]
        snippet = item.get("snippet", {})
        content_details = item.get("contentDetails", {})
        statistics = item.get("statistics", {})

        duration_seconds = self.parse_duration(str(content_details.get("duration", "") or ""))
        view_count = int(statistics.get("viewCount", 0) or 0)
        like_count = int(statistics.get("likeCount", 0) or 0)
        comment_count = int(statistics.get("commentCount", 0) or 0)
        description = str(snippet.get("description", "") or "")[:10000]
        thumbnail_url = self._get_thumbnail_url(item)

        cursor = self.conn.cursor()
        cursor.execute(
            """
            UPDATE videos
            SET duration = ?,
                view_count = ?,
                like_count = ?,
                comment_count = ?,
                description = COALESCE(NULLIF(?, ''), description),
                thumbnail_url = COALESCE(NULLIF(?, ''), thumbnail_url),
                updated_at = CURRENT_TIMESTAMP
            WHERE video_id = ?
            """,
            (
                duration_seconds,
                view_count,
                like_count,
                comment_count,
                description,
                thumbnail_url,
                video_id,
            ),
        )

        if cursor.rowcount > 0:
            self.stats["videos_updated"] += 1
        else:
            self.stats["videos_not_found"] += 1

        self.stats["videos_processed"] += 1

    def _get_thumbnail_url(self, item: dict[str, Any]) -> str:
        """Get best quality thumbnail URL."""
        thumbnails = item.get("snippet", {}).get("thumbnails", {})
        for quality in ["maxres", "standard", "high", "medium", "default"]:
            if quality in thumbnails:
                return str(thumbnails[quality].get("url", "") or "")
        return f"https://img.youtube.com/vi/{item['id']}/hqdefault.jpg"

    def print_report(self):
        """Print final report."""
        print("\n" + "=" * 60)
        print(f"📊 YOUTUBE API FETCH REPORT ({self.api_key_name})")
        print("=" * 60)

        stats = self.stats
        print(
            f"""
Videos:
  Processed:    {stats['videos_processed']:,}
  Updated:      {stats['videos_updated']:,}
  Not found:    {stats['videos_not_found']:,}

API:
  Errors:       {stats['api_errors']}
  Quota used:   {stats['quota_used']:,} units
  Quota limit:  10,000 units/day (free tier)
"""
        )

        if stats["quota_used"] >= 9000:
            print("⚠️  WARNING: Approaching daily quota limit!")
        try:
            db = OptimizedDatabase(str(self.db_path), "uploads")
            db._bump_stats_cache_version()
            db.close()
        except Exception:
            pass


def _build_api_key_pool(cli_key: str) -> list[dict[str, str]]:
    pool: list[dict[str, str]] = []
    seen: set[str] = set()

    def add_key(name: str, key: str):
        normalized = str(key or "").strip()
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        pool.append({"name": name, "key": normalized})

    add_key("ytapi_cli", cli_key)
    for index, key in enumerate(youtube_api_key_pool(), 1):
        add_key(f"ytapi_env_{index}", key)
    return pool


def main():
    parser = argparse.ArgumentParser(description="Fetch YouTube video metadata via API")
    parser.add_argument("--api-key", default=os.getenv("YOUTUBE_API_KEY", ""), help="YouTube Data API v3 key")
    parser.add_argument("--db", default="youtube_transcripts.db", help="Path to database")
    parser.add_argument("--batch-size", type=int, default=50, help="Videos per API request (max 50)")
    parser.add_argument("--limit", type=int, default=None, help="Maximum videos to process")
    parser.add_argument("--channel-id", default="", help="Batasi ke satu channel_id tertentu")
    parser.add_argument("--channel-name", default="", help="Batasi ke satu channel_name tertentu")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without updating database")
    args = parser.parse_args()

    if args.channel_id and args.channel_name:
        parser.error("Gunakan hanya salah satu dari --channel-id atau --channel-name")
    if args.batch_size < 1 or args.batch_size > 50:
        parser.error("--batch-size must be between 1 and 50")

    pool = _build_api_key_pool(args.api_key)
    if not pool:
        print("❌ No YouTube API key configured")
        return 1

    print("=" * 60)
    print("📺 YouTube Metadata Fetcher")
    print("=" * 60)
    print(f"💾 Database: {args.db}")
    print(f"🔑 API keys available: {', '.join(item['name'] for item in pool)}")
    print(f"📦 Batch size: {args.batch_size}")
    if args.limit:
        print(f"🔢 Limit: {args.limit} videos")
    if args.channel_id:
        print(f"🎯 Channel ID: {args.channel_id}")
    elif args.channel_name:
        print(f"🎯 Channel Name: {args.channel_name}")
    print()

    if args.dry_run:
        print("⚠️  DRY RUN MODE - No updates will be made")
        print()

    last_error = 1
    for index, item in enumerate(pool, 1):
        print("-" * 60)
        print(f"🔑 Trying API key {index}/{len(pool)}: {item['name']}")
        print("-" * 60)
        fetcher = None
        try:
            fetcher = YouTubeMetadataFetcher(args.db, item["key"], item["name"])
            fetcher.connect()

            if args.dry_run:
                rows = fetcher._load_target_videos(
                    limit=args.limit,
                    channel_id=str(args.channel_id or "").strip(),
                    channel_name=str(args.channel_name or "").strip(),
                )
                print(f"📊 Would process {len(rows)} videos")
                print(f"📉 Estimated quota: {len(rows)} units")
            else:
                fetcher.fetch_metadata(
                    batch_size=args.batch_size,
                    limit=args.limit,
                    channel_id=str(args.channel_id or "").strip(),
                    channel_name=str(args.channel_name or "").strip(),
                )
                fetcher.print_report()

            print("\n" + "=" * 60)
            print(f"✅ Metadata fetch completed with {item['name']}!")
            print("=" * 60)
            return 0
        except QuotaExhaustedError as exc:
            print(f"⚠️  {exc}")
            last_error = 2
        except Exception as exc:
            print("\n" + "=" * 60)
            print(f"❌ Fetch failed on {item['name']}: {exc}")
            print("=" * 60)
            import traceback

            traceback.print_exc()
            last_error = 1
        finally:
            if fetcher:
                try:
                    fetcher.close()
                except Exception:
                    pass

    return last_error


if __name__ == "__main__":
    raise SystemExit(main())
