#!/usr/bin/env python3
"""
Search YouTube videos by keyword using yt-dlp search.

This is a lightweight discovery helper for finding candidate videos before
transcript or resume processing.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import List, Optional

import yt_dlp


@dataclass
class SearchResult:
    video_id: str
    title: str
    url: str
    open_url: str
    channel: str
    channel_id: str = ""
    channel_title: str = ""
    channel_url: str = ""
    published_at: str = ""
    duration: Optional[int] = None
    duration_text: str = "n/a"
    description: str = ""
    thumbnail: str = ""
    view_count: int = 0
    caption_available: bool = False
    search_rank: int = 0


def format_duration(seconds: Optional[int]) -> str:
    if seconds is None:
        return "n/a"
    total = int(seconds)
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def _clean_text(value: object) -> str:
    return " ".join(str(value or "").split())


def _clean_int(value: object) -> int:
    try:
        if value is None:
            return 0
        text = str(value).strip().replace(",", "")
        if not text:
            return 0
        return int(float(text))
    except Exception:
        return 0


def _parse_published_at(entry: dict) -> str:
    raw = _clean_text(entry.get("upload_date") or entry.get("timestamp") or entry.get("release_timestamp"))
    if not raw:
        return ""
    if raw.isdigit() and len(raw) == 8:
        try:
            parsed = datetime.strptime(raw, "%Y%m%d").replace(tzinfo=timezone.utc)
            return parsed.isoformat()
        except Exception:
            return ""
    return raw


def search_youtube(
    query: str,
    limit: int = 10,
    *,
    proxy: Optional[str] = None,
    cookies: Optional[str] = None,
    user_agent: Optional[str] = None,
    timeout: float | None = None,
) -> List[SearchResult]:
    query = query.strip()
    if not query:
        return []

    socket_timeout = float(timeout or 20.0)
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "skip_download": True,
        "extract_flat": True,
        "socket_timeout": socket_timeout,
        "default_search": f"ytsearch{max(1, int(limit))}",
    }
    if proxy:
        ydl_opts["proxy"] = proxy
    if cookies:
        ydl_opts["cookiefile"] = cookies
    if user_agent:
        ydl_opts["http_headers"] = {"User-Agent": user_agent}

    ydl = yt_dlp.YoutubeDL(ydl_opts)
    info = ydl.extract_info(f"ytsearch{max(1, int(limit))}:{query}", download=False)

    results: List[SearchResult] = []
    for rank, entry in enumerate(info.get("entries") or [], start=1):
        if not entry:
            continue
        video_id = _clean_text(entry.get("id"))
        url = _clean_text(entry.get("webpage_url") or entry.get("url"))
        if not url and video_id:
            url = f"https://www.youtube.com/watch?v={video_id}"
        channel = _clean_text(entry.get("channel") or entry.get("uploader") or entry.get("uploader_id") or entry.get("channel_id"))
        channel_id = _clean_text(entry.get("channel_id") or entry.get("uploader_id"))
        channel_url = _clean_text(entry.get("channel_url") or entry.get("uploader_url"))
        duration_seconds = entry.get("duration") or 0
        try:
            duration_int = int(duration_seconds) if duration_seconds is not None else None
        except Exception:
            duration_int = None
        results.append(
            SearchResult(
                video_id=video_id,
                title=_clean_text(entry.get("title")) or "(untitled)",
                url=url,
                open_url=url,
                channel=channel or channel_id,
                channel_id=channel_id,
                channel_title=channel or channel_id,
                channel_url=channel_url,
                published_at=_parse_published_at(entry),
                duration=duration_int,
                duration_text=entry.get("duration_text") or format_duration(duration_int),
                description=_clean_text(entry.get("description")),
                thumbnail=_clean_text(entry.get("thumbnail")),
                view_count=_clean_int(entry.get("view_count")),
                caption_available=bool(entry.get("subtitles") or entry.get("automatic_captions") or entry.get("has_subtitles")),
                search_rank=rank,
            )
        )

    return results


def _print_table(results: List[SearchResult]) -> None:
    if not results:
        print("No results found.")
        return

    for idx, item in enumerate(results, start=1):
        print(f"{idx}. {item.title}")
        print(f"   Channel : {item.channel or '-'}")
        print(f"   Duration: {item.duration_text}")
        print(f"   URL     : {item.url}")
        if item.description:
            desc = item.description if len(item.description) <= 140 else item.description[:137] + "..."
            print(f"   Desc    : {desc}")
        print()


def main() -> int:
    parser = argparse.ArgumentParser(description="Search YouTube videos by keyword.")
    parser.add_argument("query", help="Search keyword")
    parser.add_argument("--limit", type=int, default=10, help="Number of results to return")
    parser.add_argument("--proxy", help="Proxy URL for HTTP/HTTPS")
    parser.add_argument("--cookies", help="Path to yt-dlp compatible cookies file")
    parser.add_argument("--user-agent", help="Custom User-Agent header")
    parser.add_argument("--timeout", type=float, default=20.0, help="Socket timeout in seconds")
    parser.add_argument("--json", action="store_true", help="Print JSON output")
    args = parser.parse_args()

    try:
        results = search_youtube(
            args.query,
            limit=args.limit,
            proxy=args.proxy,
            cookies=args.cookies,
            user_agent=args.user_agent,
            timeout=args.timeout,
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps([asdict(item) for item in results], ensure_ascii=False, indent=2))
    else:
        _print_table(results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
