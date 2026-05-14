#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yt_dlp
import zstandard as zstd

CHANNEL_URL = "https://www.youtube.com/@LogikaBisnisID"
DEFAULT_OUTPUT_CSV = "metadata_logikabisnis.csv"
DEFAULT_OUTPUT_JSONL = "metadata_logikabisnis.jsonl"


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def normalize_text(value: object) -> str:
    return " ".join(str(value or "").split())


def safe_json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        return json.dumps(str(value), ensure_ascii=False)


def compress_to_zstd_b64(text: str | None) -> str | None:
    if not text:
        return None
    cctx = zstd.ZstdCompressor(level=3)
    compressed = cctx.compress(str(text).encode("utf-8"))
    return base64.b64encode(compressed).decode("ascii")


def build_ydl_opts(*, flat: bool = True, proxy: str | None = None) -> dict[str, Any]:
    opts: dict[str, Any] = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "ignoreerrors": True,
        "extract_flat": "in_playlist" if flat else False,
        "noplaylist": False,
        "retries": 1,
        "fragment_retries": 1,
        "extractor_retries": 1,
        "socket_timeout": float(os.environ.get("YTDLP_SOCKET_TIMEOUT_SECONDS", "20") or 20),
    }
    if proxy:
        opts["proxy"] = proxy
    return opts


def derive_channel_defaults(result: dict[str, Any], channel_url: str) -> dict[str, str]:
    channel_title = normalize_text(result.get("channel") or result.get("uploader") or result.get("uploader_id") or "")
    channel_id = normalize_text(result.get("channel_id") or result.get("uploader_id") or "")
    channel_url_value = normalize_text(result.get("channel_url") or result.get("uploader_url") or "")

    if not channel_url_value:
        channel_url_value = channel_url
    if not channel_id and "/channel/" in channel_url_value:
        channel_id = normalize_text(channel_url_value.rsplit("/channel/", 1)[-1].split("/", 1)[0])
    return {
        "channel_title": channel_title,
        "channel_id": channel_id,
        "channel_url": channel_url_value,
    }


def derive_thumbnail_url(entry: dict[str, Any]) -> str:
    thumb = normalize_text(entry.get("thumbnail") or entry.get("thumbnail_url") or "")
    if thumb:
        return thumb
    thumbnails = entry.get("thumbnails") or []
    if isinstance(thumbnails, list) and thumbnails:
        for candidate in reversed(thumbnails):
            if isinstance(candidate, dict):
                thumb = normalize_text(candidate.get("url") or "")
                if thumb:
                    return thumb
    return ""


def map_entry(
    entry: dict[str, Any],
    *,
    channel_defaults: dict[str, str],
    enrich: dict[str, Any] | None = None,
) -> dict[str, Any]:
    merged: dict[str, Any] = dict(entry)
    if enrich:
        merged = {**merged, **enrich}

    video_id = normalize_text(merged.get("id") or merged.get("video_id") or "")
    channel_title = normalize_text(
        merged.get("channel")
        or merged.get("uploader")
        or merged.get("channel_title")
        or channel_defaults.get("channel_title")
        or ""
    )
    channel_id = normalize_text(
        merged.get("channel_id")
        or merged.get("uploader_id")
        or channel_defaults.get("channel_id")
        or ""
    )
    channel_url = normalize_text(
        merged.get("channel_url")
        or merged.get("uploader_url")
        or channel_defaults.get("channel_url")
        or ""
    )
    url = normalize_text(
        merged.get("webpage_url")
        or merged.get("url")
        or (f"https://www.youtube.com/watch?v={video_id}" if video_id else "")
    )
    upload_date = normalize_text(merged.get("upload_date") or "")
    published_at = normalize_text(merged.get("published_at") or "")
    if not published_at and upload_date:
        published_at = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:8]}T00:00:00+00:00"

    description = normalize_text(merged.get("description") or "")
    if not description and enrich and isinstance(enrich.get("description"), str):
        description = normalize_text(enrich.get("description"))

    return {
        "video_id": video_id,
        "youtube_video_id": video_id,
        "url": url,
        "title": normalize_text(merged.get("title") or ""),
        "description": description,
        "view_count": merged.get("view_count") or "",
        "duration_seconds": merged.get("duration") or merged.get("duration_seconds") or "",
        "upload_date": upload_date,
        "published_at": published_at,
        "channel_title": channel_title,
        "channel_id": channel_id,
        "channel_url": channel_url,
        "thumbnail_url": derive_thumbnail_url(merged),
        "tags_json": safe_json_dumps(merged.get("tags") or []),
        "categories_json": safe_json_dumps(merged.get("categories") or []),
        "availability": normalize_text(merged.get("availability") or ""),
        "live_status": normalize_text(merged.get("live_status") or ""),
        "caption_available": int(bool(merged.get("subtitles") or merged.get("automatic_captions") or merged.get("has_subtitles"))),
        "channel_follower_count": merged.get("channel_follower_count") or 0,
        "channel_is_verified": int(bool(merged.get("channel_is_verified"))),
        "source_system": "yt-dlp",
        "source_key": channel_id or channel_url or CHANNEL_URL,
        "needs_refresh": 0,
        "refresh_priority": 50,
        "data_quality": "enriched" if enrich else "raw_flat",
        "raw_json": safe_json_dumps(merged),
        "raw_json_zstd_b64": compress_to_zstd_b64(safe_json_dumps(merged)),
        "description_zstd_b64": compress_to_zstd_b64(description),
        "fetched_at": now_iso(),
    }


def enrich_video_metadata(video_url: str, proxy: str | None = None) -> dict[str, Any]:
    opts = build_ydl_opts(flat=False, proxy=proxy)
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(video_url, download=False)
    if not isinstance(info, dict):
        return {}
    return info


def export_channel(channel_url: str, *, output_csv: Path, output_jsonl: Path | None = None, enrich_videos: bool = False, enrich_limit: int = 0, proxy: str | None = None) -> list[dict[str, Any]]:
    _, rows = collect_channel_rows(
        channel_url,
        enrich_videos=enrich_videos,
        enrich_limit=enrich_limit,
        proxy=proxy,
    )
    write_export_rows(rows, output_csv=output_csv, output_jsonl=output_jsonl)
    return rows


def collect_channel_rows(
    channel_url: str,
    *,
    enrich_videos: bool = False,
    enrich_limit: int = 0,
    proxy: str | None = None,
) -> tuple[dict[str, str], list[dict[str, Any]]]:
    videos_url = channel_url.rstrip("/") + "/videos"
    opts = build_ydl_opts(flat=True, proxy=proxy)
    with yt_dlp.YoutubeDL(opts) as ydl:
        print(f"Sedang mengambil daftar video: {videos_url}")
        result = ydl.extract_info(videos_url, download=False)

    if not isinstance(result, dict):
        raise RuntimeError("yt-dlp tidak mengembalikan metadata channel yang valid")

    channel_defaults = derive_channel_defaults(result, channel_url)
    entries = result.get("entries") or []
    rows: list[dict[str, Any]] = []

    for idx, entry in enumerate(entries, start=1):
        if not entry:
            continue
        if not isinstance(entry, dict):
            entry = dict(getattr(entry, "__dict__", {}))
        if not entry:
            continue

        enrich_payload: dict[str, Any] | None = None
        if enrich_videos and (enrich_limit <= 0 or idx <= enrich_limit):
            video_id = normalize_text(entry.get("id") or entry.get("video_id") or "")
            video_url = normalize_text(entry.get("webpage_url") or entry.get("url") or (f"https://www.youtube.com/watch?v={video_id}" if video_id else ""))
            if video_url:
                try:
                    enrich_payload = enrich_video_metadata(video_url, proxy=proxy)
                except Exception as exc:
                    print(f"[warn] enrich gagal untuk {video_id or video_url}: {exc}")
                    enrich_payload = None

        row = map_entry(entry, channel_defaults=channel_defaults, enrich=enrich_payload)
        row["raw_json"] = safe_json_dumps(entry if enrich_payload is None else {**entry, **enrich_payload})
        row["raw_json_zstd_b64"] = compress_to_zstd_b64(row["raw_json"])
        rows.append(row)

        if idx % 50 == 0:
            print(f"  progress: {idx} videos")

    return channel_defaults, rows


def write_export_rows(rows: list[dict[str, Any]], *, output_csv: Path, output_jsonl: Path | None = None) -> None:
    if output_csv:
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = [
            "video_id",
            "youtube_video_id",
            "url",
            "title",
            "description",
            "view_count",
            "duration_seconds",
            "upload_date",
            "published_at",
            "channel_title",
            "channel_id",
            "channel_url",
            "thumbnail_url",
            "tags_json",
            "categories_json",
            "availability",
            "live_status",
            "caption_available",
            "channel_follower_count",
            "channel_is_verified",
            "source_system",
            "source_key",
            "needs_refresh",
            "refresh_priority",
            "data_quality",
            "raw_json",
            "raw_json_zstd_b64",
            "description_zstd_b64",
            "fetched_at",
        ]
        with output_csv.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

    if output_jsonl:
        output_jsonl.parent.mkdir(parents=True, exist_ok=True)
        with output_jsonl.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export YouTube channel metadata to CSV/JSONL")
    parser.add_argument("--channel-url", default=CHANNEL_URL, help="Channel URL or handle URL")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_CSV, help="Output CSV path")
    parser.add_argument("--jsonl-output", default=DEFAULT_OUTPUT_JSONL, help="Optional JSONL output path; set empty to disable")
    parser.add_argument("--enrich-video-metadata", action="store_true", help="Fetch full metadata for each video (slower, can trigger blocks)")
    parser.add_argument("--enrich-limit", type=int, default=int(os.environ.get("YTDLP_ENRICH_LIMIT", "0") or 0), help="Only enrich the first N videos; 0 means all when enabled")
    parser.add_argument("--proxy", default=os.environ.get("YTDLP_PROXY", ""), help="Optional yt-dlp proxy URL")
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()
    output_csv = Path(args.output)
    jsonl_output = Path(args.jsonl_output) if str(args.jsonl_output or "").strip() else None

    rows = export_channel(
        args.channel_url,
        output_csv=output_csv,
        output_jsonl=jsonl_output,
        enrich_videos=bool(args.enrich_video_metadata),
        enrich_limit=max(0, int(args.enrich_limit or 0)),
        proxy=str(args.proxy or "").strip() or None,
    )
    print(f"Selesai! {len(rows)} video berhasil disimpan ke {output_csv}")
    if jsonl_output:
        print(f"JSONL juga ditulis ke {jsonl_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
