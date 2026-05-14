#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import csv
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yt_dlp
import zstandard as zstd

ROOT = Path(__file__).resolve().parent
DEFAULT_CHANNELS_CSV = ROOT / "channels_export.csv"


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
    return {"channel_title": channel_title, "channel_id": channel_id, "channel_url": channel_url_value}


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


def map_entry(entry: dict[str, Any], *, channel_defaults: dict[str, str], enrich: dict[str, Any] | None = None) -> dict[str, Any]:
    merged: dict[str, Any] = dict(entry)
    if enrich:
        merged = {**merged, **enrich}
    video_id = normalize_text(merged.get("id") or merged.get("video_id") or "")
    channel_title = normalize_text(
        merged.get("channel") or merged.get("uploader") or merged.get("channel_title") or channel_defaults.get("channel_title") or ""
    )
    channel_id = normalize_text(
        merged.get("channel_id") or merged.get("uploader_id") or channel_defaults.get("channel_id") or ""
    )
    channel_url = normalize_text(
        merged.get("channel_url") or merged.get("uploader_url") or channel_defaults.get("channel_url") or ""
    )
    url = normalize_text(merged.get("webpage_url") or merged.get("url") or (f"https://www.youtube.com/watch?v={video_id}" if video_id else ""))
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
        "source_key": channel_id or channel_url or "",
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
    return info if isinstance(info, dict) else {}


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


def sanitize_filename(value: str) -> str:
    text = str(value or "").strip()
    text = re.sub(r"[^\w\-.]+", "_", text, flags=re.UNICODE)
    text = re.sub(r"_+", "_", text).strip("_")
    return text[:140] or "channel"


def build_output_basename(channel_url: str, channel_title: str, channel_id: str, name_mode: str) -> str:
    title = sanitize_filename(channel_title)
    cid = sanitize_filename(channel_id)
    if name_mode == "id":
        return cid or title
    if name_mode == "title_id":
        parts = [part for part in [title, cid] if part]
        return "__".join(parts) if parts else "channel"
    return title or cid or "channel"


def load_channel_list() -> list[dict[str, str]]:
    env_value = str(os.environ.get("YTDLP_CHANNEL_URLS", "") or "").strip()
    if env_value:
        urls = [part.strip() for part in re.split(r"[,\n;]+", env_value) if part.strip()]
        rows = [{"channel_url": url, "channel_name": "", "channel_id": ""} for url in urls]
        return rows
    static_csv = DEFAULT_CHANNELS_CSV
    if not static_csv.exists():
        raise FileNotFoundError(f"File channel list tidak ditemukan: {static_csv}")
    with static_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = [row for row in reader if str(row.get("channel_url") or "").strip()]
    return rows


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Standalone Colab-friendly YouTube metadata exporter")
    parser.add_argument("--channels-csv", default=str(DEFAULT_CHANNELS_CSV), help="CSV daftar channel")
    parser.add_argument("--output-dir", default="/content/yt_exports", help="Folder output CSV per channel")
    parser.add_argument("--jsonl-output-dir", default="/content/yt_exports_jsonl", help="Folder output JSONL per channel")
    parser.add_argument("--enrich-video-metadata", action="store_true", help="Fetch metadata detail per video (lebih lambat)")
    parser.add_argument("--enrich-limit", type=int, default=int(os.environ.get("YTDLP_ENRICH_LIMIT", "0") or 0), help="Batasi video yang di-enrich; 0 = semua")
    parser.add_argument("--proxy", default=os.environ.get("YTDLP_PROXY", ""), help="Proxy URL opsional untuk yt-dlp")
    parser.add_argument("--name-mode", choices=["title", "id", "title_id"], default="title_id", help="Dasar nama file per channel")
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()
    channels_csv = Path(args.channels_csv)
    output_dir = Path(args.output_dir)
    jsonl_output_dir = Path(args.jsonl_output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    jsonl_output_dir.mkdir(parents=True, exist_ok=True)

    with channels_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        channels = [row for row in reader if str(row.get("channel_url") or "").strip()]

    print(f"Channel terdeteksi dari CSV: {len(channels)}")
    for row in channels[:10]:
        print("-", row.get("channel_name") or row.get("channel_id"), "->", row.get("channel_url"))

    summary: list[tuple[str, int, Path]] = []
    for idx, row in enumerate(channels, start=1):
        channel_url = str(row.get("channel_url") or "").strip()
        channel_title = str(row.get("channel_name") or "").strip()
        channel_id = str(row.get("channel_id") or "").strip()
        print(f"\n[{idx}/{len(channels)}] Exporting {channel_title or channel_id or channel_url}")
        basename = build_output_basename(channel_url, channel_title, channel_id, args.name_mode)
        csv_path = output_dir / f"{basename}.csv"
        jsonl_path = jsonl_output_dir / f"{basename}.jsonl"
        channel_defaults, rows = collect_channel_rows(
            channel_url,
            enrich_videos=bool(args.enrich_video_metadata),
            enrich_limit=max(0, int(args.enrich_limit or 0)),
            proxy=str(args.proxy or "").strip() or None,
        )
        write_export_rows(rows, output_csv=csv_path, output_jsonl=jsonl_path)
        summary.append((basename, len(rows), csv_path))
        print(f"Selesai! {len(rows)} video berhasil disimpan ke {csv_path}")
    print("\nRingkasan export:")
    for name, count, path in summary:
        print(f"- {name}: {count} video -> {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
