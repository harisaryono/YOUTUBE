#!/usr/bin/env python3
"""
Google Colab friendly wrapper for exporting metadata from a YouTube channel.

Usage in Colab:
1. Upload or git-clone this repo into /content/YOUTUBE
2. Run:
   %pip install yt-dlp pandas zstandard
3. Optional: mount Google Drive
   from google.colab import drive
   drive.mount('/content/drive')
4. Execute:
   !python export_logika_bisnis_metadata_colab.py --output /content/drive/MyDrive/metadata_logikabisnis.csv

This wrapper expects the companion module:
  export_logika_bisnis_metadata.py
to be present in the same directory.
"""

from __future__ import annotations

import argparse
import os
import sys
import re
import csv
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from export_logika_bisnis_metadata import CHANNEL_URL, collect_channel_rows, write_export_rows
except Exception as exc:  # pragma: no cover
    raise RuntimeError(
        "Tidak bisa mengimpor export_logika_bisnis_metadata.py. "
        "Pastikan file itu ada di folder yang sama."
    ) from exc


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def maybe_mount_drive(mount_point: str = "/content/drive") -> bool:
    if not _truthy(os.environ.get("COLAB_MOUNT_DRIVE", "0")):
        return False
    try:
        from google.colab import drive  # type: ignore
    except Exception:
        print("[warn] google.colab tidak tersedia; lewati mount drive")
        return False
    drive.mount(mount_point)
    return True



def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Colab-friendly YouTube metadata exporter")
    parser.add_argument(
        "--channel-url",
        action="append",
        default=[],
        help="Channel URL atau handle URL; ulangi untuk banyak channel",
    )
    parser.add_argument("--output-dir", default="/content/metadata_exports", help="Folder output untuk CSV per channel")
    parser.add_argument("--jsonl-output-dir", default="/content/metadata_exports_jsonl", help="Folder output JSONL per channel; kosongkan untuk disable")
    parser.add_argument("--mount-drive", action="store_true", help="Mount Google Drive sebelum export")
    parser.add_argument("--drive-mount-point", default="/content/drive", help="Lokasi mount Google Drive")
    parser.add_argument("--enrich-video-metadata", action="store_true", help="Fetch metadata detail per video (lebih lambat)")
    parser.add_argument("--enrich-limit", type=int, default=int(os.environ.get("YTDLP_ENRICH_LIMIT", "0") or 0), help="Batasi video yang di-enrich; 0 = semua")
    parser.add_argument("--proxy", default=os.environ.get("YTDLP_PROXY", ""), help="Proxy URL opsional untuk yt-dlp")
    parser.add_argument(
        "--name-mode",
        choices=["title", "id", "title_id"],
        default="title",
        help="Dasar nama file per channel",
    )
    return parser


def sanitize_filename(value: str) -> str:
    text = str(value or "").strip()
    text = re.sub(r"[^\w\-.]+", "_", text, flags=re.UNICODE)
    text = re.sub(r"_+", "_", text).strip("_")
    return text[:140] or "channel"


def default_channel_urls() -> list[str]:
    env_value = str(os.environ.get("YTDLP_CHANNEL_URLS", "") or "").strip()
    if env_value:
        parts = [part.strip() for part in re.split(r"[,\n;]+", env_value) if part.strip()]
        return parts

    static_csv = ROOT / "channels_export.csv"
    if static_csv.exists():
        try:
            with static_csv.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                ordered: list[str] = []
                seen: set[str] = set()
                for row in reader:
                    url = str(row.get("channel_url") or "").strip()
                    if not url or url in seen:
                        continue
                    seen.add(url)
                    ordered.append(url)
                if ordered:
                    return ordered
        except Exception as exc:
            print(f"[warn] gagal baca {static_csv}: {exc}")

    db_path = ROOT / "youtube_transcripts.db"
    if db_path.exists():
        try:
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT channel_url, channel_name, channel_id FROM channels ORDER BY channel_name ASC"
            ).fetchall()
            conn.close()
            ordered: list[str] = []
            seen: set[str] = set()
            for row in rows:
                url = str(row["channel_url"] or "").strip()
                if not url or url in seen:
                    continue
                seen.add(url)
                ordered.append(url)
            if ordered:
                return ordered
        except Exception as exc:
            print(f"[warn] gagal baca channels dari {db_path}: {exc}")

    return [CHANNEL_URL]


def build_output_basename(channel_url: str, channel_title: str, channel_id: str, name_mode: str) -> str:
    title = sanitize_filename(channel_title)
    cid = sanitize_filename(channel_id)
    if name_mode == "id":
        return cid or title
    if name_mode == "title_id":
        parts = [part for part in [title, cid] if part]
        return "__".join(parts) if parts else "channel"
    return title or cid or "channel"


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    if args.mount_drive:
        maybe_mount_drive(args.drive_mount_point)

    channel_urls = args.channel_url or default_channel_urls()
    print("Channel default:")
    for item in channel_urls:
        print(f"- {item}")
    output_dir = Path(args.output_dir)
    jsonl_output_dir = Path(args.jsonl_output_dir) if str(args.jsonl_output_dir or "").strip() else None
    output_dir.mkdir(parents=True, exist_ok=True)
    if jsonl_output_dir:
        jsonl_output_dir.mkdir(parents=True, exist_ok=True)

    summary: list[tuple[str, int, Path]] = []
    for idx, channel_url in enumerate(channel_urls, start=1):
        print(f"\n[{idx}/{len(channel_urls)}] Exporting {channel_url}")
        channel_defaults, rows = collect_channel_rows(
            channel_url,
            enrich_videos=bool(args.enrich_video_metadata),
            enrich_limit=max(0, int(args.enrich_limit or 0)),
            proxy=str(args.proxy or "").strip() or None,
        )
        channel_title = str(channel_defaults.get("channel_title") or "").strip()
        channel_id = str(channel_defaults.get("channel_id") or "").strip()

        basename = build_output_basename(channel_url, channel_title, channel_id, args.name_mode)
        csv_path = output_dir / f"{basename}.csv"
        jsonl_path = (jsonl_output_dir / f"{basename}.jsonl") if jsonl_output_dir else None

        write_export_rows(rows, output_csv=csv_path, output_jsonl=jsonl_path)

        summary.append((basename, len(rows), csv_path))
        print(f"Selesai! {len(rows)} video berhasil disimpan ke {csv_path}")
        if jsonl_path:
            print(f"JSONL juga ditulis ke {jsonl_path}")

    print("\nRingkasan export:")
    for name, count, path in summary:
        print(f"- {name}: {count} video -> {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
