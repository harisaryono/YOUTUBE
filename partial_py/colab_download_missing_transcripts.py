#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
REPO_ROOT = ROOT.parent
DEFAULT_DB_PATH = REPO_ROOT / "db" / "youtube_transcripts.db"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "artifacts" / "missing_transcripts"
DEFAULT_RUNS_DIR = REPO_ROOT / "runs" / "missing_transcripts_colab"
DEFAULT_RECOVER_SCRIPT = REPO_ROOT / "recover_transcripts_from_csv.py"


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def normalize_text(value: object) -> str:
    return " ".join(str(value or "").split())


def safe_slug(value: object) -> str:
    text = normalize_text(value).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "unknown"


def connect_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def fetch_missing_transcripts(
    conn: sqlite3.Connection,
    *,
    limit: int = 0,
    channel_id: str = "",
) -> list[dict[str, Any]]:
    query = """
        SELECT
            v.video_id,
            v.title,
            v.upload_date,
            v.created_at,
            v.updated_at,
            v.transcript_downloaded,
            v.transcript_language,
            v.is_short,
            v.is_member_only,
            c.id AS channel_db_id,
            c.channel_id AS channel_key,
            c.channel_name,
            c.channel_url
        FROM videos v
        JOIN channels c ON c.id = v.channel_id
        WHERE COALESCE(v.transcript_downloaded, 0) = 0
          AND (v.transcript_language IS NULL OR v.transcript_language != 'no_subtitle')
          AND COALESCE(v.is_short, 0) = 0
    """
    params: list[Any] = []
    if channel_id:
        query += " AND c.channel_id = ?"
        params.append(channel_id)
    query += " ORDER BY c.channel_name, v.created_at DESC, v.video_id"
    if limit > 0:
        query += " LIMIT ?"
        params.append(limit)

    rows = conn.execute(query, params).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "video_id": normalize_text(row["video_id"]),
                "title": normalize_text(row["title"]),
                "channel_db_id": int(row["channel_db_id"] or 0),
                "channel_key": normalize_text(row["channel_key"]),
                "channel_name": normalize_text(row["channel_name"]),
                "channel_url": normalize_text(row["channel_url"]),
                "upload_date": normalize_text(row["upload_date"]),
                "created_at": normalize_text(row["created_at"]),
                "updated_at": normalize_text(row["updated_at"]),
                "transcript_downloaded": int(row["transcript_downloaded"] or 0),
                "transcript_language": normalize_text(row["transcript_language"]),
                "is_short": int(row["is_short"] or 0),
                "is_member_only": int(row["is_member_only"] or 0),
            }
        )
    return result


def group_by_channel(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = (
            normalize_text(row["channel_key"]),
            normalize_text(row["channel_name"]),
            normalize_text(row["channel_url"]),
        )
        grouped[key].append(row)

    result: list[dict[str, Any]] = []
    for (channel_key, channel_name, channel_url), items in grouped.items():
        items_sorted = sorted(items, key=lambda r: (r["created_at"] or "", r["video_id"]))
        result.append(
            {
                "channel_key": channel_key,
                "channel_name": channel_name,
                "channel_url": channel_url,
                "missing_count": len(items_sorted),
                "video_ids": [item["video_id"] for item in items_sorted],
                "titles": [item["title"] for item in items_sorted],
                "rows": items_sorted,
            }
        )
    result.sort(key=lambda item: (-int(item["missing_count"]), item["channel_name"].lower(), item["channel_key"].lower()))
    return result


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def export_reports(rows: list[dict[str, Any]], output_dir: Path) -> tuple[Path, Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    grouped_dir = output_dir / "by_channel"
    grouped_dir.mkdir(parents=True, exist_ok=True)

    grouped = group_by_channel(rows)
    all_csv = output_dir / "missing_transcripts_all.csv"
    report_csv = output_dir / "missing_transcripts_report.csv"
    grouped_index_csv = output_dir / "missing_transcripts_by_channel_index.csv"
    grouped_index_json = output_dir / "missing_transcripts_by_channel_index.json"

    all_rows: list[dict[str, Any]] = []
    report_rows: list[dict[str, Any]] = []
    grouped_index_rows: list[dict[str, Any]] = []

    for item in grouped:
        channel_slug = safe_slug(item["channel_key"] or item["channel_name"])
        channel_csv = grouped_dir / f"{channel_slug}.csv"
        channel_rows = [{"video_id": row["video_id"]} for row in item["rows"]]
        write_csv(channel_csv, channel_rows, ["video_id"])

        report_rows.extend(
            {
                "channel_key": item["channel_key"],
                "channel_name": item["channel_name"],
                "channel_url": item["channel_url"],
                "video_id": row["video_id"],
                "title": row["title"],
                "created_at": row["created_at"],
                "upload_date": row["upload_date"],
                "transcript_language": row["transcript_language"],
            }
            for row in item["rows"]
        )
        grouped_index_rows.append(
            {
                "channel_key": item["channel_key"],
                "channel_name": item["channel_name"],
                "channel_url": item["channel_url"],
                "missing_count": item["missing_count"],
                "video_ids_json": json.dumps(item["video_ids"], ensure_ascii=False),
                "titles_json": json.dumps(item["titles"], ensure_ascii=False),
                "csv_path": str(channel_csv.relative_to(output_dir)),
            }
        )
        all_rows.extend(channel_rows)

    write_csv(all_csv, all_rows, ["video_id"])
    write_csv(
        report_csv,
        report_rows,
        [
            "channel_key",
            "channel_name",
            "channel_url",
            "video_id",
            "title",
            "created_at",
            "upload_date",
            "transcript_language",
        ],
    )
    write_csv(
        grouped_index_csv,
        grouped_index_rows,
        ["channel_key", "channel_name", "channel_url", "missing_count", "video_ids_json", "titles_json", "csv_path"],
    )
    grouped_index_json.write_text(json.dumps(grouped_index_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    return all_csv, report_csv, grouped_index_csv, grouped_index_json


def run_recover_script(recover_script: Path, csv_path: Path, run_dir: Path) -> int:
    if not recover_script.exists():
        raise FileNotFoundError(f"recover script not found: {recover_script}")
    run_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        str(recover_script),
        "--csv",
        str(csv_path),
        "--run-dir",
        str(run_dir),
    ]
    print(f"▶ running: {' '.join(cmd)}")
    proc = subprocess.run(cmd, check=False)
    return int(proc.returncode)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate and optionally download missing YouTube transcripts from youtube_transcripts.db")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="Path to youtube_transcripts.db")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Folder output laporan dan CSV tugas")
    parser.add_argument("--runs-dir", default=str(DEFAULT_RUNS_DIR), help="Folder run reports untuk recover_transcripts_from_csv.py")
    parser.add_argument("--recover-script", default=str(DEFAULT_RECOVER_SCRIPT), help="Path recover_transcripts_from_csv.py")
    parser.add_argument("--channel-id", default="", help="Batasi satu channel_id")
    parser.add_argument("--limit", type=int, default=0, help="Batasi total video missing transcript")
    parser.add_argument("--download", action="store_true", help="Langsung jalankan downloader untuk CSV gabungan")
    parser.add_argument("--no-download", action="store_true", help="Hanya export CSV, jangan download")
    args = parser.parse_args()

    db_path = Path(args.db_path)
    output_dir = Path(args.output_dir)
    runs_dir = Path(args.runs_dir)
    recover_script = Path(args.recover_script)

    if not db_path.exists():
        raise FileNotFoundError(db_path)

    conn = connect_db(db_path)
    try:
        rows = fetch_missing_transcripts(conn, limit=args.limit, channel_id=args.channel_id)
    finally:
        conn.close()

    print(f"timestamp: {now_iso()}")
    print(f"db_path: {db_path}")
    print(f"missing transcript videos: {len(rows)}")
    grouped = group_by_channel(rows)
    print(f"channels with missing transcripts: {len(grouped)}")
    for item in grouped[:20]:
        print(f"- {item['channel_name']} [{item['channel_key']}]: {item['missing_count']}")
    if len(grouped) > 20:
        print(f"... {len(grouped) - 20} more channel(s)")

    all_csv, report_csv, grouped_index_csv, grouped_index_json = export_reports(rows, output_dir)
    print(f"report_csv: {report_csv}")
    print(f"grouped_index_csv: {grouped_index_csv}")
    print(f"grouped_index_json: {grouped_index_json}")
    print(f"all_tasks_csv: {all_csv}")
    print(f"per-channel CSV folder: {output_dir / 'by_channel'}")

    should_download = args.download and not args.no_download
    if should_download:
        run_dir = runs_dir / f"missing_transcripts_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
        rc = run_recover_script(recover_script, all_csv, run_dir)
        if rc != 0:
            print(f"download finished with exit code {rc}")
        else:
            print("download finished successfully")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
