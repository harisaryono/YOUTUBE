#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sqlite3
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parent
DEFAULT_DB_PATH = ROOT / "youtube_transcripts.db"
DEFAULT_CHANNELS_CSV = ROOT / "channels_export.csv"
DEFAULT_EXPORT_ZIP = ROOT / "youtube_exports_data-20260510T115246Z-3-001.zip"


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def normalize_text(value: object) -> str:
    return " ".join(str(value or "").split())


def normalize_key(value: object) -> str:
    text = normalize_text(value).lower()
    return re.sub(r"[^a-z0-9]+", "", text)


def to_int(value: object, default: int = 0) -> int:
    try:
        if value is None:
            return default
        text = str(value).strip()
        if not text:
            return default
        if "." in text:
            return int(float(text))
        return int(text)
    except Exception:
        return default


def to_bool_int(value: object) -> int:
    text = str(value or "").strip().lower()
    return 1 if text in {"1", "true", "yes", "on"} else 0


def connect_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def load_channel_rows(channels_csv: Path) -> list[dict[str, Any]]:
    with channels_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows: list[dict[str, Any]] = []
        for row in reader:
            cleaned = {str(k).lstrip("\ufeff"): v for k, v in row.items()}
            if str(cleaned.get("channel_id") or "").strip():
                rows.append(cleaned)
        return rows


def load_export_csvs(exports_zip: Path) -> list[tuple[str, list[dict[str, Any]]]]:
    result: list[tuple[str, list[dict[str, Any]]]] = []
    with zipfile.ZipFile(exports_zip) as zf:
        for name in sorted(zf.namelist()):
            if not name.lower().endswith(".csv"):
                continue
            with zf.open(name) as fp:
                text = fp.read().decode("utf-8-sig")
            rows: list[dict[str, Any]] = []
            reader = csv.DictReader(text.splitlines())
            for row in reader:
                if str(row.get("video_id") or "").strip():
                    rows.append(row)
            result.append((name, rows))
    return result


def load_export_jsonls(exports_zip: Path) -> list[tuple[str, list[dict[str, Any]]]]:
    result: list[tuple[str, list[dict[str, Any]]]] = []
    with zipfile.ZipFile(exports_zip) as zf:
        for name in sorted(zf.namelist()):
            if not name.lower().endswith(".jsonl"):
                continue
            with zf.open(name) as fp:
                text = fp.read().decode("utf-8-sig")
            rows: list[dict[str, Any]] = []
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if str(row.get("video_id") or row.get("youtube_video_id") or "").strip():
                    rows.append(row)
            result.append((name, rows))
    return result


def build_channel_lookup(channels_rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    lookup: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in channels_rows:
        candidates = {
            row.get("channel_id", ""),
            row.get("channel_name", ""),
            row.get("channel_title", ""),
            row.get("channel_url", ""),
        }
        channel_url = normalize_text(row.get("channel_url") or "")
        m = re.search(r"/@([^/?]+)", channel_url)
        if m:
            candidates.add(m.group(1))
            candidates.add("@" + m.group(1))
        m = re.search(r"/channel/([^/?]+)", channel_url)
        if m:
            candidates.add(m.group(1))
        for candidate in candidates:
            key = normalize_key(candidate)
            if key:
                lookup[key].append(row)
    return lookup


def resolve_channel_row(
    channel_lookup: dict[str, list[dict[str, Any]]],
    *,
    file_name: str,
    sample_row: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    candidates: dict[str, dict[str, Any]] = {}
    base_name = Path(file_name).name
    stem = base_name[:-4] if base_name.lower().endswith(".csv") else base_name
    slug, _, tail = stem.partition("__")
    sources = [
        slug,
        tail,
        sample_row.get("channel_id") if sample_row else "",
        sample_row.get("channel_name") if sample_row else "",
        sample_row.get("channel_title") if sample_row else "",
        sample_row.get("channel_url") if sample_row else "",
    ]
    for raw in sources:
        key = normalize_key(raw)
        if not key or key not in channel_lookup:
            continue
        for row in channel_lookup[key]:
            channel_id = normalize_text(row.get("channel_id") or "")
            if channel_id:
                candidates[channel_id] = row
    if len(candidates) == 1:
        return next(iter(candidates.values()))
    if len(candidates) > 1:
        prefer = normalize_key(slug or tail)
        for row in candidates.values():
            url_key = normalize_key(row.get("channel_url") or "")
            if prefer and prefer in url_key:
                return row
        for row in candidates.values():
            if normalize_key(row.get("channel_name") or "") in {normalize_key(slug), normalize_key(tail)}:
                return row
    return None


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT UNIQUE NOT NULL,
            channel_name TEXT NOT NULL,
            channel_url TEXT NOT NULL,
            subscriber_count INTEGER DEFAULT 0,
            video_count INTEGER DEFAULT 0,
            thumbnail_url TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS videos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT UNIQUE NOT NULL,
            channel_id INTEGER,
            title TEXT NOT NULL,
            description TEXT,
            duration INTEGER,
            upload_date TEXT,
            view_count INTEGER DEFAULT 0,
            like_count INTEGER DEFAULT 0,
            comment_count INTEGER DEFAULT 0,
            video_url TEXT NOT NULL,
            thumbnail_url TEXT,
            transcript_file_path TEXT,
            summary_file_path TEXT,
            transcript_downloaded BOOLEAN DEFAULT 0,
            transcript_language TEXT,
            word_count INTEGER DEFAULT 0,
            line_count INTEGER DEFAULT 0,
            is_short BOOLEAN DEFAULT 0,
            is_member_only BOOLEAN DEFAULT 0,
            transcript_text TEXT,
            summary_text TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            metadata TEXT,
            transcript_formatted_path TEXT,
            transcript_retry_after TIMESTAMP,
            transcript_retry_reason TEXT,
            transcript_retry_count INTEGER DEFAULT 0,
            FOREIGN KEY (channel_id) REFERENCES channels(id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS channels_meta (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT UNIQUE NOT NULL,
            video_count INTEGER DEFAULT 0,
            transcript_count INTEGER DEFAULT 0,
            total_views INTEGER DEFAULT 0,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def list_stat_triggers(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    rows = conn.execute(
        """
        SELECT name, sql
        FROM sqlite_master
        WHERE type = 'trigger'
          AND tbl_name IN ('channels', 'videos')
          AND name LIKE 'trg_%_bump_stats_%'
        ORDER BY name
        """
    ).fetchall()
    triggers: list[tuple[str, str]] = []
    for row in rows:
        if row["sql"]:
            triggers.append((str(row["name"]), str(row["sql"])))
    return triggers


def drop_triggers(conn: sqlite3.Connection, trigger_names: Iterable[str]) -> None:
    for name in trigger_names:
        conn.execute(f"DROP TRIGGER IF EXISTS {name}")
    conn.commit()


def restore_triggers(conn: sqlite3.Connection, triggers: Iterable[tuple[str, str]]) -> None:
    for name, sql in triggers:
        conn.execute(sql)
    conn.commit()


def bump_stats_version(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO cached_stats (key, value, updated_at)
        VALUES (
            'stats_version',
            COALESCE((SELECT CAST(value AS INTEGER) FROM cached_stats WHERE key = 'stats_version'), 0) + 1,
            CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute("DELETE FROM cached_stats WHERE key = 'global_stats'")
    conn.commit()


def upsert_channels(conn: sqlite3.Connection, channels_rows: list[dict[str, Any]]) -> dict[str, int]:
    channel_map: dict[str, int] = {}
    now = now_iso()
    sql = """
        INSERT INTO channels (
            channel_id, channel_name, channel_url, subscriber_count, video_count, thumbnail_url, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(channel_id) DO UPDATE SET
            channel_name = excluded.channel_name,
            channel_url = excluded.channel_url,
            subscriber_count = excluded.subscriber_count,
            video_count = excluded.video_count,
            thumbnail_url = excluded.thumbnail_url,
            updated_at = excluded.updated_at
    """
    batch: list[tuple[Any, ...]] = []
    for row in channels_rows:
        batch.append(
            (
                normalize_text(row.get("channel_id") or ""),
                normalize_text(row.get("channel_name") or row.get("channel_title") or ""),
                normalize_text(row.get("channel_url") or ""),
                to_int(row.get("subscriber_count")),
                to_int(row.get("video_count")),
                normalize_text(row.get("thumbnail_url") or ""),
                now,
            )
        )
    conn.executemany(sql, batch)
    conn.commit()
    for row in channels_rows:
        channel_id = normalize_text(row.get("channel_id") or "")
        db_row = conn.execute("SELECT id FROM channels WHERE channel_id = ?", (channel_id,)).fetchone()
        if db_row is not None:
            channel_map[channel_id] = int(db_row["id"])
    return channel_map


def make_video_metadata(row: dict[str, Any], *, source_file: str) -> str:
    payload = dict(row)
    payload["source_file"] = source_file
    return json.dumps(payload, ensure_ascii=False, default=str)


def upsert_videos(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
    channel_map: dict[str, int],
    *,
    source_file: str,
    batch_size: int = 25,
    progress_every: int = 1000,
    channel_db_id: int | None = None,
) -> int:
    now = now_iso()
    sql = """
        INSERT INTO videos (
            video_id, channel_id, title, description, duration, upload_date,
            view_count, like_count, comment_count, video_url, thumbnail_url,
            transcript_file_path, summary_file_path, transcript_downloaded,
            transcript_language, word_count, line_count, is_short, is_member_only,
            transcript_text, summary_text, created_at, updated_at, metadata,
            transcript_formatted_path, transcript_retry_after,
            transcript_retry_reason, transcript_retry_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(video_id) DO UPDATE SET
            channel_id = excluded.channel_id,
            title = excluded.title,
            description = excluded.description,
            duration = excluded.duration,
            upload_date = excluded.upload_date,
            view_count = excluded.view_count,
            like_count = excluded.like_count,
            comment_count = excluded.comment_count,
            video_url = excluded.video_url,
            thumbnail_url = excluded.thumbnail_url,
            is_short = excluded.is_short,
            metadata = excluded.metadata,
            updated_at = excluded.updated_at
    """
    batch: list[tuple[Any, ...]] = []
    processed = 0
    for row in rows:
        video_id = normalize_text(row.get("video_id") or row.get("youtube_video_id") or "")
        if not video_id:
            continue
        channel_key = normalize_text(row.get("channel_id") or "")
        row_channel_db_id = channel_db_id if channel_db_id is not None else channel_map.get(channel_key)
        if row_channel_db_id is None:
            row_channel_db_id = channel_map.get(normalize_text(row.get("channel_url") or ""))
        if row_channel_db_id is None:
            row_channel_db_id = channel_map.get(normalize_key(row.get("channel_title") or ""))
        video_url = normalize_text(row.get("url") or "")
        title = normalize_text(row.get("title") or "")
        description = normalize_text(row.get("description") or "")
        duration = to_int(row.get("duration_seconds"), default=0)
        upload_date = normalize_text(row.get("upload_date") or "")
        view_count = to_int(row.get("view_count"), default=0)
        is_short = 1 if duration and duration <= 60 else 0
        metadata = make_video_metadata(row, source_file=source_file)
        batch.append(
            (
                video_id,
                row_channel_db_id,
                title,
                description or None,
                duration or None,
                upload_date or None,
                view_count,
                0,
                0,
                video_url,
                normalize_text(row.get("thumbnail_url") or "") or None,
                None,
                None,
                0,
                None,
                0,
                0,
                is_short,
                0,
                None,
                None,
                now,
                now,
                metadata,
                None,
                None,
                None,
                0,
            )
        )
        processed += 1
        if len(batch) >= batch_size:
            conn.executemany(sql, batch)
            conn.commit()
            batch.clear()
            if progress_every and processed % progress_every == 0:
                print(f"  progress: {processed} videos")
    if batch:
        conn.executemany(sql, batch)
        conn.commit()
    return processed


def refresh_channels_meta(conn: sqlite3.Connection, channel_map: dict[str, int]) -> None:
    now = now_iso()
    for channel_key, channel_db_id in channel_map.items():
        row = conn.execute("SELECT channel_id FROM channels WHERE id = ?", (channel_db_id,)).fetchone()
        if row is None:
            continue
        stats = conn.execute(
            """
            SELECT COUNT(*) AS video_count,
                   COALESCE(SUM(view_count), 0) AS total_views,
                   COALESCE(SUM(CASE WHEN transcript_downloaded THEN 1 ELSE 0 END), 0) AS transcript_count
            FROM videos
            WHERE channel_id = ?
            """,
            (channel_db_id,),
        ).fetchone()
        conn.execute(
            """
            INSERT INTO channels_meta (channel_id, video_count, transcript_count, total_views, last_updated)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(channel_id) DO UPDATE SET
                video_count = excluded.video_count,
                transcript_count = excluded.transcript_count,
                total_views = excluded.total_views,
                last_updated = excluded.last_updated
            """,
            (
                channel_key,
                int(stats["video_count"] or 0),
                int(stats["transcript_count"] or 0),
                int(stats["total_views"] or 0),
                now,
            ),
        )
    conn.commit()


def import_all(
    *,
    db_path: Path,
    channels_csv: Path,
    exports_zip: Path,
    batch_size: int = 25,
) -> None:
    if not channels_csv.exists():
        raise FileNotFoundError(channels_csv)
    if not exports_zip.exists():
        raise FileNotFoundError(exports_zip)

    channels_rows = load_channel_rows(channels_csv)
    print(f"channels_csv rows: {len(channels_rows)}")
    print(f"exports_zip: {exports_zip}")

    conn = connect_db(db_path)
    ensure_schema(conn)
    channel_lookup = build_channel_lookup(channels_rows)
    stat_triggers = list_stat_triggers(conn)
    if stat_triggers:
        print(f"disabling {len(stat_triggers)} stats trigger(s) during import")
        drop_triggers(conn, [name for name, _ in stat_triggers])

    try:
        channel_map = upsert_channels(conn, channels_rows)
        print(f"channels upserted: {len(channel_map)}")

        with zipfile.ZipFile(exports_zip) as zf:
            export_names = sorted(
                name for name in zf.namelist() if name.lower().endswith(".csv") or name.lower().endswith(".jsonl")
            )
            total = 0
            for idx, name in enumerate(export_names, start=1):
                if name.lower().endswith(".csv"):
                    with zf.open(name) as fp:
                        text = fp.read().decode("utf-8-sig")
                    reader = csv.DictReader(text.splitlines())
                    rows = [row for row in reader if str(row.get("video_id") or "").strip()]
                else:
                    with zf.open(name) as fp:
                        text = fp.read().decode("utf-8-sig")
                    rows = []
                    for line in text.splitlines():
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            row = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if str(row.get("video_id") or row.get("youtube_video_id") or "").strip():
                            rows.append(row)
                print(f"[{idx}/{len(export_names)}] {name}: {len(rows)} rows")
                file_channel_row = resolve_channel_row(
                    channel_lookup,
                    file_name=name,
                    sample_row=rows[0] if rows else None,
                )
                file_channel_db_id = None
                if file_channel_row:
                    file_channel_db_id = channel_map.get(normalize_text(file_channel_row.get("channel_id") or ""))
                total += upsert_videos(
                    conn,
                    rows,
                    channel_map,
                    source_file=name,
                    batch_size=batch_size,
                    progress_every=5000,
                    channel_db_id=file_channel_db_id,
                )
            print(f"videos processed: {total}")

        refresh_channels_meta(conn, channel_map)
    finally:
        if stat_triggers:
            print("restoring stats trigger(s)")
            restore_triggers(conn, stat_triggers)
            bump_stats_version(conn)
        conn.close()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import Colab export results into youtube_transcripts.db")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="Target SQLite database")
    parser.add_argument("--channels-csv", default=str(DEFAULT_CHANNELS_CSV), help="channels_export.csv path")
    parser.add_argument("--exports-zip", default=str(DEFAULT_EXPORT_ZIP), help="ZIP file from Colab export")
    parser.add_argument("--batch-size", type=int, default=25, help="SQLite commit batch size")
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    import_all(
        db_path=Path(args.db_path),
        channels_csv=Path(args.channels_csv),
        exports_zip=Path(args.exports_zip),
        batch_size=max(1, int(args.batch_size or 25)),
    )
    print("Import selesai.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
