#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import subprocess
import sys
import shlex
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from database_optimized import OptimizedDatabase
from database_blobs import BlobStorage


DEFAULT_LOCAL_DB = REPO_ROOT / "youtube_transcripts.db"
DEFAULT_LOCAL_BLOB_DB = REPO_ROOT / "youtube_transcripts_blobs.db"
DEFAULT_REMOTE_DIR = "/root/YOUTUBE"
DEFAULT_SERVER = "tafsir-server"
DEFAULT_BACKUP_DIR = "backups"


@dataclass(frozen=True)
class TableSpec:
    name: str
    db_kind: str  # "main" or "blob"
    key_cols: tuple[str, ...]


TABLE_SPECS: list[TableSpec] = [
    TableSpec("channels", "main", ("channel_id",)),
    TableSpec("channels_meta", "main", ("channel_id",)),
    TableSpec("channel_runtime_state", "main", ("channel_id",)),
    TableSpec("videos", "main", ("video_id",)),
    TableSpec("video_asr_chunks", "main", ("video_id", "provider", "model_name", "chunk_index")),
    TableSpec("content_blobs", "blob", ("video_id", "content_type")),
]

VIDEO_FILE_COLUMNS = (
    "transcript_file_path",
    "summary_file_path",
    "transcript_formatted_path",
)


def _sqlite_connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    if not _table_exists(conn, table):
        return []
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [str(row[1]) for row in rows]


def _table_create_sql(conn: sqlite3.Connection, table: str) -> str | None:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table,),
    ).fetchone()
    if not row:
        return None
    return row["sql"]


def _row_key(row: sqlite3.Row | dict[str, Any], key_cols: tuple[str, ...]) -> tuple[Any, ...]:
    return tuple(row[col] for col in key_cols)


def _serialize_key(key: tuple[Any, ...]) -> tuple[str, ...]:
    return tuple("" if value is None else str(value) for value in key)


def _remote_query_keys(server: str, remote_db: str, table: str, key_cols: tuple[str, ...]) -> set[tuple[str, ...]]:
    if not key_cols:
        return set()

    py_code = r"""
import json
import sqlite3
import sys

db_path = sys.argv[1]
table = sys.argv[2]
key_cols = [part for part in sys.argv[3].split(",") if part]

try:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    if not key_cols:
        print("[]")
        raise SystemExit(0)
    cols = ", ".join(key_cols)
    if not conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table,),
    ).fetchone():
        print("[]")
        raise SystemExit(0)
    rows = cur.execute(f"SELECT {cols} FROM {table}").fetchall()
    payload = [tuple("" if row[col] is None else str(row[col]) for col in key_cols) for row in rows]
    print(json.dumps(payload, ensure_ascii=False))
except Exception:
    print("[]")
"""

    proc = subprocess.run(
        ["ssh", server, "python3", "-", remote_db, table, ",".join(key_cols)],
        input=py_code,
        text=True,
        capture_output=True,
    )
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        raise RuntimeError(
            f"Failed to query remote table {table} on {server}: {stderr or 'remote command failed'}"
        )

    output = (proc.stdout or "").strip()
    if not output:
        return set()
    try:
        payload = json.loads(output)
    except Exception:
        return set()

    result: set[tuple[str, ...]] = set()
    for item in payload:
        if isinstance(item, list):
            result.add(tuple("" if value is None else str(value) for value in item))
    return result


def _local_query_keys(conn: sqlite3.Connection, table: str, key_cols: tuple[str, ...]) -> set[tuple[str, ...]]:
    if not _table_exists(conn, table):
        return set()
    rows = conn.execute(
        f"SELECT {', '.join(key_cols)} FROM {table}",
    ).fetchall()
    return {_serialize_key(_row_key(row, key_cols)) for row in rows}


def _rows_for_keys(
    conn: sqlite3.Connection,
    table: str,
    key_cols: tuple[str, ...],
    keys: set[tuple[str, ...]],
) -> list[sqlite3.Row]:
    if not keys or not _table_exists(conn, table):
        return []

    tmp_name = f"tmp_sync_{table}_keys"
    conn.execute(f"DROP TABLE IF EXISTS {tmp_name}")
    conn.execute(
        f"CREATE TEMP TABLE {tmp_name} ({', '.join(f'{col} TEXT' for col in key_cols)})"
    )
    conn.executemany(
        f"INSERT INTO {tmp_name} VALUES ({', '.join('?' for _ in key_cols)})",
        [tuple("" if value is None else str(value) for value in key) for key in sorted(keys)],
    )

    join_cond = " AND ".join(
        f"CAST(t.{col} AS TEXT) = k.{col}" for col in key_cols
    )
    rows = conn.execute(
        f"SELECT t.* FROM {table} t JOIN {tmp_name} k ON {join_cond}"
    ).fetchall()
    conn.execute(f"DROP TABLE IF EXISTS {tmp_name}")
    return rows


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _normalise_upload_path(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    if text.startswith("uploads/"):
        return text
    if text.startswith("./uploads/"):
        return text[2:]
    if "/uploads/" in text:
        return text[text.index("/uploads/") + 1 :]
    path = Path(text)
    if path.is_absolute():
        try:
            return path.relative_to(REPO_ROOT).as_posix()
        except Exception:
            return ""
    return text


def _bundle_create_table(src_conn: sqlite3.Connection, bundle_conn: sqlite3.Connection, table: str) -> None:
    if _table_exists(bundle_conn, table):
        return
    sql = _table_create_sql(src_conn, table)
    if sql:
        bundle_conn.execute(sql)


def _insert_rows(
    bundle_conn: sqlite3.Connection,
    table: str,
    rows: Iterable[sqlite3.Row],
    columns: list[str],
) -> int:
    if not columns:
        return 0
    placeholders = ", ".join("?" for _ in columns)
    col_sql = ", ".join(columns)
    inserted = 0
    for row in rows:
        values = [row[col] if col in row.keys() else None for col in columns]
        bundle_conn.execute(
            f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})",
            values,
        )
        inserted += 1
    return inserted


def _build_bundle(
    local_main_db: Path,
    local_blob_db: Path,
    remote_main_keys: dict[str, set[tuple[str, ...]]],
    remote_blob_keys: dict[str, set[tuple[str, ...]]],
    bundle_dir: Path,
) -> dict[str, Any]:
    _ensure_dir(bundle_dir)
    bundle_db = bundle_dir / "bundle.sqlite3"
    if bundle_db.exists():
        bundle_db.unlink()

    main_conn = _sqlite_connect(local_main_db)
    blob_conn = _sqlite_connect(local_blob_db)
    bundle_conn = sqlite3.connect(str(bundle_db))
    bundle_conn.row_factory = sqlite3.Row

    manifest: dict[str, Any] = {
        "bundle_db": bundle_db.name,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "tables": {},
        "files": [],
    }
    missing_videos_rows: list[sqlite3.Row] = []
    missing_video_channel_ids: set[str] = set()

    # First pass: determine missing videos and dependent channels.
    remote_videos = remote_main_keys.get("videos", set())
    local_videos = _local_query_keys(main_conn, "videos", ("video_id",))
    missing_videos = local_videos - remote_videos
    if missing_videos:
        missing_videos_rows = _rows_for_keys(main_conn, "videos", ("video_id",), missing_videos)
        for row in missing_videos_rows:
            channel_id = row["channel_id"]
            if channel_id is not None:
                missing_video_channel_ids.add(str(channel_id))

    # Sync channels needed for missing videos, plus channels that are missing outright.
    local_channels_needed: set[tuple[str, ...]] = set()
    if missing_video_channel_ids and _table_exists(main_conn, "channels"):
        channel_id_to_row = {
            _serialize_key(_row_key(row, ("id",)))[0]: row
            for row in _rows_for_keys(
                main_conn,
                "channels",
                ("id",),
                {(cid,) for cid in missing_video_channel_ids},
            )
        }
        local_channels_needed.update({_serialize_key((row["channel_id"],)) for row in channel_id_to_row.values()})

        # Create a lookup from source channel integer id to full channel row.
        dependency_channel_rows = list(channel_id_to_row.values())
    else:
        dependency_channel_rows = []

    # Evaluate all tables.
    table_rows: dict[str, list[sqlite3.Row]] = {}
    table_columns: dict[str, list[str]] = {}

    # channels: include dependency channels and missing channels, deduped by channel_id string.
    if _table_exists(main_conn, "channels"):
        remote_channels = remote_main_keys.get("channels", set())
        local_channels = _local_query_keys(main_conn, "channels", ("channel_id",))
        missing_channels = local_channels - remote_channels

        rows_by_channel_id: dict[str, sqlite3.Row] = {}
        if missing_channels:
            for row in _rows_for_keys(main_conn, "channels", ("channel_id",), missing_channels):
                rows_by_channel_id[str(row["channel_id"])] = row
        for row in dependency_channel_rows:
            rows_by_channel_id[str(row["channel_id"])] = row
        table_rows["channels"] = list(rows_by_channel_id.values())
        table_columns["channels"] = _table_columns(main_conn, "channels")
        manifest["tables"]["channels"] = {
            "local": len(local_channels),
            "remote": len(remote_channels),
            "bundle": len(table_rows["channels"]),
        }

    for table in ("channels_meta", "channel_runtime_state", "video_asr_chunks"):
        if not _table_exists(main_conn, table):
            continue
        key_cols = {
            "channels_meta": ("channel_id",),
            "channel_runtime_state": ("channel_id",),
            "video_asr_chunks": ("video_id", "provider", "model_name", "chunk_index"),
        }[table]
        remote_keys = remote_main_keys.get(table, set())
        local_keys = _local_query_keys(main_conn, table, key_cols)
        missing_keys = local_keys - remote_keys
        rows = _rows_for_keys(main_conn, table, key_cols, missing_keys)
        table_rows[table] = rows
        table_columns[table] = _table_columns(main_conn, table)
        manifest["tables"][table] = {
            "local": len(local_keys),
            "remote": len(remote_keys),
            "bundle": len(rows),
        }

    if missing_videos_rows:
        table_rows["videos"] = missing_videos_rows
        table_columns["videos"] = _table_columns(main_conn, "videos")
        manifest["tables"]["videos"] = {
            "local": len(local_videos),
            "remote": len(remote_videos),
            "bundle": len(missing_videos_rows),
        }

    if _table_exists(blob_conn, "content_blobs"):
        remote_blobs = remote_blob_keys.get("content_blobs", set())
        local_blobs = _local_query_keys(blob_conn, "content_blobs", ("video_id", "content_type"))
        missing_blobs = local_blobs - remote_blobs
        blob_rows = _rows_for_keys(blob_conn, "content_blobs", ("video_id", "content_type"), missing_blobs)
        table_rows["content_blobs"] = blob_rows
        table_columns["content_blobs"] = _table_columns(blob_conn, "content_blobs")
        manifest["tables"]["content_blobs"] = {
            "local": len(local_blobs),
            "remote": len(remote_blobs),
            "bundle": len(blob_rows),
        }

    # File list for uploads assets referenced by missing videos.
    upload_files: set[str] = set()
    for row in missing_videos_rows:
        for col in VIDEO_FILE_COLUMNS:
            rel = _normalise_upload_path(row[col] if col in row.keys() else "")
            if rel:
                path = REPO_ROOT / rel
                if path.exists() and path.is_file():
                    upload_files.add(rel)

    file_list_path = bundle_dir / "files_from.txt"
    file_list_path.write_text("\n".join(sorted(upload_files)) + ("\n" if upload_files else ""), encoding="utf-8")
    manifest["files"] = sorted(upload_files)
    manifest["files_count"] = len(upload_files)

    # Write bundle tables.
    for table, rows in table_rows.items():
        if not rows:
            continue
        src_conn = main_conn if table != "content_blobs" else blob_conn
        _bundle_create_table(src_conn, bundle_conn, table)
        if not _table_exists(bundle_conn, table):
            continue
        cols = table_columns.get(table) or [col for col in _table_columns(src_conn, table) if col != "id"]
        _insert_rows(bundle_conn, table, rows, cols)

    bundle_conn.commit()
    main_conn.close()
    blob_conn.close()
    bundle_conn.close()

    manifest_path = bundle_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


def _copy_backup_file(src: Path, dst: Path) -> None:
    if src.exists():
        shutil.copy2(src, dst)
    for suffix in ("-wal", "-shm"):
        sidecar = Path(str(src) + suffix)
        if sidecar.exists():
            shutil.copy2(sidecar, Path(str(dst) + suffix))


def _backup_databases(db_path: Path, blob_db_path: Path, backup_dir: Path) -> Path:
    _ensure_dir(backup_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    target_dir = backup_dir / f"db_delta_{stamp}"
    _ensure_dir(target_dir)
    _copy_backup_file(db_path, target_dir / db_path.name)
    _copy_backup_file(blob_db_path, target_dir / blob_db_path.name)
    return target_dir


def _ensure_target_schema(main_conn: sqlite3.Connection) -> None:
    main_conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS channels_meta (
            id INTEGER PRIMARY KEY,
            channel_id TEXT,
            video_count INTEGER,
            transcript_count INTEGER,
            total_views INTEGER,
            last_updated TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS channel_runtime_state (
            id INTEGER PRIMARY KEY,
            channel_id TEXT,
            scan_enabled INTEGER,
            skip_reason TEXT,
            source_status TEXT,
            updated_at TEXT,
            created_at TEXT
        );

        CREATE TABLE IF NOT EXISTS video_asr_chunks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            provider TEXT NOT NULL,
            model_name TEXT NOT NULL,
            chunk_index INTEGER NOT NULL,
            chunk_start_ms INTEGER NOT NULL,
            chunk_end_ms INTEGER NOT NULL,
            audio_path TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'pending',
            transcript_text TEXT NOT NULL DEFAULT '',
            language TEXT NOT NULL DEFAULT '',
            raw_response_json TEXT NOT NULL DEFAULT '',
            error_text TEXT NOT NULL DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(video_id, provider, model_name, chunk_index)
        );
        CREATE INDEX IF NOT EXISTS idx_video_asr_chunks_video_status
        ON video_asr_chunks(video_id, status, chunk_index);
        CREATE INDEX IF NOT EXISTS idx_video_asr_chunks_provider_model
        ON video_asr_chunks(provider, model_name, status, chunk_index);
        """
    )


def _load_bundle_rows(bundle_db: Path, table: str) -> list[sqlite3.Row]:
    if not bundle_db.exists():
        return []
    conn = _sqlite_connect(bundle_db)
    try:
        if not _table_exists(conn, table):
            return []
        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
        return rows
    finally:
        conn.close()


def _existing_target_key_map(conn: sqlite3.Connection, table: str, key_cols: tuple[str, ...]) -> set[tuple[str, ...]]:
    return _local_query_keys(conn, table, key_cols)


def _insert_if_missing(
    conn: sqlite3.Connection,
    table: str,
    row: sqlite3.Row,
    key_cols: tuple[str, ...],
    columns: list[str],
) -> bool:
    key_values = _serialize_key(_row_key(row, key_cols))
    placeholders = " AND ".join(f"CAST({col} AS TEXT) = ?" for col in key_cols)
    exists = conn.execute(
        f"SELECT 1 FROM {table} WHERE {placeholders} LIMIT 1",
        key_values,
    ).fetchone()
    if exists:
        return False

    if not columns:
        return False

    insert_cols = ", ".join(columns)
    insert_placeholders = ", ".join("?" for _ in columns)
    values = [row[col] if col in row.keys() else None for col in columns]
    conn.execute(
        f"INSERT INTO {table} ({insert_cols}) VALUES ({insert_placeholders})",
        values,
    )
    return True


def _apply_bundle(
    bundle_db: Path,
    db_path: Path,
    blob_db_path: Path,
    uploads_dir: Path,
    backup_dir: Path,
    verify: bool = True,
) -> dict[str, int]:
    if not bundle_db.exists():
        raise FileNotFoundError(f"Bundle database not found: {bundle_db}")

    backup_path = _backup_databases(db_path, blob_db_path, backup_dir)

    # Ensure the target schema exists before we start inserting.
    target = OptimizedDatabase(str(db_path), str(uploads_dir))
    main_conn = target.conn
    BlobStorage(str(blob_db_path))
    blob_conn = sqlite3.connect(str(blob_db_path))
    blob_conn.row_factory = sqlite3.Row
    bundle_conn = _sqlite_connect(bundle_db)

    inserted: dict[str, int] = {spec.name: 0 for spec in TABLE_SPECS}

    try:
        _ensure_target_schema(main_conn)

        # Channels first so that video foreign keys can be remapped.
        channel_map: dict[int, int] = {}
        channel_lookup: dict[int, str] = {}
        if _table_exists(bundle_conn, "channels"):
            bundle_channel_cols = _table_columns(bundle_conn, "channels")
            target_channel_cols = [col for col in _table_columns(main_conn, "channels") if col != "id"]
            rows = bundle_conn.execute("SELECT * FROM channels ORDER BY id").fetchall()
            for row in rows:
                source_id = int(row["id"])
                source_channel_id = str(row["channel_id"])
                channel_lookup[source_id] = source_channel_id
                existing = main_conn.execute(
                    "SELECT id FROM channels WHERE channel_id = ? LIMIT 1",
                    (source_channel_id,),
                ).fetchone()
                if existing:
                    channel_map[source_id] = int(existing["id"])
                    continue
                cols = [col for col in target_channel_cols if col in bundle_channel_cols]
                values = [row[col] if col in row.keys() else None for col in cols]
                main_conn.execute(
                    f"INSERT INTO channels ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})",
                    values,
                )
                new_id = main_conn.execute(
                    "SELECT id FROM channels WHERE channel_id = ? LIMIT 1",
                    (source_channel_id,),
                ).fetchone()
                if new_id:
                    channel_map[source_id] = int(new_id["id"])
                    inserted["channels"] += 1

        def _apply_simple_table(table: str, key_cols: tuple[str, ...], conn: sqlite3.Connection) -> int:
            if not _table_exists(bundle_conn, table):
                return 0
            bundle_cols = _table_columns(bundle_conn, table)
            target_cols = [col for col in _table_columns(conn, table) if col != "id"]
            rows = bundle_conn.execute(f"SELECT * FROM {table}").fetchall()
            applied = 0
            for row in rows:
                exists = conn.execute(
                    f"""
                    SELECT 1
                    FROM {table}
                    WHERE {' AND '.join(f'CAST({col} AS TEXT) = ?' for col in key_cols)}
                    LIMIT 1
                    """,
                    _serialize_key(_row_key(row, key_cols)),
                ).fetchone()
                if exists:
                    continue
                cols = [col for col in target_cols if col in bundle_cols]
                values = [row[col] if col in row.keys() else None for col in cols]
                conn.execute(
                    f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})",
                    values,
                )
                applied += 1
            return applied

        inserted["channels_meta"] = _apply_simple_table("channels_meta", ("channel_id",), main_conn)
        inserted["channel_runtime_state"] = _apply_simple_table("channel_runtime_state", ("channel_id",), main_conn)

        # Videos need channel_id remapping.
        if _table_exists(bundle_conn, "videos"):
            bundle_cols = _table_columns(bundle_conn, "videos")
            target_cols = [col for col in _table_columns(main_conn, "videos") if col != "id"]
            rows = bundle_conn.execute("SELECT * FROM videos").fetchall()
            for row in rows:
                video_id = str(row["video_id"])
                exists = main_conn.execute(
                    "SELECT 1 FROM videos WHERE video_id = ? LIMIT 1",
                    (video_id,),
                ).fetchone()
                if exists:
                    continue
                source_channel_id = row["channel_id"]
                if source_channel_id is not None:
                    source_channel_id = int(source_channel_id)
                    target_channel_id = channel_map.get(source_channel_id)
                    if target_channel_id is None and source_channel_id in channel_lookup:
                        lookup = main_conn.execute(
                            "SELECT id FROM channels WHERE channel_id = ? LIMIT 1",
                            (channel_lookup[source_channel_id],),
                        ).fetchone()
                        if lookup:
                            target_channel_id = int(lookup["id"])
                            channel_map[source_channel_id] = target_channel_id
                    if target_channel_id is None:
                        raise RuntimeError(f"Missing channel mapping for video {video_id}")
                else:
                    target_channel_id = None
                row_dict = dict(row)
                row_dict["channel_id"] = target_channel_id
                cols = [col for col in target_cols if col in bundle_cols]
                values = [row_dict.get(col) for col in cols]
                main_conn.execute(
                    f"INSERT INTO videos ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})",
                    values,
                )
                inserted["videos"] += 1

        # ASR chunks can be inserted directly.
        inserted["video_asr_chunks"] = _apply_simple_table(
            "video_asr_chunks",
            ("video_id", "provider", "model_name", "chunk_index"),
            main_conn,
        )

        # Blob storage keeps compressed data; insert rows directly.
        if _table_exists(bundle_conn, "content_blobs"):
            bundle_cols = _table_columns(bundle_conn, "content_blobs")
            target_cols = [col for col in _table_columns(blob_conn, "content_blobs") if col != "rowid"]
            rows = bundle_conn.execute("SELECT * FROM content_blobs").fetchall()
            for row in rows:
                exists = blob_conn.execute(
                    """
                    SELECT 1
                    FROM content_blobs
                    WHERE video_id = ? AND content_type = ?
                    LIMIT 1
                    """,
                    (row["video_id"], row["content_type"]),
                ).fetchone()
                if exists:
                    continue
                cols = [col for col in target_cols if col in bundle_cols]
                values = [row[col] if col in row.keys() else None for col in cols]
                blob_conn.execute(
                    f"INSERT INTO content_blobs ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})",
                    values,
                )
                inserted["content_blobs"] += 1

        main_conn.commit()
        blob_conn.commit()

        try:
            target._bump_stats_cache_version()
        except Exception:
            pass

        if verify:
            main_check = main_conn.execute("PRAGMA quick_check").fetchone()[0]
            blob_check = blob_conn.execute("PRAGMA quick_check").fetchone()[0]
            if str(main_check).lower() != "ok":
                raise RuntimeError(f"Main DB quick_check failed: {main_check}")
            if str(blob_check).lower() != "ok":
                raise RuntimeError(f"Blob DB quick_check failed: {blob_check}")

        print(f"Backup saved to: {backup_path}")
        return inserted
    finally:
        bundle_conn.close()
        blob_conn.close()
        target.close()


def _sync_to_server(args: argparse.Namespace) -> int:
    local_main_db = Path(args.local_db).resolve()
    local_blob_db = Path(args.local_blob_db).resolve()
    remote_dir = args.remote_dir.rstrip("/")
    remote_main_db = args.remote_db
    remote_blob_db = args.remote_blob_db

    if not local_main_db.exists():
        raise FileNotFoundError(f"Local DB not found: {local_main_db}")
    if not local_blob_db.exists():
        raise FileNotFoundError(f"Local blob DB not found: {local_blob_db}")

    bundle_dir = Path(args.bundle_dir or f"tmp/db_delta_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    if bundle_dir.is_absolute():
        raise ValueError("bundle_dir must be relative to repo root")
    bundle_abs = REPO_ROOT / bundle_dir
    if bundle_abs.exists():
        shutil.rmtree(bundle_abs)
    _ensure_dir(bundle_abs)

    remote_main_keys: dict[str, set[tuple[str, ...]]] = {}
    remote_blob_keys: dict[str, set[tuple[str, ...]]] = {}
    for spec in TABLE_SPECS:
        if spec.db_kind == "main":
            remote_main_keys[spec.name] = _remote_query_keys(args.server, f"{remote_dir}/{remote_main_db}", spec.name, spec.key_cols)
        else:
            remote_blob_keys[spec.name] = _remote_query_keys(args.server, f"{remote_dir}/{remote_blob_db}", spec.name, spec.key_cols)

    manifest = _build_bundle(
        local_main_db=local_main_db,
        local_blob_db=local_blob_db,
        remote_main_keys=remote_main_keys,
        remote_blob_keys=remote_blob_keys,
        bundle_dir=bundle_abs,
    )

    total_bundle_rows = sum(int(v.get("bundle", 0)) for v in manifest["tables"].values())
    total_files = int(manifest.get("files_count", 0))
    print(f"Bundle prepared at: {bundle_abs}")
    print(f"Tables with rows: {len(manifest['tables'])}, bundled rows: {total_bundle_rows}, files: {total_files}")
    for table, info in manifest["tables"].items():
        print(f"  - {table}: local={info.get('local', 0)} remote={info.get('remote', 0)} bundle={info.get('bundle', 0)}")

    if args.dry_run:
        print("Dry run only; nothing transferred.")
        return 0

    remote_bundle_dir = f"{remote_dir}/{bundle_dir.as_posix()}"
    subprocess.run(
        ["ssh", args.server, f"mkdir -p {remote_bundle_dir}"],
        check=True,
    )
    subprocess.run(
        ["rsync", "-a", f"{bundle_abs.as_posix()}/", f"{args.server}:{remote_bundle_dir}/"],
        check=True,
    )

    files_from = bundle_abs / "files_from.txt"
    if files_from.exists() and files_from.read_text(encoding="utf-8").strip():
        subprocess.run(
            [
                "rsync",
                "-a",
                "--files-from",
                str(files_from),
                "./",
                f"{args.server}:{remote_dir}/",
            ],
            cwd=str(REPO_ROOT),
            check=True,
        )
        print(f"Synced {total_files} upload file(s).")

    remote_cmd = (
        f"cd {shlex.quote(remote_dir)} && "
        f"python3 scripts/sync_missing_rows_to_server.py apply "
        f"--bundle {shlex.quote(f'{bundle_dir.as_posix()}/bundle.sqlite3')} "
        f"--db {shlex.quote(remote_main_db)} "
        f"--blob-db {shlex.quote(remote_blob_db)} "
        f"--backup-dir {shlex.quote(DEFAULT_BACKUP_DIR)}"
    )
    subprocess.run(["ssh", args.server, remote_cmd], check=True)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync only missing DB rows and files to a server")
    sub = parser.add_subparsers(dest="command", required=True)

    sync = sub.add_parser("sync", help="Compare with a remote DB and push a delta bundle to the server")
    sync.add_argument("--server", default=DEFAULT_SERVER)
    sync.add_argument("--remote-dir", default=DEFAULT_REMOTE_DIR)
    sync.add_argument("--local-db", default=str(DEFAULT_LOCAL_DB))
    sync.add_argument("--local-blob-db", default=str(DEFAULT_LOCAL_BLOB_DB))
    sync.add_argument("--remote-db", default="youtube_transcripts.db")
    sync.add_argument("--remote-blob-db", default="youtube_transcripts_blobs.db")
    sync.add_argument("--bundle-dir", default="")
    sync.add_argument("--dry-run", action="store_true")

    apply_p = sub.add_parser("apply", help="Apply a bundle locally")
    apply_p.add_argument("--bundle", required=True)
    apply_p.add_argument("--db", default=str(DEFAULT_LOCAL_DB))
    apply_p.add_argument("--blob-db", default=str(DEFAULT_LOCAL_BLOB_DB))
    apply_p.add_argument("--uploads-dir", default=str(REPO_ROOT / "uploads"))
    apply_p.add_argument("--backup-dir", default=DEFAULT_BACKUP_DIR)
    apply_p.add_argument("--no-verify", action="store_true")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "sync":
        return _sync_to_server(args)

    if args.command == "apply":
        bundle_db = Path(args.bundle).resolve()
        db_path = Path(args.db).resolve()
        blob_db_path = Path(args.blob_db).resolve()
        uploads_dir = Path(args.uploads_dir).resolve()
        backup_dir = Path(args.backup_dir).resolve()
        inserted = _apply_bundle(
            bundle_db=bundle_db,
            db_path=db_path,
            blob_db_path=blob_db_path,
            uploads_dir=uploads_dir,
            backup_dir=backup_dir,
            verify=not args.no_verify,
        )
        print(json.dumps(inserted, indent=2, sort_keys=True))
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
