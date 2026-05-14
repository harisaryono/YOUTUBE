#!/usr/bin/env python3
"""
Optimized Database Module untuk YouTube Transcript Framework
Menangani penyimpanan data video dan references ke file transkrip/resume
"""

import sqlite3
import json
import re
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
from contextlib import contextmanager
from database_blobs import BlobStorage


def _normalize_channel_url(channel_id: str, channel_url: str) -> str:
    token = str(channel_id or "").strip().lstrip("@")
    raw = str(channel_url or "").strip()
    if raw:
        normalized = raw
        if normalized.startswith(("youtube.com/", "www.youtube.com/")):
            normalized = f"https://{normalized}"
        if normalized.startswith(("http://", "https://")):
            normalized = normalized.rstrip("/")
            match = re.search(r"youtube\.com/(?P<path>(?:@|c/|channel/)[^/?#]+)", normalized)
            if match:
                path = match.group("path")
                if path.startswith("channel/"):
                    return f"https://www.youtube.com/{path}"
                if path.startswith("c/"):
                    return f"https://www.youtube.com/{path}"
                if path.startswith("@"):
                    if token.startswith("UC") and re.fullmatch(r"UC[\w-]+", token):
                        return f"https://www.youtube.com/channel/{token}"
                    return f"https://www.youtube.com/{path}"
            return normalized
    if token.startswith("UC") and re.fullmatch(r"UC[\w-]+", token):
        return f"https://www.youtube.com/channel/{token}"
    return f"https://www.youtube.com/@{token}"


def _normalize_channel_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _extract_channel_handle(channel_url: str) -> str:
    raw = str(channel_url or "").strip()
    if not raw:
        return ""
    match = re.search(r"youtube\.com/@(?P<handle>[^/?#]+)", raw, re.I)
    if match:
        return match.group("handle").strip()
    match = re.search(r"youtube\.com/c/(?P<handle>[^/?#]+)", raw, re.I)
    if match:
        return match.group("handle").strip()
    return ""


def _channel_alias_candidates(channel_id: str, channel_name: str = "", channel_url: str = "") -> list[tuple[str, str, str]]:
    candidates: list[tuple[str, str, str]] = []

    def add(alias_value: str, alias_kind: str):
        value = str(alias_value or "").strip()
        if not value:
            return
        alias_key = _normalize_channel_token(value)
        if not alias_key:
            return
        candidate = (value, alias_kind, alias_key)
        if candidate not in candidates:
            candidates.append(candidate)

    raw_id = str(channel_id or "").strip()
    if raw_id:
        add(raw_id, "channel_id")
        if raw_id.startswith("@"):
            add(raw_id.lstrip("@"), "channel_handle")
        elif not raw_id.startswith("UC"):
            add(f"@{raw_id}", "channel_handle")

    handle = _extract_channel_handle(channel_url)
    if handle:
        add(handle, "channel_handle")
        add(f"@{handle.lstrip('@')}", "channel_handle")

    if str(channel_name or "").strip():
        add(channel_name, "display_name")

    return candidates


class OptimizedDatabase:
    def __init__(self, db_path: str = "youtube_transcripts.db", base_dir: str = "uploads"):
        """
        Inisialisasi Optimized Database
        
        Args:
            db_path: Path ke file database
            base_dir: Base directory untuk file uploads
        """
        self.db_path = db_path
        self.base_dir = Path(base_dir)
        self.conn = None
        
        # Initialize Blob Storage (separate DB)
        blob_db_path = str(Path(db_path).parent / "youtube_transcripts_blobs.db")
        self.blob_storage = BlobStorage(blob_db_path)
        
        # Create base directory
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
        self._connect()
        self._create_tables()
    
    def _connect(self):
        """Koneksi ke database dengan check_same_thread=False untuk Flask threading"""
        try:
            # Enable threading for Flask request handling
            self.conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30,
            )
            self.conn.row_factory = sqlite3.Row
            
            # Enable WAL mode for better concurrency
            self.conn.execute('PRAGMA journal_mode=WAL')
            self.conn.execute('PRAGMA busy_timeout=30000')
            self.conn.execute('PRAGMA synchronous=NORMAL')
            
        except Exception as e:
            raise Exception(f"Gagal koneksi database: {str(e)}")
    
    def _create_tables(self):
        """Membuat tabel-tabel yang diperlukan"""
        try:
            cursor = self.conn.cursor()
            
            # Tabel channels
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS channels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel_id TEXT UNIQUE NOT NULL,
                    channel_name TEXT NOT NULL,
                    channel_handle TEXT,
                    channel_url TEXT NOT NULL,
                    subscriber_count INTEGER DEFAULT 0,
                    video_count INTEGER DEFAULT 0,
                    thumbnail_url TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Tabel videos - optimized dengan file paths
            cursor.execute("""
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
                    metadata TEXT,
                    transcript_formatted_path TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (channel_id) REFERENCES channels(id)
                )
            """)
            
            # Tabel cached_stats untuk performa query berat
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cached_stats (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS videos_search_cache (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id TEXT UNIQUE NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT,
                    transcript_search TEXT,
                    summary_search TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_videos_search_cache_video_id
                ON videos_search_cache(video_id)
            """)
            cursor.execute("""
                CREATE VIRTUAL TABLE IF NOT EXISTS videos_search_fts
                USING fts5(title, description, transcript_search, summary_search,
                           content='videos_search_cache', content_rowid='id')
            """)
            cursor.execute("DROP TRIGGER IF EXISTS videos_search_cache_ai")
            cursor.execute("DROP TRIGGER IF EXISTS videos_search_cache_ad")
            cursor.execute("DROP TRIGGER IF EXISTS videos_search_cache_au")
            cursor.execute("""
                CREATE TRIGGER videos_search_cache_ai AFTER INSERT ON videos_search_cache BEGIN
                    INSERT INTO videos_search_fts(rowid, title, description, transcript_search, summary_search)
                    VALUES (new.id, new.title, new.description, new.transcript_search, new.summary_search);
                END
            """)
            cursor.execute("""
                CREATE TRIGGER videos_search_cache_ad AFTER DELETE ON videos_search_cache BEGIN
                    INSERT INTO videos_search_fts(videos_search_fts, rowid, title, description, transcript_search, summary_search)
                    VALUES('delete', old.id, old.title, old.description, old.transcript_search, old.summary_search);
                END
            """)
            cursor.execute("""
                CREATE TRIGGER videos_search_cache_au AFTER UPDATE ON videos_search_cache BEGIN
                    INSERT INTO videos_search_fts(videos_search_fts, rowid, title, description, transcript_search, summary_search)
                    VALUES('delete', old.id, old.title, old.description, old.transcript_search, old.summary_search);
                    INSERT INTO videos_search_fts(rowid, title, description, transcript_search, summary_search)
                    VALUES (new.id, new.title, new.description, new.transcript_search, new.summary_search);
                END
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS channel_aliases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel_db_id INTEGER NOT NULL,
                    alias_value TEXT NOT NULL,
                    alias_key TEXT NOT NULL,
                    alias_kind TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(channel_db_id, alias_key, alias_kind),
                    FOREIGN KEY (channel_db_id) REFERENCES channels(id) ON DELETE CASCADE
                )
            """)
            
            # Index untuk performa
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_videos_video_id ON videos(video_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_videos_channel_id ON videos(channel_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_videos_transcript_downloaded ON videos(transcript_downloaded)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_videos_title ON videos(title)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_videos_is_short ON videos(is_short)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_videos_is_member_only ON videos(is_member_only)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_videos_channel_transcript
                ON videos(channel_id, transcript_downloaded)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_videos_channel_upload_created_id
                ON videos(channel_id, upload_date DESC, created_at DESC, id DESC)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_videos_upload_created_id
                ON videos(upload_date DESC, created_at DESC, id DESC)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_channels_video_count_id
                ON channels(video_count DESC, id DESC)
            """)

            cursor.execute("PRAGMA table_info(videos)")
            video_columns = {str(row[1]) for row in cursor.fetchall()}
            video_retry_columns = [
                ("transcript_retry_after", "TIMESTAMP"),
                ("transcript_retry_reason", "TEXT"),
                ("transcript_retry_count", "INTEGER DEFAULT 0"),
            ]
            for column_name, column_def in video_retry_columns:
                if column_name not in video_columns:
                    cursor.execute(f"ALTER TABLE videos ADD COLUMN {column_name} {column_def}")
            if "channel_rank" not in video_columns:
                cursor.execute("ALTER TABLE videos ADD COLUMN channel_rank INTEGER")
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_videos_channel_rank
                ON videos(channel_id, channel_rank)
            """)

            cursor.execute("PRAGMA table_info(channels)")
            channel_columns = {str(row[1]) for row in cursor.fetchall()}
            if "channel_handle" not in channel_columns:
                cursor.execute("ALTER TABLE channels ADD COLUMN channel_handle TEXT")
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_channels_channel_handle
                ON channels(channel_handle)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_channel_aliases_alias_key
                ON channel_aliases(alias_key)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_channel_aliases_channel_db_id
                ON channel_aliases(channel_db_id)
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS admin_jobs (
                    job_id TEXT PRIMARY KEY,
                    job_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'wrapper',
                    pid INTEGER,
                    command TEXT,
                    log_path TEXT,
                    run_dir TEXT,
                    target_channel_id TEXT,
                    target_video_id TEXT,
                    exit_code INTEGER,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP,
                    finished_at TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_admin_jobs_updated_created_job
                ON admin_jobs(updated_at DESC, created_at DESC, job_id DESC)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_admin_jobs_status_updated_created_job
                ON admin_jobs(status, updated_at DESC, created_at DESC, job_id DESC)
            """)

            cursor.execute("""
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
                )
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_video_asr_chunks_video_status
                ON video_asr_chunks(video_id, status, chunk_index)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_video_asr_chunks_provider_model
                ON video_asr_chunks(provider, model_name, status, chunk_index)
            """)

            cursor.execute("DROP TRIGGER IF EXISTS trg_videos_bump_stats_insert")
            cursor.execute("DROP TRIGGER IF EXISTS trg_videos_bump_stats_update")
            cursor.execute("DROP TRIGGER IF EXISTS trg_videos_bump_stats_delete")
            cursor.execute("DROP TRIGGER IF EXISTS trg_channels_bump_stats_insert")
            cursor.execute("DROP TRIGGER IF EXISTS trg_channels_bump_stats_update")
            cursor.execute("DROP TRIGGER IF EXISTS trg_channels_bump_stats_delete")
            cursor.execute("""
                CREATE TRIGGER trg_videos_bump_stats_insert
                AFTER INSERT ON videos
                BEGIN
                    UPDATE cached_stats
                       SET value = CAST(
                               COALESCE(
                                   (SELECT CAST(value AS INTEGER) FROM cached_stats WHERE key = 'stats_version'),
                                   0
                               ) + 1 AS TEXT
                           ),
                           updated_at = CURRENT_TIMESTAMP
                     WHERE key = 'stats_version';
                    INSERT INTO cached_stats (key, value, updated_at)
                    SELECT 'stats_version', '1', CURRENT_TIMESTAMP
                    WHERE changes() = 0;
                    DELETE FROM cached_stats WHERE key = 'global_stats';
                END;
            """)
            cursor.execute("""
                CREATE TRIGGER trg_videos_bump_stats_update
                AFTER UPDATE ON videos
                BEGIN
                    UPDATE cached_stats
                       SET value = CAST(
                               COALESCE(
                                   (SELECT CAST(value AS INTEGER) FROM cached_stats WHERE key = 'stats_version'),
                                   0
                               ) + 1 AS TEXT
                           ),
                           updated_at = CURRENT_TIMESTAMP
                     WHERE key = 'stats_version';
                    INSERT INTO cached_stats (key, value, updated_at)
                    SELECT 'stats_version', '1', CURRENT_TIMESTAMP
                    WHERE changes() = 0;
                    DELETE FROM cached_stats WHERE key = 'global_stats';
                END;
            """)
            cursor.execute("""
                CREATE TRIGGER trg_videos_bump_stats_delete
                AFTER DELETE ON videos
                BEGIN
                    UPDATE cached_stats
                       SET value = CAST(
                               COALESCE(
                                   (SELECT CAST(value AS INTEGER) FROM cached_stats WHERE key = 'stats_version'),
                                   0
                               ) + 1 AS TEXT
                           ),
                           updated_at = CURRENT_TIMESTAMP
                     WHERE key = 'stats_version';
                    INSERT INTO cached_stats (key, value, updated_at)
                    SELECT 'stats_version', '1', CURRENT_TIMESTAMP
                    WHERE changes() = 0;
                    DELETE FROM cached_stats WHERE key = 'global_stats';
                END;
            """)

            cursor.execute("""
                CREATE TRIGGER trg_channels_bump_stats_insert
                AFTER INSERT ON channels
                BEGIN
                    UPDATE cached_stats
                       SET value = CAST(
                               COALESCE(
                                   (SELECT CAST(value AS INTEGER) FROM cached_stats WHERE key = 'stats_version'),
                                   0
                               ) + 1 AS TEXT
                           ),
                           updated_at = CURRENT_TIMESTAMP
                     WHERE key = 'stats_version';
                    INSERT INTO cached_stats (key, value, updated_at)
                    SELECT 'stats_version', '1', CURRENT_TIMESTAMP
                    WHERE changes() = 0;
                    DELETE FROM cached_stats WHERE key = 'global_stats';
                END;
            """)
            cursor.execute("""
                CREATE TRIGGER trg_channels_bump_stats_update
                AFTER UPDATE ON channels
                BEGIN
                    UPDATE cached_stats
                       SET value = CAST(
                               COALESCE(
                                   (SELECT CAST(value AS INTEGER) FROM cached_stats WHERE key = 'stats_version'),
                                   0
                               ) + 1 AS TEXT
                           ),
                           updated_at = CURRENT_TIMESTAMP
                     WHERE key = 'stats_version';
                    INSERT INTO cached_stats (key, value, updated_at)
                    SELECT 'stats_version', '1', CURRENT_TIMESTAMP
                    WHERE changes() = 0;
                    DELETE FROM cached_stats WHERE key = 'global_stats';
                END;
            """)
            cursor.execute("""
                CREATE TRIGGER trg_channels_bump_stats_delete
                AFTER DELETE ON channels
                BEGIN
                    UPDATE cached_stats
                       SET value = CAST(
                               COALESCE(
                                   (SELECT CAST(value AS INTEGER) FROM cached_stats WHERE key = 'stats_version'),
                                   0
                               ) + 1 AS TEXT
                           ),
                           updated_at = CURRENT_TIMESTAMP
                     WHERE key = 'stats_version';
                    INSERT INTO cached_stats (key, value, updated_at)
                    SELECT 'stats_version', '1', CURRENT_TIMESTAMP
                    WHERE changes() = 0;
                    DELETE FROM cached_stats WHERE key = 'global_stats';
                END;
            """)

            self._backfill_channel_aliases(cursor)
            
            self.conn.commit()
        except Exception as e:
            raise Exception(f"Gagal membuat tabel: {str(e)}")
    
    @contextmanager
    def _get_cursor(self):
        """Context manager untuk cursor dengan automatic commit/rollback"""
        cursor = self.conn.cursor()
        try:
            yield cursor
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise e

    def _upsert_channel_aliases(self, cursor, channel_db_id: int, channel_id: str, channel_name: str = "", channel_url: str = "") -> None:
        for alias_value, alias_kind, alias_key in _channel_alias_candidates(channel_id, channel_name, channel_url):
            cursor.execute(
                """
                INSERT INTO channel_aliases (channel_db_id, alias_value, alias_key, alias_kind, created_at, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT(channel_db_id, alias_key, alias_kind) DO UPDATE SET
                    alias_value = excluded.alias_value,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (int(channel_db_id), alias_value, alias_key, alias_kind),
            )

    def _resolve_channel_row(self, cursor, channel_id: str) -> Optional[sqlite3.Row]:
        raw = str(channel_id or "").strip()
        if not raw:
            return None

        row = cursor.execute("SELECT * FROM channels WHERE channel_id = ? LIMIT 1", (raw,)).fetchone()
        if row:
            return row

        alias_key = _normalize_channel_token(raw)
        if alias_key:
            row = cursor.execute(
                """
                SELECT c.*
                FROM channel_aliases a
                JOIN channels c ON c.id = a.channel_db_id
                WHERE a.alias_key = ?
                ORDER BY
                    CASE a.alias_kind
                        WHEN 'channel_id' THEN 0
                        WHEN 'channel_handle' THEN 1
                        WHEN 'display_name' THEN 2
                        ELSE 3
                    END,
                    c.video_count DESC,
                    c.id DESC
                LIMIT 1
                """,
                (alias_key,),
            ).fetchone()
            if row:
                return row

            row = cursor.execute(
                """
                SELECT *
                FROM channels
                WHERE LOWER(REPLACE(REPLACE(channel_name, ' ', ''), '-', '')) = ?
                LIMIT 1
                """,
                (alias_key,),
            ).fetchone()
            if row:
                return row

        return None

    def _backfill_channel_aliases(self, cursor) -> None:
        cursor.execute("PRAGMA table_info(channel_aliases)")
        alias_columns = {str(row[1]) for row in cursor.fetchall()}
        if not alias_columns:
            return

        rows = cursor.execute(
            """
            SELECT id, channel_id, channel_name, channel_url, COALESCE(channel_handle, '') AS channel_handle
            FROM channels
            """
        ).fetchall()
        for row in rows:
            handle = str(row["channel_handle"] or "").strip()
            if not handle:
                handle = _extract_channel_handle(row["channel_url"])
            if handle:
                cursor.execute(
                    """
                    UPDATE channels
                    SET channel_handle = COALESCE(NULLIF(channel_handle, ''), ?)
                    WHERE id = ?
                    """,
                    (handle, row["id"]),
                )
            self._upsert_channel_aliases(
                cursor,
                int(row["id"]),
                str(row["channel_id"] or ""),
                str(row["channel_name"] or ""),
                str(row["channel_url"] or ""),
            )
    
    def add_channel(self, channel_id: str, channel_name: str, channel_url: str, 
                   subscriber_count: int = 0, video_count: int = 0, 
                   thumbnail_url: str = None) -> int:
        """Menambahkan channel baru ke database"""
        try:
            normalized_channel_url = _normalize_channel_url(channel_id, channel_url)
            handle = _extract_channel_handle(normalized_channel_url) or (
                str(channel_id or "").strip().lstrip("@") if str(channel_id or "").strip().startswith("@") else ""
            )
            with self._get_cursor() as cursor:
                existing = self._resolve_channel_row(cursor, channel_id)
                if existing:
                    resolved_name = str(channel_name or existing["channel_name"] or "").strip() or str(existing["channel_name"] or "")
                    resolved_handle = handle or str(existing["channel_handle"] or "").strip()
                    resolved_url = normalized_channel_url or str(existing["channel_url"] or "")
                    cursor.execute(
                        """
                        UPDATE channels
                        SET channel_name = ?,
                            channel_handle = COALESCE(NULLIF(?, ''), channel_handle),
                            channel_url = ?,
                            subscriber_count = CASE WHEN ? > 0 THEN ? ELSE subscriber_count END,
                            video_count = CASE WHEN ? > 0 THEN ? ELSE video_count END,
                            thumbnail_url = COALESCE(NULLIF(?, ''), thumbnail_url),
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (
                            resolved_name,
                            resolved_handle,
                            resolved_url,
                            int(subscriber_count or 0),
                            int(subscriber_count or 0),
                            int(video_count or 0),
                            int(video_count or 0),
                            thumbnail_url or "",
                            int(existing["id"]),
                        ),
                    )
                    channel_db_id = int(existing["id"])
                else:
                    cursor.execute(
                        """
                        INSERT INTO channels
                        (channel_id, channel_name, channel_handle, channel_url, subscriber_count, video_count, thumbnail_url)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            channel_id,
                            str(channel_name or channel_id or "").strip() or str(channel_id or ""),
                            handle or None,
                            normalized_channel_url,
                            subscriber_count,
                            video_count,
                            thumbnail_url,
                        ),
                    )
                    channel_db_id = int(cursor.lastrowid)

                self._upsert_channel_aliases(cursor, channel_db_id, channel_id, channel_name, normalized_channel_url)
                if handle:
                    self._upsert_channel_aliases(cursor, channel_db_id, handle, channel_name, normalized_channel_url)

            return channel_db_id
            
        except Exception as e:
            raise Exception(f"Gagal menambahkan channel: {str(e)}")
    
    def add_video(self, video_id: str, channel_id: int, title: str, video_url: str,
                  description: str = None, duration: int = None, upload_date: str = None,
                  view_count: int = 0, like_count: int = 0, comment_count: int = 0,
                  thumbnail_url: str = None, metadata: dict = None) -> int:
        """Menambahkan video baru ke database"""
        try:
            # Serialize metadata to JSON string
            metadata_json = json.dumps(metadata) if metadata else None
            metadata_flags = metadata.get('flags') if metadata else {}
            is_member_only = int(bool(
                metadata
                and (
                    metadata.get('member_only')
                    or metadata.get('is_member_only')
                    or (metadata_flags or {}).get('member_only')
                    or metadata.get('upload_date_reason') == 'member_only'
                    or (metadata_flags or {}).get('upload_date_reason') == 'member_only'
                )
            ))
            
            with self._get_cursor() as cursor:
                cursor.execute("""
                    INSERT OR REPLACE INTO videos 
                    (video_id, channel_id, title, description, duration, upload_date, 
                     view_count, like_count, comment_count, video_url, thumbnail_url, metadata, is_member_only)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (video_id, channel_id, title, description, duration, upload_date,
                      view_count, like_count, comment_count, video_url, thumbnail_url, None, is_member_only))

            if metadata_json:
                try:
                    self.save_metadata_content(video_id, metadata_json)
                except Exception:
                    pass
            try:
                self.refresh_video_search_cache(video_id)
            except Exception:
                pass

            # Rank is maintained by the repair script and falls back safely when absent.

            with self._get_cursor() as cursor:
                cursor.execute("SELECT id FROM videos WHERE video_id = ?", (video_id,))
                result = cursor.fetchone()
                return result['id'] if result else None
            
        except Exception as e:
            raise Exception(f"Gagal menambahkan video: {str(e)}")
    
    def update_video_with_transcript(self, video_id: str, transcript_file_path: str,
                                     summary_file_path: str, transcript_language: str,
                                     word_count: int = 0, line_count: int = 0,
                                     transcript_text: Optional[str] = None):
        """Update video dengan info transcript file paths"""
        try:
            if transcript_text is not None:
                try:
                    self.save_transcript_content(video_id, transcript_text)
                except Exception:
                    pass
            fields = [
                "transcript_file_path = ?",
                "summary_file_path = ?",
                "transcript_downloaded = 1",
                "transcript_language = ?",
                "word_count = ?",
                "line_count = ?",
            ]
            params: list[object] = [
                transcript_file_path,
                summary_file_path,
                transcript_language,
                word_count,
                line_count,
            ]
            if transcript_text is not None:
                fields.append("transcript_text = ?")
                params.append(transcript_text)
            fields.extend([
                "transcript_retry_after = NULL",
                "transcript_retry_reason = NULL",
                "transcript_retry_count = 0",
                "updated_at = CURRENT_TIMESTAMP",
            ])
            params.append(video_id)

            with self._get_cursor() as cursor:
                cursor.execute(
                    f"""
                    UPDATE videos
                    SET {", ".join(fields)}
                    WHERE video_id = ?
                    """,
                    params,
                )
            try:
                self.refresh_video_search_cache(video_id)
            except Exception:
                pass
            
        except Exception as e:
            raise Exception(f"Gagal update video dengan transcript: {str(e)}")

    def upsert_video_asr_chunk(
        self,
        video_id: str,
        provider: str,
        model_name: str,
        chunk_index: int,
        chunk_start_ms: int,
        chunk_end_ms: int,
        audio_path: str,
        status: str,
        transcript_text: str = "",
        language: str = "",
        raw_response_json: str = "",
        error_text: str = "",
    ) -> None:
        """Simpan hasil ASR per chunk agar job bisa di-resume."""
        try:
            with self._get_cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO video_asr_chunks (
                        video_id, provider, model_name, chunk_index,
                        chunk_start_ms, chunk_end_ms, audio_path,
                        status, transcript_text, language,
                        raw_response_json, error_text, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT(video_id, provider, model_name, chunk_index) DO UPDATE SET
                        chunk_start_ms = excluded.chunk_start_ms,
                        chunk_end_ms = excluded.chunk_end_ms,
                        audio_path = excluded.audio_path,
                        status = excluded.status,
                        transcript_text = excluded.transcript_text,
                        language = excluded.language,
                        raw_response_json = excluded.raw_response_json,
                        error_text = excluded.error_text,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        video_id,
                        provider,
                        model_name,
                        int(chunk_index),
                        int(chunk_start_ms),
                        int(chunk_end_ms),
                        str(audio_path or ""),
                        str(status or "pending"),
                        str(transcript_text or ""),
                        str(language or ""),
                        str(raw_response_json or ""),
                        str(error_text or ""),
                    ),
                )
        except Exception as e:
            raise Exception(f"Gagal upsert chunk ASR: {str(e)}")

    def update_video_with_summary(self, video_id: str, summary_file_path: str, summary_text: str = ""):
        """Persist hasil resume langsung ke DB."""
        try:
            if summary_text:
                try:
                    self.save_summary_content(video_id, summary_text)
                except Exception:
                    pass
            with self._get_cursor() as cursor:
                cursor.execute("""
                    UPDATE videos
                    SET summary_file_path = ?,
                        summary_text = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE video_id = ?
                """, (summary_file_path, summary_text, video_id))
            try:
                self.refresh_video_search_cache(video_id)
            except Exception:
                pass
        except Exception as e:
            raise Exception(f"Gagal update video dengan summary: {str(e)}")

    def update_video_with_formatted(self, video_id: str, formatted_file_path: str):
        """Persist hasil formatting langsung ke DB."""
        try:
            with self._get_cursor() as cursor:
                cursor.execute("""
                    UPDATE videos
                    SET transcript_formatted_path = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE video_id = ?
                """, (formatted_file_path, video_id))
        except Exception as e:
            raise Exception(f"Gagal update video dengan formatted transcript: {str(e)}")

    def mark_video_transcript_retry_later(self, video_id: str, reason: str, retry_after_hours: int = 24):
        """Tandai video untuk dicoba ulang nanti tanpa mengubah state transcript utama."""
        hours = max(1, int(retry_after_hours or 24))
        modifier = f"+{hours} hours"
        try:
            with self._get_cursor() as cursor:
                cursor.execute("""
                    UPDATE videos
                    SET transcript_retry_after = datetime('now', ?),
                        transcript_retry_reason = ?,
                        transcript_retry_count = COALESCE(transcript_retry_count, 0) + 1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE video_id = ?
                """, (modifier, str(reason or "")[:500], video_id))
        except Exception as e:
            raise Exception(f"Gagal menandai retry later untuk transcript: {str(e)}")

    def save_transcript_content(self, video_id: str, content: str):
        """Simpan konten transcript ke Blob Storage"""
        ok = self.blob_storage.save_blob(video_id, 'transcript', content)
        if ok:
            try:
                self.refresh_video_search_cache(video_id)
            except Exception:
                pass
        return ok

    def save_summary_content(self, video_id: str, content: str):
        """Simpan konten summary ke Blob Storage"""
        ok = self.blob_storage.save_blob(video_id, 'resume', content)
        if ok:
            try:
                self.refresh_video_search_cache(video_id)
            except Exception:
                pass
        return ok

    def save_formatted_content(self, video_id: str, content: str):
        """Simpan konten formatted transcript ke Blob Storage"""
        return self.blob_storage.save_blob(video_id, 'formatted', content)

    def save_metadata_content(self, video_id: str, content: str):
        """Simpan metadata mentah ke Blob Storage."""
        return self.blob_storage.save_blob(video_id, 'metadata', content)

    def _build_search_cache_payload(self, video_id: str) -> Optional[Dict[str, object]]:
        try:
            with self._get_cursor() as cursor:
                cursor.execute(
                    """
                    SELECT video_id, title, description
                    FROM videos
                    WHERE video_id = ?
                    LIMIT 1
                    """,
                    (video_id,),
                )
                row = cursor.fetchone()
                if not row:
                    return None
                transcript_search = self.read_transcript(video_id) or ""
                summary_search = self.read_summary(video_id) or ""
                return {
                    "video_id": str(row["video_id"] or "").strip(),
                    "title": str(row["title"] or "").strip(),
                    "description": str(row["description"] or ""),
                    "transcript_search": transcript_search,
                    "summary_search": summary_search,
                }
        except Exception:
            return None

    def refresh_video_search_cache(self, video_id: str) -> None:
        """Refresh blob-first FTS search cache for one video."""
        payload = self._build_search_cache_payload(video_id)
        try:
            with self._get_cursor() as cursor:
                if not payload:
                    cursor.execute("DELETE FROM videos_search_cache WHERE video_id = ?", (video_id,))
                    return
                cursor.execute(
                    """
                    INSERT INTO videos_search_cache (
                        video_id, title, description, transcript_search, summary_search, updated_at
                    ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(video_id) DO UPDATE SET
                        title = excluded.title,
                        description = excluded.description,
                        transcript_search = excluded.transcript_search,
                        summary_search = excluded.summary_search,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        payload["video_id"],
                        payload["title"],
                        payload["description"],
                        payload["transcript_search"],
                        payload["summary_search"],
                    ),
                )
        except Exception:
            pass

    def get_metadata_content(self, video_id: str) -> Optional[str]:
        """Ambil metadata mentah dari blob dulu, fallback ke kolom lama."""
        content = self.blob_storage.get_blob(video_id, 'metadata')
        if content:
            return content
        try:
            with self._get_cursor() as cursor:
                cursor.execute("SELECT metadata FROM videos WHERE video_id = ? LIMIT 1", (video_id,))
                row = cursor.fetchone()
                if row and row["metadata"]:
                    return str(row["metadata"])
        except Exception:
            pass
        return None
    
    def get_channel_by_id(self, channel_id: str) -> Optional[Dict]:
        """Mendapatkan info channel berdasarkan channel_id"""
        try:
            with self._get_cursor() as cursor:
                result = self._resolve_channel_row(cursor, channel_id)
                return dict(result) if result else None
        except Exception as e:
            raise Exception(f"Gagal mengambil channel: {str(e)}")
    
    def get_all_channels(self, limit: int = None, offset: int = 0) -> List[Dict]:
        """Mendapatkan semua channel"""
        try:
            with self._get_cursor() as cursor:
                query = """
                    SELECT c.*
                    FROM channels c
                    ORDER BY c.video_count DESC, c.id DESC
                """

                if limit is not None:
                    query += " LIMIT ? OFFSET ?"
                    cursor.execute(query, (limit, offset))
                else:
                    cursor.execute(query)

                rows = [dict(row) for row in cursor.fetchall()]
                if not rows:
                    return rows

                channel_ids = [row["id"] for row in rows]
                placeholders = ",".join("?" for _ in channel_ids)

                cursor.execute(
                    f"""
                    SELECT channel_id, COUNT(*) AS count
                    FROM videos
                    WHERE channel_id IN ({placeholders})
                      AND is_short = 0
                      AND is_member_only = 0
                    GROUP BY channel_id
                    """,
                    channel_ids,
                )
                actual_video_counts = {
                    row["channel_id"]: int(row["count"])
                    for row in cursor.fetchall()
                }

                cursor.execute(
                    f"""
                    SELECT channel_id, COUNT(*) AS count
                    FROM videos
                    WHERE channel_id IN ({placeholders})
                      AND transcript_downloaded = 1
                      AND is_short = 0
                      AND is_member_only = 0
                    GROUP BY channel_id
                    """,
                    channel_ids,
                )
                transcript_counts = {
                    row["channel_id"]: int(row["count"])
                    for row in cursor.fetchall()
                }

                for row in rows:
                    cid = row["id"]
                    row["actual_video_count"] = actual_video_counts.get(cid, 0)
                    row["transcript_count"] = transcript_counts.get(cid, 0)

                return rows

        except Exception as e:
            raise Exception(f"Gagal mengambil channels: {str(e)}")

    def count_all_channels(self) -> int:
        """Menghitung total channel."""
        try:
            with self._get_cursor() as cursor:
                cursor.execute("SELECT COUNT(*) as count FROM channels")
                result = cursor.fetchone()
                return int(result["count"]) if result else 0
        except Exception as e:
            raise Exception(f"Gagal menghitung channels: {str(e)}")
    
    def get_video_by_id(self, video_id: str) -> Optional[Dict]:
        """Mendapatkan info video lengkap berdasarkan video_id"""
        try:
            with self._get_cursor() as cursor:
                cursor.execute("""
                    SELECT v.id, v.video_id, v.title, v.video_url, v.description, v.duration, 
                           v.upload_date, v.view_count, v.thumbnail_url, v.transcript_downloaded,
                           v.transcript_language, v.transcript_file_path, v.summary_file_path,
                           v.transcript_formatted_path,
                           v.word_count, v.line_count, v.created_at, v.metadata, v.is_short,
                           v.is_member_only,
                           c.channel_id, c.channel_name, c.channel_url, c.thumbnail_url as channel_thumbnail
                    FROM videos v
                    JOIN channels c ON v.channel_id = c.id
                    WHERE v.video_id = ?
                """, (video_id,))
                result = cursor.fetchone()
                if not result:
                    return None
                video = dict(result)
                metadata_raw = self.get_metadata_content(video_id)
                if metadata_raw:
                    video["metadata"] = metadata_raw
                return video
        except Exception as e:
            raise Exception(f"Gagal mengambil video: {str(e)}")

    def has_active_admin_job(self, *, target_video_id: str = "", target_channel_id: str = "", job_type: str = "") -> bool:
        """Check whether there is an active admin job for a target."""
        try:
            return self.get_active_admin_job(
                target_video_id=target_video_id,
                target_channel_id=target_channel_id,
                job_type=job_type,
            ) is not None
        except Exception:
            return False

    def get_active_admin_job(self, *, target_video_id: str = "", target_channel_id: str = "", job_type: str = "") -> Dict | None:
        """Return the most recent active admin job for a target, if any."""
        try:
            with self._get_cursor() as cursor:
                query = """
                    SELECT *
                    FROM admin_jobs
                    WHERE status IN ('queued', 'running', 'in_progress')
                """
                params: list[object] = []
                if target_video_id:
                    query += " AND COALESCE(target_video_id, '') = ?"
                    params.append(target_video_id)
                if target_channel_id:
                    query += " AND COALESCE(target_channel_id, '') = ?"
                    params.append(target_channel_id)
                if job_type:
                    query += " AND job_type = ?"
                    params.append(job_type)
                query += " ORDER BY updated_at DESC, created_at DESC, job_id DESC LIMIT 1"
                cursor.execute(query, params)
                row = cursor.fetchone()
                return dict(row) if row else None
        except Exception:
            return None
    
    def get_videos_by_channel(self, channel_id: str,
                             transcript_downloaded: bool = None,
                             limit: int = None, offset: int = 0) -> List[Dict]:
        """Mendapatkan daftar video dari channel tertentu"""
        try:
            with self._get_cursor() as cursor:
                resolved = self._resolve_channel_row(cursor, channel_id)
                if not resolved:
                    return []
                query = """
                    SELECT v.id, v.video_id, v.title, v.video_url, v.duration, v.upload_date, 
                           v.view_count, v.thumbnail_url, v.transcript_downloaded, v.word_count,
                           COALESCE(v.summary_file_path, '') AS summary_file_path,
                           c.channel_id, c.channel_name
                    FROM videos v
                    JOIN channels c ON v.channel_id = c.id
                    WHERE v.channel_id = ?
                      AND v.is_short = 0
                      AND v.is_member_only = 0
                """
                params = [int(resolved["id"])]

                if transcript_downloaded is not None:
                    query += " AND v.transcript_downloaded = ?"
                    params.append(transcript_downloaded)

                query += """
                    ORDER BY
                        CASE WHEN v.channel_rank IS NULL THEN 1 ELSE 0 END,
                        v.channel_rank ASC,
                        v.upload_date DESC,
                        v.created_at DESC,
                        v.id DESC
                    LIMIT ? OFFSET ?
                """
                params.extend([limit or 50, offset])

                cursor.execute(query, params)
                results = cursor.fetchall()

                return [dict(row) for row in results]

        except Exception as e:
            raise Exception(f"Gagal mengambil videos channel: {str(e)}")

    def get_total_videos_by_channel(self, channel_id: str) -> int:
        """Get total video count for a channel"""
        try:
            with self._get_cursor() as cursor:
                resolved = self._resolve_channel_row(cursor, channel_id)
                if not resolved:
                    return 0
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM videos v
                    JOIN channels c ON v.channel_id = c.id
                    WHERE v.channel_id = ?
                      AND v.is_short = 0
                      AND v.is_member_only = 0
                """, [int(resolved["id"])])
                result = cursor.fetchone()
                return result['count'] if result else 0
        except Exception as e:
            raise Exception(f"Gagal menghitung total videos channel: {str(e)}")

    def get_transcript_count_by_channel(self, channel_id: str) -> int:
        """Get total transcript count for a channel."""
        try:
            with self._get_cursor() as cursor:
                resolved = self._resolve_channel_row(cursor, channel_id)
                if not resolved:
                    return 0
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM videos v
                    JOIN channels c ON v.channel_id = c.id
                    WHERE v.channel_id = ?
                      AND v.transcript_downloaded = 1
                      AND v.is_short = 0
                      AND v.is_member_only = 0
                """, [int(resolved["id"])])
                result = cursor.fetchone()
                return result['count'] if result else 0
        except Exception as e:
            raise Exception(f"Gagal menghitung transcript channel: {str(e)}")

    def get_channel_aliases(self, channel_id: str) -> List[Dict]:
        """Return all aliases for a channel."""
        try:
            with self._get_cursor() as cursor:
                resolved = self._resolve_channel_row(cursor, channel_id)
                if not resolved:
                    return []
                cursor.execute(
                    """
                    SELECT alias_value, alias_key, alias_kind, created_at, updated_at
                    FROM channel_aliases
                    WHERE channel_db_id = ?
                    ORDER BY
                        CASE alias_kind
                            WHEN 'channel_id' THEN 0
                            WHEN 'channel_handle' THEN 1
                            WHEN 'display_name' THEN 2
                            ELSE 3
                        END,
                        alias_value ASC
                    """,
                    (int(resolved["id"]),),
                )
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            raise Exception(f"Gagal mengambil alias channel: {str(e)}")

    def search_channels(self, query: str, limit: int = 20) -> List[Dict]:
        """Search channels by id, handle, display name, or alias."""
        try:
            q = str(query or "").strip()
            if not q:
                return []
            token = q.lower()
            alias_key = _normalize_channel_token(q)
            like = f"%{token}%"
            alias_like = f"%{alias_key}%"
            with self._get_cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        c.*,
                        COALESCE(alias_counts.alias_count, 0) AS alias_count
                    FROM channels c
                    LEFT JOIN (
                        SELECT channel_db_id, COUNT(*) AS alias_count
                        FROM channel_aliases
                        GROUP BY channel_db_id
                    ) alias_counts ON alias_counts.channel_db_id = c.id
                    WHERE
                        LOWER(c.channel_id) LIKE ?
                        OR LOWER(COALESCE(c.channel_handle, '')) LIKE ?
                        OR LOWER(c.channel_name) LIKE ?
                        OR EXISTS (
                            SELECT 1 FROM channel_aliases a
                            WHERE a.channel_db_id = c.id
                              AND a.alias_key LIKE ?
                        )
                    ORDER BY
                        CASE
                            WHEN LOWER(c.channel_id) = ? THEN 0
                            WHEN LOWER(COALESCE(c.channel_handle, '')) = ? THEN 1
                            WHEN LOWER(c.channel_name) = ? THEN 2
                            WHEN EXISTS (
                                SELECT 1 FROM channel_aliases a2
                                WHERE a2.channel_db_id = c.id
                                  AND a2.alias_key = ?
                            ) THEN 3
                            ELSE 9
                        END,
                        c.video_count DESC,
                        c.id DESC
                    LIMIT ?
                    """,
                    (
                        like,
                        like,
                        like,
                        alias_like,
                        token,
                        token,
                        q,
                        alias_key,
                        int(limit or 20),
                    ),
                )
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            raise Exception(f"Gagal mencari channel: {str(e)}")

    def rebuild_channel_aliases(self, channel_id: str | None = None) -> int:
        """Rebuild alias table from the current channel records."""
        try:
            with self._get_cursor() as cursor:
                if channel_id:
                    resolved = self._resolve_channel_row(cursor, channel_id)
                    if not resolved:
                        return 0
                    rows = [resolved]
                    cursor.execute("DELETE FROM channel_aliases WHERE channel_db_id = ?", (int(resolved["id"]),))
                else:
                    rows = cursor.execute(
                        """
                        SELECT id, channel_id, channel_name, channel_url, COALESCE(channel_handle, '') AS channel_handle
                        FROM channels
                        """
                    ).fetchall()
                    cursor.execute("DELETE FROM channel_aliases")

                total = 0
                for row in rows:
                    handle = str(row["channel_handle"] or "").strip()
                    if not handle:
                        handle = _extract_channel_handle(row["channel_url"])
                    if handle:
                        cursor.execute(
                            """
                            UPDATE channels
                            SET channel_handle = COALESCE(NULLIF(channel_handle, ''), ?)
                            WHERE id = ?
                            """,
                            (handle, int(row["id"])),
                        )
                    self._upsert_channel_aliases(
                        cursor,
                        int(row["id"]),
                        str(row["channel_id"] or ""),
                        str(row["channel_name"] or ""),
                        str(row["channel_url"] or ""),
                    )
                    total += 1
                return total
        except Exception as e:
            raise Exception(f"Gagal membangun ulang alias channel: {str(e)}")

    def get_adjacent_videos_by_video_id(self, video_id: str) -> Dict[str, Optional[Dict]]:
        """Get adjacent videos within the same channel ordering.

        The public UI walks the channel list from newest to oldest, so
        ``previous`` is treated as the older neighbor and ``next`` as the newer
        neighbor to match how the legacy detail page is used.
        """
        try:
            with self._get_cursor() as cursor:
                current = cursor.execute("""
                    SELECT v.id, v.video_id, v.title, v.upload_date, v.created_at, v.channel_id, v.channel_rank
                    FROM videos v
                    WHERE v.video_id = ?
                    LIMIT 1
                """, (video_id,)).fetchone()
                if not current:
                    return {"previous": None, "next": None}

                def _is_member_only_row(row) -> bool:
                    if not row:
                        return False
                    if int(row["is_member_only"] or 0):
                        return True
                    metadata_raw = row["metadata"] if "metadata" in row.keys() else None
                    if not metadata_raw and "video_id" in row.keys():
                        metadata_raw = self.get_metadata_content(str(row["video_id"]))
                    if not metadata_raw:
                        return False
                    try:
                        metadata = json.loads(metadata_raw)
                    except Exception:
                        return False
                    flags = metadata.get("flags") or {}
                    return bool(
                        metadata.get("member_only")
                        or flags.get("member_only")
                        or metadata.get("upload_date_reason") == "member_only"
                        or flags.get("upload_date_reason") == "member_only"
                    )

                def _row_to_nav(row):
                    return {
                        "id": row["id"],
                        "video_id": row["video_id"],
                        "title": row["title"],
                    }

                previous_video = None
                next_video = None
                if current["channel_rank"] is not None:
                    current_rank = int(current["channel_rank"])

                    previous_row = cursor.execute("""
                        SELECT v.id, v.video_id, v.title, v.is_short, v.is_member_only, v.metadata
                        FROM videos v
                        WHERE v.channel_id = ?
                          AND v.channel_rank = ?
                          AND (v.is_short = 0 OR v.is_short IS NULL)
                    """, (
                        current["channel_id"],
                        current_rank + 1,
                    )).fetchone()
                    if previous_row and not _is_member_only_row(previous_row):
                        previous_video = _row_to_nav(previous_row)

                    next_row = cursor.execute("""
                        SELECT v.id, v.video_id, v.title, v.is_short, v.is_member_only, v.metadata
                        FROM videos v
                        WHERE v.channel_id = ?
                          AND v.channel_rank = ?
                          AND (v.is_short = 0 OR v.is_short IS NULL)
                    """, (
                        current["channel_id"],
                        current_rank - 1,
                    )).fetchone()
                    if next_row and not _is_member_only_row(next_row):
                        next_video = _row_to_nav(next_row)

                if previous_video is None and next_video is None:
                    ordered_candidates = cursor.execute("""
                        SELECT v.id, v.video_id, v.title, v.is_short, v.is_member_only, v.metadata
                        FROM videos v
                        WHERE v.channel_id = ?
                          AND (v.is_short = 0 OR v.is_short IS NULL)
                        ORDER BY
                            CASE WHEN v.channel_rank IS NULL THEN 1 ELSE 0 END,
                            v.channel_rank ASC,
                            COALESCE(NULLIF(v.upload_date, ''), '00000000') DESC,
                            COALESCE(NULLIF(v.created_at, ''), '0000-00-00 00:00:00') DESC,
                            v.id DESC
                    """, (
                        current["channel_id"],
                    )).fetchall()
                    ordered_videos = [row for row in ordered_candidates if not _is_member_only_row(row)]
                    current_index = next(
                        (idx for idx, row in enumerate(ordered_videos) if row["id"] == current["id"]),
                        None,
                    )
                    if current_index is None:
                        return {"previous": None, "next": None}

                    if current_index + 1 < len(ordered_videos):
                        previous_video = _row_to_nav(ordered_videos[current_index + 1])
                    if current_index - 1 >= 0:
                        next_video = _row_to_nav(ordered_videos[current_index - 1])

                return {"previous": previous_video, "next": next_video}
        except Exception as e:
            raise Exception(f"Gagal mengambil video sebelumnya/selanjutnya: {str(e)}")

    def recompute_channel_ranks(self, channel_id: Optional[int] = None) -> int:
        """Recompute explicit channel-local ranks for public videos.

        Rank 1 means the newest visible video in a channel.
        """
        try:
            with self._get_cursor() as cursor:
                if channel_id is None:
                    channels = cursor.execute("SELECT id FROM channels ORDER BY id").fetchall()
                    channel_ids = [row["id"] for row in channels]
                else:
                    channel_ids = [channel_id]

                total_updated = 0
                for ch_id in channel_ids:
                    rows = cursor.execute("""
                        SELECT id
                        FROM videos
                        WHERE channel_id = ?
                          AND (is_short = 0 OR is_short IS NULL)
                          AND (is_member_only = 0 OR is_member_only IS NULL)
                        ORDER BY
                            COALESCE(NULLIF(upload_date, ''), '00000000') DESC,
                            COALESCE(NULLIF(created_at, ''), '0000-00-00 00:00:00') DESC,
                            id DESC
                    """, (ch_id,)).fetchall()

                    batch = []
                    for rank, row in enumerate(rows, start=1):
                        batch.append((rank, row["id"]))
                        if len(batch) >= 25:
                            cursor.executemany(
                                "UPDATE videos SET channel_rank = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                                batch,
                            )
                            total_updated += len(batch)
                            batch.clear()
                    if batch:
                        cursor.executemany(
                            "UPDATE videos SET channel_rank = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                            batch,
                        )
                        total_updated += len(batch)
                        batch.clear()
                    self.conn.commit()

                return total_updated
        except Exception as e:
            raise Exception(f"Gagal recompute channel rank: {str(e)}")
    
    def get_all_videos(self, transcript_downloaded: bool = None, 
                       limit: int = 50, offset: int = 0,
                       force_refresh: bool = False) -> List[Dict]:
        """Mendapatkan semua video dengan pagination dan caching."""
        cache_key = f'all_videos_{transcript_downloaded}_{limit}_{offset}'
        if not force_refresh:
            try:
                with self._get_cursor() as cursor:
                    cursor.execute("SELECT value, updated_at FROM cached_stats WHERE key = ?", (cache_key,))
                    row = cursor.fetchone()
                    if row:
                        from datetime import datetime
                        updated_at = datetime.fromisoformat(row['updated_at'])
                        if (datetime.now() - updated_at).total_seconds() < 300: # 5 Menit
                            return json.loads(row['value'])
            except Exception:
                pass

        try:
            with self._get_cursor() as cursor:
                query = """
                    WITH ranked_videos AS (
                        SELECT v.id, v.video_id, v.channel_id, v.title, v.video_url, v.duration,
                               v.upload_date, v.view_count, v.thumbnail_url, v.transcript_downloaded,
                               v.word_count, v.created_at
                        FROM videos v
                        WHERE (v.is_short = 0 OR v.is_short IS NULL)
                          AND (v.is_member_only = 0 OR v.is_member_only IS NULL)
                """
                params = []
                
                if transcript_downloaded is not None:
                    query += " AND v.transcript_downloaded = ?"
                    params.append(1 if transcript_downloaded else 0)
                
                query += """
                        ORDER BY
                            v.upload_date DESC,
                            v.created_at DESC,
                            v.id DESC
                        LIMIT ? OFFSET ?
                    )
                    SELECT v.id, v.video_id, v.title, v.video_url, v.duration, v.upload_date,
                           v.view_count, v.thumbnail_url, v.transcript_downloaded, v.word_count, v.created_at,
                           c.channel_id, c.channel_name
                    FROM ranked_videos v
                    JOIN channels c ON v.channel_id = c.id
                """
                params.extend([limit, offset])
                
                cursor.execute(query, params)
                results = cursor.fetchall()
                data = [dict(row) for row in results]
                
                # Update cache
                try:
                    cursor.execute(
                        "INSERT OR REPLACE INTO cached_stats (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                        (cache_key, json.dumps(data))
                    )
                except Exception:
                    pass
                
                return data
            
        except Exception as e:
            raise Exception(f"Gagal mengambil videos: {str(e)}")

    def get_latest_videos_per_channel(self, limit: int = 12, force_refresh: bool = False) -> List[Dict]:
        """Mendapatkan video terbaru per channel, lalu diurutkan berdasarkan recency channel dengan caching."""
        cache_key = f'latest_videos_per_channel_{limit}'
        if not force_refresh:
            try:
                with self._get_cursor() as cursor:
                    cursor.execute("SELECT value, updated_at FROM cached_stats WHERE key = ?", (cache_key,))
                    row = cursor.fetchone()
                    if row:
                        from datetime import datetime
                        updated_at = datetime.fromisoformat(row['updated_at'])
                        if (datetime.now() - updated_at).total_seconds() < 600: # 10 Menit
                            return json.loads(row['value'])
            except Exception:
                pass

        try:
            with self._get_cursor() as cursor:
                cursor.execute(
                    """
                    WITH ranked AS (
                        SELECT v.id, v.video_id, v.title, v.video_url, v.duration, v.upload_date,
                               v.view_count, v.thumbnail_url, v.transcript_downloaded, v.word_count,
                               v.created_at, c.channel_id, c.channel_name,
                               ROW_NUMBER() OVER (
                                   PARTITION BY c.id
                                   ORDER BY
                                       v.upload_date DESC,
                                       v.created_at DESC,
                                       v.id DESC
                               ) AS rn
                        FROM videos v
                        JOIN channels c ON v.channel_id = c.id
                        WHERE (v.is_short = 0 OR v.is_short IS NULL)
                          AND (v.is_member_only = 0 OR v.is_member_only IS NULL)
                    )
                    SELECT *
                    FROM ranked
                    WHERE rn = 1
                    ORDER BY
                        upload_date DESC,
                        created_at DESC,
                        id DESC
                    LIMIT ?
                    """,
                    (limit,),
                )
                results = cursor.fetchall()
                data = [dict(row) for row in results]
                
                # Update cache
                try:
                    cursor.execute(
                        "INSERT OR REPLACE INTO cached_stats (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                        (cache_key, json.dumps(data))
                    )
                except Exception:
                    pass
                    
                return data
        except Exception as e:
            raise Exception(f"Gagal mengambil latest videos per channel: {str(e)}")

    def get_latest_videos(self, limit: int = 12, force_refresh: bool = False) -> List[Dict]:
        """Mendapatkan video terbaru secara global dengan caching."""
        cache_key = f'latest_videos_global_{limit}'
        if not force_refresh:
            try:
                with self._get_cursor() as cursor:
                    cursor.execute("SELECT value, updated_at FROM cached_stats WHERE key = ?", (cache_key,))
                    row = cursor.fetchone()
                    if row:
                        from datetime import datetime
                        updated_at = datetime.fromisoformat(row['updated_at'])
                        if (datetime.now() - updated_at).total_seconds() < 600: # 10 Menit
                            return json.loads(row['value'])
            except Exception:
                pass

        try:
            with self._get_cursor() as cursor:
                cursor.execute(
                    """
                    WITH ranked_videos AS (
                        SELECT v.id, v.video_id, v.channel_id, v.title, v.video_url, v.duration,
                               v.upload_date, v.view_count, v.thumbnail_url, v.transcript_downloaded,
                               v.word_count, v.created_at
                        FROM videos v
                        WHERE (v.is_short = 0 OR v.is_short IS NULL)
                          AND (v.is_member_only = 0 OR v.is_member_only IS NULL)
                        ORDER BY
                            v.upload_date DESC,
                            v.created_at DESC,
                            v.id DESC
                        LIMIT ?
                    )
                    SELECT v.id, v.video_id, v.title, v.video_url, v.duration, v.upload_date,
                           v.view_count, v.thumbnail_url, v.transcript_downloaded, v.word_count,
                           v.created_at, c.channel_id, c.channel_name
                    FROM ranked_videos v
                    JOIN channels c ON v.channel_id = c.id
                    """,
                    (limit,),
                )
                results = cursor.fetchall()
                data = [dict(row) for row in results]

                # Update cache
                try:
                    cursor.execute(
                        "INSERT OR REPLACE INTO cached_stats (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                        (cache_key, json.dumps(data))
                    )
                except Exception:
                    pass
                
                return data
        except Exception as e:
            raise Exception(f"Gagal mengambil latest videos: {str(e)}")

    def count_all_videos(self, transcript_downloaded: bool = None) -> int:
        """Menghitung total video dengan optional filter transcript"""
        try:
            with self._get_cursor() as cursor:
                query = """
                    SELECT COUNT(*) as count
                    FROM videos v
                    WHERE v.is_short = 0
                      AND v.is_member_only = 0
                """
                params = []

                if transcript_downloaded is not None:
                    query += " AND v.transcript_downloaded = ?"
                    params.append(transcript_downloaded)

                cursor.execute(query, params)
                result = cursor.fetchone()
                return int(result["count"]) if result else 0
        except Exception as e:
            raise Exception(f"Gagal menghitung videos: {str(e)}")
    
    def search_videos(self, query: str, limit: int = 20, offset: int = 0,
                      force_refresh: bool = False) -> List[Dict]:
        """Mencari video berdasarkan judul atau deskripsi dengan caching."""
        cache_key = f"search_{self.get_statistics_version()}_{query}_{limit}_{offset}"
        if not force_refresh:
            try:
                with self._get_cursor() as cursor:
                    cursor.execute("SELECT value, updated_at FROM cached_stats WHERE key = ?", (cache_key,))
                    row = cursor.fetchone()
                    if row:
                        from datetime import datetime
                        updated_at = datetime.fromisoformat(row['updated_at'])
                        if (datetime.now() - updated_at).total_seconds() < 300: # 5 Menit
                            return json.loads(row['value'])
            except Exception:
                pass

        try:
            with self._get_cursor() as cursor:
                fts_query = self._build_fts_query(query)
                if fts_query:
                    cursor.execute("""
                        SELECT v.id, v.video_id, v.title, v.video_url, v.duration, v.upload_date,
                               v.view_count, v.thumbnail_url, v.transcript_downloaded, v.word_count, v.created_at,
                               c.channel_id, c.channel_name
                        FROM videos_search_fts
                        JOIN videos_search_cache sc ON sc.id = videos_search_fts.rowid
                        JOIN videos v ON v.video_id = sc.video_id
                        JOIN channels c ON v.channel_id = c.id
                        WHERE videos_search_fts MATCH ?
                          AND (v.is_short = 0 OR v.is_short IS NULL)
                          AND (v.is_member_only = 0 OR v.is_member_only IS NULL)
                        ORDER BY bm25(videos_search_fts), v.created_at DESC
                        LIMIT ? OFFSET ?
                    """, (fts_query, limit, offset))
                else:
                    search_pattern = f"%{query}%"
                    cursor.execute("""
                        SELECT v.id, v.video_id, v.title, v.video_url, v.duration, v.upload_date,
                               v.view_count, v.thumbnail_url, v.transcript_downloaded, v.word_count, v.created_at,
                               c.channel_id, c.channel_name
                        FROM videos v
                        JOIN channels c ON v.channel_id = c.id
                        WHERE (v.title LIKE ? OR v.description LIKE ?)
                          AND (v.is_short = 0 OR v.is_short IS NULL)
                          AND (v.is_member_only = 0 OR v.is_member_only IS NULL)
                        ORDER BY v.created_at DESC
                        LIMIT ? OFFSET ?
                    """, (search_pattern, search_pattern, limit, offset))
                
                results = cursor.fetchall()
                data = [dict(row) for row in results]
                
                # Update cache
                try:
                    cursor.execute(
                        "INSERT OR REPLACE INTO cached_stats (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                        (cache_key, json.dumps(data))
                    )
                except Exception:
                    pass
                
                return data
            
        except Exception as e:
            raise Exception(f"Gagal mencari video: {str(e)}")

    def count_search_videos(self, query: str, force_refresh: bool = False) -> int:
        """Menghitung total hasil pencarian video berdasarkan judul atau deskripsi dengan caching."""
        cache_key = f"count_search_{self.get_statistics_version()}_{query}"
        if not force_refresh:
            try:
                with self._get_cursor() as cursor:
                    cursor.execute("SELECT value, updated_at FROM cached_stats WHERE key = ?", (cache_key,))
                    row = cursor.fetchone()
                    if row:
                        from datetime import datetime
                        updated_at = datetime.fromisoformat(row['updated_at'])
                        if (datetime.now() - updated_at).total_seconds() < 300: # 5 Menit
                            return int(row['value'])
            except Exception:
                pass

        try:
            with self._get_cursor() as cursor:
                fts_query = self._build_fts_query(query)
                if fts_query:
                    cursor.execute("""
                        SELECT COUNT(*) as count
                        FROM videos_search_fts
                        JOIN videos_search_cache sc ON sc.id = videos_search_fts.rowid
                        JOIN videos v ON v.video_id = sc.video_id
                        WHERE videos_search_fts MATCH ?
                          AND (v.is_short = 0 OR v.is_short IS NULL)
                          AND (v.is_member_only = 0 OR v.is_member_only IS NULL)
                    """, (fts_query,))
                else:
                    search_pattern = f"%{query}%"
                    cursor.execute("""
                        SELECT COUNT(*) as count
                        FROM videos v
                        WHERE (v.title LIKE ? OR v.description LIKE ?)
                          AND (v.is_short = 0 OR v.is_short IS NULL)
                          AND (v.is_member_only = 0 OR v.is_member_only IS NULL)
                    """, (search_pattern, search_pattern))
                result = cursor.fetchone()
                count = int(result["count"]) if result else 0
                
                # Update cache
                try:
                    cursor.execute(
                        "INSERT OR REPLACE INTO cached_stats (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                        (cache_key, str(count))
                    )
                except Exception:
                    pass
                
                return count
        except Exception as e:
            raise Exception(f"Gagal menghitung hasil pencarian video: {str(e)}")
    
    def get_transcript_file(self, video_id: str) -> Optional[Path]:
        """Mendapatkan path file transcript"""
        try:
            video = self.get_video_by_id(video_id)
            if video and video['transcript_file_path']:
                # Handle both absolute and relative paths
                path = Path(video['transcript_file_path'])
                if not path.is_absolute():
                    # Path in DB might be:
                    # 1. "uploads/channel/text/file.txt" - already includes uploads/
                    # 2. "channel/text/file.txt" - needs base_dir prefix
                    if str(path).startswith('uploads/'):
                        # Already includes uploads/, resolve from project root
                        path = Path(self.base_dir.parent) / path
                    else:
                        # Just channel/name, prepend base_dir
                        path = self.base_dir / path
                if path.exists():
                    return path
            return None
        except Exception as e:
            return None

    def get_transcript_content(self, video_id: str) -> Optional[str]:
        """Ambil konten transkrip dari blob (priority) atau file."""
        # 1. Coba dari blob
        content = self.blob_storage.get_blob(video_id, "transcript")
        if content:
            return content
            
        # 2. Fallback ke file
        path = self.get_transcript_file(video_id)
        if path and path.exists():
            try:
                return path.read_text(encoding='utf-8')
            except Exception:
                return None
        return None

    def get_summary_file(self, video_id: str) -> Optional[Path]:
        """Mendapatkan path file summary"""
        try:
            video = self.get_video_by_id(video_id)
            if video and video['summary_file_path']:
                # Handle both absolute and relative paths
                path = Path(video['summary_file_path'])
                if not path.is_absolute():
                    # Path in DB might be:
                    # 1. "uploads/channel/resume/file.md" - already includes uploads/
                    # 2. "channel/resume/file.md" - needs base_dir prefix
                    if str(path).startswith('uploads/'):
                        # Already includes uploads/, resolve from project root
                        path = Path(self.base_dir.parent) / path
                    else:
                        # Just channel/name, prepend base_dir
                        path = self.base_dir / path
                if path.exists():
                    return path
            return None
        except Exception as e:
            return None
    
    def read_transcript(self, video_id: str) -> Optional[str]:
        """Membaca konten transcript dari Blob Storage, fallback ke file"""
        # 1. Try Blob Storage
        content = self.blob_storage.get_blob(video_id, 'transcript')
        if content:
            return content
            
        # 2. Fallback ke file
        file_path = self.get_transcript_file(video_id)
        if file_path and file_path.exists():
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return f.read()
            except Exception:
                return None
        return None
    
    def read_summary(self, video_id: str) -> Optional[str]:
        """Membaca konten summary dari Blob Storage, fallback ke file"""
        # 1. Try Blob Storage
        content = self.blob_storage.get_blob(video_id, 'resume')
        if content:
            return content
            
        # 2. Fallback ke file
        file_path = self.get_summary_file(video_id)
        if file_path and file_path.exists():
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return f.read()
            except Exception:
                return None
        return None

    def read_formatted_transcript(self, video_id: str) -> Optional[str]:
        """Membaca konten formatted transcript dari Blob Storage, fallback ke file"""
        # 1. Try Blob Storage
        content = self.blob_storage.get_blob(video_id, 'formatted')
        if content:
            return content
            
        # 2. Fallback ke file
        file_path = self.get_formatted_transcript_file(video_id)
        if file_path and file_path.exists():
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return f.read()
            except Exception:
                return None
        return None

    def get_formatted_transcript_file(self, video_id: str) -> Optional[Path]:
        """Mendapatkan path ke formatted transcript file"""
        try:
            with self._get_cursor() as cursor:
                cursor.execute("""
                    SELECT v.video_id, c.channel_id, 
                           v.transcript_formatted_path as formatted_path
                    FROM videos v
                    JOIN channels c ON v.channel_id = c.id
                    WHERE v.video_id = ?
                """, (video_id,))

                row = cursor.fetchone()
                if row and row['formatted_path']:
                    path = Path(str(row['formatted_path']))
                    if not path.is_absolute():
                        if str(path).startswith('uploads/'):
                            path = Path(self.base_dir.parent) / path
                        else:
                            path = self.base_dir / path
                    if path.exists():
                        return path

                    # Legacy fallback: some older rows only store channel slug in the DB.
                    channel_slug = str(row['channel_id'] or '').replace('@', '').replace('/', '_')
                    legacy_path = self.base_dir / channel_slug / "text_formatted" / f"{video_id}.txt"
                    if legacy_path.exists():
                        return legacy_path
            return None
        except Exception:
            return None

    def _build_fts_query(self, query: str) -> str:
        tokens = []
        for token in re.findall(r"[\w\u0600-\u06FF]+", str(query or "").lower()):
            token = token.strip()
            if len(token) >= 2:
                token = token.replace('"', "")
                tokens.append(f'"{token}"')
        if not tokens:
            return ""
        return " OR ".join(tokens[:8])

    def get_statistics_version(self) -> int:
        try:
            with self._get_cursor() as cursor:
                cursor.execute("SELECT value FROM cached_stats WHERE key = 'stats_version' LIMIT 1")
                row = cursor.fetchone()
                return int(row["value"]) if row and row["value"] is not None else 0
        except Exception:
            return 0

    def refresh_stats_cache(self):
        """Metode untuk dipanggil oleh cron job: merefresh statistik global."""
        return self.get_statistics(force_refresh=True)

    def get_statistics(self, force_refresh: bool = False) -> Dict:
        """Mendapatkan statistik database dengan caching."""
        # Jika tidak force refresh, coba ambil dari cache
        if not force_refresh:
            try:
                with self._get_cursor() as cursor:
                    cursor.execute(
                        "SELECT value, updated_at FROM cached_stats WHERE key = 'global_stats'"
                    )
                    row = cursor.fetchone()
                    if row:
                        from datetime import datetime
                        updated_at = datetime.fromisoformat(row['updated_at'])
                        # Jika cache kurang dari 5 menit, gunakan cache
                        if (datetime.now() - updated_at).total_seconds() < 300:
                            return json.loads(row['value'])
            except Exception:
                pass # Fallback ke hitung manual jika cache error
                
        try:
            with self._get_cursor() as cursor:
                stats = {}
                cursor.execute("SELECT COUNT(*) as count FROM channels")
                stats['total_channels'] = cursor.fetchone()['count']

                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS total_videos_all,
                        COALESCE(SUM(CASE WHEN is_short = 0 AND is_member_only = 0 THEN 1 ELSE 0 END), 0) AS public_videos,
                        COALESCE(SUM(CASE WHEN is_short = 1 THEN 1 ELSE 0 END), 0) AS shorts_videos,
                        COALESCE(SUM(CASE WHEN is_member_only = 1 THEN 1 ELSE 0 END), 0) AS member_only_videos,
                        COALESCE(SUM(CASE WHEN is_short = 0 AND is_member_only = 0 AND transcript_downloaded = 1 THEN 1 ELSE 0 END), 0) AS videos_with_transcript,
                        COALESCE(SUM(CASE WHEN is_short = 0 AND is_member_only = 0 AND transcript_downloaded = 0 AND transcript_language = 'no_subtitle' THEN 1 ELSE 0 END), 0) AS videos_no_subtitle,
                        COALESCE(SUM(CASE WHEN is_short = 0 AND is_member_only = 0 AND transcript_downloaded = 0
                                           AND (transcript_language IS NULL OR transcript_language != 'no_subtitle')
                                           AND LOWER(COALESCE(transcript_retry_reason, '')) LIKE '%proxy_block%' THEN 1 ELSE 0 END), 0) AS videos_proxy_block,
                        COALESCE(SUM(CASE WHEN is_short = 0 AND is_member_only = 0 AND transcript_downloaded = 0
                                           AND (transcript_language IS NULL OR transcript_language != 'no_subtitle')
                                           AND transcript_retry_after IS NOT NULL
                                           AND LOWER(COALESCE(transcript_retry_reason, '')) NOT LIKE '%proxy_block%' THEN 1 ELSE 0 END), 0) AS videos_retry_later,
                        COALESCE(SUM(CASE WHEN is_short = 0 AND is_member_only = 0 AND transcript_downloaded = 0
                                           AND (transcript_language IS NULL OR transcript_language != 'no_subtitle')
                                           AND transcript_retry_after IS NULL
                                           AND LOWER(COALESCE(transcript_retry_reason, '')) NOT LIKE '%proxy_block%' THEN 1 ELSE 0 END), 0) AS videos_pending_other,
                        COALESCE(SUM(CASE WHEN transcript_downloaded = 1 THEN word_count ELSE 0 END), 0) AS total_word_count,
                        COALESCE(SUM(CASE WHEN is_short = 0 AND is_member_only = 0 THEN duration ELSE 0 END), 0) AS total_duration_seconds
                    FROM videos
                    """
                )
                result = cursor.fetchone()
                stats['total_videos_all'] = result['total_videos_all'] if result else 0
                stats['total_videos'] = result['public_videos'] if result else 0
                stats['public_videos'] = result['public_videos'] if result else 0
                stats['shorts_videos'] = result['shorts_videos'] if result else 0
                stats['member_only_videos'] = result['member_only_videos'] if result else 0
                stats['videos_with_transcript'] = result['videos_with_transcript'] if result else 0
                stats['videos_no_subtitle'] = result['videos_no_subtitle'] if result else 0
                stats['videos_proxy_block'] = result['videos_proxy_block'] if result else 0
                stats['videos_retry_later'] = result['videos_retry_later'] if result else 0
                stats['videos_pending_other'] = result['videos_pending_other'] if result else 0
                stats['videos_without_transcript'] = stats['public_videos'] - stats['videos_with_transcript']
                stats['total_word_count'] = result['total_word_count'] if result else 0
                stats['total_duration_seconds'] = result['total_duration_seconds'] if result else 0
                stats['total_duration_hours'] = stats['total_duration_seconds'] / 3600
                
                # Simpan ke cache
                try:
                    cursor.execute(
                        "INSERT OR REPLACE INTO cached_stats (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                        ('global_stats', json.dumps(stats))
                    )
                except Exception:
                    pass
                    
                return stats
            
        except Exception as e:
            raise Exception(f"Gagal mengambil statistik: {str(e)}")

    def upsert_admin_job(
        self,
        job_id: str,
        job_type: str,
        status: str,
        *,
        source: str = 'wrapper',
        pid: int = None,
        command: str = None,
        log_path: str = None,
        run_dir: str = None,
        target_channel_id: str = None,
        target_video_id: str = None,
        exit_code: int = None,
        error_message: str = None,
        started_at: str = None,
        finished_at: str = None,
    ) -> None:
        """Insert or update a persistent admin job record."""
        try:
            with self._get_cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO admin_jobs (
                        job_id, job_type, status, source, pid, command, log_path,
                        run_dir, target_channel_id, target_video_id, exit_code,
                        error_message, started_at, finished_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(job_id) DO UPDATE SET
                        job_type = excluded.job_type,
                        status = excluded.status,
                        source = excluded.source,
                        pid = excluded.pid,
                        command = excluded.command,
                        log_path = excluded.log_path,
                        run_dir = excluded.run_dir,
                        target_channel_id = excluded.target_channel_id,
                        target_video_id = excluded.target_video_id,
                        exit_code = excluded.exit_code,
                        error_message = excluded.error_message,
                        started_at = CASE
                            WHEN excluded.started_at IS NULL OR excluded.started_at = '' THEN admin_jobs.started_at
                            ELSE excluded.started_at
                        END,
                        finished_at = CASE
                            WHEN excluded.finished_at IS NULL OR excluded.finished_at = '' THEN admin_jobs.finished_at
                            ELSE excluded.finished_at
                        END,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        job_id,
                        job_type,
                        status,
                        source,
                        pid,
                        command,
                        log_path,
                        run_dir,
                        target_channel_id,
                        target_video_id,
                        exit_code,
                        error_message,
                        started_at,
                        finished_at,
                    ),
                )
        except Exception as e:
            raise Exception(f"Gagal upsert admin job: {str(e)}")

    def list_admin_jobs(self, limit: int = 50, status: str = None, since_days: int | None = None) -> List[Dict]:
        """Return persistent admin jobs ordered by most recent update."""
        try:
            with self._get_cursor() as cursor:
                query = """
                    SELECT *
                    FROM admin_jobs
                """
                params = []
                conditions = []
                if status:
                    conditions.append("status = ?")
                    params.append(status)
                if since_days is not None:
                    conditions.append("datetime(created_at) >= datetime('now', ?)")
                    params.append(f"-{int(since_days)} day")
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
                query += " ORDER BY updated_at DESC, created_at DESC, job_id DESC LIMIT ?"
                params.append(limit)
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            raise Exception(f"Gagal mengambil admin jobs: {str(e)}")

    def get_admin_job(self, job_id: str) -> Dict | None:
        """Return one admin job by id."""
        try:
            with self._get_cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM admin_jobs
                    WHERE job_id = ?
                    LIMIT 1
                    """,
                    (job_id,),
                )
                row = cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            raise Exception(f"Gagal mengambil admin job: {str(e)}")

    def get_channel_folder_path(self, channel_id: str) -> Path:
        """
        Mendapatkan path folder untuk channel tertentu
        
        Args:
            channel_id: Channel ID string
        
        Returns:
            Path ke folder channel
        """
        # Sanitize channel ID untuk nama folder yang valid
        safe_channel_id = self._sanitize_channel_id(channel_id)
        return self.base_dir / safe_channel_id
    
    def _sanitize_channel_id(self, channel_id: str) -> str:
        """
        Sanitize channel ID untuk nama folder yang valid
        """
        # Replace special characters dengan underscore
        safe_id = channel_id.replace('/', '_').replace('\\', '_')
        safe_id = safe_id.replace(':', '_').replace('*', '_').replace('?', '_')
        safe_id = safe_id.replace('"', '_').replace('<', '_').replace('>', '_').replace('|', '_')
        safe_id = safe_id.replace(' ', '_')  # Replace spaces dengan underscores
        
        # Limit length
        if len(safe_id) > 100:
            safe_id = safe_id[:100]
        
        return safe_id
    
    def get_channel_transcripts_dir(self, channel_id: str) -> Path:
        """
        Mendapatkan path directory transcripts untuk channel tertentu
        
        Args:
            channel_id: Channel ID string
        
        Returns:
            Path ke folder transcripts channel
        """
        channel_folder = self.get_channel_folder_path(channel_id)
        transcripts_dir = channel_folder / "text"
        transcripts_dir.mkdir(parents=True, exist_ok=True)
        return transcripts_dir
    
    def get_channel_summaries_dir(self, channel_id: str) -> Path:
        """
        Mendapatkan path directory summaries untuk channel tertentu
        
        Args:
            channel_id: Channel ID string
        
        Returns:
            Path ke folder summaries channel
        """
        channel_folder = self.get_channel_folder_path(channel_id)
        summaries_dir = channel_folder / "resume"
        summaries_dir.mkdir(parents=True, exist_ok=True)
        return summaries_dir
    
    def get_file_paths(self) -> Dict:
        """Dapatkan statistik file paths"""
        try:
            with self._get_cursor() as cursor:
                stats = {
                    'transcripts': {
                        'total': 0,
                        'total_size': 0
                    },
                    'summaries': {
                        'total': 0,
                        'total_size': 0
                    }
                }
                
                # Count transcript files
                cursor.execute("SELECT COUNT(*) as count FROM videos WHERE transcript_downloaded = 1 AND transcript_file_path IS NOT NULL")
                stats['transcripts']['total'] = cursor.fetchone()['count']
                
                # Calculate transcript file sizes
                cursor.execute("SELECT transcript_file_path FROM videos WHERE transcript_downloaded = 1 AND transcript_file_path IS NOT NULL")
                for row in cursor.fetchall():
                    file_path = Path(row['transcript_file_path'])
                    if file_path.exists():
                        stats['transcripts']['total_size'] += file_path.stat().st_size
                
                # Count summary files
                cursor.execute("SELECT COUNT(*) as count FROM videos WHERE transcript_downloaded = 1 AND summary_file_path IS NOT NULL")
                stats['summaries']['total'] = cursor.fetchone()['count']
                
                # Calculate summary file sizes
                cursor.execute("SELECT summary_file_path FROM videos WHERE transcript_downloaded = 1 AND summary_file_path IS NOT NULL")
                for row in cursor.fetchall():
                    file_path = Path(row['summary_file_path'])
                    if file_path.exists():
                        stats['summaries']['total_size'] += file_path.stat().st_size
                
                return stats
            
        except Exception as e:
            raise Exception(f"Gagal mengambil statistik file: {str(e)}")
    
    def export_to_json(self, output_path: str):
        """Export metadata database ke file JSON (tanpa file content)"""
        try:
            with self._get_cursor() as cursor:
                data = {
                    'export_date': datetime.now().isoformat(),
                    'statistics': self.get_statistics(),
                    'channels': [],
                    'videos': []
                }
                
                # Export channels
                cursor.execute("SELECT * FROM channels")
                data['channels'] = [dict(row) for row in cursor.fetchall()]
                
                # Export videos (metadata only)
                cursor.execute("""
                    SELECT v.*, c.channel_name
                    FROM videos v
                    JOIN channels c ON v.channel_id = c.id
                """)
                data['videos'] = [dict(row) for row in cursor.fetchall()]
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
            
        except Exception as e:
            raise Exception(f"Gagal export database: {str(e)}")
    
    def vacuum_database(self):
        """Optimize database dengan VACUUM"""
        try:
            with self._get_cursor() as cursor:
                cursor.execute("VACUUM")
        except Exception as e:
            raise Exception(f"Gagal vacuum database: {str(e)}")
    
    def close(self):
        """Tutup koneksi database"""
        if self.conn:
            self.conn.close()

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# Database initialization helper
def init_optimized_database(db_path: str = "youtube_transcripts.db", 
                            base_dir: str = "uploads") -> OptimizedDatabase:
    """Helper untuk inisialisasi optimized database"""
    return OptimizedDatabase(db_path, base_dir)


if __name__ == "__main__":
    # Test database initialization
    db = OptimizedDatabase("test_optimized.db", "uploads")
    print("✅ Optimized Database initialized successfully!")
    print(f"📊 Statistics: {db.get_statistics()}")
    print(f"📁 File paths: {db.get_file_paths()}")
    db.close()
