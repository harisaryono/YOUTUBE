#!/usr/bin/env python3
"""
Database Module untuk YouTube Transcript Framework
Menangani penyimpanan data video dan transkrip
"""

import sqlite3
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
import hashlib
import re


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


class TranscriptDatabase:
    def __init__(self, db_path: str = "youtube_transcripts.db"):
        """
        Inisialisasi Database
        
        Args:
            db_path: Path ke file database
        """
        self.db_path = db_path
        self.conn = None
        self._connect()
        self._create_tables()
    
    def _connect(self):
        """Koneksi ke database"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
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
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Tabel videos
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
                    transcript_downloaded BOOLEAN DEFAULT 0,
                    transcript_file_path TEXT,
                    summary_file_path TEXT,
                    metadata TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (channel_id) REFERENCES channels(id)
                )
            """)
            
            # Tabel transcripts
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transcripts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id INTEGER NOT NULL,
                    language TEXT NOT NULL,
                    transcript_data TEXT NOT NULL,
                    format_type TEXT NOT NULL,
                    word_count INTEGER DEFAULT 0,
                    duration REAL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (video_id) REFERENCES videos(id)
                )
            """)
            
            # Tabel summaries
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS summaries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id INTEGER NOT NULL,
                    summary_text TEXT NOT NULL,
                    word_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (video_id) REFERENCES videos(id)
                )
            """)
            
            # Tabel download_queue
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS download_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id INTEGER NOT NULL,
                    channel_id INTEGER,
                    status TEXT DEFAULT 'pending',
                    error_message TEXT,
                    retry_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (video_id) REFERENCES videos(id),
                    FOREIGN KEY (channel_id) REFERENCES channels(id)
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
                CREATE INDEX IF NOT EXISTS idx_transcripts_video_id ON transcripts(video_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_summaries_video_id ON summaries(video_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_download_queue_status ON download_queue(status)
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
                    FOREIGN KEY(channel_db_id) REFERENCES channels(id) ON DELETE CASCADE
                )
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_channel_aliases_alias_key
                ON channel_aliases(alias_key)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_channel_aliases_channel_db_id
                ON channel_aliases(channel_db_id)
            """)
            cursor.execute("PRAGMA table_info(channels)")
            channel_columns = {str(row[1]) for row in cursor.fetchall()}
            if "channel_handle" not in channel_columns:
                cursor.execute("ALTER TABLE channels ADD COLUMN channel_handle TEXT")
            self._backfill_channel_aliases(cursor)
            
            self.conn.commit()
        except Exception as e:
            raise Exception(f"Gagal membuat tabel: {str(e)}")

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

    def _resolve_channel_row(self, cursor, channel_id: str) -> Optional[Dict]:
        raw = str(channel_id or "").strip()
        if not raw:
            return None

        cursor.execute("SELECT * FROM channels WHERE channel_id = ?", (raw,))
        result = cursor.fetchone()
        if result:
            return dict(result)

        alias_key = _normalize_channel_token(raw)
        if alias_key:
            cursor.execute(
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
            )
            result = cursor.fetchone()
            if result:
                return dict(result)

            cursor.execute(
                """
                SELECT *
                FROM channels
                WHERE LOWER(REPLACE(REPLACE(channel_name, ' ', ''), '-', '')) = ?
                LIMIT 1
                """,
                (alias_key,),
            )
            result = cursor.fetchone()
            if result:
                return dict(result)

        return None

    def _backfill_channel_aliases(self, cursor) -> None:
        cursor.execute("PRAGMA table_info(channel_aliases)")
        if not cursor.fetchall():
            return
        cursor.execute("""
            SELECT id, channel_id, channel_name, channel_url, COALESCE(channel_handle, '') AS channel_handle
            FROM channels
        """)
        for row in cursor.fetchall():
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
                   subscriber_count: int = 0, video_count: int = 0) -> int:
        """
        Menambahkan channel baru ke database
        
        Returns:
            ID channel yang baru ditambahkan atau sudah ada
        """
        try:
            cursor = self.conn.cursor()
            normalized_channel_url = channel_url
            handle = _extract_channel_handle(normalized_channel_url) or (
                str(channel_id or "").strip().lstrip("@") if str(channel_id or "").strip().startswith("@") else ""
            )
            existing = self._resolve_channel_row(cursor, channel_id)

            if existing:
                cursor.execute(
                    """
                    UPDATE channels
                    SET channel_name = ?,
                        channel_handle = COALESCE(NULLIF(?, ''), channel_handle),
                        channel_url = ?,
                        subscriber_count = CASE WHEN ? > 0 THEN ? ELSE subscriber_count END,
                        video_count = CASE WHEN ? > 0 THEN ? ELSE video_count END,
                        last_updated = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (
                        str(channel_name or existing["channel_name"] or "").strip() or str(existing["channel_name"] or ""),
                        handle or "",
                        normalized_channel_url or existing["channel_url"],
                        int(subscriber_count or 0),
                        int(subscriber_count or 0),
                        int(video_count or 0),
                        int(video_count or 0),
                        int(existing["id"]),
                    ),
                )
                channel_db_id = int(existing["id"])
            else:
                cursor.execute("""
                    INSERT INTO channels 
                    (channel_id, channel_name, channel_handle, channel_url, subscriber_count, video_count)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    channel_id,
                    str(channel_name or channel_id or "").strip() or str(channel_id or ""),
                    handle or None,
                    normalized_channel_url,
                    subscriber_count,
                    video_count,
                ))
                channel_db_id = int(cursor.lastrowid)

            self._upsert_channel_aliases(cursor, channel_db_id, channel_id, channel_name, normalized_channel_url)
            if handle:
                self._upsert_channel_aliases(cursor, channel_db_id, handle, channel_name, normalized_channel_url)

            self.conn.commit()
            return channel_db_id
            
        except Exception as e:
            raise Exception(f"Gagal menambahkan channel: {str(e)}")
    
    def update_channel_stats(self, channel_id: str, subscriber_count: int = None, 
                            video_count: int = None):
        """
        Update statistik channel
        """
        try:
            cursor = self.conn.cursor()
            resolved = self._resolve_channel_row(cursor, channel_id)
            if resolved:
                channel_id = resolved["channel_id"]
            
            updates = []
            params = []
            
            if subscriber_count is not None:
                updates.append("subscriber_count = ?")
                params.append(subscriber_count)
            
            if video_count is not None:
                updates.append("video_count = ?")
                params.append(video_count)
            
            if updates:
                updates.append("last_updated = CURRENT_TIMESTAMP")
                params.append(channel_id)
                
                query = f"UPDATE channels SET {', '.join(updates)} WHERE channel_id = ?"
                cursor.execute(query, params)
                self.conn.commit()
                
        except Exception as e:
            raise Exception(f"Gagal update channel: {str(e)}")
    
    def add_video(self, video_id: str, channel_id: int, title: str, video_url: str,
                  description: str = None, duration: int = None, upload_date: str = None,
                  view_count: int = 0, like_count: int = 0, comment_count: int = 0,
                  thumbnail_url: str = None, metadata: Dict = None) -> int:
        """
        Menambahkan video baru ke database
        
        Returns:
            ID video yang baru ditambahkan atau sudah ada
        """
        try:
            cursor = self.conn.cursor()
            
            metadata_json = json.dumps(metadata) if metadata else None
            
            cursor.execute("""
                INSERT OR IGNORE INTO videos 
                (video_id, channel_id, title, description, duration, upload_date, 
                 view_count, like_count, comment_count, video_url, thumbnail_url, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (video_id, channel_id, title, description, duration, upload_date,
                  view_count, like_count, comment_count, video_url, thumbnail_url, metadata_json))
            
            self.conn.commit()
            
            # Get video ID
            cursor.execute("SELECT id FROM videos WHERE video_id = ?", (video_id,))
            result = cursor.fetchone()
            
            return result['id'] if result else None
            
        except Exception as e:
            raise Exception(f"Gagal menambahkan video: {str(e)}")
    
    def update_transcript_status(self, video_id: str, downloaded: bool = False,
                                 format_type: str = None, transcript_path: str = None,
                                 summary_path: str = None, word_count: int = None,
                                 line_count: int = None, language: str = None):
        """
        Update status transcript video
        """
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                UPDATE videos 
                SET transcript_downloaded = ?,
                    transcript_format = ?,
                    transcript_file_path = ?,
                    summary_file_path = ?,
                    word_count = COALESCE(?, word_count),
                    line_count = COALESCE(?, line_count),
                    transcript_language = COALESCE(?, transcript_language),
                    updated_at = CURRENT_TIMESTAMP
                WHERE video_id = ?
            """, (downloaded, format_type, transcript_path, summary_path, 
                  word_count, line_count, language, video_id))
            
            self.conn.commit()
            
        except Exception as e:
            raise Exception(f"Gagal update status transcript: {str(e)}")
            
    def update_video_thumbnail(self, video_id: str, thumbnail_url: str):
        """Update thumbnail URL atau path lokal untuk video"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                UPDATE videos 
                SET thumbnail_url = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE video_id = ?
            """, (thumbnail_url, video_id))
            self.conn.commit()
        except Exception as e:
            raise Exception(f"Gagal update thumbnail video: {str(e)}")
            
    def add_transcript(self, video_id: int, language: str, transcript_data: str,
                      format_type: str, word_count: int = 0, duration: float = 0) -> int:
        """
        Menambahkan transcript ke database
        
        Returns:
            ID transcript yang baru ditambahkan
        """
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                INSERT INTO transcripts 
                (video_id, language, transcript_data, format_type, word_count, duration)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (video_id, language, transcript_data, format_type, word_count, duration))
            
            self.conn.commit()
            
            return cursor.lastrowid
            
        except Exception as e:
            raise Exception(f"Gagal menambahkan transcript: {str(e)}")
    
    def add_summary(self, video_id: int, summary_text: str, word_count: int = 0) -> int:
        """
        Menambahkan ringkasan ke database
        
        Returns:
            ID summary yang baru ditambahkan
        """
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                INSERT INTO summaries 
                (video_id, summary_text, word_count)
                VALUES (?, ?, ?)
            """, (video_id, summary_text, word_count))
            
            self.conn.commit()
            
            return cursor.lastrowid
            
        except Exception as e:
            raise Exception(f"Gagal menambahkan summary: {str(e)}")
    
    def add_to_queue(self, video_id: int, channel_id: int = None) -> int:
        """
        Menambahkan video ke antrian download
        
        Returns:
            ID queue yang baru ditambahkan
        """
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                INSERT INTO download_queue (video_id, channel_id)
                VALUES (?, ?)
            """, (video_id, channel_id))
            
            self.conn.commit()
            
            return cursor.lastrowid
            
        except Exception as e:
            raise Exception(f"Gagal menambahkan ke queue: {str(e)}")
    
    def update_queue_status(self, queue_id: int, status: str, error_message: str = None):
        """
        Update status antrian download
        """
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                UPDATE download_queue 
                SET status = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (status, error_message, queue_id))
            
            self.conn.commit()
            
        except Exception as e:
            raise Exception(f"Gagal update queue status: {str(e)}")
    
    def increment_queue_retry(self, queue_id: int):
        """Increment retry count"""
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                UPDATE download_queue 
                SET retry_count = retry_count + 1, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (queue_id,))
            
            self.conn.commit()
            
        except Exception as e:
            raise Exception(f"Gagal increment retry: {str(e)}")
    
    def get_channel_by_id(self, channel_id: str) -> Optional[Dict]:
        """Mendapatkan info channel berdasarkan channel_id"""
        try:
            cursor = self.conn.cursor()
            result = self._resolve_channel_row(cursor, channel_id)
            return result
        except Exception as e:
            raise Exception(f"Gagal mengambil channel: {str(e)}")
    
    def get_video_by_id(self, video_id: str) -> Optional[Dict]:
        """Mendapatkan info video berdasarkan video_id"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM videos WHERE video_id = ?", (video_id,))
            result = cursor.fetchone()
            return dict(result) if result else None
        except Exception as e:
            raise Exception(f"Gagal mengambil video: {str(e)}")
    
    def get_videos_by_channel(self, channel_id: str, 
                             transcript_downloaded: bool = None,
                             limit: int = None) -> List[Dict]:
        """
        Mendapatkan daftar video dari channel tertentu
        
        Args:
            channel_id: ID channel
            transcript_downloaded: Filter berdasarkan status transcript
            limit: Batas jumlah hasil
        
        Returns:
            List video
        """
        try:
            cursor = self.conn.cursor()
            resolved = self._resolve_channel_row(cursor, channel_id)
            if not resolved:
                return []
            
            query = """
                SELECT v.*, c.channel_name, c.channel_url
                FROM videos v
                JOIN channels c ON v.channel_id = c.id
                WHERE v.channel_id = ?
            """
            params = [resolved["id"]]
            
            if transcript_downloaded is not None:
                query += " AND v.transcript_downloaded = ?"
                params.append(transcript_downloaded)
            
            query += " ORDER BY v.upload_date DESC"
            
            if limit:
                query += " LIMIT ?"
                params.append(limit)
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            return [dict(row) for row in results]
            
        except Exception as e:
            raise Exception(f"Gagal mengambil videos channel: {str(e)}")

    def get_channel_aliases(self, channel_id: str) -> List[Dict]:
        """Return all aliases for a channel."""
        try:
            cursor = self.conn.cursor()
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
                (resolved["id"],),
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
            cursor = self.conn.cursor()
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
                (like, like, like, alias_like, token, token, q, alias_key, int(limit or 20)),
            )
            return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            raise Exception(f"Gagal mencari channel: {str(e)}")

    def rebuild_channel_aliases(self, channel_id: str = None) -> int:
        """Rebuild alias table from current channel records."""
        try:
            cursor = self.conn.cursor()
            if channel_id:
                resolved = self._resolve_channel_row(cursor, channel_id)
                if not resolved:
                    return 0
                rows = [resolved]
                cursor.execute("DELETE FROM channel_aliases WHERE channel_db_id = ?", (resolved["id"],))
            else:
                cursor.execute("""
                    SELECT id, channel_id, channel_name, channel_url, COALESCE(channel_handle, '') AS channel_handle
                    FROM channels
                """)
                rows = cursor.fetchall()
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
                        (handle, row["id"]),
                    )
                self._upsert_channel_aliases(
                    cursor,
                    int(row["id"]),
                    str(row["channel_id"] or ""),
                    str(row["channel_name"] or ""),
                    str(row["channel_url"] or ""),
                )
                total += 1

            self.conn.commit()
            return total
        except Exception as e:
            raise Exception(f"Gagal membangun ulang alias channel: {str(e)}")
    
    def get_pending_queue(self, limit: int = None) -> List[Dict]:
        """Mendapatkan antrian yang pending"""
        try:
            cursor = self.conn.cursor()
            
            query = """
                SELECT q.*, v.video_id, v.title, c.channel_name
                FROM download_queue q
                JOIN videos v ON q.video_id = v.id
                JOIN channels c ON v.channel_id = c.id
                WHERE q.status = 'pending'
                ORDER BY q.created_at ASC
            """
            
            if limit:
                query += " LIMIT ?"
                cursor.execute(query, (limit,))
            else:
                cursor.execute(query)
            
            results = cursor.fetchall()
            return [dict(row) for row in results]
            
        except Exception as e:
            raise Exception(f"Gagal mengambil pending queue: {str(e)}")
    
    def get_statistics(self) -> Dict:
        """Mendapatkan statistik database"""
        try:
            cursor = self.conn.cursor()
            
            stats = {}
            
            # Count channels
            cursor.execute("SELECT COUNT(*) as count FROM channels")
            stats['total_channels'] = cursor.fetchone()['count']
            
            # Count videos
            cursor.execute("SELECT COUNT(*) as count FROM videos")
            stats['total_videos'] = cursor.fetchone()['count']
            
            # Count videos with transcript
            cursor.execute("SELECT COUNT(*) as count FROM videos WHERE transcript_downloaded = 1")
            stats['videos_with_transcript'] = cursor.fetchone()['count']
            
            # Count transcripts
            cursor.execute("SELECT COUNT(*) as count FROM transcripts")
            stats['total_transcripts'] = cursor.fetchone()['count']
            
            # Count summaries
            cursor.execute("SELECT COUNT(*) as count FROM summaries")
            stats['total_summaries'] = cursor.fetchone()['count']
            
            # Queue stats
            cursor.execute("SELECT COUNT(*) as count FROM download_queue WHERE status = 'pending'")
            stats['pending_downloads'] = cursor.fetchone()['count']
            
            cursor.execute("SELECT COUNT(*) as count FROM download_queue WHERE status = 'completed'")
            stats['completed_downloads'] = cursor.fetchone()['count']
            
            cursor.execute("SELECT COUNT(*) as count FROM download_queue WHERE status = 'failed'")
            stats['failed_downloads'] = cursor.fetchone()['count']
            
            return stats
            
        except Exception as e:
            raise Exception(f"Gagal mengambil statistik: {str(e)}")
    
    def search_videos(self, query: str, limit: int = 20) -> List[Dict]:
        """
        Mencari video berdasarkan judul atau deskripsi
        
        Returns:
            List video yang match dengan query
        """
        try:
            cursor = self.conn.cursor()
            
            search_pattern = f"%{query}%"
            
            cursor.execute("""
                SELECT v.*, c.channel_name
                FROM videos v
                JOIN channels c ON v.channel_id = c.id
                WHERE v.title LIKE ? OR v.description LIKE ?
                ORDER BY v.upload_date DESC
                LIMIT ?
            """, (search_pattern, search_pattern, limit))
            
            results = cursor.fetchall()
            return [dict(row) for row in results]
            
        except Exception as e:
            raise Exception(f"Gagal mencari video: {str(e)}")
    
    def get_transcript_by_video_id(self, video_id: str, language: str = None) -> Optional[Dict]:
        """
        Mendapatkan transcript berdasarkan video_id
        
        Returns:
            Transcript data atau None jika tidak ditemukan
        """
        try:
            cursor = self.conn.cursor()
            
            query = """
                SELECT t.*, v.video_id
                FROM transcripts t
                JOIN videos v ON t.video_id = v.id
                WHERE v.video_id = ?
            """
            params = [video_id]
            
            if language:
                query += " AND t.language = ?"
                params.append(language)
            
            query += " ORDER BY t.created_at DESC LIMIT 1"
            
            cursor.execute(query, params)
            result = cursor.fetchone()
            
            return dict(result) if result else None
            
        except Exception as e:
            raise Exception(f"Gagal mengambil transcript: {str(e)}")
    
    def get_summary_by_video_id(self, video_id: str) -> Optional[Dict]:
        """Mendapatkan ringkasan berdasarkan video_id"""
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                SELECT s.*, v.video_id
                FROM summaries s
                JOIN videos v ON s.video_id = v.id
                WHERE v.video_id = ?
                ORDER BY s.created_at DESC
                LIMIT 1
            """, (video_id,))
            
            result = cursor.fetchone()
            return dict(result) if result else None
            
        except Exception as e:
            raise Exception(f"Gagal mengambil summary: {str(e)}")
    
    def export_to_json(self, output_path: str):
        """Export semua data ke file JSON"""
        try:
            data = {
                'export_date': datetime.now().isoformat(),
                'statistics': self.get_statistics(),
                'channels': [],
                'videos': []
            }
            
            # Export channels
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM channels")
            data['channels'] = [dict(row) for row in cursor.fetchall()]
            
            # Export videos with transcripts and summaries
            cursor.execute("""
                SELECT v.*, 
                       GROUP_CONCAT(DISTINCT t.id) as transcript_ids,
                       GROUP_CONCAT(DISTINCT s.id) as summary_ids
                FROM videos v
                LEFT JOIN transcripts t ON v.id = t.video_id
                LEFT JOIN summaries s ON v.id = s.video_id
                GROUP BY v.id
            """)
            data['videos'] = [dict(row) for row in cursor.fetchall()]
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
        except Exception as e:
            raise Exception(f"Gagal export database: {str(e)}")
    
    def close(self):
        """Tutup koneksi database"""
        if self.conn:
            self.conn.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# Database initialization helper
def init_database(db_path: str = "youtube_transcripts.db") -> TranscriptDatabase:
    """
    Helper untuk inisialisasi database
    
    Args:
        db_path: Path ke file database
    
    Returns:
        TranscriptDatabase instance
    """
    return TranscriptDatabase(db_path)


if __name__ == "__main__":
    # Test database initialization
    db = TranscriptDatabase("test_transcripts.db")
    print("✅ Database initialized successfully!")
    print(f"📊 Statistics: {db.get_statistics()}")
    db.close()
