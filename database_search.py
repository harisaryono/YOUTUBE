#!/usr/bin/env python3
"""Separate storage for blob-first video search cache and FTS."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional


def _sqlite_path_literal(path: str | Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


class SearchStorage:
    def __init__(self, search_db_path: str | Path, source_db_path: str | Path):
        self.search_db_path = str(search_db_path)
        self.source_db_path = str(source_db_path)
        self.conn = sqlite3.connect(self.search_db_path, check_same_thread=False, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute(
            f"ATTACH DATABASE {_sqlite_path_literal(self.source_db_path)} AS source_db"
        )
        self._create_tables()

    def _create_tables(self) -> None:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS videos_search_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                transcript_search TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_videos_search_cache_video_id
            ON videos_search_cache(video_id)
            """
        )
        cursor.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS videos_search_fts
            USING fts5(title, description, transcript_search,
                       content='videos_search_cache', content_rowid='id')
            """
        )
        cursor.execute("DROP TRIGGER IF EXISTS videos_search_cache_ai")
        cursor.execute("DROP TRIGGER IF EXISTS videos_search_cache_ad")
        cursor.execute("DROP TRIGGER IF EXISTS videos_search_cache_au")
        cursor.execute(
            """
            CREATE TRIGGER videos_search_cache_ai AFTER INSERT ON videos_search_cache BEGIN
                INSERT INTO videos_search_fts(rowid, title, description, transcript_search)
                VALUES (new.id, new.title, new.description, new.transcript_search);
            END
            """
        )
        cursor.execute(
            """
            CREATE TRIGGER videos_search_cache_ad AFTER DELETE ON videos_search_cache BEGIN
                INSERT INTO videos_search_fts(videos_search_fts, rowid, title, description, transcript_search)
                VALUES('delete', old.id, old.title, old.description, old.transcript_search);
            END
            """
        )
        cursor.execute(
            """
            CREATE TRIGGER videos_search_cache_au AFTER UPDATE ON videos_search_cache BEGIN
                INSERT INTO videos_search_fts(videos_search_fts, rowid, title, description, transcript_search)
                VALUES('delete', old.id, old.title, old.description, old.transcript_search);
                INSERT INTO videos_search_fts(rowid, title, description, transcript_search)
                VALUES (new.id, new.title, new.description, new.transcript_search);
            END
            """
        )
        self.conn.commit()

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def rebuild(self) -> None:
        with self.conn:
            self.conn.execute("INSERT INTO videos_search_fts(videos_search_fts) VALUES('rebuild')")

    def upsert_cache(
        self,
        video_id: str,
        title: str,
        description: str,
        transcript_search: str = "",
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO videos_search_cache (
                    video_id, title, description, transcript_search, updated_at
                ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(video_id) DO UPDATE SET
                    title = excluded.title,
                    description = excluded.description,
                    transcript_search = excluded.transcript_search,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    str(video_id or "").strip(),
                    str(title or "").strip(),
                    str(description or ""),
                    str(transcript_search or ""),
                ),
            )

    def bulk_replace_cache(self, rows: List[tuple[str, str, str, str]]) -> None:
        if not rows:
            return
        with self.conn:
            self.conn.executemany(
                """
                INSERT INTO videos_search_cache (
                    video_id, title, description, transcript_search, updated_at
                ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(video_id) DO UPDATE SET
                    title = excluded.title,
                    description = excluded.description,
                    transcript_search = excluded.transcript_search,
                    updated_at = CURRENT_TIMESTAMP
                """,
                [
                    (
                        str(video_id or "").strip(),
                        str(title or "").strip(),
                        str(description or ""),
                        str(transcript_search or ""),
                    )
                    for video_id, title, description, transcript_search in rows
                ],
            )

    def delete_cache(self, video_id: str) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM videos_search_cache WHERE video_id = ?", (str(video_id or "").strip(),))

    def _build_fts_query(self, query: str) -> str:
        tokens = [tok for tok in str(query or "").strip().split() if tok]
        return " ".join(f'"{tok}"' for tok in tokens)

    def search_videos(self, query: str, limit: int = 20, offset: int = 0) -> List[Dict[str, Any]]:
        fts_query = self._build_fts_query(query)
        cursor = self.conn.cursor()
        if fts_query:
            cursor.execute(
                """
                SELECT v.id, v.video_id, v.title, v.video_url, v.duration, v.upload_date,
                       v.view_count, v.thumbnail_url, v.transcript_downloaded, v.word_count, v.created_at,
                       c.channel_id, c.channel_name
                FROM videos_search_fts
                JOIN videos_search_cache sc ON sc.id = videos_search_fts.rowid
                JOIN source_db.videos v ON v.video_id = sc.video_id
                JOIN source_db.channels c ON v.channel_id = c.id
                WHERE videos_search_fts MATCH ?
                  AND (v.is_short = 0 OR v.is_short IS NULL)
                  AND (v.is_member_only = 0 OR v.is_member_only IS NULL)
                ORDER BY bm25(videos_search_fts), v.created_at DESC
                LIMIT ? OFFSET ?
                """,
                (fts_query, int(limit), int(offset)),
            )
        else:
            search_pattern = f"%{query}%"
            cursor.execute(
                """
                SELECT v.id, v.video_id, v.title, v.video_url, v.duration, v.upload_date,
                       v.view_count, v.thumbnail_url, v.transcript_downloaded, v.word_count, v.created_at,
                       c.channel_id, c.channel_name
                FROM source_db.videos v
                JOIN source_db.channels c ON v.channel_id = c.id
                WHERE (v.title LIKE ? OR v.description LIKE ?)
                  AND (v.is_short = 0 OR v.is_short IS NULL)
                  AND (v.is_member_only = 0 OR v.is_member_only IS NULL)
                ORDER BY v.created_at DESC
                LIMIT ? OFFSET ?
                """,
                (search_pattern, search_pattern, int(limit), int(offset)),
            )
        return [dict(row) for row in cursor.fetchall()]

    def count_search_videos(self, query: str) -> int:
        fts_query = self._build_fts_query(query)
        cursor = self.conn.cursor()
        if fts_query:
            cursor.execute(
                """
                SELECT COUNT(*) AS count
                FROM videos_search_fts
                JOIN videos_search_cache sc ON sc.id = videos_search_fts.rowid
                JOIN source_db.videos v ON v.video_id = sc.video_id
                WHERE videos_search_fts MATCH ?
                  AND (v.is_short = 0 OR v.is_short IS NULL)
                  AND (v.is_member_only = 0 OR v.is_member_only IS NULL)
                """,
                (fts_query,),
            )
        else:
            search_pattern = f"%{query}%"
            cursor.execute(
                """
                SELECT COUNT(*) AS count
                FROM source_db.videos v
                WHERE (v.title LIKE ? OR v.description LIKE ?)
                  AND (v.is_short = 0 OR v.is_short IS NULL)
                  AND (v.is_member_only = 0 OR v.is_member_only IS NULL)
                """,
                (search_pattern, search_pattern),
            )
        row = cursor.fetchone()
        return int(row["count"]) if row else 0
