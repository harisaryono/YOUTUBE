#!/usr/bin/env python3
import sqlite3
import gzip
import os
from pathlib import Path
from datetime import datetime
from typing import Optional, Union

class BlobStorage:
    def __init__(self, db_path: str = "youtube_transcripts_blobs.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialize the blobs database and tables."""
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS content_blobs (
                video_id TEXT NOT NULL,
                content_type TEXT NOT NULL,
                data BLOB NOT NULL,
                original_size INTEGER NOT NULL,
                compressed_size INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (video_id, content_type)
            )
        """)
        # Indexes for faster lookups
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_blobs_video_id ON content_blobs(video_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_blobs_type ON content_blobs(content_type)")
        conn.commit()
        conn.close()

    def _get_connection(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def save_blob(self, video_id: str, content_type: str, content: Union[str, bytes]) -> bool:
        """Compress and save content as a blob."""
        if not content:
            return False
            
        if isinstance(content, str):
            data = content.encode('utf-8')
        else:
            data = content
            
        original_size = len(data)
        compressed_data = gzip.compress(data)
        compressed_size = len(compressed_data)
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO content_blobs 
                (video_id, content_type, data, original_size, compressed_size, created_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (video_id, content_type, compressed_data, original_size, compressed_size))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error saving blob for {video_id} ({content_type}): {e}")
            return False

    def get_blob(self, video_id: str, content_type: str) -> Optional[str]:
        """Retrieve and decompress a blob."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT data FROM content_blobs 
                WHERE video_id = ? AND content_type = ?
            """, (video_id, content_type))
            row = cursor.fetchone()
            conn.close()
            
            if row:
                decompressed = gzip.decompress(row['data'])
                return decompressed.decode('utf-8')
            return None
        except Exception as e:
            print(f"Error retrieving blob for {video_id} ({content_type}): {e}")
            return None

    def exists(self, video_id: str, content_type: str) -> bool:
        """Check if a blob exists."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 1 FROM content_blobs 
            WHERE video_id = ? AND content_type = ?
        """, (video_id, content_type))
        exists = cursor.fetchone() is not None
        conn.close()
        return exists

    def delete_blob(self, video_id: str, content_type: str) -> bool:
        """Delete a stored blob if it exists."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                DELETE FROM content_blobs
                WHERE video_id = ? AND content_type = ?
            """, (video_id, content_type))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error deleting blob for {video_id} ({content_type}): {e}")
            return False
