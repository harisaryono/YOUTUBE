#!/usr/bin/env python3
from pathlib import Path
"""
Import Kok Bisa Videos

Import daftar video Kok Bisa dari database target ke source database.
"""

import sqlite3
import sys

SOURCE_DB = str(Path(__file__).resolve().parent.parent.parent / "GIT/yt_channel/channels.db")
TARGET_DB = str(Path(__file__).resolve().parent.parent / "youtube_transcripts.db")

SOURCE_CHANNEL_ID = 201  # Kok Bisa di source DB
TARGET_CHANNEL_ID = 8    # Kok Bisa di target DB


def main():
    print("=" * 60)
    print("📥 Import Kok Bisa Videos")
    print("=" * 60)
    print()
    
    # Connect to both databases
    try:
        source_conn = sqlite3.connect(SOURCE_DB)
        source_cursor = source_conn.cursor()
        
        target_conn = sqlite3.connect(TARGET_DB)
        target_cursor = target_conn.cursor()
        
        print("✅ Connected to both databases")
    except Exception as e:
        print(f"❌ Connection error: {str(e)}")
        return 1
    
    # Get videos from target database
    target_cursor.execute("""
        SELECT video_id, title, upload_date
        FROM videos
        WHERE channel_id = ?
        ORDER BY upload_date DESC
    """, (TARGET_CHANNEL_ID,))
    
    target_videos = target_cursor.fetchall()
    print(f"📊 Found {len(target_videos)} videos in target database")
    
    # Import to source database
    added = 0
    skipped = 0
    
    for video in target_videos:
        video_id, title, upload_date = video
        
        try:
            # Check if already exists
            source_cursor.execute("""
                SELECT id FROM videos 
                WHERE channel_id = ? AND video_id = ?
            """, (SOURCE_CHANNEL_ID, video_id))
            
            if source_cursor.fetchone():
                skipped += 1
                continue
            
            # Insert new video
            source_cursor.execute("""
                INSERT INTO videos 
                (channel_id, video_id, title, upload_date, status_download)
                VALUES (?, ?, ?, ?, 'pending')
            """, (SOURCE_CHANNEL_ID, video_id, title, upload_date or ''))
            
            added += 1
            
        except Exception as e:
            print(f"  ❌ Error adding {video_id}: {str(e)}")
    
    source_conn.commit()
    source_conn.close()
    target_conn.close()
    
    print()
    print("=" * 60)
    print("📈 Summary:")
    print(f"   ✅ Added: {added}")
    print(f"   ⊘ Skipped: {skipped}")
    print(f"   📊 Total: {len(target_videos)}")
    print("=" * 60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
