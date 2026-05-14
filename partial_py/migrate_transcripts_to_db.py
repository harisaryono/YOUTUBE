#!/usr/bin/env python3
import sqlite3
import os
import sys
from pathlib import Path

# Add current directory to path
sys.path.insert(0, os.getcwd())

from database_optimized import OptimizedDatabase

def migrate():
    db_path = "youtube_transcripts.db"
    base_dir = "uploads"
    
    print(f"Initializing connection to {db_path}...")
    db = OptimizedDatabase(db_path, base_dir)
    
    # Get all videos with transcripts
    print("Fetching videos with transcripts...")
    cursor = db.conn.cursor()
    cursor.execute("SELECT video_id, id FROM videos WHERE transcript_downloaded = 1")
    videos = cursor.fetchall()
    total = len(videos)
    print(f"Found {total} videos to process.")
    
    batch_size = 500
    count = 0
    
    for i in range(0, total, batch_size):
        batch = videos[i : i + batch_size]
        
        with db._get_cursor() as cursor:
            for video_id, vid_id in batch:
                transcript_text = db.read_transcript(video_id)
                summary_text = db.read_summary(video_id)
                
                if transcript_text or summary_text:
                    cursor.execute("""
                        UPDATE videos 
                        SET transcript_text = ?, 
                            summary_text = ? 
                        WHERE id = ?
                    """, (transcript_text, summary_text, vid_id))
                    count += 1
        
        print(f"Processed {min(i + batch_size, total)}/{total} videos... (Updated {count} so far)")
        
    print(f"\nMigration complete. Total updated: {count}")
    
    db.close()
    print("Done!")

if __name__ == "__main__":
    migrate()
