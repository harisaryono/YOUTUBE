#!/usr/bin/env python3
import sqlite3
import os
from pathlib import Path

def get_content(file_path_str, base_dir):
    if not file_path_str:
        return None
    path = Path(file_path_str)
    if not path.is_absolute():
        if str(path).startswith('uploads/'):
            path = Path('.').resolve() / path
        else:
            path = base_dir / path
    
    if path.exists():
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                return f.read()
        except Exception:
            return None
    return None

def migrate():
    db_path = "youtube_transcripts.db"
    base_dir = Path("uploads")
    
    print(f"Connecting to {db_path}...")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    cursor = conn.cursor()
    
    print("Fetching videos...")
    cursor.execute("SELECT id, video_id, transcript_file_path, summary_file_path FROM videos WHERE transcript_downloaded = 1")
    videos = cursor.fetchall()
    total = len(videos)
    print(f"Found {total} videos.")
    
    batch_size = 500
    updated_count = 0
    
    for i in range(0, total, batch_size):
        batch = videos[i : i + batch_size]
        
        cursor.execute("BEGIN TRANSACTION")
        try:
            for vid_id, video_id, t_path, s_path in batch:
                transcript_text = get_content(t_path, base_dir)
                summary_text = get_content(s_path, base_dir)
                
                if transcript_text or summary_text:
                    cursor.execute("""
                        UPDATE videos 
                        SET transcript_text = ?, 
                            summary_text = ? 
                        WHERE id = ?
                    """, (transcript_text, summary_text, vid_id))
                    updated_count += 1
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"Error in batch {i}: {e}")
            break
            
        print(f"Processed {min(i + batch_size, total)}/{total}... Updated: {updated_count}")
    
    print(f"\nMigration complete. Total updated: {updated_count}")
    
    # Optional: Rebuild FTS here or separately
    # print("Rebuilding FTS index (this may take a while)...")
    # cursor.execute("DROP TABLE IF EXISTS videos_fts")
    # cursor.execute("CREATE VIRTUAL TABLE videos_fts USING fts5(title, description, transcript, summary, content='videos', content_rowid='id')")
    # cursor.execute("INSERT INTO videos_fts(rowid, title, description, transcript, summary) SELECT id, title, description, transcript_text, summary_text FROM videos")
    # conn.commit()
    
    print("Verifying integrity...")
    res = conn.execute("PRAGMA integrity_check").fetchone()
    print(f"Integrity check: {res[0]}")
    
    conn.close()
    print("Done!")

if __name__ == "__main__":
    migrate()
