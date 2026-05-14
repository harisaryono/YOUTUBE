#!/usr/bin/env python3
"""Update Kok Bisa videos with correct file paths"""

import sqlite3
from pathlib import Path

conn = sqlite3.connect('youtube_transcripts.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

# Get Kok Bisa channel
cursor.execute("SELECT id FROM channels WHERE channel_id = '@KokBisa'")
channel = cursor.fetchone()
channel_id = channel['id']

# Get all videos for this channel
cursor.execute("SELECT video_id FROM videos WHERE channel_id = ?", (channel_id,))
videos = cursor.fetchall()

print(f"Total videos: {len(videos)}")

updated = 0
not_found = 0

for video in videos:
    video_id = video['video_id']
    
    # Check if transcript file exists
    text_files = list(Path("uploads/Kok_Bisa_/text").glob(f"{video_id}_transcript_*.txt"))
    
    # Check if resume file exists
    resume_files = list(Path("uploads/Kok_Bisa_/resume").glob(f"{video_id}_summary_*.txt"))
    
    # Check if thumbnail exists
    thumb_file = Path(f"uploads/Kok_Bisa_/thumbnails/{video_id}.jpg")
    
    if text_files:
        transcript_path = str(text_files[0])
    else:
        transcript_path = None
    
    if resume_files:
        summary_path = str(resume_files[0])
    else:
        summary_path = None
    
    if thumb_file.exists():
        thumbnail_url = f"uploads/Kok_Bisa_/thumbnails/{video_id}.jpg"
    else:
        thumbnail_url = f"https://img.youtube.com/vi/{video_id}/hqdefault.jpg"
    
    # Update database
    cursor.execute("""
        UPDATE videos 
        SET transcript_file_path = ?,
            summary_file_path = ?,
            thumbnail_url = ?,
            transcript_downloaded = CASE WHEN ? IS NOT NULL THEN 1 ELSE transcript_downloaded END,
            updated_at = CURRENT_TIMESTAMP
        WHERE video_id = ?
    """, (transcript_path, summary_path, thumbnail_url, transcript_path, video_id))
    
    if cursor.rowcount > 0:
        if transcript_path:
            updated += 1
        else:
            not_found += 1

conn.commit()
conn.close()

print(f"Updated with files: {updated}")
print(f"No files found: {not_found}")
