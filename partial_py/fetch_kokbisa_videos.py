#!/usr/bin/env python3
"""
Fetch Kok Bisa Videos

Ambil daftar video dari channel Kok Bisa dan masukkan ke database.
"""

import sqlite3
import json
import subprocess
import sys
from pathlib import Path

SOURCE_DB = str(Path(__file__).resolve().parent.parent.parent / "GIT/yt_channel/channels.db")
CHANNEL_ID = 201  # Kok Bisa
CHANNEL_URL = "https://www.youtube.com/@KokBisa/videos"


def fetch_video_list():
    """Fetch video list menggunakan yt-dlp"""
    print("📡 Fetching video list from YouTube...")
    
    cmd = [
        sys.executable, "-m", "yt_dlp",
        "--flat-playlist",
        "--dump-json",
        "--no-warnings",
        CHANNEL_URL
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        
        videos = []
        for line in result.stdout.strip().split('\n'):
            if line:
                try:
                    video = json.loads(line)
                    videos.append({
                        'video_id': video.get('id', ''),
                        'title': video.get('title', ''),
                        'upload_date': video.get('upload_date', ''),
                        'url': f"https://www.youtube.com/watch?v={video.get('id', '')}"
                    })
                except json.JSONDecodeError:
                    continue
        
        print(f"✅ Found {len(videos)} videos")
        return videos
        
    except subprocess.TimeoutExpired:
        print("❌ Timeout fetching video list")
        return []
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return []


def add_to_database(videos: list):
    """Add videos ke database"""
    conn = sqlite3.connect(SOURCE_DB)
    cursor = conn.cursor()
    
    added = 0
    skipped = 0
    
    for video in videos:
        try:
            # Check if already exists
            cursor.execute("""
                SELECT id FROM videos 
                WHERE channel_id = ? AND video_id = ?
            """, (CHANNEL_ID, video['video_id']))
            
            if cursor.fetchone():
                skipped += 1
                continue
            
            # Insert new video
            cursor.execute("""
                INSERT INTO videos 
                (channel_id, video_id, title, upload_date, status_download)
                VALUES (?, ?, ?, ?, 'pending')
            """, (CHANNEL_ID, video['video_id'], video['title'], video['upload_date']))
            
            added += 1
            
        except Exception as e:
            print(f"  ❌ Error adding {video['video_id']}: {str(e)}")
    
    conn.commit()
    conn.close()
    
    return added, skipped


def main():
    print("=" * 60)
    print("📺 Fetch Kok Bisa Videos")
    print("=" * 60)
    print()
    
    # Fetch video list
    videos = fetch_video_list()
    
    if not videos:
        print("❌ No videos found")
        return 1
    
    # Add to database
    print()
    print("📊 Adding to database...")
    added, skipped = add_to_database(videos)
    
    print()
    print("=" * 60)
    print("📈 Summary:")
    print(f"   ✅ Added: {added}")
    print(f"   ⊘ Skipped: {skipped}")
    print(f"   📊 Total in list: {len(videos)}")
    print("=" * 60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
