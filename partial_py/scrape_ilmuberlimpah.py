#!/usr/bin/env python3
"""
Scrape Ilmu Berlimpah 369 Channel

Download transcript dan resume untuk channel Ilmu Berlimpah 369
"""

import sqlite3
import subprocess
import sys
import json
from pathlib import Path
from datetime import datetime

# Configuration
SOURCE_DB = str(Path(__file__).resolve().parent.parent.parent / "GIT/yt_channel/channels.db")
TARGET_DB = str(Path(__file__).resolve().parent.parent / "youtube_transcripts.db")
OUT_BASE = Path(str(Path(__file__).resolve().parent.parent.parent / "GIT/yt_channel/out"))
CHANNEL_SLUG = "IlmuBerlimpah369"
CHANNEL_URL = "https://www.youtube.com/@IlmuBerlimpah369/videos"

SOURCE_CHANNEL_ID = 202


def fetch_video_list():
    """Fetch video list dari YouTube"""
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
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return []


def add_to_source_db(videos: list):
    """Add videos ke source database"""
    conn = sqlite3.connect(SOURCE_DB)
    cursor = conn.cursor()
    
    added = 0
    skipped = 0
    
    for video in videos:
        try:
            cursor.execute("""
                SELECT id FROM videos 
                WHERE channel_id = ? AND video_id = ?
            """, (SOURCE_CHANNEL_ID, video['video_id']))
            
            if cursor.fetchone():
                skipped += 1
                continue
            
            cursor.execute("""
                INSERT INTO videos 
                (channel_id, video_id, title, upload_date, status_download)
                VALUES (?, ?, ?, ?, 'pending')
            """, (SOURCE_CHANNEL_ID, video['video_id'], video['title'], video['upload_date'] or ''))
            
            added += 1
            
        except Exception as e:
            print(f"  ❌ Error: {video['video_id']} - {str(e)}")
    
    conn.commit()
    conn.close()
    
    return added, skipped


def download_transcripts(videos: list):
    """Download transcript untuk semua videos"""
    output_dir = OUT_BASE / CHANNEL_SLUG / "text"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    success = 0
    failed = 0
    
    for i, video in enumerate(videos, 1):
        video_id = video['video_id']
        print(f"[{i}/{len(videos)}] {video_id}")
        
        try:
            cmd = [
                sys.executable, "-m", "yt_dlp",
                "--skip-download",
                "--write-subs",
                "--write-auto-subs",
                "--sub-langs", "id.*,id,en.*,en",
                "--sub-format", "vtt",
                "--no-warnings",
                "-o", str(output_dir / f"%(id)s.%(ext)s"),
                f"https://www.youtube.com/watch?v={video_id}"
            ]
            
            result = subprocess.run(cmd, capture_output=True, timeout=60)
            
            # Check for subtitle files
            vtt_files = list(output_dir.glob(f"{video_id}*.vtt"))
            
            if vtt_files:
                # Update database
                conn = sqlite3.connect(SOURCE_DB)
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE videos 
                    SET status_download = 'downloaded',
                        last_attempt = datetime('now')
                    WHERE channel_id = ? AND video_id = ?
                """, (SOURCE_CHANNEL_ID, video_id))
                conn.commit()
                conn.close()
                
                success += 1
                print(f"  ✅ Downloaded")
            else:
                failed += 1
                print(f"  ⚠️  No subs")
                
        except Exception as e:
            failed += 1
            print(f"  ❌ Error: {str(e)}")
        
        # Progress
        if i % 10 == 0:
            print(f"   ⏳ Progress: {success} OK, {failed} Failed")
    
    return success, failed


def main():
    print("=" * 60)
    print("🎬 Ilmu Berlimpah 369 Scraper")
    print("=" * 60)
    print()
    
    # Step 1: Fetch video list
    videos = fetch_video_list()
    
    if not videos:
        print("❌ No videos found")
        return 1
    
    # Step 2: Add to source database
    print()
    print("📊 Adding to source database...")
    added, skipped = add_to_source_db(videos)
    print(f"   ✅ Added: {added}, Skipped: {skipped}")
    
    # Step 3: Download transcripts
    print()
    print("📥 Downloading transcripts...")
    success, failed = download_transcripts(videos)
    
    print()
    print("=" * 60)
    print("📈 Summary:")
    print(f"   ✅ Success: {success}")
    print(f"   ❌ Failed: {failed}")
    print(f"   📁 Output: {OUT_BASE / CHANNEL_SLUG / 'text'}")
    print("=" * 60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
