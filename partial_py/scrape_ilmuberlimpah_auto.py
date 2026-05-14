#!/usr/bin/env python3
"""
Scrape Ilmu Berlimpah 369 - Full Auto

Fetch video list dan download transcript
"""

import sqlite3
import subprocess
import sys
from pathlib import Path

from local_services import yt_dlp_command

# Configuration
SOURCE_DB = str(Path(__file__).resolve().parent.parent.parent / "GIT/yt_channel/channels.db")
OUT_BASE = Path(str(Path(__file__).resolve().parent.parent.parent / "GIT/yt_channel/out"))
CHANNEL_SLUG = "IlmuBerlimpah369"
CHANNEL_ID = 202
CHANNEL_URL = "https://www.youtube.com/@IlmuBerlimpah369/videos"


def fetch_video_ids():
    """Fetch all video IDs dari channel"""
    print("📡 Fetching video IDs from YouTube...")
    
    cmd = [
        *yt_dlp_command(),
        "--flat-playlist",
        "--no-warnings",
        "--print", "%(id)s",
        CHANNEL_URL
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        
        video_ids = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
        
        print(f"✅ Found {len(video_ids)} videos")
        return video_ids
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return []


def add_to_database(video_ids: list):
    """Add videos ke database"""
    conn = sqlite3.connect(SOURCE_DB)
    cursor = conn.cursor()
    
    added = 0
    skipped = 0
    
    for video_id in video_ids:
        try:
            cursor.execute("""
                SELECT id FROM videos 
                WHERE channel_id = ? AND video_id = ?
            """, (CHANNEL_ID, video_id))
            
            if cursor.fetchone():
                skipped += 1
                continue
            
            cursor.execute("""
                INSERT INTO videos 
                (channel_id, video_id, title, upload_date, status_download)
                VALUES (?, ?, 'Pending', '', 'pending')
            """, (CHANNEL_ID, video_id))
            
            added += 1
            
        except Exception as e:
            print(f"  ❌ Error: {video_id} - {str(e)}")
    
    conn.commit()
    conn.close()
    
    return added, skipped


def download_transcripts(video_ids: list):
    """Download transcript untuk semua videos"""
    output_dir = OUT_BASE / CHANNEL_SLUG / "text"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    success = 0
    failed = 0
    no_subs = 0
    
    for i, video_id in enumerate(video_ids, 1):
        print(f"[{i}/{len(video_ids)}] {video_id}")
        
        try:
            cmd = [
                *yt_dlp_command(),
                "--skip-download",
                "--write-subs",
                "--write-auto-subs",
                "--sub-langs", "id.*,id,en.*,en",
                "--sub-format", "vtt",
                "--no-warnings",
                "-o", str(output_dir / f"%(id)s.%(ext)s"),
                f"https://www.youtube.com/watch?v={video_id}"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            
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
                """, (CHANNEL_ID, video_id))
                conn.commit()
                conn.close()
                
                success += 1
                print(f"  ✅ Downloaded")
            else:
                no_subs += 1
                print(f"  ⚠️  No subs")
                
        except subprocess.TimeoutExpired:
            failed += 1
            print(f"  ❌ Timeout")
        except Exception as e:
            failed += 1
            print(f"  ❌ Error: {str(e)}")
        
        # Progress
        if i % 10 == 0:
            print(f"   ⏳ Progress: {success} OK, {no_subs} No Subs, {failed} Failed")
    
    return success, no_subs, failed


def main():
    print("=" * 60)
    print("🎬 Ilmu Berlimpah 369 Auto Scraper")
    print("=" * 60)
    print()
    
    # Step 1: Fetch video IDs
    video_ids = fetch_video_ids()
    
    if not video_ids:
        print("❌ No videos found")
        return 1
    
    # Step 2: Add to database
    print()
    print("📊 Adding to database...")
    added, skipped = add_to_database(video_ids)
    print(f"   ✅ Added: {added}, Skipped: {skipped}")
    
    # Step 3: Download transcripts
    print()
    print("📥 Downloading transcripts...")
    success, no_subs, failed = download_transcripts(video_ids)
    
    print()
    print("=" * 60)
    print("📈 Summary:")
    print(f"   ✅ Success: {success}")
    print(f"   ⚠️  No Subs: {no_subs}")
    print(f"   ❌ Failed: {failed}")
    print(f"   📁 Output: {OUT_BASE / CHANNEL_SLUG / 'text'}")
    print("=" * 60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
