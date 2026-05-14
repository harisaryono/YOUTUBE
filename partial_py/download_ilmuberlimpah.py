#!/usr/bin/env python3
"""
Download transcripts for Ilmu Berlimpah 369
Reads video IDs from /tmp/ilmuberlimpah_ids.txt
"""

import sqlite3
import subprocess
import sys
from pathlib import Path

from local_services import yt_dlp_command

# Configuration
SOURCE_DB = str(Path(__file__).resolve().parent.parent.parent / "GIT/yt_channel/channels.db")
CHANNEL_ID = 202
CHANNEL_SLUG = "IlmuBerlimpah369"
VIDEO_IDS_FILE = "/tmp/ilmuberlimpah_ids.txt"
OUT_DIR = Path(str(Path(__file__).resolve().parent.parent.parent / "GIT/yt_channel/out" / CHANNEL_SLUG / "text"))


def load_video_ids():
    """Load video IDs from file"""
    with open(VIDEO_IDS_FILE, 'r') as f:
        return [line.strip() for line in f if line.strip()]


def add_to_database(video_ids):
    """Add videos to database"""
    print("📊 Adding videos to database...")
    
    conn = sqlite3.connect(SOURCE_DB)
    cursor = conn.cursor()
    
    added = 0
    skipped = 0
    
    for video_id in video_ids:
        try:
            cursor.execute("SELECT id FROM videos WHERE channel_id = ? AND video_id = ?", (CHANNEL_ID, video_id))
            
            if cursor.fetchone():
                skipped += 1
                continue
            
            cursor.execute("""
                INSERT INTO videos (channel_id, video_id, title, upload_date, status_download)
                VALUES (?, ?, 'Pending', '', 'pending')
            """, (CHANNEL_ID, video_id))
            
            added += 1
            
        except Exception as e:
            print(f"  ❌ Error {video_id}: {str(e)}")
    
    conn.commit()
    conn.close()
    
    print(f"   ✅ Added: {added}, Skipped: {skipped}")
    return added


def download_transcripts(video_ids):
    """Download transcripts for all videos"""
    print(f"\n📥 Downloading transcripts for {len(video_ids)} videos...")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    
    success = 0
    no_subs = 0
    failed = 0
    
    for i, video_id in enumerate(video_ids, 1):
        print(f"[{i}/{len(video_ids)}] {video_id}", end=" -> ")
        
        try:
            cmd = [
                *yt_dlp_command(),
                "--skip-download",
                "--write-subs",
                "--write-auto-subs",
                "--sub-langs", "id.*,id,en.*,en",
                "--sub-format", "vtt",
                "--no-warnings",
                "-o", f"{OUT_DIR}/%(id)s.%(ext)s",
                f"https://www.youtube.com/watch?v={video_id}"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            
            # Check for subtitle files
            vtt_files = list(OUT_DIR.glob(f"{video_id}*.vtt"))
            
            if vtt_files:
                # Update database
                conn = sqlite3.connect(SOURCE_DB)
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE videos 
                    SET status_download = 'downloaded', last_attempt = datetime('now')
                    WHERE channel_id = ? AND video_id = ?
                """, (CHANNEL_ID, video_id))
                conn.commit()
                conn.close()
                
                success += 1
                print("✅ Downloaded")
            else:
                no_subs += 1
                print("⚠️  No subs")
                
        except subprocess.TimeoutExpired:
            failed += 1
            print("❌ Timeout")
        except Exception as e:
            failed += 1
            print(f"❌ Error: {str(e)[:50]}")
        
        # Progress every 10 videos
        if i % 10 == 0:
            print(f"   ⏳ Progress: {success} OK, {no_subs} No Subs, {failed} Failed")
    
    return success, no_subs, failed


def main():
    print("=" * 60)
    print("🎬 Ilmu Berlimpah 369 - Transcript Downloader")
    print("=" * 60)
    print()
    
    # Load video IDs
    video_ids = load_video_ids()
    print(f"📊 Loaded {len(video_ids)} video IDs")
    
    if not video_ids:
        print("❌ No video IDs found")
        return 1
    
    # Add to database
    add_to_database(video_ids)
    
    # Download transcripts
    success, no_subs, failed = download_transcripts(video_ids)
    
    print()
    print("=" * 60)
    print("📈 Summary:")
    print(f"   ✅ Success: {success}")
    print(f"   ⚠️  No Subs: {no_subs}")
    print(f"   ❌ Failed: {failed}")
    print(f"   📁 Output: {OUT_DIR}")
    print("=" * 60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
