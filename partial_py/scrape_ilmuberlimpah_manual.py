#!/usr/bin/env python3
"""
Scrape Ilmu Berlimpah 369 - Manual Video IDs

Script untuk download transcript jika Anda sudah punya daftar video ID
"""

import sqlite3
import subprocess
import sys
from pathlib import Path

# Configuration
SOURCE_DB = str(Path(__file__).resolve().parent.parent.parent / "GIT/yt_channel/channels.db")
TARGET_DB = str(Path(__file__).resolve().parent.parent / "youtube_transcripts.db")
OUT_BASE = Path(str(Path(__file__).resolve().parent.parent.parent / "GIT/yt_channel/out"))
CHANNEL_SLUG = "IlmuBerlimpah369"
CHANNEL_ID = 202  # ID di source database

# Masukkan video ID di sini (bisa copy dari URL YouTube)
# Contoh: https://www.youtube.com/watch?v=VIDEO_ID
VIDEO_IDS = [
    # Tambahkan video ID di sini, contoh:
    # "f4VKnL6Dt1Y",
    # "abc123xyz",
]


def add_videos_to_db(video_ids: list):
    """Add videos ke database"""
    conn = sqlite3.connect(SOURCE_DB)
    cursor = conn.cursor()
    
    added = 0
    for video_id in video_ids:
        try:
            cursor.execute("""
                SELECT id FROM videos 
                WHERE channel_id = ? AND video_id = ?
            """, (CHANNEL_ID, video_id))
            
            if cursor.fetchone():
                continue
            
            cursor.execute("""
                INSERT INTO videos 
                (channel_id, video_id, title, upload_date, status_download)
                VALUES (?, ?, ?, ?, 'pending')
            """, (CHANNEL_ID, video_id, f'Video {video_id}', ''))
            
            added += 1
            
        except Exception as e:
            print(f"  ❌ Error: {video_id} - {str(e)}")
    
    conn.commit()
    conn.close()
    return added


def download_transcripts(video_ids: list):
    """Download transcript untuk semua videos"""
    output_dir = OUT_BASE / CHANNEL_SLUG / "text"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    success = 0
    failed = 0
    
    for i, video_id in enumerate(video_ids, 1):
        print(f"[{i}/{len(video_ids)}] {video_id}")
        
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
            
            result = subprocess.run(cmd, capture_output=True, timeout=120)
            
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
                failed += 1
                print(f"  ⚠️  No subs")
                
        except Exception as e:
            failed += 1
            print(f"  ❌ Error: {str(e)}")
    
    return success, failed


def main():
    print("=" * 60)
    print("🎬 Ilmu Berlimpah 369 Scraper")
    print("=" * 60)
    print()
    
    if not VIDEO_IDS:
        print("❌ No video IDs provided!")
        print()
        print("Cara mendapatkan video ID:")
        print("1. Buka https://www.youtube.com/@IlmuBerlimpah369/videos")
        print("2. Copy video URL, contoh: https://www.youtube.com/watch?v=abc123xyz")
        print("3. Extract video ID: abc123xyz")
        print("4. Tambahkan ke VIDEO_IDS list di script ini")
        print()
        print("Atau gunakan script fetch untuk auto-get video IDs")
        return 1
    
    print(f"📊 Processing {len(VIDEO_IDS)} videos")
    print()
    
    # Add to database
    print("📊 Adding to database...")
    added = add_videos_to_db(VIDEO_IDS)
    print(f"   ✅ Added: {added}")
    
    # Download transcripts
    print()
    print("📥 Downloading transcripts...")
    success, failed = download_transcripts(VIDEO_IDS)
    
    print()
    print("=" * 60)
    print("📈 Summary:")
    print(f"   ✅ Success: {success}")
    print(f"   ❌ Failed: {failed}")
    print(f"   📁 Output: {output_dir}")
    print("=" * 60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
