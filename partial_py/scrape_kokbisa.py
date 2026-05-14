#!/usr/bin/env python3
"""
Scrape Kok Bisa Channel

Script untuk download transcript dan resume untuk channel Kok Bisa
yang belum selesai di-scrape.

Usage:
    cd /media/harry/128NEW1/GIT/yt_channel
    python3 scrape_kokbisa.py
"""

import sqlite3
import subprocess
import sys
from pathlib import Path
from datetime import datetime

# Configuration
SOURCE_DB = str(Path(__file__).resolve().parent.parent.parent / "GIT/yt_channel/channels.db")
OUT_BASE = Path(str(Path(__file__).resolve().parent.parent.parent / "GIT/yt_channel/out"))
CHANNEL_SLUG = "KokBisa"
CHANNEL_URL = "https://www.youtube.com/@KokBisa/videos"


def get_pending_videos():
    """Get videos yang belum didownload transcript-nya"""
    conn = sqlite3.connect(SOURCE_DB)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Check if channel exists
    cursor.execute("""
        SELECT id, slug, url FROM channels 
        WHERE slug = ? OR url LIKE '%kokbisa%' OR url LIKE '%KokBisa%'
    """, (CHANNEL_SLUG,))
    
    channel = cursor.fetchone()
    
    if not channel:
        # Channel belum ada di database, perlu ditambahkan dulu
        print(f"❌ Channel '{CHANNEL_SLUG}' tidak ada di database")
        print(f"   URL: {CHANNEL_URL}")
        print()
        print("Tambahkan channel dulu dengan:")
        print(f"  sqlite3 {SOURCE_DB} \"INSERT INTO channels (url, slug, last_scanned) VALUES ('{CHANNEL_URL}', '{CHANNEL_SLUG}', datetime('now'));\"")
        return None, []
    
    channel_id = channel['id']
    print(f"✅ Channel found: {channel['slug']} (ID: {channel_id})")
    
    # Get videos that are not downloaded yet
    cursor.execute("""
        SELECT id, video_id, title, upload_date, status_download
        FROM videos
        WHERE channel_id = ?
        AND status_download != 'downloaded'
        ORDER BY upload_date DESC
    """, (channel_id,))
    
    pending = cursor.fetchall()
    conn.close()
    
    return channel_id, pending


def download_transcript(video_id: str, output_dir: Path) -> bool:
    """Download transcript untuk satu video"""
    try:
        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # yt-dlp command untuk download subtitle
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
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        # Check if any subtitle file was created
        vtt_files = list(output_dir.glob("*.vtt"))
        
        if vtt_files:
            print(f"  ✅ Downloaded: {video_id}")
            return True
        else:
            print(f"  ⚠️  No subs: {video_id}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"  ❌ Timeout: {video_id}")
        return False
    except Exception as e:
        print(f"  ❌ Error: {video_id} - {str(e)}")
        return False


def main():
    print("=" * 60)
    print("🎬 Kok Bisa Channel Scraper")
    print("=" * 60)
    print()
    
    channel_id, pending = get_pending_videos()
    
    if channel_id is None:
        return 1
    
    if not pending:
        print("✅ Semua videos sudah didownload!")
        return 0
    
    print(f"📊 Found {len(pending)} pending videos")
    print()
    
    # Process videos
    output_dir = OUT_BASE / CHANNEL_SLUG / "text"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    success = 0
    failed = 0
    
    for i, video in enumerate(pending, 1):
        video_id = video['video_id']
        print(f"[{i}/{len(pending)}] Processing: {video_id}")
        
        if download_transcript(video_id, output_dir):
            success += 1
            
            # Update database
            conn = sqlite3.connect(SOURCE_DB)
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE videos 
                SET status_download = 'downloaded',
                    last_attempt = datetime('now')
                WHERE id = ?
            """, (video['id'],))
            conn.commit()
            conn.close()
        else:
            failed += 1
        
        # Progress
        if i % 10 == 0:
            print(f"   ⏳ Progress: {success} OK, {failed} Failed")
    
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
