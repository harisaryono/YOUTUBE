#!/usr/bin/env python3
"""
Scrape Ilmu Berlimpah 369 - Using RSS Feed

Alternative approach menggunakan YouTube RSS untuk get video list
"""

import sqlite3
import subprocess
import sys
import feedparser
from pathlib import Path
from datetime import datetime

# Configuration
SOURCE_DB = str(Path(__file__).resolve().parent.parent.parent / "GIT/yt_channel/channels.db")
OUT_BASE = Path(str(Path(__file__).resolve().parent.parent.parent / "GIT/yt_channel/out"))
CHANNEL_SLUG = "IlmuBerlimpah369"
CHANNEL_ID = 202
RSS_URL = "https://www.youtube.com/feeds/videos.xml?channel_id=UCm44PmruoSbuNbZn7jFeXUw"  # Need to find actual channel ID


def get_channel_id_from_url():
    """Get channel ID dari URL"""
    print("🔍 Finding channel ID...")
    
    cmd = [
        sys.executable, "-m", "yt_dlp",
        "--dump-json",
        "--no-warnings",
        "--playlist-end", "1",
        "https://www.youtube.com/@IlmuBerlimpah369/videos"
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        # Parse output to get channel info
        for line in result.stdout.strip().split('\n'):
            if line.startswith('{'):
                import json
                data = json.loads(line)
                channel_id = data.get('channel_id', '')
                channel = data.get('channel', '')
                return channel_id, channel
        
        # Fallback: try to extract from URL
        return None, None
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return None, None


def fetch_from_rss(channel_id: str):
    """Fetch videos dari RSS feed"""
    rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    
    try:
        feed = feedparser.parse(rss_url)
        
        videos = []
        for entry in feed.entries:
            videos.append({
                'video_id': entry.get('yt:videoid', ''),
                'title': entry.get('title', ''),
                'upload_date': entry.get('published', '')[:8].replace('-', '') if entry.get('published') else '',
                'url': entry.get('link', '')
            })
        
        return videos
        
    except Exception as e:
        print(f"❌ RSS Error: {str(e)}")
        return []


def main():
    print("=" * 60)
    print("🎬 Ilmu Berlimpah 369 Scraper (RSS)")
    print("=" * 60)
    print()
    
    # Get channel ID
    channel_id, channel_name = get_channel_id_from_url()
    
    if not channel_id:
        print("❌ Could not find channel ID")
        print()
        print("Manual step required:")
        print("1. Buka https://www.youtube.com/@IlmuBerlimpah369/videos")
        print("2. View page source")
        print("3. Cari 'channelId' atau 'UCm...'")
        print("4. Run script dengan channel ID yang benar")
        return 1
    
    print(f"✅ Channel ID: {channel_id}")
    print(f"   Name: {channel_name}")
    
    # Fetch from RSS
    print()
    print("📡 Fetching from RSS...")
    videos = fetch_from_rss(channel_id)
    
    print(f"📊 Found {len(videos)} videos")
    
    if not videos:
        print("❌ No videos found in RSS")
        return 1
    
    # Add to database
    conn = sqlite3.connect(SOURCE_DB)
    cursor = conn.cursor()
    
    added = 0
    skipped = 0
    
    for video in videos:
        try:
            cursor.execute("""
                SELECT id FROM videos 
                WHERE channel_id = ? AND video_id = ?
            """, (CHANNEL_ID, video['video_id']))
            
            if cursor.fetchone():
                skipped += 1
                continue
            
            cursor.execute("""
                INSERT INTO videos 
                (channel_id, video_id, title, upload_date, status_download)
                VALUES (?, ?, ?, ?, 'pending')
            """, (CHANNEL_ID, video['video_id'], video['title'], video['upload_date']))
            
            added += 1
            
        except Exception as e:
            print(f"  ❌ Error: {video['video_id']} - {str(e)}")
    
    conn.commit()
    conn.close()
    
    print()
    print("=" * 60)
    print("📈 Summary:")
    print(f"   ✅ Added: {added}")
    print(f"   ⊘ Skipped: {skipped}")
    print("=" * 60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
