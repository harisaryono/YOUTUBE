#!/usr/bin/env python3
import sys
import os
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database_optimized import OptimizedDatabase

def verify():
    db = OptimizedDatabase()
    try:
        # Get first video ID from DB
        cursor = db.conn.cursor()
        cursor.execute("SELECT video_id FROM videos LIMIT 1")
        row = cursor.fetchone()
        if not row:
            print("⚠️ No videos in database to test.")
            return

        video_id = row['video_id']
        print(f"🔍 Testing get_video_by_id for: {video_id}")
        video = db.get_video_by_id(video_id)
        if video:
            print(f"✅ Successfully retrieved video: {video['title']}")
            print(f"📊 Metadata: {video.get('metadata')}")
        else:
            print("❌ Video not found.")

    except Exception as e:
        print(f"❌ Verification failed: {e}")
        sys.exit(1)
    finally:
        db.close()

if __name__ == "__main__":
    verify()
