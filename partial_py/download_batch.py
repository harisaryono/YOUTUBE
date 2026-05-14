#!/usr/bin/env python3
"""Download transcripts for a batch of videos"""

import sqlite3
import subprocess
import sys
from pathlib import Path

from local_services import yt_dlp_command

CHANNEL_ID = 202
CHANNEL_SLUG = "IlmuBerlimpah369"
VIDEO_IDS_FILE = sys.argv[1] if len(sys.argv) > 1 else "/tmp/ilmu_batch1.txt"
OUT_DIR = Path(str(Path(__file__).resolve().parent.parent.parent / "GIT/yt_channel/out" / CHANNEL_SLUG / "text"))
SOURCE_DB = str(Path(__file__).resolve().parent.parent.parent / "GIT/yt_channel/channels.db")


def load_video_ids(filepath):
    with open(filepath, 'r') as f:
        return [line.strip() for line in f if line.strip()]


def main():
    video_ids = load_video_ids(VIDEO_IDS_FILE)
    print(f"📊 Processing {len(video_ids)} videos from {VIDEO_IDS_FILE}")
    
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    
    success = 0
    no_subs = 0
    failed = 0
    
    for i, video_id in enumerate(video_ids, 1):
        print(f"[{i}/{len(video_ids)}] {video_id}", end=" ")
        
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
            vtt_files = list(OUT_DIR.glob(f"{video_id}*.vtt"))
            
            if vtt_files:
                conn = sqlite3.connect(SOURCE_DB)
                cursor = conn.cursor()
                cursor.execute("UPDATE videos SET status_download = 'downloaded' WHERE channel_id = ? AND video_id = ?", (CHANNEL_ID, video_id))
                conn.commit()
                conn.close()
                success += 1
                print("✅")
            else:
                no_subs += 1
                print("⚠️")
                
        except Exception as e:
            failed += 1
            print(f"❌ {str(e)[:30]}")
    
    print(f"\n✅ {success} | ⚠️ {no_subs} | ❌ {failed}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
