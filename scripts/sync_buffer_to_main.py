#!/usr/bin/env python3
import os
import json
import sqlite3
import time
from pathlib import Path
from datetime import datetime

# Path Configuration
ROOT_DIR = Path(__file__).parent.parent
DB_PATH = ROOT_DIR / "youtube_transcripts.db"
BUFFER_DIR = ROOT_DIR / "pending_updates"

def sync():
    if not BUFFER_DIR.exists():
        print(f"Buffer directory {BUFFER_DIR} does not exist. Nothing to sync.")
        return

    files = list(BUFFER_DIR.glob("*.json"))
    if not files:
        print("No pending updates found.")
        return

    print(f"Found {len(files)} pending updates. Syncing to database...")
    
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    
    success_count = 0
    try:
        cur = con.cursor()
        for f in files:
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                video_id = data.get("video_id")
                summary_path = data.get("summary_file_path")
                
                if video_id and summary_path:
                    cur.execute(
                        "UPDATE videos SET summary_file_path = ? WHERE video_id = ?",
                        (summary_path, video_id)
                    )
                    success_count += 1
                
                # Delete the buffer file after success
                f.unlink()
            except Exception as e:
                print(f"Error processing {f.name}: {e}")
        
        con.commit()
        print(f"Successfully synced {success_count} updates.")
        
        # Trigger Stats Cache Refresh
        print("Refreshing statistics cache...")
        from database_optimized import OptimizedDatabase
        db = OptimizedDatabase(str(DB_PATH), "uploads")
        db._bump_stats_cache_version()
        db.get_statistics(force_refresh=True)
        db.close()
        print("Stats cache refreshed.")

    except Exception as e:
        print(f"Critical error during sync: {e}")
        con.rollback()
    finally:
        con.close()

if __name__ == "__main__":
    sync()
