#!/usr/bin/env python3
import os
import tarfile
import sqlite3
import re
from pathlib import Path

from database_optimized import OptimizedDatabase

DB_PATH = "youtube_transcripts.db"
TAR_DIR = "uploads_tar"

def extract_video_id(filename):
    match = re.match(r'^([a-zA-Z0-9_-]{11})', filename)
    if match:
        return match.group(1)
    return re.split(r'[\._]', filename)[0]

def update_db():
    if not os.path.exists(DB_PATH):
        print(f"❌ Database not found: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    tar_files = sorted([f for f in os.listdir(TAR_DIR) if f.endswith(".tar.gz")])
    print(f"📦 Found {len(tar_files)} tarballs to scan for DB update.")

    videos_updated = 0
    total_files_mapped = 0

    for tar_name in tar_files:
        tar_path = os.path.join(TAR_DIR, tar_name)
        print(f"🔍 Scanning {tar_name}...")
        
        try:
            with tarfile.open(tar_path, "r:gz") as tar:
                video_files = {} # video_id -> {type: path}
                
                for member in tar.getmembers():
                    if not member.isfile():
                        continue
                    
                    parts = member.name.split('/')
                    if len(parts) < 3:
                        continue
                        
                    folder = parts[1]
                    filename = parts[2]
                    video_id = extract_video_id(filename)
                    
                    if video_id not in video_files:
                        video_files[video_id] = {}
                    
                    rel_path = f"uploads/{member.name}"
                    
                    if folder == "text":
                        if "_transcript_" in filename:
                            video_files[video_id]['transcript'] = rel_path
                        elif "_summary_" in filename:
                            video_files[video_id]['resume'] = rel_path
                    elif folder == "transcripts":
                        video_files[video_id]['transcript'] = rel_path
                    elif folder == "resumes" or folder == "resume":
                        video_files[video_id]['resume'] = rel_path
                    elif folder == "text_formatted":
                        video_files[video_id]['formatted'] = rel_path

                if video_files:
                    conn.execute("BEGIN TRANSACTION")
                    try:
                        for vid, paths in video_files.items():
                            updates = []
                            params = []
                            
                            if 'transcript' in paths:
                                updates.append("transcript_file_path = ?")
                                params.append(paths['transcript'])
                            if 'resume' in paths:
                                updates.append("summary_file_path = ?")
                                params.append(paths['resume'])
                            if 'formatted' in paths:
                                updates.append("transcript_formatted_path = ?")
                                params.append(paths['formatted'])
                                
                            if updates:
                                query = f"UPDATE videos SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP WHERE video_id = ?"
                                params.append(vid)
                                cursor.execute(query, params)
                                if cursor.rowcount > 0:
                                    videos_updated += 1
                        conn.commit()
                    except Exception as e:
                        conn.rollback()
                        print(f"❌ Error updating DB for {tar_name}: {e}")
                
                total_files_mapped += len(video_files)

        except Exception as e:
            print(f"❌ Error reading tarball {tar_name}: {e}")

    print(f"\n✅ DB Update complete!")
    print(f"📊 Total videos updated in DB: {videos_updated}")
    
    conn.close()
    try:
        db = OptimizedDatabase(DB_PATH, "uploads")
        db._bump_stats_cache_version()
        db.close()
    except Exception:
        pass

if __name__ == "__main__":
    update_db()
