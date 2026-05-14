#!/usr/bin/env python3
"""
Script migrasi untuk menormalkan path di database menjadi relatif terhadap project root.
"""

import sys
from pathlib import Path

# Add project root to path
root = Path(__file__).resolve().parent
sys.path.insert(0, str(root))

from database_optimized import OptimizedDatabase

def migrate():
    db_path = root / "youtube_transcripts.db"
    base_dir = root / "uploads"
    
    print(f"Menggunakan database: {db_path}")
    print(f"Project root: {root}")
    
    db = OptimizedDatabase(str(db_path), str(base_dir))
    
    with db._get_cursor() as cursor:
        # 1. Update videos table
        cursor.execute("SELECT id, video_id, transcript_file_path, summary_file_path, thumbnail_url FROM videos")
        rows = cursor.fetchall()
        
        updated_count = 0
        for row in rows:
            video_id = row['video_id']
            t_path = db._to_relative_path(row['transcript_file_path'])
            s_path = db._to_relative_path(row['summary_file_path'])
            thumb = db._to_relative_path(row['thumbnail_url'])
            
            if (t_path != row['transcript_file_path'] or 
                s_path != row['summary_file_path'] or 
                thumb != row['thumbnail_url']):
                
                cursor.execute("""
                    UPDATE videos 
                    SET transcript_file_path = ?, 
                        summary_file_path = ?, 
                        thumbnail_url = ? 
                    WHERE video_id = ?
                """, (t_path, s_path, thumb, video_id))
                updated_count += 1
        
        print(f"Selesai update tabel videos: {updated_count} baris diperbarui.")
        
        # 2. Update admin_jobs if it exists (check tables first)
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='admin_jobs'")
        if cursor.fetchone():
            cursor.execute("SELECT job_id, log_path, run_dir FROM admin_jobs")
            jobs = cursor.fetchall()
            
            job_updated = 0
            for job in jobs:
                job_id = job['job_id']
                l_path = db._to_relative_path(job['log_path'])
                r_dir = db._to_relative_path(job['run_dir'])
                
                if l_path != job['log_path'] or r_dir != job['run_dir']:
                    cursor.execute("""
                        UPDATE admin_jobs 
                        SET log_path = ?, 
                            run_dir = ? 
                        WHERE job_id = ?
                    """, (l_path, r_dir, job_id))
                    job_updated += 1
            print(f"Selesai update tabel admin_jobs: {job_updated} baris diperbarui.")
        
    print("Migrasi selesai!")

if __name__ == "__main__":
    migrate()
