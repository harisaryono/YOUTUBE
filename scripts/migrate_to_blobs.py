#!/usr/bin/env python3
import os
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database_optimized import OptimizedDatabase
from database_blobs import BlobStorage
import argparse

def migrate(limit=None):
    db = OptimizedDatabase()
    blob_db = BlobStorage()
    
    print(f"🚀 Starting migration to blobs...")
    
    # Get videos with transcripts or summaries
    conn = db.conn
    cursor = conn.cursor()
    
    query = "SELECT video_id, transcript_file_path, summary_file_path, transcript_formatted_path FROM videos WHERE transcript_downloaded = 1"
    if limit:
        query += f" LIMIT {limit}"
        
    cursor.execute(query)
    rows = cursor.fetchall()
    
    total = len(rows)
    print(f"📊 Found {total} videos to process.")
    
    processed = 0
    blobs_saved = 0
    errors = 0
    
    for row in rows:
        video_id = row['video_id']
        processed += 1
        
        # Types of content to migrate
        targets = [
            ('transcript', row['transcript_file_path']),
            ('resume', row['summary_file_path']),
            ('formatted', row['transcript_formatted_path'])
        ]
        
        for content_type, file_path in targets:
            if not file_path:
                continue
                
            # Resolve file path
            full_path = Path(file_path)
            if not full_path.is_absolute():
                full_path = Path(db.base_dir) / file_path
            
            if full_path.exists() and full_path.is_file():
                try:
                    with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                    
                    if content.strip():
                        if blob_db.save_blob(video_id, content_type, content):
                            blobs_saved += 1
                except Exception as e:
                    print(f"❌ Error processing {video_id} ({content_type}): {e}")
                    errors += 1
        
        if processed % 100 == 0:
            print(f"⏳ Processed {processed}/{total} videos... ({blobs_saved} blobs saved)")

    print(f"\n✅ Migration finished!")
    print(f"📈 Total videos scanned: {processed}")
    print(f"💎 Total blobs saved: {blobs_saved}")
    print(f"⚠️ Total errors: {errors}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate transcripts/resumes to Blob storage")
    parser.add_argument("--limit", type=int, help="Limit number of videos to process")
    args = parser.parse_args()
    
    migrate(limit=args.limit)
