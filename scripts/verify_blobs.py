#!/usr/bin/env python3
import sys
from pathlib import Path

# Add current dir to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database_blobs import BlobStorage

def verify():
    blob_storage = BlobStorage("youtube_transcripts_blobs.db")
    
    # Get some video IDs to test
    import sqlite3
    conn = sqlite3.connect("youtube_transcripts_blobs.db")
    cursor = conn.cursor()
    
    print("📋 Checking blob counts...")
    cursor.execute("SELECT content_type, COUNT(*) FROM content_blobs GROUP BY content_type")
    for row in cursor.fetchall():
        print(f"  - {row[0]}: {row[1]}")
        
    print("\n🧪 Testing data retrieval...")
    for ct in ['transcript', 'resume', 'formatted']:
        cursor.execute("SELECT video_id FROM content_blobs WHERE content_type = ? LIMIT 1", (ct,))
        row = cursor.fetchone()
        if row:
            vid = row[0]
            content = blob_storage.get_blob(vid, ct)
            if content:
                print(f"  ✅ {ct} retrieval successful for {vid} (len: {len(content)})")
                # print(f"     Preview: {content[:100]}...")
            else:
                print(f"  ❌ {ct} retrieval failed for {vid}")
        else:
            print(f"  ⚠️ No blobs found for type: {ct}")
            
    conn.close()

if __name__ == "__main__":
    verify()
