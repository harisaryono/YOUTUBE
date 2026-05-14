import sqlite3
import os
from pathlib import Path

def main():
    db_path = "youtube_transcripts.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all videos with transcript_downloaded=1 but missing/incorrect transcript_file_path
    # OR those that were failing in fill_resumes_fast.py
    cursor.execute("SELECT id, video_id, transcript_file_path FROM videos WHERE transcript_downloaded = 1 AND (summary_file_path IS NULL OR summary_file_path = '');")
    rows = cursor.fetchall()
    
    print(f"Checking {len(rows)} videos for path correction...")
    
    fixed = 0
    for row_id, video_id, old_path in rows:
        # Search for video_id_transcript*.txt in uploads/
        found_path = ""
        # Search for any file containing video_id and ends with .txt or transcript*.txt
        # Better search by pattern
        for p in Path("uploads").rglob(f"{video_id}_transcript*.txt"):
            found_path = str(p)
            break
            
        if found_path:
            if found_path != old_path:
                cursor.execute("UPDATE videos SET transcript_file_path = ? WHERE id = ?", (found_path, row_id))
                fixed += 1
                print(f"Fixed {video_id}: {old_path} -> {found_path}")
        else:
            print(f"Still not found on disk: {video_id}")

    conn.commit()
    conn.close()
    print(f"Total paths fixed: {fixed}")

if __name__ == "__main__":
    main()
