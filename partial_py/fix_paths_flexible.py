import sqlite3
import os
from pathlib import Path

def main():
    db_path = "youtube_transcripts.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all videos with transcript_downloaded=1 but missing/incorrect summary
    cursor.execute("SELECT id, video_id, transcript_file_path FROM videos WHERE transcript_downloaded = 1 AND (summary_file_path IS NULL OR summary_file_path = '');")
    rows = cursor.fetchall()
    
    print(f"Checking {len(rows)} videos for flexible path correction...")
    
    fixed = 0
    for row_id, video_id, old_path in rows:
        found_path = ""
        # Flexible search: any .txt file that starts with or contains video_id as a word
        # In practice, most are {video_id}_transcript_... or {video_id}.txt
        # Try both common patterns
        patterns = [f"{video_id}_transcript*.txt", f"{video_id}.txt", f"*{video_id}*.txt"]
        for pat in patterns:
            for p in Path("uploads").rglob(pat):
                found_path = str(p)
                break
            if found_path: break
            
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
