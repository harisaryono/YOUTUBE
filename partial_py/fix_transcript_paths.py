import sqlite3
import os
from pathlib import Path

def main():
    db_path = "youtube_transcripts.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Find videos with missing transcript_file_path but having transcript_path
    cursor.execute("SELECT id, transcript_path FROM videos WHERE (transcript_file_path IS NULL OR transcript_file_path = '') AND transcript_path IS NOT NULL AND transcript_path != '';")
    rows = cursor.fetchall()
    
    print(f"Checking {len(rows)} potential path fixes...")
    
    fixed = 0
    for row_id, t_path_old in rows:
        # t_path_old like 'transcripts/DYF7dk44rJU_transcript_20260325_134406.txt'
        filename = os.path.basename(t_path_old)
        
        # Search for this filename in uploads/
        # Simplified: we know it's likely in Kenapa_Itu_Ya_ for these 15?
        # Let's search properly
        found_path = ""
        for p in Path("uploads").rglob(filename):
            found_path = str(p)
            break
            
        if found_path:
            cursor.execute("UPDATE videos SET transcript_file_path = ? WHERE id = ?", (found_path, row_id))
            fixed += 1
            print(f"Fixed {row_id}: {t_path_old} -> {found_path}")
        else:
            print(f"Not found: {filename}")

    conn.commit()
    conn.close()
    print(f"Total fixed: {fixed}")

if __name__ == "__main__":
    main()
