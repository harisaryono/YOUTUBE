#!/usr/bin/env python3
import sqlite3
import os
from pathlib import Path
from datetime import datetime

def sanitize_name(name):
    # Match the logic in database_optimized.py
    safe = name.replace('/', '_').replace('\\', '_')
    safe = safe.replace(':', '_').replace('*', '_').replace('?', '_')
    safe = safe.replace('"', '_').replace('<', '_').replace('>', '_').replace('|', '_')
    safe = safe.replace(' ', '_')
    return safe

def main():
    db_path = "youtube_transcripts.db"
    base_dir = Path(".") # Run from project root
    
    if not os.path.exists(db_path):
        print(f"Error: {db_path} not found.")
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Query 168 rows from summaries
    query = """
    SELECT s.id as summary_id, v.id as video_id, v.video_id as video_slug, 
           s.summary_text, c.channel_name, v.title
    FROM summaries s
    JOIN videos v ON s.video_id = v.id
    JOIN channels c ON v.channel_id = c.id
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    
    print(f"Found {len(rows)} summaries to migrate.")
    
    migrated_count = 0
    
    for row in rows:
        channel_name = row['channel_name']
        video_slug = row['video_slug']
        summary_text = row['summary_text']
        
        # Determine sanitized channel folder
        # We saw Kenapa Itu Ya? -> Kenapa_Itu_Ya_
        # We saw Kok Bisa? -> Kok_Bisa_
        folder_name = sanitize_name(channel_name)
        
        # Override for specific known folders if sanitize_name differs slightly
        if channel_name == "Kenapa Itu Ya?": folder_name = "Kenapa_Itu_Ya_"
        if channel_name == "Kok Bisa?": folder_name = "Kok_Bisa_"
        
        resume_dir = base_dir / "uploads" / folder_name / "resume"
        resume_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{video_slug}_summary_{timestamp}.md"
        file_path = resume_dir / filename
        
        # Write to file
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(summary_text)
            
            # Relative path for DB
            rel_path = os.path.relpath(file_path, base_dir)
            
            # Update video record
            cursor.execute("UPDATE videos SET summary_file_path = ? WHERE id = ?", (rel_path, row['video_id']))
            
            migrated_count += 1
            print(f"[{migrated_count}/{len(rows)}] Migrated: {video_slug} -> {rel_path}")
            
        except Exception as e:
            print(f"Error migrating {video_slug}: {e}")

    conn.commit()
    conn.close()
    
    print("\n" + "="*30)
    print(f"Migration Completed: {migrated_count} records processed.")
    print("="*30)

if __name__ == "__main__":
    main()
