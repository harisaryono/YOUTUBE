#!/usr/bin/env python3
"""
Merge duplicate Harta Alam channels.
- Master: HartaAlamyangTerlupakan (ID: 49)
- Slave: @HartaAlamyangTerlupakan (ID: 6) - to be deleted

Also merge file folders:
- Source: uploads/Harta_Alam_yang_Terlupakan/
- Target: uploads/HartaAlamyangTerlupakan/
"""

import sqlite3
import shutil
from pathlib import Path

DB_PATH = "youtube_transcripts.db"
UPLOADS_DIR = Path("uploads")

# Channel IDs
MASTER_CHANNEL_ID = "HartaAlamyangTerlupakan"  # ID 49
SLAVE_CHANNEL_ID = "@HartaAlamyangTerlupakan"  # ID 6

# Folder names
SOURCE_FOLDER = "Harta_Alam_yang_Terlupakan"
TARGET_FOLDER = "HartaAlamyangTerlupakan"


def merge_channels():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    
    try:
        # Get channel IDs
        master = con.execute(
            "SELECT id, channel_id, channel_name FROM channels WHERE channel_id = ?",
            (MASTER_CHANNEL_ID,)
        ).fetchone()
        
        slave = con.execute(
            "SELECT id, channel_id, channel_name FROM channels WHERE channel_id = ?",
            (SLAVE_CHANNEL_ID,)
        ).fetchone()
        
        if not master or not slave:
            print("ERROR: One or both channels not found!")
            return False
        
        master_id = master['id']
        slave_id = slave['id']
        
        print(f"Master channel: {MASTER_CHANNEL_ID} (ID: {master_id})")
        print(f"Slave channel: {SLAVE_CHANNEL_ID} (ID: {slave_id})")
        
        # Step 1: Move videos from slave to master
        print("\n[1/4] Moving videos from slave to master...")
        moved = con.execute("""
            UPDATE videos
            SET channel_id = ?
            WHERE channel_id = ?
        """, (master_id, slave_id)).rowcount
        
        print(f"  Moved {moved} videos")
        
        # Step 2: Update video paths to use target folder
        print("\n[2/4] Updating file paths...")
        
        # Update transcript paths
        updated = con.execute("""
            UPDATE videos
            SET transcript_file_path = REPLACE(
                transcript_file_path,
                ?,
                ?
            )
            WHERE channel_id = ?
        """, (
            f"{SOURCE_FOLDER}/text/",
            f"{TARGET_FOLDER}/text/",
            master_id
        )).rowcount
        print(f"  Updated {updated} transcript paths")
        
        # Update summary paths
        updated = con.execute("""
            UPDATE videos
            SET summary_file_path = REPLACE(
                summary_file_path,
                ?,
                ?
            )
            WHERE channel_id = ?
        """, (
            f"{SOURCE_FOLDER}/resume/",
            f"{TARGET_FOLDER}/resume/",
            master_id
        )).rowcount
        print(f"  Updated {updated} summary paths")
        
        # Step 3: Delete slave channel
        print("\n[3/4] Deleting slave channel...")
        con.execute("DELETE FROM channels WHERE id = ?", (slave_id,))
        print(f"  Deleted channel ID {slave_id}")
        
        # Step 4: Merge folders
        print("\n[4/4] Merging file folders...")
        source_dir = UPLOADS_DIR / SOURCE_FOLDER
        target_dir = UPLOADS_DIR / TARGET_FOLDER
        
        if source_dir.exists():
            # Copy text files
            source_text = source_dir / "text"
            target_text = target_dir / "text"
            
            if source_text.exists():
                target_text.mkdir(parents=True, exist_ok=True)
                copied = 0
                for f in source_text.glob("*.txt"):
                    shutil.copy2(f, target_text / f.name)
                    copied += 1
                print(f"  Copied {copied} text files")
            
            # Copy resume files
            source_resume = source_dir / "resume"
            target_resume = target_dir / "resume"
            
            if source_resume.exists():
                target_resume.mkdir(parents=True, exist_ok=True)
                copied = 0
                for f in source_resume.glob("*.md"):
                    shutil.copy2(f, target_resume / f.name)
                    copied += 1
                print(f"  Copied {copied} resume files")
            
            # Optionally remove source folder
            # shutil.rmtree(source_dir)
            print(f"  Source folder: {source_dir} (kept for backup)")
        else:
            print(f"  Source folder {source_dir} not found")
        
        con.commit()
        
        # Verify
        print("\n[VERIFICATION]")
        count = con.execute(
            "SELECT COUNT(*) as count FROM videos WHERE channel_id = ?",
            (master_id,)
        ).fetchone()['count']
        print(f"  Total videos in master: {count}")
        
        channels = con.execute("SELECT id, channel_id, channel_name FROM channels WHERE channel_name LIKE '%Harta%'").fetchall()
        print(f"  Remaining channels: {len(channels)}")
        for c in channels:
            print(f"    - {c['channel_id']} ({c['channel_name']})")
        
        print("\n✅ Merge completed successfully!")
        return True
        
    except Exception as e:
        print(f"ERROR: {e}")
        con.rollback()
        return False
        
    finally:
        con.close()


if __name__ == "__main__":
    merge_channels()
