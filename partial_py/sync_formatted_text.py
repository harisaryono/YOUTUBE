#!/usr/bin/env python3
"""
Sync Formatted Text Script

Script ini menyalin text_formatted (transkrip tanpa timestamp) ke database target.

Source: /media/harry/128NEW1/GIT/yt_channel/out/{channel}/text_formatted/{id}_{video_id}.txt
Target: /media/harry/128NEW1/GIT/YOUTUBE/uploads/{channel}/text_formatted/{video_id}.txt

Database field: transcript_formatted_path
"""

import sqlite3
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional


class FormattedTextSync:
    """Synchronizer untuk text_formatted (transkrip tanpa timestamp)"""
    
    def __init__(self, source_db: str, target_db: str,
                 source_base: str, target_base: str):
        self.source_db = Path(source_db)
        self.target_db = Path(target_db)
        self.source_base = Path(source_base)
        self.target_base = Path(target_base)
        
        self.source_conn = None
        self.target_conn = None
        
        self.stats = {
            'files_copied': 0,
            'files_skipped': 0,
            'files_not_found': 0,
            'db_updated': 0,
            'errors': []
        }
        
        self.channel_map: Dict[int, str] = {}
    
    def connect(self):
        """Buka koneksi ke database"""
        if not self.source_db.exists():
            raise FileNotFoundError(f"Source database not found: {self.source_db}")
        if not self.target_db.exists():
            raise FileNotFoundError(f"Target database not found: {self.target_db}")
        
        self.source_conn = sqlite3.connect(str(self.source_db))
        self.source_conn.row_factory = sqlite3.Row
        
        self.target_conn = sqlite3.connect(str(self.target_db))
        self.target_conn.row_factory = sqlite3.Row
        
        print(f"✅ Connected to source DB: {self.source_db}")
        print(f"✅ Connected to target DB: {self.target_db}")
    
    def close(self):
        """Tutup koneksi database"""
        if self.source_conn:
            self.source_conn.close()
        if self.target_conn:
            self.target_conn.close()
        print("✅ Database connections closed")
    
    def load_channel_mapping(self):
        """Load mapping channel dari target database"""
        cursor = self.target_conn.cursor()
        cursor.execute("SELECT id, channel_id FROM channels")
        
        for row in cursor.fetchall():
            self.channel_map[row['id']] = row['channel_id']
        
        print(f"📊 Loaded {len(self.channel_map)} channel mappings")
    
    def get_channel_slug_from_source(self, source_channel_id: int) -> str:
        """Dapatkan slug channel dari source database"""
        cursor = self.source_conn.cursor()
        cursor.execute("SELECT slug FROM channels WHERE id = ?", (source_channel_id,))
        result = cursor.fetchone()
        return result['slug'] if result else None
    
    def sync_formatted_text(self):
        """Sync semua text_formatted files"""
        print("\n" + "=" * 60)
        print("📝 SYNC FORMATTED TEXT (WITHOUT TIMESTAMPS)")
        print("=" * 60)
        
        self.load_channel_mapping()
        
        cursor = self.source_conn.cursor()
        
        # Get all videos dengan text_formatted
        cursor.execute("""
            SELECT 
                v.id, v.channel_id, v.video_id,
                v.transcript_formatted_path AS formatted_path
            FROM videos v
            WHERE v.transcript_formatted_path IS NOT NULL 
              AND v.transcript_formatted_path != ''
        """)
        
        videos = cursor.fetchall()
        print(f"📊 Found {len(videos)} videos with formatted text")
        
        for video in videos:
            source_channel_id = video['channel_id']
            video_id = video['video_id']
            link_formatted = video['formatted_path']
            
            if not link_formatted:
                continue
            
            # Dapatkan slug channel
            channel_slug = self.get_channel_slug_from_source(source_channel_id)
            if not channel_slug:
                continue
            
            # Dapatkan numeric channel_id di target
            target_channel_id = None
            for tid, slug in self.channel_map.items():
                if slug == channel_slug:
                    target_channel_id = tid
                    break
            
            if not target_channel_id:
                continue
            
            # Copy file
            self._copy_file(link_formatted, channel_slug, video_id, target_channel_id)
        
        self.target_conn.commit()
        
        print(f"\n📈 Formatted Text Sync Summary:")
        print(f"   ✓ Copied: {self.stats['files_copied']}")
        print(f"   ⊘ Skipped: {self.stats['files_skipped']}")
        print(f"   ✗ Not found: {self.stats['files_not_found']}")
        print(f"   📊 DB updated: {self.stats['db_updated']}")
    
    def _copy_file(self, source_rel_path: str, channel_slug: str,
                   video_id: str, target_channel_id: int):
        """Copy single file dan update database"""
        
        # Source: out/{channel}/text_formatted/{id}_{video_id}.txt
        source_file = self.source_base / channel_slug / source_rel_path
        
        # Target: uploads/{channel}/text_formatted/{video_id}.txt
        file_name = f"{video_id}.txt"
        target_dir = self.target_base / channel_slug / "text_formatted"
        target_file = target_dir / file_name
        
        # Check source
        if not source_file.exists():
            self.stats['files_not_found'] += 1
            return
        
        # Create target dir
        target_dir.mkdir(parents=True, exist_ok=True)
        
        # Copy if not exists
        if target_file.exists():
            self.stats['files_skipped'] += 1
        else:
            shutil.copy2(source_file, target_file)
            self.stats['files_copied'] += 1
        
        # Update database
        rel_path = str(Path(channel_slug) / "text_formatted" / file_name)
        
        target_cursor = self.target_conn.cursor()
        target_cursor.execute("""
            UPDATE videos 
            SET transcript_file_path = ?, 
                transcript_downloaded = 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE video_id = ? AND channel_id = ?
        """, (rel_path, video_id, target_channel_id))
        
        if target_cursor.rowcount > 0:
            self.stats['db_updated'] += 1
    
    def print_report(self):
        """Print laporan"""
        print("\n" + "=" * 60)
        print("📊 FORMATTED TEXT SYNC REPORT")
        print("=" * 60)
        
        stats = self.stats
        
        total = stats['files_copied'] + stats['files_skipped'] + stats['files_not_found']
        
        print(f"""
Files:
  ✓ Copied:     {stats['files_copied']:,}
  ⊘ Skipped:    {stats['files_skipped']:,}
  ✗ Not found:  {stats['files_not_found']:,}
  ─────────────────────────────
  Total:        {total:,}

Database:
  📊 Updated:   {stats['db_updated']:,} records

Errors:
  Total:        {len(stats['errors'])}
""")


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Sync formatted text files"
    )
    parser.add_argument(
        "--source-db",
        default=str(Path(__file__).resolve().parent.parent.parent / "GIT/yt_channel/channels.db"),
        help="Path ke source database"
    )
    parser.add_argument(
        "--target-db",
        default="youtube_transcripts.db",
        help="Path ke target database"
    )
    parser.add_argument(
        "--source-base",
        default=str(Path(__file__).resolve().parent.parent.parent / "GIT/yt_channel/out"),
        help="Base directory source files"
    )
    parser.add_argument(
        "--target-base",
        default="uploads",
        help="Base directory target files"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("📝 Formatted Text Synchronizer")
    print("=" * 60)
    print(f"📁 Source DB: {args.source_db}")
    print(f"💾 Target DB: {args.target_db}")
    print(f"📂 Source base: {args.source_base}")
    print(f"📂 Target base: {args.target_base}")
    print()
    
    try:
        sync = FormattedTextSync(args.source_db, args.target_db,
                                 args.source_base, args.target_base)
        sync.connect()
        
        sync.sync_formatted_text()
        sync.print_report()
        
        sync.close()
        
        print("\n" + "=" * 60)
        print("✅ Formatted text sync completed!")
        print("=" * 60)
        
        return 0
        
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"❌ Sync failed: {str(e)}")
        print("=" * 60)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
