#!/usr/bin/env python3
"""
Sync Files Script - Menyalin file text dan resume dari yt_channel ke uploads

Script ini menyalin file transkrip (.txt) dan resume (.md) dari:
  /media/harry/128NEW1/GIT/yt_channel/out/{channel}/text/
  /media/harry/128NEW1/GIT/yt_channel/out/{channel}/resume/

Ke:
  /media/harry/128NEW1/GIT/YOUTUBE/uploads/{channel}/text/
  /media/harry/128NEW1/GIT/YOUTUBE/uploads/{channel}/resume/

Dan update database dengan path yang benar.
"""

import sqlite3
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple


class FileSync:
    """Synchronizer untuk file transkrip dan resume"""
    
    def __init__(self, source_db: str, target_db: str, 
                 source_base: str, target_base: str):
        self.source_db = Path(source_db)
        self.target_db = Path(target_db)
        self.source_base = Path(source_base)  # /media/harry/128NEW1/GIT/yt_channel/out
        self.target_base = Path(target_base)  # /media/harry/128NEW1/GIT/YOUTUBE/uploads
        
        self.source_conn = None
        self.target_conn = None
        
        self.stats = {
            'text_files_copied': 0,
            'text_files_skipped': 0,
            'text_files_not_found': 0,
            'resume_files_copied': 0,
            'resume_files_skipped': 0,
            'resume_files_not_found': 0,
            'db_updated': 0,
            'errors': []
        }
        
        # Channel mapping: channel_id (numeric) -> channel_slug
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
            # channel_id di target adalah slug (e.g., "FirandaAndirjaOfficial")
            self.channel_map[row['id']] = row['channel_id']
        
        print(f"📊 Loaded {len(self.channel_map)} channel mappings")
    
    def get_channel_slug_from_source(self, source_channel_id: int) -> str:
        """Dapatkan slug channel dari source database"""
        cursor = self.source_conn.cursor()
        cursor.execute("""
            SELECT slug FROM channels WHERE id = ?
        """, (source_channel_id,))
        
        result = cursor.fetchone()
        return result['slug'] if result else None
    
    def sync_files(self, batch_size: int = 1000):
        """Sync semua file dari source ke target"""
        print("\n" + "=" * 60)
        print("📁 SYNC FILES")
        print("=" * 60)
        
        self.load_channel_mapping()
        
        cursor = self.source_conn.cursor()
        target_cursor = self.target_conn.cursor()
        
        # Get all videos dengan file paths dari source
        cursor.execute("""
            SELECT 
                v.id, v.channel_id, v.video_id, 
                v.link_file, v.link_resume,
                v.transcript_formatted_path AS formatted_path,
                v.resume_text
            FROM videos v
            WHERE v.link_file IS NOT NULL OR v.link_resume IS NOT NULL OR v.transcript_formatted_path IS NOT NULL
        """)
        
        videos = cursor.fetchall()
        print(f"📊 Found {len(videos)} videos with files in source database")
        
        processed = 0
        
        for video in videos:
            source_channel_id = video['channel_id']
            video_id = video['video_id']
            
            # Dapatkan slug channel
            channel_slug = self.get_channel_slug_from_source(source_channel_id)
            if not channel_slug:
                self.stats['errors'].append(f"Video {video_id}: Channel {source_channel_id} not found")
                continue
            
            # Dapatkan numeric channel_id di target
            target_channel_id = None
            for tid, slug in self.channel_map.items():
                if slug == channel_slug:
                    target_channel_id = tid
                    break
            
            if not target_channel_id:
                self.stats['errors'].append(f"Video {video_id}: Channel {channel_slug} not in target mapping")
                continue
            
            # Process text file
            if video['link_file']:
                self._copy_file(
                    video['link_file'],
                    channel_slug,
                    video_id,
                    'text',
                    target_channel_id,
                    'transcript_file_path'
                )
            
            # Process resume file
            if video['link_resume']:
                self._copy_file(
                    video['link_resume'],
                    channel_slug,
                    video_id,
                    'resume',
                    target_channel_id,
                    'summary_file_path'
                )
            
            processed += 1
            if processed % 5000 == 0:
                print(f"   ⏳ Processed {processed}/{len(videos)} videos...")
        
        self.target_conn.commit()
        
        print(f"\n📈 File Sync Summary:")
        print(f"   ✓ Text files copied: {self.stats['text_files_copied']}")
        print(f"   ⊘ Text files skipped: {self.stats['text_files_skipped']}")
        print(f"   ✗ Text files not found: {self.stats['text_files_not_found']}")
        print(f"   ✓ Resume files copied: {self.stats['resume_files_copied']}")
        print(f"   ⊘ Resume files skipped: {self.stats['resume_files_skipped']}")
        print(f"   ✗ Resume files not found: {self.stats['resume_files_not_found']}")
        print(f"   📊 DB records updated: {self.stats['db_updated']}")
    
    def _copy_file(self, source_rel_path: str, channel_slug: str, 
                   video_id: str, file_type: str, target_channel_id: int,
                   db_field: str):
        """Copy single file dan update database"""
        
        # Source file path: /out/{channel}/{file_type}/{id}_{video_id}.ext
        source_file = self.source_base / channel_slug / source_rel_path
        
        # Target file path: /uploads/{channel}/{file_type}/{video_id}.ext
        file_name = f"{video_id}{Path(source_rel_path).suffix}"
        target_dir = self.target_base / channel_slug / file_type
        target_file = target_dir / file_name
        
        # Check if source exists
        if not source_file.exists():
            self.stats[f'{file_type}_files_not_found'] += 1
            return
        
        # Create target directory if not exists
        target_dir.mkdir(parents=True, exist_ok=True)
        
        # Copy file if not exists or different
        if target_file.exists():
            self.stats[f'{file_type}_files_skipped'] += 1
        else:
            shutil.copy2(source_file, target_file)
            self.stats[f'{file_type}_files_copied'] += 1
        
        # Update database with new path (relative to uploads dir)
        rel_path = str(Path(channel_slug) / file_type / file_name)
        
        target_cursor = self.target_conn.cursor()
        target_cursor.execute(f"""
            UPDATE videos 
            SET {db_field} = ?, updated_at = CURRENT_TIMESTAMP
            WHERE video_id = ? AND channel_id = ?
        """, (rel_path, video_id, target_channel_id))
        
        if target_cursor.rowcount > 0:
            self.stats['db_updated'] += 1
    
    def print_report(self):
        """Print laporan"""
        print("\n" + "=" * 60)
        print("📊 FILE SYNC REPORT")
        print("=" * 60)
        
        stats = self.stats
        
        total_text = stats['text_files_copied'] + stats['text_files_skipped'] + stats['text_files_not_found']
        total_resume = stats['resume_files_copied'] + stats['resume_files_skipped'] + stats['resume_files_not_found']
        
        print(f"""
Text Files:
  ✓ Copied:     {stats['text_files_copied']:,}
  ⊘ Skipped:    {stats['text_files_skipped']:,}
  ✗ Not found:  {stats['text_files_not_found']:,}
  ─────────────────────────────
  Total:        {total_text:,}

Resume Files:
  ✓ Copied:     {stats['resume_files_copied']:,}
  ⊘ Skipped:    {stats['resume_files_skipped']:,}
  ✗ Not found:  {stats['resume_files_not_found']:,}
  ─────────────────────────────
  Total:        {total_resume:,}

Database:
  📊 Updated:   {stats['db_updated']:,} records

Errors:
  Total:        {len(stats['errors'])}
""")
        
        if stats['errors'] and len(stats['errors']) <= 10:
            print("Error details:")
            for error in stats['errors'][:10]:
                print(f"  - {error}")
        elif len(stats['errors']) > 10:
            print(f"First 10 errors:")
            for error in stats['errors'][:10]:
                print(f"  - {error}")
            print(f"  ... and {len(stats['errors']) - 10} more")


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Sync transcript and resume files"
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
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulasi tanpa copy file"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("📁 YouTube File Synchronizer")
    print("=" * 60)
    print(f"📁 Source DB: {args.source_db}")
    print(f"💾 Target DB: {args.target_db}")
    print(f"📂 Source base: {args.source_base}")
    print(f"📂 Target base: {args.target_base}")
    print()
    
    if args.dry_run:
        print("⚠️  DRY RUN MODE - No files will be copied")
        print()
    
    try:
        sync = FileSync(args.source_db, args.target_db, 
                       args.source_base, args.target_base)
        sync.connect()
        
        if args.dry_run:
            sync.load_channel_mapping()
            print(f"\n✅ Dry run completed. Would process files for {len(sync.channel_map)} channels.")
        else:
            sync.sync_files()
            sync.print_report()
        
        sync.close()
        
        print("\n" + "=" * 60)
        print("✅ File synchronization completed!")
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
