#!/usr/bin/env python3
"""
Migrate Channels and Videos dari yt_channel/channels.db ke youtube_transcripts.db

Script ini melakukan migrasi data dari database lama (channels.db) ke database baru
(youtube_transcripts.db) dengan mapping field yang sesuai.
"""

import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple


class DatabaseMigrator:
    """Migrator untuk memindahkan data antar database"""
    
    def __init__(self, source_db: str, target_db: str):
        """
        Initialize migrator
        
        Args:
            source_db: Path ke source database (channels.db)
            target_db: Path ke target database (youtube_transcripts.db)
        """
        self.source_db = Path(source_db)
        self.target_db = Path(target_db)
        self.source_conn = None
        self.target_conn = None
        
        # Mapping untuk ID channel (old_id -> new_id)
        self.channel_id_map: Dict[int, int] = {}
        
        # Statistics
        self.stats = {
            'channels_migrated': 0,
            'channels_skipped': 0,
            'videos_migrated': 0,
            'videos_skipped': 0,
            'errors': []
        }
    
    def connect(self):
        """Buka koneksi ke kedua database"""
        if not self.source_db.exists():
            raise FileNotFoundError(f"Source database not found: {self.source_db}")
        
        if not self.target_db.exists():
            raise FileNotFoundError(f"Target database not found: {self.target_db}")
        
        # Source connection
        self.source_conn = sqlite3.connect(str(self.source_db))
        self.source_conn.row_factory = sqlite3.Row
        
        # Target connection
        self.target_conn = sqlite3.connect(str(self.target_db))
        self.target_conn.row_factory = sqlite3.Row
        
        print(f"✅ Connected to source: {self.source_db}")
        print(f"✅ Connected to target: {self.target_db}")
    
    def close(self):
        """Tutup koneksi database"""
        if self.source_conn:
            self.source_conn.close()
        if self.target_conn:
            self.target_conn.close()
        print("✅ Database connections closed")
    
    def migrate_channels(self) -> Dict[int, int]:
        """
        Migrasi semua channels dari source ke target
        
        Returns:
            Dictionary mapping old channel ID ke new channel ID
        """
        print("\n" + "=" * 60)
        print("📺 MIGRASI CHANNELS")
        print("=" * 60)
        
        cursor = self.source_conn.cursor()
        target_cursor = self.target_conn.cursor()
        
        # Get all channels from source
        cursor.execute("""
            SELECT c.id, c.url, c.slug, c.last_scanned, c.category_id,
                   cat.name as category_name
            FROM channels c
            LEFT JOIN categories cat ON c.category_id = cat.id
            ORDER BY c.id
        """)
        
        channels = cursor.fetchall()
        print(f"📊 Found {len(channels)} channels in source database")
        
        for channel in channels:
            old_id = channel['id']
            channel_name = channel['slug']
            channel_url = channel['url']
            
            try:
                # Insert or ignore, then get the ID
                target_cursor.execute("""
                    INSERT OR IGNORE INTO channels 
                    (channel_id, channel_name, channel_url, last_updated)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """, (channel_name, channel_name, channel_url))
                
                # Get the channel ID (either new or existing)
                target_cursor.execute("""
                    SELECT id FROM channels WHERE channel_id = ?
                """, (channel_name,))
                
                result = target_cursor.fetchone()
                if result:
                    new_id = result['id']
                    self.channel_id_map[old_id] = new_id
                    
                    if target_cursor.rowcount > 0:
                        self.stats['channels_migrated'] += 1
                        print(f"   ✓ Migrated: {channel_name} (ID: {old_id} → {new_id})")
                    else:
                        self.stats['channels_skipped'] += 1
                        print(f"   ⊘ Skipped (exists): {channel_name} (ID: {new_id})")
                
                self.target_conn.commit()
                
            except Exception as e:
                error_msg = f"Channel {old_id} ({channel_name}): {str(e)}"
                self.stats['errors'].append(error_msg)
                print(f"   ✗ Error: {error_msg}")
                self.target_conn.rollback()
        
        print(f"\n📈 Channel Migration Summary:")
        print(f"   ✓ Migrated: {self.stats['channels_migrated']}")
        print(f"   ⊘ Skipped: {self.stats['channels_skipped']}")
        print(f"   ✗ Errors: {len([e for e in self.stats['errors'] if 'Channel' in e])}")
        
        return self.channel_id_map
    
    def migrate_videos(self, batch_size: int = 1000) -> int:
        """
        Migrasi semua videos dari source ke target
        
        Args:
            batch_size: Jumlah video per batch untuk commit
            
        Returns:
            Jumlah video yang berhasil dimigrasi
        """
        print("\n" + "=" * 60)
        print("🎬 MIGRASI VIDEOS")
        print("=" * 60)
        
        if not self.channel_id_map:
            print("❗ Run migrate_channels() first!")
            return 0
        
        source_cursor = self.source_conn.cursor()
        target_cursor = self.target_conn.cursor()
        
        # Get all videos from source
        source_cursor.execute("""
            SELECT 
                id, channel_id, video_id, title, upload_date, 
                status_download, link_file, transcript_lang, link_resume,
                resume_text, seq_num
            FROM videos
            ORDER BY channel_id, upload_date
        """)
        
        videos = source_cursor.fetchall()
        print(f"📊 Found {len(videos)} videos in source database")
        
        # Convert status_download to boolean
        def status_to_bool(status: str) -> int:
            return 1 if status == 'downloaded' else 0
        
        # Process in batches
        batch = []
        total_processed = 0
        
        for video in videos:
            # Map channel_id
            old_channel_id = video['channel_id']
            if old_channel_id not in self.channel_id_map:
                error_msg = f"Video {video['video_id']}: Channel {old_channel_id} not mapped"
                self.stats['errors'].append(error_msg)
                continue
            
            new_channel_id = self.channel_id_map[old_channel_id]
            
            # Prepare video data
            video_data = {
                'video_id': video['video_id'],
                'channel_id': new_channel_id,
                'title': video['title'],
                'upload_date': video['upload_date'],  # Format: YYYYMMDD
                'transcript_downloaded': status_to_bool(video['status_download']),
                'transcript_language': video['transcript_lang'] or '',
                'transcript_file_path': video['link_file'] or '',
                'summary_file_path': video['link_resume'] or '',
                'video_url': f"https://www.youtube.com/watch?v={video['video_id']}",
            }
            
            batch.append(video_data)
            
            # Insert batch
            if len(batch) >= batch_size:
                self._insert_video_batch(batch, target_cursor)
                total_processed += len(batch)
                batch = []
                
                if total_processed % 5000 == 0:
                    print(f"   ⏳ Processed {total_processed}/{len(videos)} videos...")
        
        # Insert remaining batch
        if batch:
            self._insert_video_batch(batch, target_cursor)
            total_processed += len(batch)
        
        self.target_conn.commit()
        
        print(f"\n📈 Video Migration Summary:")
        print(f"   ✓ Migrated: {self.stats['videos_migrated']}")
        print(f"   ⊘ Skipped: {self.stats['videos_skipped']}")
        print(f"   ✗ Errors: {len([e for e in self.stats['errors'] if 'Video' in e or 'Channel' in e])}")
        
        return self.stats['videos_migrated']
    
    def _insert_video_batch(self, batch: List[Dict], cursor):
        """Insert batch of videos"""
        for video in batch:
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO videos (
                        video_id, channel_id, title, upload_date,
                        transcript_downloaded, transcript_language,
                        transcript_file_path, summary_file_path, video_url,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """, (
                    video['video_id'],
                    video['channel_id'],
                    video['title'],
                    video['upload_date'],
                    video['transcript_downloaded'],
                    video['transcript_language'],
                    video['transcript_file_path'],
                    video['summary_file_path'],
                    video['video_url']
                ))
                
                if cursor.rowcount > 0:
                    self.stats['videos_migrated'] += 1
                else:
                    self.stats['videos_skipped'] += 1
                    
            except Exception as e:
                error_msg = f"Video {video['video_id']}: {str(e)}"
                self.stats['errors'].append(error_msg)
    
    def get_statistics(self) -> Dict:
        """Get migration statistics"""
        return self.stats
    
    def print_report(self):
        """Print detailed migration report"""
        print("\n" + "=" * 60)
        print("📊 MIGRATION REPORT")
        print("=" * 60)
        
        stats = self.stats
        
        print(f"""
Channels:
  ✓ Migrated:  {stats['channels_migrated']}
  ⊘ Skipped:   {stats['channels_skipped']}

Videos:
  ✓ Migrated:  {stats['videos_migrated']}
  ⊘ Skipped:   {stats['videos_skipped']}

Errors:
  Total:       {len(stats['errors'])}
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
    """Main function untuk migrasi"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Migrate data from channels.db to youtube_transcripts.db"
    )
    parser.add_argument(
        "--source",
        default=str(Path(__file__).resolve().parent.parent.parent / "GIT/yt_channel/channels.db"),
        help="Path ke source database (default: /media/harry/128NEW1/GIT/yt_channel/channels.db)"
    )
    parser.add_argument(
        "--target",
        default="youtube_transcripts.db",
        help="Path ke target database (default: youtube_transcripts.db)"
    )
    parser.add_argument(
        "--channels-only",
        action="store_true",
        help="Hanya migrasi channels, tidak termasuk videos"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulasi tanpa menulis ke database"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("🔄 YouTube Database Migrator")
    print("=" * 60)
    print(f"📁 Source: {args.source}")
    print(f"💾 Target: {args.target}")
    print()
    
    if args.dry_run:
        print("⚠️  DRY RUN MODE - No changes will be made")
        print()
    
    try:
        migrator = DatabaseMigrator(args.source, args.target)
        migrator.connect()
        
        # Migrate channels
        migrator.migrate_channels()
        
        # Migrate videos (unless channels-only)
        if not args.channels_only:
            migrator.migrate_videos()
        
        # Print report
        migrator.print_report()
        
        migrator.close()
        
        print("\n" + "=" * 60)
        print("✅ Migration completed successfully!")
        print("=" * 60)
        
        return 0
        
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"❌ Migration failed: {str(e)}")
        print("=" * 60)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
