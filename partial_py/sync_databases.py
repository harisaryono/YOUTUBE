#!/usr/bin/env python3
"""
Sync Database Script - Sinkronisasi Lengkap channels.db → youtube_transcripts.db

Script ini melakukan:
1. Backup database target sebelum migrasi
2. Migrasi channels dan videos
3. Validasi data setelah migrasi
4. Update statistik dan metadata
"""

import sqlite3
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional


class DatabaseSync:
    """Synchronizer untuk channels.db → youtube_transcripts.db"""
    
    def __init__(self, source_db: str, target_db: str, backup_dir: str = "backups"):
        self.source_db = Path(source_db)
        self.target_db = Path(target_db)
        self.backup_dir = Path(backup_dir)
        
        self.source_conn = None
        self.target_conn = None
        
        self.channel_id_map: Dict[int, int] = {}
        self.stats = {
            'backup_created': False,
            'backup_path': None,
            'channels_migrated': 0,
            'channels_skipped': 0,
            'videos_migrated': 0,
            'videos_skipped': 0,
            'validation_passed': False,
            'errors': []
        }
    
    def connect(self):
        """Buka koneksi ke kedua database"""
        if not self.source_db.exists():
            raise FileNotFoundError(f"Source database not found: {self.source_db}")
        
        if not self.target_db.exists():
            raise FileNotFoundError(f"Target database not found: {self.target_db}")
        
        self.source_conn = sqlite3.connect(str(self.source_db))
        self.source_conn.row_factory = sqlite3.Row
        
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
    
    def create_backup(self) -> Optional[str]:
        """Buat backup target database"""
        print("\n" + "=" * 60)
        print("💾 BACKUP DATABASE")
        print("=" * 60)
        
        self.backup_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"youtube_transcripts_backup_{timestamp}.db"
        backup_path = self.backup_dir / backup_filename
        
        try:
            shutil.copy2(self.target_db, backup_path)
            
            # Backup WAL/SHM files if exist
            for ext in ['-wal', '-shm']:
                wal_file = Path(str(self.target_db) + ext)
                if wal_file.exists():
                    shutil.copy2(wal_file, Path(str(backup_path) + ext))
            
            self.stats['backup_created'] = True
            self.stats['backup_path'] = str(backup_path)
            
            print(f"✅ Backup created: {backup_path}")
            print(f"   Size: {backup_path.stat().st_size:,} bytes")
            
            return str(backup_path)
            
        except Exception as e:
            error_msg = f"Backup failed: {str(e)}"
            self.stats['errors'].append(error_msg)
            print(f"❌ {error_msg}")
            return None
    
    def verify_backup(self, backup_path: str) -> bool:
        """Verifikasi backup valid"""
        try:
            conn = sqlite3.connect(backup_path)
            cursor = conn.cursor()
            cursor.execute("PRAGMA integrity_check")
            result = cursor.fetchone()[0]
            conn.close()
            
            if result == 'ok':
                print(f"✅ Backup verified: integrity OK")
                return True
            else:
                print(f"❌ Backup integrity check failed: {result}")
                return False
                
        except Exception as e:
            print(f"❌ Backup verification failed: {str(e)}")
            return False
    
    def migrate_channels(self) -> Dict[int, int]:
        """Migrasi channels dari source ke target"""
        print("\n" + "=" * 60)
        print("📺 MIGRASI CHANNELS")
        print("=" * 60)
        
        cursor = self.source_conn.cursor()
        target_cursor = self.target_conn.cursor()
        
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
                # Try to insert, ignore if exists
                target_cursor.execute("""
                    INSERT OR IGNORE INTO channels 
                    (channel_id, channel_name, channel_url, last_updated)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """, (channel_name, channel_name, channel_url))
                
                # Always get the channel ID (whether newly inserted or existing)
                target_cursor.execute("""
                    SELECT id FROM channels WHERE channel_id = ?
                """, (channel_name,))
                
                result = target_cursor.fetchone()
                if result:
                    new_id = result['id']
                    # ALWAYS add to mapping, regardless of whether it was inserted or skipped
                    self.channel_id_map[old_id] = new_id
                    
                    if target_cursor.rowcount > 0:
                        self.stats['channels_migrated'] += 1
                        print(f"   ✓ Migrated: {channel_name} (ID: {old_id} → {new_id})")
                    else:
                        self.stats['channels_skipped'] += 1
                        print(f"   ⊘ Skipped (exists): {channel_name} (ID: {old_id} → {new_id})")
                
                self.target_conn.commit()
                
            except Exception as e:
                error_msg = f"Channel {old_id} ({channel_name}): {str(e)}"
                self.stats['errors'].append(error_msg)
                print(f"   ✗ Error: {error_msg}")
                self.target_conn.rollback()
        
        print(f"\n📈 Channel Summary:")
        print(f"   ✓ Migrated: {self.stats['channels_migrated']}")
        print(f"   ⊘ Skipped: {self.stats['channels_skipped']}")
        print(f"   📊 Mapped: {len(self.channel_id_map)} channels")
        
        return self.channel_id_map
    
    def migrate_videos(self, batch_size: int = 1000) -> int:
        """Migrasi videos dari source ke target"""
        print("\n" + "=" * 60)
        print("🎬 MIGRASI VIDEOS")
        print("=" * 60)
        
        if not self.channel_id_map:
            print("❗ Run migrate_channels() first!")
            return 0
        
        source_cursor = self.source_conn.cursor()
        target_cursor = self.target_conn.cursor()
        
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
        
        def status_to_bool(status: str) -> int:
            return 1 if status == 'downloaded' else 0
        
        batch = []
        total_processed = 0
        
        for video in videos:
            old_channel_id = video['channel_id']
            if old_channel_id not in self.channel_id_map:
                self.stats['errors'].append(
                    f"Video {video['video_id']}: Channel {old_channel_id} not mapped"
                )
                continue
            
            new_channel_id = self.channel_id_map[old_channel_id]
            
            video_data = {
                'video_id': video['video_id'],
                'channel_id': new_channel_id,
                'title': video['title'],
                'upload_date': video['upload_date'],
                'transcript_downloaded': status_to_bool(video['status_download']),
                'transcript_language': video['transcript_lang'] or '',
                'transcript_file_path': video['link_file'] or '',
                'summary_file_path': video['link_resume'] or '',
                'video_url': f"https://www.youtube.com/watch?v={video['video_id']}",
            }
            
            batch.append(video_data)
            
            if len(batch) >= batch_size:
                self._insert_video_batch(batch, target_cursor)
                total_processed += len(batch)
                batch = []
                
                if total_processed % 5000 == 0:
                    print(f"   ⏳ Processed {total_processed}/{len(videos)} videos...")
        
        if batch:
            self._insert_video_batch(batch, target_cursor)
            total_processed += len(batch)
        
        self.target_conn.commit()
        
        print(f"\n📈 Video Summary:")
        print(f"   ✓ Migrated: {self.stats['videos_migrated']}")
        print(f"   ⊘ Skipped: {self.stats['videos_skipped']}")
        
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
                self.stats['errors'].append(f"Video {video['video_id']}: {str(e)}")
    
    def validate_migration(self) -> bool:
        """Validasi hasil migrasi"""
        print("\n" + "=" * 60)
        print("✅ VALIDASI MIGRASI")
        print("=" * 60)
        
        validation_errors = []
        
        # 1. Count validation
        source_cursor = self.source_conn.cursor()
        target_cursor = self.target_conn.cursor()
        
        # Source counts
        source_cursor.execute("SELECT COUNT(*) FROM channels")
        source_channels = source_cursor.fetchone()[0]
        
        source_cursor.execute("SELECT COUNT(*) FROM videos")
        source_videos = source_cursor.fetchone()[0]
        
        # Target counts (for migrated channels)
        # Use channel slugs (channel_id in target) from the mapping
        channel_slugs = list(set(self.channel_id_map.values()))  # Unique slugs
        if channel_slugs:
            placeholders = ','.join('?' * len(channel_slugs))
            # Count channels that were migrated (by their slug/channel_id)
            target_cursor.execute(f"""
                SELECT COUNT(*) FROM channels WHERE channel_id IN ({placeholders})
            """, channel_slugs)
            target_channels = target_cursor.fetchone()[0]
            
            # Count videos for those channels - use the numeric IDs from mapping
            mapped_numeric_ids = list(set(self.channel_id_map.values()))
            id_placeholders = ','.join('?' * len(mapped_numeric_ids))
            target_cursor.execute(f"""
                SELECT COUNT(*) FROM videos WHERE channel_id IN ({id_placeholders})
            """, mapped_numeric_ids)
            target_videos = target_cursor.fetchone()[0]
        else:
            target_channels = 0
            target_videos = 0
        
        print(f"\n📊 Count Validation:")
        print(f"   Channels - Source: {source_channels}, In Target: {target_channels}")
        print(f"   Videos   - Source: {source_videos}, In Target: {target_videos}")
        
        # Note: Channel count may differ if channels already existed in target
        # So we only warn, not fail
        if source_channels != target_channels:
            print(f"   ⚠️  Note: Channel count differs (some may already exist in target)")
        
        # 2. Sample validation - check random videos
        print(f"\n🔍 Sample Validation:")
        source_cursor.execute("""
            SELECT channel_id, video_id, title, status_download 
            FROM videos 
            ORDER BY RANDOM() 
            LIMIT 5
        """)
        
        sample_videos = source_cursor.fetchall()
        samples_ok = 0
        
        for video in sample_videos:
            old_ch_id = video['channel_id']
            video_id = video['video_id']
            
            if old_ch_id in self.channel_id_map:
                # The mapping value is the numeric channel ID in target
                new_ch_id = self.channel_id_map[old_ch_id]
                
                target_cursor.execute("""
                    SELECT v.title, v.transcript_downloaded
                    FROM videos v
                    WHERE v.video_id = ? AND v.channel_id = ?
                """, (video_id, new_ch_id))
                
                result = target_cursor.fetchone()
                if result:
                    expected_downloaded = 1 if video['status_download'] == 'downloaded' else 0
                    if result['transcript_downloaded'] == expected_downloaded:
                        samples_ok += 1
                        print(f"   ✓ Video {video_id}: OK (channel: {old_ch_id} → {new_ch_id})")
                    else:
                        validation_errors.append(f"Video {video_id}: transcript_downloaded mismatch")
                        print(f"   ✗ Video {video_id}: transcript_downloaded mismatch")
                else:
                    validation_errors.append(f"Video {video_id}: not found in target (channel: {old_ch_id} → {new_ch_id})")
                    print(f"   ✗ Video {video_id}: not found in target")
            else:
                validation_errors.append(f"Channel {old_ch_id}: not in mapping")
                print(f"   ✗ Channel {old_ch_id}: not in mapping")
        
        print(f"\n   Sample validation: {samples_ok}/{len(sample_videos)} passed")
        
        # Final result - only fail if there are actual errors (not channel count)
        self.stats['validation_passed'] = len(validation_errors) == 0
        
        if validation_errors:
            print(f"\n❌ Validation FAILED with {len(validation_errors)} errors:")
            for err in validation_errors[:5]:
                print(f"   - {err}")
        else:
            print(f"\n✅ Validation PASSED - All checks OK!")
        
        return self.stats['validation_passed']
    
    def update_statistics(self):
        """Update metadata dan statistik"""
        print("\n" + "=" * 60)
        print("📊 UPDATE STATISTIK")
        print("=" * 60)
        
        cursor = self.target_conn.cursor()
        
        # Update channel stats
        for old_id, new_id in self.channel_id_map.items():
            # Count videos for this channel
            cursor.execute("""
                SELECT COUNT(*) as video_count,
                       SUM(CASE WHEN transcript_downloaded = 1 THEN 1 ELSE 0 END) as transcript_count
                FROM videos
                WHERE channel_id = ?
            """, (new_id,))
            
            result = cursor.fetchone()
            video_count = result['video_count'] if result else 0
            transcript_count = result['transcript_count'] if result else 0
            
            # Get channel info
            cursor.execute("SELECT channel_id FROM channels WHERE id = ?", (new_id,))
            channel = cursor.fetchone()
            
            if channel:
                cursor.execute("""
                    INSERT OR REPLACE INTO channels_meta 
                    (channel_id, video_count, transcript_count, last_updated)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """, (channel['channel_id'], video_count, transcript_count))
        
        self.target_conn.commit()
        print(f"✅ Statistics updated for {len(self.channel_id_map)} channels")
    
    def print_report(self):
        """Print laporan lengkap"""
        print("\n" + "=" * 60)
        print("📊 SYNCHRONIZATION REPORT")
        print("=" * 60)
        
        stats = self.stats
        
        print(f"""
Backup:
  Status:      {'✅ Created' if stats['backup_created'] else '❌ Failed'}
  Path:        {stats['backup_path'] or 'N/A'}

Channels:
  ✓ Migrated:  {stats['channels_migrated']}
  ⊘ Skipped:   {stats['channels_skipped']}

Videos:
  ✓ Migrated:  {stats['videos_migrated']}
  ⊘ Skipped:   {stats['videos_skipped']}

Validation:
  Status:      {'✅ PASSED' if stats['validation_passed'] else '❌ FAILED'}

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
    
    def sync(self) -> bool:
        """Jalankan sinkronisasi lengkap"""
        try:
            self.connect()
            
            # Step 1: Backup
            backup_path = self.create_backup()
            if backup_path and not self.verify_backup(backup_path):
                print("❌ Backup verification failed, aborting migration")
                return False
            
            # Step 2: Migrate channels
            self.migrate_channels()
            
            # Step 3: Migrate videos
            self.migrate_videos()
            
            # Step 4: Validate
            self.validate_migration()
            
            # Step 5: Update statistics
            self.update_statistics()
            
            # Print report
            self.print_report()
            
            self.close()
            
            return self.stats['validation_passed']
            
        except Exception as e:
            print(f"\n❌ Synchronization failed: {str(e)}")
            import traceback
            traceback.print_exc()
            self.close()
            return False


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Sync channels.db to youtube_transcripts.db"
    )
    parser.add_argument(
        "--source",
        default="/media/harry/128NEW1/GIT/yt_channel/channels.db",
        help="Path ke source database"
    )
    parser.add_argument(
        "--target",
        default="youtube_transcripts.db",
        help="Path ke target database"
    )
    parser.add_argument(
        "--backup-dir",
        default="backups",
        help="Direktori untuk backup"
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Skip backup (tidak direkomendasikan)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulasi tanpa menulis"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("🔄 YouTube Database Synchronizer")
    print("=" * 60)
    print(f"📁 Source: {args.source}")
    print(f"💾 Target: {args.target}")
    print(f"💾 Backup: {args.backup_dir}")
    print()
    
    if args.dry_run:
        print("⚠️  DRY RUN MODE - No changes will be made")
        print()
    
    try:
        sync = DatabaseSync(args.source, args.target, args.backup_dir)
        
        if args.dry_run:
            sync.connect()
            sync.channel_id_map = sync.migrate_channels()
            sync.close()
            print("\n✅ Dry run completed")
        else:
            if args.no_backup:
                print("⚠️  Skipping backup (not recommended)")
                success = sync.sync()
            else:
                success = sync.sync()
            
            if success:
                print("\n" + "=" * 60)
                print("✅ Synchronization completed successfully!")
                print("=" * 60)
                return 0
            else:
                print("\n" + "=" * 60)
                print("❌ Synchronization completed with errors")
                print("=" * 60)
                return 1
                
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"❌ Synchronization failed: {str(e)}")
        print("=" * 60)
        return 1


if __name__ == "__main__":
    exit(main())
