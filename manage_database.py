#!/usr/bin/env python3
"""
Database Management Utilities
Untuk mengelola database YouTube Transcript Framework
"""

import sqlite3
from partial_py.database import TranscriptDatabase
from partial_py.youtube_transcript_complete import YouTubeTranscriptComplete


class DatabaseManager:
    def __init__(self, db_path: str = "youtube_transcripts.db"):
        """Inisialisasi Database Manager"""
        self.db_path = db_path
        self.db = TranscriptDatabase(db_path)
    
    def show_statistics(self):
        """Tampilkan statistik database"""
        print("\n📊 DATABASE STATISTICS")
        print("=" * 50)
        
        stats = self.db.get_statistics()
        
        print(f"\n📺 Channels:")
        print(f"   Total: {stats['total_channels']}")
        
        print(f"\n📹 Videos:")
        print(f"   Total: {stats['total_videos']}")
        print(f"   With Transcript: {stats['videos_with_transcript']}")
        print(f"   Without Transcript: {stats['total_videos'] - stats['videos_with_transcript']}")
        
        print(f"\n📝 Transcripts:")
        print(f"   Total: {stats['total_transcripts']}")
        
        print(f"\n📄 Summaries:")
        print(f"   Total: {stats['total_summaries']}")
        
        print(f"\n📥 Download Queue:")
        print(f"   ⏳ Pending: {stats['pending_downloads']}")
        print(f"   ✅ Completed: {stats['completed_downloads']}")
        print(f"   ❌ Failed: {stats['failed_downloads']}")
        
        print("\n" + "=" * 50)
    
    def show_channels(self, limit: int = 20):
        """Tampilkan daftar channel"""
        print("\n📺 CHANNELS")
        print("=" * 80)
        
        cursor = self.db.conn.cursor()
        cursor.execute("""
            SELECT * FROM channels 
            ORDER BY video_count DESC 
            LIMIT ?
        """, (limit,))
        
        results = cursor.fetchall()
        
        if not results:
            print("Tidak ada channel dalam database.")
            return
        
        for row in results:
            channel = dict(row)
            print(f"\n📺 {channel['channel_name']}")
            print(f"   ID: {channel['channel_id']}")
            print(f"   URL: {channel['channel_url']}")
            print(f"   Videos: {channel['video_count']}")
            print(f"   Subscribers: {channel['subscriber_count']:,}")
            print(f"   Last Updated: {channel['last_updated']}")
    
    def show_videos(self, channel_id: str = None, limit: int = 20, 
                   only_with_transcript: bool = False, only_without: bool = False):
        """Tampilkan daftar video"""
        print("\n📹 VIDEOS")
        print("=" * 80)
        
        cursor = self.db.conn.cursor()
        
        if channel_id:
            # Videos dari specific channel
            query = """
                SELECT v.*, c.channel_name 
                FROM videos v 
                JOIN channels c ON v.channel_id = c.id
                WHERE c.channel_id = ?
            """
            params = [channel_id]
        else:
            # Semua videos
            query = """
                SELECT v.*, c.channel_name 
                FROM videos v 
                JOIN channels c ON v.channel_id = c.id
                WHERE 1=1
            """
            params = []
        
        if only_with_transcript:
            query += " AND v.transcript_downloaded = 1"
        elif only_without:
            query += " AND v.transcript_downloaded = 0"
        
        query += " ORDER BY v.upload_date DESC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        if not results:
            print("Tidak ada video dalam database.")
            return
        
        for row in results:
            video = dict(row)
            status = "✅" if video['transcript_downloaded'] else "❌"
            
            print(f"\n{status} {video['title'][:60]}")
            print(f"   📺 Channel: {video['channel_name']}")
            print(f"   📹 Video ID: {video['video_id']}")
            print(f"   ⏱️  Duration: {video['duration']}s")
            print(f"   👁️  Views: {video['view_count']:,}")
            print(f"   📅 Upload: {video['upload_date']}")
            
            if video['transcript_path']:
                print(f"   💾 Transcript: {video['transcript_path']}")
            if video['summary_path']:
                print(f"   📄 Summary: {video['summary_path']}")
    
    def show_pending_downloads(self):
        """Tampilkan antrian download yang pending"""
        print("\n⏳ PENDING DOWNLOADS")
        print("=" * 80)
        
        pending = self.db.get_pending_queue(limit=50)
        
        if not pending:
            print("Tidak ada antrian pending.")
            return
        
        print(f"\nTotal: {len(pending)} pending downloads\n")
        
        for item in pending:
            print(f"📹 {item['title'][:60]}")
            print(f"   📺 Channel: {item['channel_name']}")
            print(f"   📹 Video ID: {item['video_id']}")
            print(f"   Retry Count: {item['retry_count']}")
            print(f"   Created: {item['created_at']}")
            if item['error_message']:
                print(f"   ❌ Error: {item['error_message']}")
            print()
    
    def search_database(self, query: str, limit: int = 20):
        """Cari video dalam database"""
        print(f"\n🔍 SEARCH RESULTS: '{query}'")
        print("=" * 80)
        
        results = self.db.search_videos(query, limit)
        
        if not results:
            print(f"Tidak ditemukan hasil untuk '{query}'")
            return
        
        print(f"\nDitemukan {len(results)} video:\n")
        
        for i, video in enumerate(results, 1):
            status = "✅" if video['transcript_downloaded'] else "❌"
            
            print(f"{i}. {status} {video['title'][:60]}")
            print(f"   📺 Channel: {video['channel_name']}")
            print(f"   📹 Video ID: {video['video_id']}")
            print(f"   👁️  Views: {video['view_count']:,}")
            print()
    
    def show_video_details(self, video_id: str):
        """Tampilkan detail lengkap video"""
        print(f"\n📹 VIDEO DETAILS: {video_id}")
        print("=" * 80)
        
        video = self.db.get_video_by_id(video_id)
        
        if not video:
            print(f"Video dengan ID '{video_id}' tidak ditemukan dalam database.")
            return
        
        print(f"\n📝 Title: {video['title']}")
        print(f"📺 Channel: {video.get('channel_name', 'Unknown')}")
        print(f"📹 Video ID: {video['video_id']}")
        print(f"🔗 URL: {video['video_url']}")
        print(f"⏱️  Duration: {video['duration']}s")
        print(f"👁️  Views: {video['view_count']:,}")
        print(f"📅 Upload: {video['upload_date']}")
        print(f"✅ Transcript Downloaded: {video['transcript_downloaded']}")
        
        if video['description']:
            desc = video['description'][:200] + "..." if len(video['description']) > 200 else video['description']
            print(f"\n📄 Description: {desc}")
        
        # Tampilkan transcript jika ada
        transcript = self.db.get_transcript_by_video_id(video_id)
        if transcript:
            print(f"\n📝 Transcript ({transcript['language']}):")
            print(f"   Word Count: {transcript['word_count']}")
            print(f"   Duration: {transcript['duration']:.2f}s")
            print(f"   Format: {transcript['format_type']}")
        
        # Tampilkan summary jika ada
        summary = self.db.get_summary_by_video_id(video_id)
        if summary:
            print(f"\n📄 Summary:")
            print(f"   {summary['summary_text']}")
    
    def retry_failed_downloads(self, limit: int = 10):
        """Retry failed downloads"""
        print("\n🔄 RETRY FAILED DOWNLOADS")
        print("=" * 80)
        
        # Get failed downloads
        cursor = self.db.conn.cursor()
        cursor.execute("""
            SELECT q.*, v.video_id, v.title, c.channel_name
            FROM download_queue q
            JOIN videos v ON q.video_id = v.id
            JOIN channels c ON v.channel_id = c.id
            WHERE q.status = 'failed'
            ORDER BY q.updated_at DESC
            LIMIT ?
        """, (limit,))
        
        failed = cursor.fetchall()
        
        if not failed:
            print("Tidak ada failed download untuk diretry.")
            return
        
        print(f"\nDitemukan {len(failed)} failed downloads.\n")
        
        # Retry each failed download
        yt = YouTubeTranscriptComplete(database=self.db)
        
        for i, item in enumerate(failed, 1):
            video = dict(item)
            print(f"[{i}/{len(failed)}] Retry: {video['title'][:50]}")
            
            try:
                # Reset status to pending
                self.db.update_queue_status(video['id'], 'pending')
                
                # Download transcript
                video_url = f"https://www.youtube.com/watch?v={video['video_id']}"
                result = yt.download_single_transcript(video_url, 'id', save_to_db=True)
                
                # Update status to completed
                self.db.update_queue_status(video['id'], 'completed')
                
                print(f"   ✅ Berhasil: {result['stats']['word_count']} kata")
                
            except Exception as e:
                error_msg = f"{str(e)}"
                print(f"   ❌ Gagal: {error_msg}")
                
                # Update status to failed
                self.db.update_queue_status(video['id'], 'failed', error_msg)
                self.db.increment_queue_retry(video['id'])
    
    def export_database(self, output_path: str = "database_export.json"):
        """Export database ke file JSON"""
        print(f"\n💾 EXPORTING DATABASE TO: {output_path}")
        print("=" * 80)
        
        self.db.export_to_json(output_path)
        
        file_size = Path(output_path).stat().st_size
        print(f"✅ Export berhasil!")
        print(f"📁 File: {output_path}")
        print(f"📊 Size: {file_size:,} bytes ({file_size/1024:.2f} KB)")
    
    def cleanup_old_files(self, days: int = 30):
        """Cleanup transcript files yang lama"""
        print(f"\n🧹 CLEANUP FILES OLDER THAN {days} DAYS")
        print("=" * 80)
        
        import os
        from datetime import datetime, timedelta
        
        # Get all transcript files
        cursor = self.db.conn.cursor()
        cursor.execute("SELECT video_id, transcript_path, summary_path FROM videos WHERE transcript_downloaded = 1")
        videos = cursor.fetchall()
        
        cutoff_date = datetime.now() - timedelta(days=days)
        deleted_count = 0
        total_size = 0
        
        for video in videos:
            video = dict(video)
            
            for file_path in [video['transcript_path'], video['summary_path']]:
                if file_path and os.path.exists(file_path):
                    file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                    
                    if file_time < cutoff_date:
                        file_size = os.path.getsize(file_path)
                        os.remove(file_path)
                        total_size += file_size
                        deleted_count += 1
                        print(f"🗑️  Deleted: {file_path}")
        
        print(f"\n✅ Cleanup selesai!")
        print(f"📁 Files deleted: {deleted_count}")
        print(f"💾 Space freed: {total_size:,} bytes ({total_size/1024:.2f} KB)")
    
    def vacuum_database(self):
        """Optimize database dengan VACUUM"""
        print("\n🧹 OPTIMIZING DATABASE")
        print("=" * 80)
        
        try:
            cursor = self.db.conn.cursor()
            cursor.execute("VACUUM")
            self.db.conn.commit()
            
            # Get database size
            db_size = Path(self.db_path).stat().st_size
            
            print("✅ Database vacuumed successfully!")
            print(f"📁 Database: {self.db_path}")
            print(f"💾 Size: {db_size:,} bytes ({db_size/1024/1024:.2f} MB)")
            
        except Exception as e:
            print(f"❌ Vacuum failed: {str(e)}")
    
    def close(self):
        """Tutup koneksi database"""
        self.db.close()


def main():
    """Fungsi utama untuk command line"""
    import sys
    
    if len(sys.argv) < 2:
        print("Database Management Utilities")
        print("=" * 50)
        print("\nUsage:")
        print("  python manage_database.py <command> [options]")
        print("\nCommands:")
        print("  stats              Show database statistics")
        print("  channels           Show all channels")
        print("  videos             Show all videos")
        print("  videos-with        Show videos with transcript")
        print("  videos-without     Show videos without transcript")
        print("  videos <channel>   Show videos from specific channel")
        print("  pending            Show pending downloads")
        print("  search <query>     Search in database")
        print("  video <video_id>   Show video details")
        print("  retry              Retry failed downloads")
        print("  export <file>      Export database to JSON")
        print("  cleanup <days>     Cleanup files older than X days")
        print("  vacuum             Optimize database")
        print("\nExamples:")
        print("  python manage_database.py stats")
        print("  python manage_database.py videos-with")
        print("  python manage_database.py search \"tutorial\"")
        print("  python manage_database.py video dQw4w9WgXcQ")
        print("  python manage_database.py cleanup 30")
        sys.exit(1)
    
    command = sys.argv[1]
    
    try:
        manager = DatabaseManager()
        
        if command == "stats":
            manager.show_statistics()
        
        elif command == "channels":
            limit = int(sys.argv[2]) if len(sys.argv) > 2 else 20
            manager.show_channels(limit)
        
        elif command == "videos":
            if len(sys.argv) > 2:
                # Channel specific
                manager.show_videos(channel_id=sys.argv[2])
            else:
                manager.show_videos()
        
        elif command == "videos-with":
            manager.show_videos(only_with_transcript=True)
        
        elif command == "videos-without":
            manager.show_videos(only_without=True)
        
        elif command == "pending":
            manager.show_pending_downloads()
        
        elif command == "search":
            if len(sys.argv) < 3:
                print("Error: Please provide search query")
                sys.exit(1)
            query = " ".join(sys.argv[2:])
            manager.search_database(query)
        
        elif command == "video":
            if len(sys.argv) < 3:
                print("Error: Please provide video ID")
                sys.exit(1)
            manager.show_video_details(sys.argv[2])
        
        elif command == "retry":
            limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
            manager.retry_failed_downloads(limit)
        
        elif command == "export":
            output_file = sys.argv[2] if len(sys.argv) > 2 else "database_export.json"
            manager.export_database(output_file)
        
        elif command == "cleanup":
            days = int(sys.argv[2]) if len(sys.argv) > 2 else 30
            manager.cleanup_old_files(days)
        
        elif command == "vacuum":
            manager.vacuum_database()
        
        else:
            print(f"Unknown command: {command}")
            sys.exit(1)
        
        manager.close()
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
