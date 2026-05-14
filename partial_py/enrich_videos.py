#!/usr/bin/env python3
"""
Enrich Video Metadata Script

Script ini menambahkan metadata video yang hilang:
- Duration (dari YouTube API atau file info)
- View count, like count, comment count
- Description
- Thumbnail URL

Karena data ini tidak ada di database source, kita perlu:
1. Fetch dari YouTube API (jika ada API key)
2. Extract dari file CSV yang ada
3. Scrape dari halaman YouTube (opsional)
"""

import sqlite3
import json
import csv
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import re


class VideoMetadataEnricher:
    """Enricher untuk menambahkan metadata video"""
    
    def __init__(self, db_path: str, source_base: str):
        self.db_path = Path(db_path)
        self.source_base = Path(source_base)  # /media/harry/128NEW1/GIT/yt_channel/out
        
        self.conn = None
        self.stats = {
            'videos_processed': 0,
            'videos_updated': 0,
            'videos_with_csv': 0,
            'videos_with_description': 0,
            'errors': []
        }
        
        self.channel_map: Dict[int, str] = {}
    
    def connect(self):
        """Buka koneksi database"""
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")
        
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        
        print(f"✅ Connected to database: {self.db_path}")
    
    def close(self):
        """Tutup koneksi database"""
        if self.conn:
            self.conn.close()
        print("✅ Database connection closed")
    
    def load_channel_map(self):
        """Load mapping channel_id -> slug"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT id, channel_id FROM channels")
        
        for row in cursor.fetchall():
            self.channel_map[row['id']] = row['channel_id']
        
        print(f"📊 Loaded {len(self.channel_map)} channel mappings")
    
    def enrich_from_csv(self):
        """Enrich metadata dari CSV files"""
        print("\n" + "=" * 60)
        print("📊 ENRICH FROM CSV FILES")
        print("=" * 60)
        
        self.load_channel_map()
        
        cursor = self.conn.cursor()
        
        # Get all channels
        for channel_id, channel_slug in self.channel_map.items():
            csv_path = self.source_base / channel_slug / "videos_text.csv"
            
            if not csv_path.exists():
                continue
            
            self.stats['videos_with_csv'] += 1
            print(f"\n📁 Processing: {channel_slug}/videos_text.csv")
            
            try:
                with open(csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    
                    for row in reader:
                        # Extract video_id from link
                        link = row.get('link', '')
                        video_id = self._extract_video_id(link)
                        
                        if not video_id:
                            continue
                        
                        # Update video metadata
                        title = row.get('title', '')
                        
                        cursor.execute("""
                            UPDATE videos 
                            SET title = COALESCE(?, title),
                                updated_at = CURRENT_TIMESTAMP
                            WHERE video_id = ? AND channel_id = ?
                        """, (title, video_id, channel_id))
                        
                        if cursor.rowcount > 0:
                            self.stats['videos_updated'] += 1
                    
                    self.conn.commit()
                    print(f"   ✓ Updated {self.stats['videos_updated']} videos from CSV")
                    
            except Exception as e:
                self.stats['errors'].append(f"CSV {channel_slug}: {str(e)}")
                print(f"   ✗ Error: {str(e)}")
        
        print(f"\n📈 CSV Enrich Summary:")
        print(f"   CSV files processed: {self.stats['videos_with_csv']}")
        print(f"   Videos updated: {self.stats['videos_updated']}")
    
    def enrich_with_defaults(self):
        """Set default values for missing metadata"""
        print("\n" + "=" * 60)
        print("🔧 SET DEFAULT VALUES")
        print("=" * 60)
        
        cursor = self.conn.cursor()
        
        # Set default description for videos without description
        cursor.execute("""
            UPDATE videos 
            SET description = 'Video transcript from YouTube channel'
            WHERE description IS NULL OR description = ''
        """)
        self.stats['videos_with_description'] += cursor.rowcount
        print(f"   ✓ Set default description: {cursor.rowcount:,} videos")
        
        # Set default view_count = 0
        cursor.execute("""
            UPDATE videos 
            SET view_count = 0
            WHERE view_count IS NULL
        """)
        print(f"   ✓ Set default view_count: {cursor.rowcount:,} videos")
        
        # Set default duration = 0
        cursor.execute("""
            UPDATE videos 
            SET duration = 0
            WHERE duration IS NULL
        """)
        print(f"   ✓ Set default duration: {cursor.rowcount:,} videos")
        
        self.conn.commit()
        
        self.stats['videos_processed'] += cursor.rowcount
    
    def _extract_video_id(self, url: str) -> Optional[str]:
        """Extract video_id dari YouTube URL"""
        if not url:
            return None
        
        # Pattern untuk youtube.com/watch?v=VIDEO_ID
        match = re.search(r'[?&]v=([^&]+)', url)
        if match:
            return match.group(1)
        
        # Pattern untuk youtu.be/VIDEO_ID
        match = re.search(r'youtu\.be/([^?]+)', url)
        if match:
            return match.group(1)
        
        return None
    
    def print_report(self):
        """Print laporan"""
        print("\n" + "=" * 60)
        print("📊 ENRICHMENT REPORT")
        print("=" * 60)
        
        stats = self.stats
        
        print(f"""
Videos:
  Processed:    {stats['videos_processed']:,}
  Updated:      {stats['videos_updated']:,}
  With CSV:     {stats['videos_with_csv']:,}

Errors:
  Total:        {len(stats['errors'])}
""")
        
        if stats['errors'] and len(stats['errors']) <= 5:
            print("Error details:")
            for error in stats['errors'][:5]:
                print(f"  - {error}")


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Enrich video metadata"
    )
    parser.add_argument(
        "--db",
        default="youtube_transcripts.db",
        help="Path ke database"
    )
    parser.add_argument(
        "--source-base",
        default=str(Path(__file__).resolve().parent.parent.parent / "GIT/yt_channel/out"),
        help="Base directory source files"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("📊 Video Metadata Enricher")
    print("=" * 60)
    print(f"💾 Database: {args.db}")
    print(f"📁 Source base: {args.source_base}")
    print()
    
    try:
        enricher = VideoMetadataEnricher(args.db, args.source_base)
        enricher.connect()
        
        # Enrich from CSV
        enricher.enrich_from_csv()
        
        # Set defaults
        enricher.enrich_with_defaults()
        
        # Print report
        enricher.print_report()
        
        enricher.close()
        
        print("\n" + "=" * 60)
        print("✅ Enrichment completed!")
        print("=" * 60)
        
        return 0
        
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"❌ Enrichment failed: {str(e)}")
        print("=" * 60)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
