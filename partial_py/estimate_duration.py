#!/usr/bin/env python3
"""
Estimate Video Duration from Transcript

Script ini mengestimasi durasi video berdasarkan:
- Jumlah kata dalam transcript
- Average speaking rate (150 words/minute untuk bahasa Indonesia)

Ini adalah alternatif jika YouTube API tidak tersedia.
"""

import sqlite3
from pathlib import Path
from typing import Optional


def estimate_duration(word_count: int, language: str = 'id') -> int:
    """
    Estimate duration in seconds from word count
    
    Speaking rates:
    - Indonesian: ~150 words/minute
    - English: ~160 words/minute
    - Arabic: ~140 words/minute
    
    Args:
        word_count: Number of words in transcript
        language: Language code (id, en, ar)
    
    Returns:
        Estimated duration in seconds
    """
    speaking_rates = {
        'id': 150,  # Indonesian
        'en': 160,  # English
        'ar': 140,  # Arabic
    }
    
    rate = speaking_rates.get(language, 150)
    
    # Calculate duration in seconds
    if word_count > 0:
        duration_seconds = int((word_count / rate) * 60)
        return duration_seconds
    
    return 0


class DurationEstimator:
    """Estimator untuk durasi video dari transcript"""
    
    def __init__(self, db_path: str, uploads_dir: str = "uploads"):
        self.db_path = Path(db_path)
        self.uploads_dir = Path(uploads_dir)
        self.conn = None
        
        self.stats = {
            'videos_processed': 0,
            'videos_updated': 0,
            'videos_with_wordcount': 0,
            'videos_read_transcript': 0,
            'errors': 0
        }
    
    def connect(self):
        """Buka koneksi database"""
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")
        
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        
        print(f"✅ Connected to database: {self.db_path}")
    
    def close(self):
        """Tutup koneksi"""
        if self.conn:
            self.conn.close()
        print("✅ Database connection closed")
    
    def count_words_in_file(self, file_path: Path) -> int:
        """Count words in transcript file"""
        try:
            if not file_path.exists():
                return 0
            
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                # Skip metadata lines (Kind:, Language:, etc.)
                lines = [line for line in content.split('\n') 
                        if not line.startswith(('Kind:', 'Language:'))]
                text = ' '.join(lines)
                return len(text.split())
        
        except Exception:
            return 0
    
    def estimate_all(self, batch_size: int = 1000):
        """Estimate duration untuk semua videos"""
        print("\n" + "=" * 60)
        print("⏱️  ESTIMATE VIDEO DURATION FROM TRANSCRIPT")
        print("=" * 60)
        
        cursor = self.conn.cursor()
        
        # Get videos without duration
        cursor.execute("""
            SELECT id, video_id, channel_id, duration, word_count,
                   transcript_file_path, transcript_language
            FROM videos
            WHERE duration = 0 OR duration IS NULL
        """)
        
        videos = cursor.fetchall()
        print(f"📊 Found {len(videos)} videos needing duration estimate")
        
        for video in videos:
            video_id = video['video_id']
            duration = video['duration']
            word_count = video['word_count']
            transcript_path = video['transcript_file_path']
            language = video['transcript_language'] or 'id'
            
            estimated_duration = 0
            
            # Option 1: Use existing word_count
            if word_count and word_count > 0:
                estimated_duration = estimate_duration(word_count, language)
                self.stats['videos_with_wordcount'] += 1
            
            # Option 2: Read transcript file and count words
            elif transcript_path:
                file_path = self.uploads_dir.parent / transcript_path
                word_count = self.count_words_in_file(file_path)
                
                if word_count > 0:
                    estimated_duration = estimate_duration(word_count, language)
                    self.stats['videos_read_transcript'] += 1
                    
                    # Update word_count in database
                    cursor.execute("""
                        UPDATE videos SET word_count = ? WHERE id = ?
                    """, (word_count, video['id']))
            
            # Update duration if estimated
            if estimated_duration > 0 and (not duration or duration == 0):
                cursor.execute("""
                    UPDATE videos 
                    SET duration = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (estimated_duration, video['id']))
                
                self.stats['videos_updated'] += 1
            
            self.stats['videos_processed'] += 1
            
            # Progress
            if self.stats['videos_processed'] % 5000 == 0:
                print(f"   ⏳ Processed: {self.stats['videos_processed']}/{len(videos)}")
                print(f"   ✅ Updated: {self.stats['videos_updated']}")
        
        self.conn.commit()
        
        print(f"\n📈 Duration Estimation Summary:")
        print(f"   Videos processed: {self.stats['videos_processed']:,}")
        print(f"   Videos updated: {self.stats['videos_updated']:,}")
        print(f"   From word_count: {self.stats['videos_with_wordcount']:,}")
        print(f"   From transcript: {self.stats['videos_read_transcript']:,}")
    
    def print_report(self):
        """Print laporan"""
        print("\n" + "=" * 60)
        print("📊 DURATION ESTIMATION REPORT")
        print("=" * 60)
        
        stats = self.stats
        
        print(f"""
Videos:
  Processed:        {stats['videos_processed']:,}
  Updated:          {stats['videos_updated']:,}
  From word_count:  {stats['videos_with_wordcount']:,}
  From transcript:  {stats['videos_read_transcript']:,}

Errors:
  Total:            {stats['errors']}
""")


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Estimate video duration from transcript"
    )
    parser.add_argument(
        "--db",
        default="youtube_transcripts.db",
        help="Path to database"
    )
    parser.add_argument(
        "--uploads",
        default="uploads",
        help="Path to uploads directory"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("⏱️  Video Duration Estimator")
    print("=" * 60)
    print(f"💾 Database: {args.db}")
    print(f"📁 Uploads: {args.uploads}")
    print()
    
    try:
        estimator = DurationEstimator(args.db, args.uploads)
        estimator.connect()
        
        estimator.estimate_all()
        estimator.print_report()
        
        estimator.close()
        
        print("\n" + "=" * 60)
        print("✅ Duration estimation completed!")
        print("=" * 60)
        
        return 0
        
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"❌ Estimation failed: {str(e)}")
        print("=" * 60)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
