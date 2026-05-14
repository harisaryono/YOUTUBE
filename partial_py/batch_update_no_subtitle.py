#!/usr/bin/env python3
"""
Batch update all videos without transcript to no_subtitle status
after verifying they actually have transcripts disabled
"""

import sqlite3
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound
import time
from datetime import datetime


def check_video_has_transcript(video_id: str):
    """
    Check if a video has available transcripts
    Returns: True if has transcript, False if no subtitle, None if error
    """
    try:
        api = YouTubeTranscriptApi()
        transcript_list = api.list(video_id)
        
        # Check if there are any transcripts available
        for transcript in transcript_list:
            return True  # Has at least one transcript
            
        return False  # No transcripts available
            
    except TranscriptsDisabled:
        return False
    except NoTranscriptFound:
        return False
    except Exception as e:
        return None


def main():
    # Connect to database
    conn = sqlite3.connect('youtube_transcripts.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Get videos without transcript and not marked as no_subtitle
    cursor.execute("""
        SELECT video_id, title
        FROM videos
        WHERE (transcript_downloaded = 0 OR transcript_downloaded IS NULL)
        AND transcript_language != 'no_subtitle'
        ORDER BY created_at DESC
    """)
    
    videos = cursor.fetchall()
    total = len(videos)
    
    print(f"Total videos to check: {total}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    updated_count = 0
    has_transcript_count = 0
    error_count = 0
    skip_count = 0
    
    for i, video in enumerate(videos, 1):
        video_id = video['video_id']
        title = video['title'][:50] + "..." if len(video['title']) > 50 else video['title']
        
        # Check transcript availability
        result = check_video_has_transcript(video_id)
        
        if result is False:
            # No subtitle - update database
            cursor.execute("""
                UPDATE videos 
                SET transcript_language = 'no_subtitle',
                    transcript_downloaded = 0,
                    updated_at = CURRENT_TIMESTAMP
                WHERE video_id = ?
            """, (video_id,))
            conn.commit()
            updated_count += 1
            print(f"[{i}/{total}] ❌ {title} -> marked as no_subtitle")
            
        elif result is True:
            has_transcript_count += 1
            print(f"[{i}/{total}] ✅ {title} -> HAS TRANSCRIPT (should download)")
            
        else:
            error_count += 1
            print(f"[{i}/{total}] ⚠️  {title} -> ERROR (skipped)")
            # Skip this one, don't update
        
        # Rate limiting - be nice to YouTube API
        if i % 10 == 0:
            print(f"  ... processed {i}/{total}, sleeping 2 seconds ...")
            time.sleep(2)
    
    conn.commit()
    conn.close()
    
    print("\n" + "=" * 80)
    print(f"\n📊 FINAL SUMMARY:")
    print(f"   Total checked: {total}")
    print(f"   ❌ Updated to no_subtitle: {updated_count}")
    print(f"   ✅ Has transcript (skip): {has_transcript_count}")
    print(f"   ⚠️  Errors (skipped): {error_count}")
    print(f"\n   Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
