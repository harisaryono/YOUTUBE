#!/usr/bin/env python3
"""
Script to check videos without transcript status
Verifies if they actually have subtitles or should be marked as no_subtitle
"""

import sqlite3
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound
import re
from datetime import datetime


def get_video_id_from_url(url: str) -> str:
    """Extract video ID from YouTube URL"""
    patterns = [
        r'(?:youtube\.com\/watch\?v=|youtu\.be\/|youtube\.com\/embed\/)([a-zA-Z0-9_-]+)',
        r'^([a-zA-Z0-9_-]+)$'
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return url


def check_video_transcript(video_id: str, video_title: str):
    """
    Check if a video has available transcripts
    Returns: (has_transcript, reason)
    """
    try:
        # Try to get transcript list (new API syntax)
        api = YouTubeTranscriptApi()
        transcript_list = api.list(video_id)
        
        # Check if there are any transcripts available
        transcripts = []
        try:
            for transcript in transcript_list:
                transcripts.append(transcript.language_code)
        except Exception:
            pass
        
        if transcripts:
            return True, f"Available languages: {', '.join(transcripts[:5])}"
        else:
            return False, "No transcripts available"
            
    except TranscriptsDisabled:
        return False, "Transcripts disabled"
    except NoTranscriptFound:
        return False, "No transcript found"
    except Exception as e:
        return None, f"Error: {str(e)}"


def main():
    # Connect to database
    conn = sqlite3.connect('youtube_transcripts.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Get videos without transcript and not marked as no_subtitle
    cursor.execute("""
        SELECT video_id, title, transcript_file_path, transcript_language
        FROM videos
        WHERE (transcript_downloaded = 0 OR transcript_downloaded IS NULL)
        AND transcript_language != 'no_subtitle'
        ORDER BY created_at DESC
        LIMIT 50
    """)
    
    videos = cursor.fetchall()
    
    print(f"Checking {len(videos)} videos...\n")
    print("=" * 80)
    
    has_transcript = []
    no_subtitle_needed = []
    errors = []
    
    for i, video in enumerate(videos, 1):
        video_id = video['video_id']
        title = video['title'][:60] + "..." if len(video['title']) > 60 else video['title']
        
        print(f"\n[{i}/{len(videos)}] {title}")
        print(f"     Video ID: {video_id}")
        print(f"     URL: https://youtube.com/watch?v={video_id}")
        
        result, reason = check_video_transcript(video_id, title)
        
        if result is True:
            has_transcript.append((video_id, title, reason))
            print(f"     ✅ HAS TRANSCRIPT - {reason}")
        elif result is False:
            no_subtitle_needed.append((video_id, title, reason))
            print(f"     ❌ NO SUBTITLE - {reason}")
        else:
            errors.append((video_id, title, reason))
            print(f"     ⚠️  ERROR - {reason}")
    
    print("\n" + "=" * 80)
    print("\n📊 SUMMARY:")
    print(f"   Total checked: {len(videos)}")
    print(f"   ✅ Has transcript: {len(has_transcript)}")
    print(f"   ❌ Need no_subtitle status: {len(no_subtitle_needed)}")
    print(f"   ⚠️  Errors: {len(errors)}")
    
    if has_transcript:
        print("\n📝 VIDEOS WITH TRANSCRIPTS (should be re-downloaded):")
        for vid, title, reason in has_transcript:
            print(f"   - {vid}: {title[:50]}... ({reason})")
    
    if no_subtitle_needed:
        print("\n📝 VIDEOS NEEDING no_subtitle STATUS:")
        for vid, title, reason in no_subtitle_needed:
            print(f"   - {vid}: {title[:50]}... ({reason})")
    
    # Option to update database
    if no_subtitle_needed:
        print("\n" + "=" * 80)
        response = input(f"\nUpdate {len(no_subtitle_needed)} videos to 'no_subtitle' status? (y/n): ")
        if response.lower() == 'y':
            for vid, _, _ in no_subtitle_needed:
                cursor.execute("""
                    UPDATE videos 
                    SET transcript_language = 'no_subtitle',
                        transcript_downloaded = 0,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE video_id = ?
                """, (vid,))
            conn.commit()
            print(f"✅ Updated {len(no_subtitle_needed)} videos to 'no_subtitle' status")
    
    conn.close()


if __name__ == "__main__":
    main()
