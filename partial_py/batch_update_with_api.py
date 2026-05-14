#!/usr/bin/env python3
"""
Batch update all videos without transcript to no_subtitle status
Using YouTube Data API v3 with API key rotation for rate limit handling
"""

import sqlite3
import requests
import time
from datetime import datetime
from itertools import cycle


# API Keys for rotation
API_KEYS = [
    "AIzaSyDWyLO_thm2vu8hxGMp1fEU7vh2QGqiZIQ",  # affcenter
    "AIzaSyCJxnFiz-61yHvsEjnLG-b8mK8WcUTRSKM",  # ytHarry
]
CAPTIONS_URL = "https://www.googleapis.com/youtube/v3/captions"

# Create cycle iterator for key rotation
key_cycle = cycle(API_KEYS)
current_key_index = 0


def check_video_has_captions(video_id: str, retry_count: int = 0) -> tuple:
    """
    Check if a video has captions/subtitles using YouTube Data API
    With automatic API key rotation on rate limit
    Returns: (has_captions, reason)
    """
    global current_key_index
    
    try:
        # Get current API key
        api_key = API_KEYS[current_key_index]
        
        params = {
            'part': 'snippet',
            'videoId': video_id,
            'key': api_key
        }
        
        response = requests.get(CAPTIONS_URL, params=params, timeout=10)
        
        # Handle rate limiting (429) or quota exceeded (403)
        if response.status_code in [429, 403] and retry_count < len(API_KEYS) - 1:
            # Try next API key
            current_key_index = (current_key_index + 1) % len(API_KEYS)
            time.sleep(1)  # Brief delay before retry
            return check_video_has_captions(video_id, retry_count + 1)
        
        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])
            
            if items:
                languages = [item['snippet']['language'] for item in items[:5]]
                return True, f"Available: {', '.join(languages)}"
            else:
                return False, "No captions available"
                
        elif response.status_code == 403:
            # Captions disabled or private video
            return False, "Captions disabled"
            
        elif response.status_code == 404:
            return False, "Video not found or no captions"
            
        elif response.status_code == 429:
            return None, f"Rate limited (try again later)"
            
        else:
            return None, f"API Error: {response.status_code}"
            
    except requests.exceptions.RequestException as e:
        return None, f"Request Error: {str(e)}"
    except Exception as e:
        return None, f"Error: {str(e)}"


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
    print(f"Using YouTube Data API v3 with API Key Rotation")
    print(f"API Keys: {len(API_KEYS)} keys configured")
    for i, key in enumerate(API_KEYS, 1):
        print(f"  Key {i}: {key[:15]}...{key[-5:]}")
    print("=" * 80)
    
    updated_count = 0
    has_captions_count = 0
    error_count = 0
    
    # Process in batches to avoid rate limiting
    batch_size = 50
    for i in range(0, total, batch_size):
        batch = videos[i:i+batch_size]
        
        for j, video in enumerate(batch, 1):
            video_id = video['video_id']
            title = video['title'][:50] + "..." if len(video['title']) > 50 else video['title']
            current_num = i + j
            
            # Check caption availability
            has_captions, reason = check_video_has_captions(video_id)
            
            if has_captions is False:
                # No captions - update database
                cursor.execute("""
                    UPDATE videos 
                    SET transcript_language = 'no_subtitle',
                        transcript_downloaded = 0,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE video_id = ?
                """, (video_id,))
                conn.commit()
                updated_count += 1
                print(f"[{current_num}/{total}] ❌ {title} -> marked as no_subtitle")
                
            elif has_captions is True:
                has_captions_count += 1
                print(f"[{current_num}/{total}] ✅ {title} -> HAS CAPTIONS ({reason})")
                
            else:
                error_count += 1
                print(f"[{current_num}/{total}] ⚠️  {title} -> {reason} (skipped)")
        
        # Rate limiting: 100 requests per 100 seconds (YouTube API limit)
        if i + batch_size < total:
            print(f"\n   ... processed {min(i+batch_size, total)}/{total}, sleeping 5 seconds ...")
            time.sleep(5)
    
    conn.commit()
    conn.close()
    
    print("\n" + "=" * 80)
    print(f"\n📊 FINAL SUMMARY:")
    print(f"   Total checked: {updated_count + has_captions_count + error_count}")
    print(f"   ❌ Updated to no_subtitle: {updated_count}")
    print(f"   ✅ Has captions (skip): {has_captions_count}")
    print(f"   ⚠️  Errors (skipped): {error_count}")
    print(f"\n   Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
