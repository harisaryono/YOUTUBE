import os
import sys
import logging
from recover_transcripts import TranscriptRecoverer

# Setup minimalist logging to stdout
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s', stream=sys.stdout)

def test_single_video(video_id):
    recoverer = TranscriptRecoverer()
    print(f"Testing recovery for: {video_id}")
    result = recoverer.download_transcript(video_id)
    
    if result:
        print(f"✅ Success! Word count: {result['word_count']}")
        print(f"Snippet: {result['formatted'][:200]}...")
    else:
        print(f"❌ Failed to recover transcript for: {video_id}")

if __name__ == "__main__":
    vid = "Cu9g39w1pJc"
    test_single_video(vid)
