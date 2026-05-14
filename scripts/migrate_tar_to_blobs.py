#!/usr/bin/env python3
import os
import sys
import tarfile
import re
from pathlib import Path

# Add current dir to sys.path to import BlobStorage
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database_blobs import BlobStorage

TAR_DIR = "uploads_tar"
BLOB_DB_PATH = "youtube_transcripts_blobs.db"

def extract_video_id(filename):
    """
    Extracts video ID from standard filename patterns:
    - fO6Qj_PfoSU_transcript_20260401_185213.txt
    - fO6Qj_PfoSU_summary_20260401_185213.md
    - fO6Qj_PfoSU_summary.md
    - fO6Qj_PfoSU.txt
    """
    # 11-character video ID is most common
    match = re.match(r'^([a-zA-Z0-9_-]{11})', filename)
    if match:
        return match.group(1)
    # Fallback: take until first dot or underscore
    return re.split(r'[\._]', filename)[0]

def migrate():
    blob_storage = BlobStorage(BLOB_DB_PATH)
    
    tar_files = sorted([f for f in os.listdir(TAR_DIR) if f.endswith(".tar.gz")])
    print(f"🚀 Starting COMPLETE migration from {len(tar_files)} tarballs...")

    total_blobs = {'transcript': 0, 'resume': 0, 'formatted': 0}
    errors = 0
    channels_processed = 0

    for tar_name in tar_files:
        tar_path = os.path.join(TAR_DIR, tar_name)
        channels_processed += 1
        print(f"📦 Processing {tar_name} ({channels_processed}/{len(tar_files)})...")
        
        try:
            with tarfile.open(tar_path, "r:gz") as tar:
                for member in tar.getmembers():
                    if not member.isfile():
                        continue
                        
                    parts = member.name.split('/')
                    if len(parts) < 3:
                        continue
                        
                    folder = parts[1]
                    filename = parts[2]
                    video_id = extract_video_id(filename)
                    
                    content_type = None
                    if folder == "text":
                        if "_transcript_" in filename:
                            content_type = "transcript"
                        elif "_summary_" in filename:
                            content_type = "resume"
                    elif folder == "transcripts":
                        content_type = "transcript"
                    elif folder == "resumes" or folder == "resume":
                        content_type = "resume"
                    elif folder == "text_formatted":
                        content_type = "formatted"
                        
                    if content_type:
                        try:
                            f = tar.extractfile(member)
                            if f:
                                content = f.read().decode('utf-8', errors='ignore')
                                if content.strip():
                                    if blob_storage.save_blob(video_id, content_type, content):
                                        total_blobs[content_type] += 1
                        except Exception as e:
                            print(f"  ❌ Error extracting {member.name}: {e}")
                            errors += 1
                            
                if channels_processed % 5 == 0:
                    print(f"⏳ Current Counts: {total_blobs}")

        except Exception as e:
            print(f"❌ Error reading tarball {tar_name}: {e}")
            errors += 1

    print(f"\n✅ Migration finished!")
    print(f"📊 Summary:")
    for ct, count in total_blobs.items():
        print(f"  - {ct}: {count} blobs")
    print(f"💎 Total blobs saved: {sum(total_blobs.values())}")
    print(f"⚠️ Total errors: {errors}")

if __name__ == "__main__":
    migrate()
