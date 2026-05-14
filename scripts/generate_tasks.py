#!/usr/bin/env python3
import argparse
import csv
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database_optimized import OptimizedDatabase

REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = REPO_ROOT / "youtube_transcripts.db"

def generate_transcript_tasks(con, csv_path, limit, channel_id, video_id):
    query = """
    SELECT v.video_id 
    FROM videos v
    WHERE v.transcript_downloaded = 0 
      AND (v.transcript_language IS NULL OR v.transcript_language <> 'no_subtitle')
      AND v.is_short = 0
    """
    params = []
    if channel_id:
        # channel_id in videos is integer id, but user provides string channel_id
        query += " AND v.channel_id = (SELECT id FROM channels WHERE channel_id = ?)"
        params.append(channel_id)
    if video_id:
        query += " AND v.video_id = ?"
        params.append(video_id)
    
    if limit > 0:
        query += f" LIMIT {limit}"
        
    rows = con.execute(query, params).fetchall()
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["video_id"])
        writer.writeheader()
        writer.writerows([{"video_id": row[0]} for row in rows])
    return len(rows)

def generate_resume_tasks(con, csv_path, limit, channel_id, video_id):
    query = """
    SELECT v.video_id 
    FROM videos v
    WHERE v.transcript_downloaded = 1 
      AND (v.summary_file_path IS NULL OR v.summary_file_path = '')
      AND v.is_short = 0
    """
    params = []
    if channel_id:
        query += " AND v.channel_id = (SELECT id FROM channels WHERE channel_id = ?)"
        params.append(channel_id)
    if video_id:
        query += " AND v.video_id = ?"
        params.append(video_id)
        
    if limit > 0:
        query += f" LIMIT {limit}"
        
    rows = con.execute(query, params).fetchall()
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["video_id"])
        writer.writeheader()
        writer.writerows([{"video_id": row[0]} for row in rows])
    return len(rows)

def generate_format_tasks(con, csv_path, limit, channel_id=None):
    query = """
    SELECT v.id, v.video_id, c.channel_id as channel_slug, v.title
    FROM videos v
    JOIN channels c ON c.id = v.channel_id
    WHERE v.transcript_downloaded = 1 
      AND (v.transcript_formatted_path IS NULL OR v.transcript_formatted_path = '' OR v.transcript_formatted_path = '.')
      AND v.is_short = 0
    """
    params = []
    if channel_id:
        query += " AND c.channel_id = ?"
        params.append(channel_id)

    if limit > 0:
        query += f" LIMIT {limit}"
        
    rows = con.execute(query, params).fetchall()
    db = OptimizedDatabase(str(DB_PATH))
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "video_id", "channel_slug", "title", "transcript_file_path", "transcript_text"])
        writer.writeheader()
        for row in rows:
            video_id = row[1]
            writer.writerow({
                "id": row[0],
                "video_id": video_id,
                "channel_slug": row[2],
                "title": row[3],
                "transcript_file_path": "",
                "transcript_text": db.read_transcript(video_id) or ""
            })
    try:
        db.close()
    except Exception:
        pass
    return len(rows)

def generate_audio_tasks(con, csv_path, limit, channel_id=None, video_id=None):
    query = """
    SELECT v.video_id
    FROM videos v
    JOIN channels c ON c.id = v.channel_id
    WHERE COALESCE(v.transcript_downloaded, 0) = 0
      AND COALESCE(v.transcript_language, '') = 'no_subtitle'
      AND COALESCE(v.is_short, 0) = 0
      AND COALESCE(v.is_member_only, 0) = 0
    """
    params = []
    if channel_id:
        query += " AND (c.channel_id = ? OR c.channel_id = ?)"
        params.extend([channel_id, channel_id.lstrip("@")])
    if video_id:
        query += " AND v.video_id = ?"
        params.append(video_id)
    query += " ORDER BY v.created_at DESC, v.id DESC"
    if limit > 0:
        query += f" LIMIT {limit}"
    rows = con.execute(query, params).fetchall()
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["video_id"])
        writer.writeheader()
        writer.writerows([{"video_id": row[0]} for row in rows])
    return len(rows)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", required=True, choices=["transcript", "resume", "format", "audio"])
    parser.add_argument("--csv", required=True)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--channel-id", default="")
    parser.add_argument("--video-id", default="")
    args = parser.parse_args()
    
    con = sqlite3.connect(str(DB_PATH))
    count = 0
    if args.type == "transcript":
        count = generate_transcript_tasks(con, args.csv, args.limit, args.channel_id, args.video_id)
    elif args.type == "resume":
        count = generate_resume_tasks(con, args.csv, args.limit, args.channel_id, args.video_id)
    elif args.type == "format":
        count = generate_format_tasks(con, args.csv, args.limit, args.channel_id)
    elif args.type == "audio":
        count = generate_audio_tasks(con, args.csv, args.limit, args.channel_id, args.video_id)
    
    print(f"Generated {count} tasks in {args.csv}")

if __name__ == "__main__":
    main()
