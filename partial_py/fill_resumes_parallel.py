import os
import sqlite3
from pathlib import Path
from openai import OpenAI
import time
import threading
from queue import Queue

# Load env manually
def load_env():
    env = {}
    with open(".env", "r") as f:
        for line in f:
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    return env

def worker(q, api_key, thread_id):
    client = OpenAI(api_key=api_key, base_url="https://integrate.api.nvidia.com/v1")
    model = "openai/gpt-oss-120b"
    db_path = "youtube_transcripts.db"
    
    prompt_template = """Anda adalah penyusun catatan belajar yang teliti, faktual, dan sangat detail.
Judul video: {title}
Tugas: Buat resume lengkap dan poin kunci dari transkrip berikut:
{transcript}
"""

    while not q.empty():
        try:
            row = q.get_nowait()
        except:
            break
            
        video_id = row['video_id']
        t_path = Path(row['transcript_file_path'])
        
        print(f"[Thread-{thread_id}] Processing {video_id} - {row['title']}")
        
        if not t_path.exists():
            print(f"[Thread-{thread_id}] Transcript missing for {video_id}")
            q.task_done()
            continue
            
        transcript = t_path.read_text(encoding="utf-8")
        prompt = prompt_template.format(title=row['title'], transcript=transcript[:30000])
        
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                max_tokens=4096
            )
            summary = response.choices[0].message.content
            
            # Save file
            # Correct path structure: uploads/[channel]/resume/[video_id]_summary.md
            # Check if t_path isuploads/[channel]/text/[video_id]_transcript.txt
            s_path = t_path.parent.parent / "resume" / f"{video_id}_summary.md"
            s_path.parent.mkdir(parents=True, exist_ok=True)
            s_path.write_text(summary, encoding="utf-8")
            
            # Update DB (Using new connection per thread to avoid locking issues)
            conn = sqlite3.connect(db_path, timeout=30)
            cursor = conn.cursor()
            rel_path = str(s_path)
            cursor.execute("UPDATE videos SET summary_file_path = ? WHERE id = ?", (rel_path, row['id']))
            conn.commit()
            conn.close()
            print(f"[Thread-{thread_id}] Done: {video_id}")
            
        except Exception as e:
            print(f"[Thread-{thread_id}] Error {video_id}: {e}")
            time.sleep(5)
            # Re-queue for another attempt?
            # q.put(row) # Might cause infinite loop
            
        q.task_done()

def main():
    env = load_env()
    keys = [env.get(f"NVIDIA_API_KEY_{i}") for i in range(1, 4) if env.get(f"NVIDIA_API_KEY_{i}")]
    if not keys:
        keys = [env.get("NVIDIA_API_KEY")]
    
    if not keys or not keys[0]:
        print("No API keys found.")
        return

    db_path = "youtube_transcripts.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    query = """
    SELECT v.id, v.video_id, v.title, v.transcript_file_path, c.channel_name
    FROM videos v
    JOIN channels c ON v.channel_id = c.id
    WHERE v.transcript_downloaded = 1 
      AND (v.summary_file_path IS NULL OR v.summary_file_path = '')
      AND v.transcript_file_path IS NOT NULL AND v.transcript_file_path != ''
    ORDER BY v.id DESC
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    
    print(f"Starting parallel processing for {len(rows)} targets using {len(keys)} keys.")
    
    q = Queue()
    for row in rows:
        q.put(row)
        
    threads = []
    for i, key in enumerate(keys):
        t = threading.Thread(target=worker, args=(q, key, i+1))
        t.start()
        threads.append(t)
        
    for t in threads:
        t.join()

    print("All tasks completed.")

if __name__ == "__main__":
    main()
