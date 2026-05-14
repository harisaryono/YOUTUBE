import os
import sqlite3
import threading
import time
from pathlib import Path
from queue import Queue
from openai import OpenAI

def load_env():
    env = {}
    if os.path.exists(".env"):
        with open(".env", "r") as f:
            for line in f:
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip().strip('"').strip("'")
    return env

def worker(q, api_key, key_id):
    client = OpenAI(api_key=api_key, base_url="https://integrate.api.nvidia.com/v1")
    model = "openai/gpt-oss-120b"
    
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
        
        print(f"[Key-{key_id}] Processing {video_id}")
        
        if not t_path.exists():
            print(f"[Key-{key_id}] Missing file: {t_path}")
            q.task_done()
            continue
            
        try:
            transcript = t_path.read_text(encoding="utf-8")
            prompt = prompt_template.format(title=row['title'], transcript=transcript[:30000])
            
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                max_tokens=4096
            )
            summary = response.choices[0].message.content
            
            s_path = t_path.parent.parent / "resume" / f"{video_id}_summary.md"
            s_path.parent.mkdir(parents=True, exist_ok=True)
            s_path.write_text(summary, encoding="utf-8")
            
            conn = sqlite3.connect("youtube_transcripts.db", timeout=30)
            cursor = conn.cursor()
            cursor.execute("UPDATE videos SET summary_file_path = ? WHERE id = ?", (str(s_path), row['id']))
            conn.commit()
            conn.close()
            print(f"[Key-{key_id}] Done: {video_id}")
        except Exception as e:
            print(f"[Key-{key_id}] Error {video_id}: {e}")
            time.sleep(1)
            
        q.task_done()

def main():
    env = load_env()
    keys = [env.get(f"NVIDIA_API_KEY_{i}") for i in range(1, 4) if env.get(f"NVIDIA_API_KEY_{i}")]
    
    conn = sqlite3.connect("youtube_transcripts.db")
    conn.row_factory = sqlite3.Row
    tasks = conn.execute("""
        SELECT v.id, v.video_id, v.title, v.transcript_file_path
        FROM videos v
        WHERE v.transcript_downloaded = 1 
          AND (v.summary_file_path IS NULL OR v.summary_file_path = '')
          AND v.transcript_file_path IS NOT NULL AND v.transcript_file_path != ''
    """).fetchall()
    conn.close()
    
    if not tasks:
        print("No tasks left.")
        return
        
    print(f"Safe processing: {len(tasks)} tasks using {len(keys)} keys (1 thread each).")
    q = Queue()
    for t in tasks:
        q.put(t)
        
    threads = []
    for i, key in enumerate(keys):
        t = threading.Thread(target=worker, args=(q, key, i+1))
        t.start()
        threads.append(t)
            
    for t in threads:
        t.join()
    print("All tasks finished.")

if __name__ == "__main__":
    main()
