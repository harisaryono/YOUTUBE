import os
import sqlite3
import json
import threading
import time
from pathlib import Path
from queue import Queue
from base64 import b64decode, b64encode
from openai import OpenAI

try:
    from cryptography.fernet import Fernet
except ImportError:
    Fernet = None

# Constants from local_services.py and .env
DEFAULT_PROVIDERS_DB = os.environ.get("YT_PROVIDERS_DB", str(Path(__file__).resolve().parent.parent.parent / "services/provider_accounts.sqlite3"))
YOUTUBE_DB = "youtube_transcripts.db"
JOBS_PER_KEY = 2

def load_env():
    env = {}
    if os.path.exists(".env"):
        with open(".env", "r") as f:
            for line in f:
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip().strip('"').strip("'")
    return env

def decrypt_api_key(encrypted, encryption_key):
    if not encrypted or not encrypted.startswith("ENC:"):
        return encrypted
    if encryption_key is None or Fernet is None:
        return encrypted
    try:
        # Fernet key from 32 bytes
        key_bytes = encryption_key.encode("utf-8")
        if len(key_bytes) != 32:
            key_bytes = (key_bytes + b"0" * 32)[:32]
        fernet_key = b64encode(key_bytes)
        cipher = Fernet(fernet_key)
        encrypted_bytes = b64decode(encrypted[4:].encode("utf-8"))
        decrypted = cipher.decrypt(encrypted_bytes)
        return decrypted.decode("utf-8")
    except:
        return encrypted

def worker(q, api_key, account_name, job_id):
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
        
        print(f"[{account_name}-{job_id}] Processing {video_id}")
        
        if not t_path.exists():
            print(f"[{account_name}-{job_id}] Transcript missing: {t_path}")
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
            
            s_path = t_path.parent.parent / "resume" / f"{video_id}_summary.md"
            s_path.parent.mkdir(parents=True, exist_ok=True)
            s_path.write_text(summary, encoding="utf-8")
            
            conn = sqlite3.connect(YOUTUBE_DB, timeout=30)
            cursor = conn.cursor()
            rel_path = str(s_path)
            cursor.execute("UPDATE videos SET summary_file_path = ? WHERE id = ?", (rel_path, row['id']))
            conn.commit()
            conn.close()
            print(f"[{account_name}-{job_id}] Done: {video_id}")
        except Exception as e:
            print(f"[{account_name}-{job_id}] Error {video_id}: {e}")
            time.sleep(2)
            
        q.task_done()

def main():
    env = load_env()
    enc_key = env.get("PROVIDER_ENCRYPTION_KEY")
    
    # Load accounts from DB
    accounts = []
    con = sqlite3.connect(DEFAULT_PROVIDERS_DB)
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT account_name, api_key FROM provider_accounts WHERE provider = 'nvidia' AND is_active = 1;").fetchall()
    for r in rows:
        key_raw = r['api_key']
        key_dec = decrypt_api_key(key_raw, enc_key)
        accounts.append({'name': r['account_name'], 'key': key_dec})
    con.close()
    
    # Load tasks from youtube_transcripts.db
    con = sqlite3.connect(YOUTUBE_DB)
    con.row_factory = sqlite3.Row
    tasks = con.execute("""
        SELECT v.id, v.video_id, v.title, v.transcript_file_path
        FROM videos v
        WHERE v.transcript_downloaded = 1 
          AND (v.summary_file_path IS NULL OR v.summary_file_path = '')
          AND v.transcript_file_path IS NOT NULL AND v.transcript_file_path != ''
    """).fetchall()
    con.close()
    
    if not tasks:
        print("No tasks found.")
        return
        
    print(f"Starting maximum parallel processing: {len(tasks)} tasks, {len(accounts)} accounts ({len(accounts)*JOBS_PER_KEY} threads).")
    
    q = Queue()
    for t in tasks:
        q.put(t)
        
    threads = []
    for acc in accounts:
        for j in range(JOBS_PER_KEY):
            t = threading.Thread(target=worker, args=(q, acc['key'], acc['name'], j+1))
            t.start()
            threads.append(t)
            
    for t in threads:
        t.join()
    print("All tasks finished.")

if __name__ == "__main__":
    main()
