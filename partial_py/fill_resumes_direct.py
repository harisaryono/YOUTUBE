import os
import sqlite3
from pathlib import Path
from openai import OpenAI
import time

# Load env manually for simplicity
def load_env():
    env = {}
    with open(".env", "r") as f:
        for line in f:
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    return env

def main():
    env = load_env()
    api_key = env.get("NVIDIA_API_KEY_1") or env.get("NVIDIA_API_KEY")
    base_url = "https://integrate.api.nvidia.com/v1"
    model = "openai/gpt-oss-120b"
    
    if not api_key:
        print("No API key found in .env")
        return

    client = OpenAI(api_key=api_key, base_url=base_url)
    
    db_path = "youtube_transcripts.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Query missed resumes
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
    print(f"Found {len(rows)} targets.")

    prompt_template = """Anda adalah penyusun catatan belajar yang teliti, faktual, dan sangat detail.
Judul video: {title}
Tugas: Buat resume lengkap dan poin kunci dari transkrip berikut:
{transcript}
"""

    for i, row in enumerate(rows):
        video_id = row['video_id']
        t_path = Path(row['transcript_file_path'])
        
        if not t_path.exists():
            print(f"Transcript missing for {video_id}: {t_path}")
            continue
            
        print(f"[{i+1}/{len(rows)}] Processing {video_id} - {row['title']}")
        
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
            s_path = t_path.parent.parent / "resume" / f"{video_id}_summary.md"
            s_path.parent.mkdir(parents=True, exist_ok=True)
            s_path.write_text(summary, encoding="utf-8")
            
            # Update DB
            rel_path = str(s_path)
            cursor.execute("UPDATE videos SET summary_file_path = ? WHERE id = ?", (rel_path, row['id']))
            conn.commit()
            print(f"  Done: {rel_path}")
            
        except Exception as e:
            print(f"  Error {video_id}: {e}")
            time.sleep(5)

    conn.close()

if __name__ == "__main__":
    main()
