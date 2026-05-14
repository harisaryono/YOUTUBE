#!/usr/bin/env python3
"""
Format Transcripts Script

Memformat transkrip YouTube agar lebih mudah dibaca TANPA merangkum atau menerjemahkan.
Menggunakan urutan provider berdasarkan PROGRESS.md:
1. nvidia/openai/gpt-oss-120b (baseline)
2. groq/moonshotai/kimi-k2-instruct (fallback utama)
3. cerebras/qwen-3-235b-a22b-instruct-2507 (fallback berikutnya)
4. z.ai/glm-4.7 (fallback rewrite agresif)
5. nvidia/mistralai/mistral-small-24b-instruct (usable, lambat)
6. groq/openai/gpt-oss-20b (usable, lebih pendek)
7. cerebras/llama3.1-8b (usable, lebih lemah)

Output disimpan di:
- File: uploads/{channel}/text_formatted/{video_id}.txt
- Database: kolom transcript_formatted_path di youtube_transcripts.db
"""

import argparse
import csv
import json
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict
from zoneinfo import ZoneInfo

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

LOCAL_TZ = ZoneInfo("Asia/Jakarta")
DEFAULT_DB = "youtube_transcripts.db"
DEFAULT_UPLOADS = "uploads"

# Provider priority order
PROVIDER_ORDER = [
    {"provider": "nvidia", "model": "openai/gpt-oss-120b", "priority": 1},
    {"provider": "groq", "model": "moonshotai/kimi-k2-instruct", "priority": 2},
    {"provider": "cerebras", "model": "qwen-3-235b-a22b-instruct-2507", "priority": 3},
    {"provider": "z.ai", "model": "glm-4.7", "priority": 4},
    {"provider": "nvidia", "model": "mistralai/mistral-small-24b-instruct", "priority": 5},
    {"provider": "groq", "model": "openai/gpt-oss-20b", "priority": 6},
    {"provider": "cerebras", "model": "llama3.1-8b", "priority": 7},
]

# Model limits for chunking
MODEL_LIMITS = {
    "nvidia/openai/gpt-oss-120b": {
        "chunk_chars": 20000,
        "chunk_max_tokens": 8000,
        "chunk_retry_tokens": 4000,
    },
    "groq/moonshotai/kimi-k2-instruct": {
        "chunk_chars": 15000,
        "chunk_max_tokens": 6000,
        "chunk_retry_tokens": 3000,
    },
    "cerebras/qwen-3-235b-a22b-instruct-2507": {
        "chunk_chars": 18000,
        "chunk_max_tokens": 7000,
        "chunk_retry_tokens": 3500,
    },
    "z.ai/glm-4.7": {
        "chunk_chars": 12000,
        "chunk_max_tokens": 5000,
        "chunk_retry_tokens": 2500,
    },
    "nvidia/mistralai/mistral-small-24b-instruct": {
        "chunk_chars": 16000,
        "chunk_max_tokens": 6500,
        "chunk_retry_tokens": 3200,
    },
    "groq/openai/gpt-oss-20b": {
        "chunk_chars": 14000,
        "chunk_max_tokens": 5500,
        "chunk_retry_tokens": 2700,
    },
    "cerebras/llama3.1-8b": {
        "chunk_chars": 10000,
        "chunk_max_tokens": 4000,
        "chunk_retry_tokens": 2000,
    },
}

PROMPT_FORMAT = """Tugas: Format transkrip berikut agar lebih mudah dibaca.

Instruksi:
1. Hapus semua timestamp (format [00:00:00.000] atau sejenisnya)
2. Pertahankan SEMUA isi konten asli - jangan ringkas, jangan terjemahkan
3. Perbaiki pemenggalan kata yang terpotong di akhir baris
4. Tambahkan paragraf yang logis berdasarkan alur bicara
5. Pertahankan nama orang, istilah teknis, angka, dan fakta persis seperti aslinya
6. Gunakan bahasa yang sama dengan transkrip asli (jangan ubah bahasa)
7. Jangan tambahkan komentar, interpretasi, atau kesimpulan yang tidak ada di transkrip

Output:
- Hanya kembalikan transkrip yang sudah diformat
- Jangan tambahkan penjelasan tentang apa yang Anda lakukan
- Jangan menyebut diri Anda sebagai AI

Transkrip asli:
{transcript}

Transkrip terformat:"""


@dataclass
class VideoTask:
    id: int
    channel_id: int
    video_id: str
    title: str
    transcript_path: str
    transcript_text: str
    channel_slug: str


class TranscriptFormatter:
    def __init__(self, db_path: str, uploads_dir: str, coordinator_url: str = None):
        self.db_path = db_path
        # uploads_dir should be absolute or relative to current directory
        # We'll use it directly as the base for all file operations
        self.uploads_dir = Path(uploads_dir).resolve() if not Path(uploads_dir).is_absolute() else Path(uploads_dir)
        self.coordinator_url = coordinator_url or "http://127.0.0.1:8788"
        
        self.db_conn = None
        self.stats = {
            'total': 0,
            'formatted': 0,
            'skipped_already_formatted': 0,
            'skipped_no_transcript': 0,
            'failed': 0,
            'errors': []
        }
        
        self._connect_db()
        self._ensure_formatted_column()
        
    def _connect_db(self):
        """Koneksi ke database"""
        if not Path(self.db_path).exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")
        
        self.db_conn = sqlite3.connect(self.db_path)
        self.db_conn.row_factory = sqlite3.Row
        print(f"✅ Connected to database: {self.db_path}")
        
    def _ensure_formatted_column(self):
        """Pastikan kolom transcript_formatted_path ada"""
        cursor = self.db_conn.cursor()
        
        # Cek apakah kolom sudah ada
        cursor.execute("PRAGMA table_info(videos)")
        columns = [col['name'] for col in cursor.fetchall()]
        
        if 'transcript_formatted_path' not in columns:
            print("➕ Adding transcript_formatted_path column...")
            cursor.execute("""
                ALTER TABLE videos 
                ADD COLUMN transcript_formatted_path TEXT
            """)
            self.db_conn.commit()
            print("✅ Column transcript_formatted_path added")
        else:
            print("✅ Column transcript_formatted_path exists")
            
    def close(self):
        """Tutup koneksi database"""
        if self.db_conn:
            self.db_conn.close()
        print("✅ Database connection closed")
        
    def get_pending_videos(self, limit: int = None) -> List[VideoTask]:
        """Ambil video yang belum diformat"""
        cursor = self.db_conn.cursor()
        
        query = """
            SELECT v.id, v.channel_id, v.video_id, v.title, 
                   v.transcript_file_path,
                   v.transcript_text,
                   REPLACE(REPLACE(c.channel_id, '@', ''), '/', '_') as channel_slug
            FROM videos v
            JOIN channels c ON v.channel_id = c.id
            WHERE v.transcript_downloaded = 1
              AND (v.transcript_formatted_path IS NULL OR v.transcript_formatted_path = '')
              AND (
                    COALESCE(v.transcript_text, '') != ''
                    OR COALESCE(v.transcript_file_path, '') != ''
                  )
            ORDER BY v.created_at ASC
        """
        
        if limit:
            query += f" LIMIT {limit}"
            
        cursor.execute(query)
        rows = cursor.fetchall()
        
        tasks = []
        for row in rows:
            tasks.append(VideoTask(
                id=row['id'],
                channel_id=row['channel_id'],
                video_id=row['video_id'],
                title=row['title'],
                transcript_path=row['transcript_file_path'],
                transcript_text=row['transcript_text'] or '',
                channel_slug=row['channel_slug']
            ))
            
        self.stats['total'] = len(tasks)
        print(f"📊 Found {len(tasks)} videos pending formatting")
        
        return tasks
        
    def read_transcript(self, task: VideoTask) -> Optional[str]:
        """Baca file transkrip"""
        if str(task.transcript_text or "").strip():
            return task.transcript_text
        # Path di database sudah relatif terhadap current directory
        transcript_file = Path(task.transcript_path)
        
        if not transcript_file.exists():
            print(f"   ⚠️ Transcript file not found: {transcript_file}")
            return None
            
        try:
            with open(transcript_file, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            print(f"   ❌ Error reading transcript: {e}")
            return None
            
    def format_with_provider(self, transcript: str, provider: str, model: str, api_key: str) -> Optional[str]:
        """Format transkrip menggunakan provider tertentu"""
        if OpenAI is None:
            print("   ❌ OpenAI library not installed")
            return None
            
        # Setup client berdasarkan provider
        if provider == "nvidia":
            base_url = "https://integrate.api.nvidia.com/v1"
        elif provider == "groq":
            base_url = "https://api.groq.com/openai/v1"
        elif provider == "cerebras":
            base_url = "https://api.cerebras.ai/v1"
        elif provider == "z.ai":
            base_url = "https://api.z.ai/v1"
        else:
            print(f"   ❌ Unknown provider: {provider}")
            return None
            
        try:
            client = OpenAI(
                api_key=api_key,
                base_url=base_url
            )
            
            # Chunk transcript jika terlalu panjang
            limits = MODEL_LIMITS.get(f"{provider}/{model}", {
                "chunk_chars": 15000,
                "chunk_max_tokens": 6000
            })
            
            chunks = self._chunk_transcript(transcript, limits["chunk_chars"])
            formatted_chunks = []
            
            for i, chunk in enumerate(chunks):
                print(f"      Processing chunk {i+1}/{len(chunks)}...")
                
                prompt = PROMPT_FORMAT.format(transcript=chunk)
                
                response = client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": "Anda adalah formatter transkrip profesional. Tugas Anda hanya memformat transkrip, bukan merangkum atau menerjemahkan."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.3,
                    max_tokens=limits["chunk_max_tokens"]
                )
                
                formatted = response.choices[0].message.content.strip()
                formatted_chunks.append(formatted)
                
                # Small delay between chunks
                if i < len(chunks) - 1:
                    time.sleep(0.5)
                    
            # Gabungkan semua chunk
            return "\n\n".join(formatted_chunks)
            
        except Exception as e:
            print(f"   ❌ Error with {provider}/{model}: {e}")
            return None
            
    def _chunk_transcript(self, transcript: str, max_chars: int) -> List[str]:
        """Pecah transkrip menjadi chunk-kchunk"""
        if len(transcript) <= max_chars:
            return [transcript]
            
        chunks = []
        current_chunk = ""
        
        # Split by paragraphs first
        paragraphs = re.split(r'\n\s*\n', transcript)
        
        for para in paragraphs:
            if len(current_chunk) + len(para) + 2 <= max_chars:
                if current_chunk:
                    current_chunk += "\n\n" + para
                else:
                    current_chunk = para
            else:
                if current_chunk:
                    chunks.append(current_chunk)
                current_chunk = para
                
        if current_chunk:
            chunks.append(current_chunk)
            
        return chunks
        
    def save_formatted(self, task: VideoTask, formatted_text: str) -> str:
        """Simpan hasil format ke file dan database"""
        # Buat direktori di dalam uploads/ - path relatif terhadap current directory
        # Path transcript di database format: uploads/{channel}/text/...
        # Jadi formatted juga: uploads/{channel}/text_formatted/...
        output_dir = Path("uploads") / task.channel_slug / "text_formatted"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Simpan ke file
        output_file = output_dir / f"{task.video_id}.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(formatted_text)
            
        # Update database - path relatif terhadap current directory
        rel_path = str(Path("uploads") / task.channel_slug / "text_formatted" / f"{task.video_id}.txt")
        
        cursor = self.db_conn.cursor()
        cursor.execute("""
            UPDATE videos 
            SET transcript_formatted_path = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (rel_path, task.id))
        
        self.db_conn.commit()
        
        return rel_path
        
    def process_video(self, task: VideoTask, api_keys: Dict) -> bool:
        """Proses satu video dengan urutan provider"""
        print(f"\n📹 Processing: {task.video_id} - {task.title[:50]}...")
        
        # Baca transkrip
        transcript = self.read_transcript(task)
        if not transcript:
            self.stats['skipped_no_transcript'] += 1
            return False
            
        print(f"   📄 Read {len(transcript):,} characters")
        
        # Coba setiap provider secara berurutan
        for provider_config in PROVIDER_ORDER:
            provider = provider_config["provider"]
            model = provider_config["model"]
            
            # Dapatkan API key
            key_name = f"{provider.upper()}_API_KEY"
            api_key = api_keys.get(key_name) or os.environ.get(key_name)
            
            if not api_key:
                print(f"   ⏭️ Skip {provider}/{model} (no API key)")
                continue
                
            print(f"   🔄 Trying {provider}/{model}...")
            
            formatted = self.format_with_provider(transcript, provider, model, api_key)
            
            if formatted:
                # Validasi output
                if self._validate_formatted(formatted, transcript):
                    # Simpan hasil
                    rel_path = self.save_formatted(task, formatted)
                    print(f"   ✅ Formatted successfully: {rel_path}")
                    self.stats['formatted'] += 1
                    return True
                else:
                    print(f"   ⚠️ Output validation failed for {provider}/{model}")
            else:
                print(f"   ❌ Failed with {provider}/{model}")
                
        # Semua provider gagal
        self.stats['failed'] += 1
        self.stats['errors'].append({
            'video_id': task.video_id,
            'title': task.title,
            'error': 'All providers failed'
        })
        return False
        
    def _validate_formatted(self, formatted: str, original: str) -> bool:
        """Validasi hasil formatting"""
        # Cek apakah output terlalu pendek (mungkin diringkas)
        if len(formatted) < len(original) * 0.5:
            print(f"   ⚠️ Output too short ({len(formatted)} vs {len(original)})")
            return False
            
        # Cek apakah ada reasoning leak
        if '<think>' in formatted or '</think>' in formatted:
            print(f"   ⚠️ Reasoning leak detected")
            return False
            
        # Cek apakah terlalu meta
        meta_patterns = [
            r"saya adalah",
            r"saya akan",
            r"berikut adalah",
            r"i am an",
            r"i will",
            r"here is",
        ]
        formatted_lower = formatted.lower()
        for pattern in meta_patterns:
            if pattern in formatted_lower and formatted_lower.count(pattern) > 2:
                print(f"   ⚠️ Too meta: {pattern}")
                return False
                
        return True
        
    def print_report(self):
        """Cetak laporan"""
        print("\n" + "=" * 60)
        print("📊 FORMATTING REPORT")
        print("=" * 60)
        
        stats = self.stats
        print(f"""
Total pending:     {stats['total']:,}
✅ Formatted:      {stats['formatted']:,}
⏭️ Skipped (done):  {stats['skipped_already_formatted']:,}
⏭️ Skipped (no tx): {stats['skipped_no_transcript']:,}
❌ Failed:          {stats['failed']:,}
""")
        
        if stats['errors']:
            print(f"\n❌ Errors ({len(stats['errors'])}):")
            for err in stats['errors'][:10]:
                print(f"   - {err['video_id']}: {err['title'][:40]}")
                

def load_api_keys(env_file: str = ".env") -> Dict:
    """Load API keys dari .env file"""
    keys = {}
    env_path = Path(env_file)
    
    if env_path.exists():
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    if 'API_KEY' in key.upper():
                        keys[key.strip()] = value.strip().strip('"\'')
                        
    # Also load from environment
    for key in ['NVIDIA_API_KEY', 'GROQ_API_KEY', 'CEREBRAS_API_KEY', 'ZAI_API_KEY']:
        if os.environ.get(key):
            keys[key] = os.environ[key]
            
    return keys


def main():
    parser = argparse.ArgumentParser(
        description="Format YouTube transcripts for better readability"
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB,
        help=f"Path to database (default: {DEFAULT_DB})"
    )
    parser.add_argument(
        "--uploads",
        default=DEFAULT_UPLOADS,
        help=f"Uploads directory (default: {DEFAULT_UPLOADS})"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of videos to process (default: all)"
    )
    parser.add_argument(
        "--env",
        default=".env",
        help="Path to .env file with API keys"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("📝 YouTube Transcript Formatter")
    print("=" * 60)
    print(f"📁 Database: {args.db}")
    print(f"📂 Uploads: {args.uploads}")
    print(f"🔑 Env file: {args.env}")
    print(f"📊 Limit: {args.limit or 'all'}")
    print()
    
    # Load API keys
    api_keys = load_api_keys(args.env)
    available_keys = [k for k in api_keys.keys() if 'API_KEY' in k]
    print(f"🔑 Available API keys: {', '.join(available_keys)}")
    print()
    
    try:
        formatter = TranscriptFormatter(args.db, args.uploads)
        
        # Get pending videos
        videos = formatter.get_pending_videos(limit=args.limit)
        
        if not videos:
            print("\n✅ No videos pending formatting!")
            return 0
            
        # Process each video
        for i, video in enumerate(videos, 1):
            print(f"\n[{i}/{len(videos)}]")
            formatter.process_video(video, api_keys)
            
        # Print report
        formatter.print_report()
        formatter.close()
        
        print("\n" + "=" * 60)
        print("✅ Formatting completed!")
        print("=" * 60)
        
        return 0 if formatter.stats['failed'] == 0 else 1
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
