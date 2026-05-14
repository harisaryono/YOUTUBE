# YouTube Transcript Database - Sync & Enrichment Guide

Panduan lengkap untuk sinkronisasi dan enrichment data dari `yt_channel/channels.db` ke `youtube_transcripts.db`.

## 📊 Ringkasan Data

| Metrik | Jumlah |
|--------|--------|
| **Channels** | 43 |
| **Videos** | 27,374 |
| **Videos dengan Transcript** | 23,140 |
| **Videos dengan Summary** | 27,171 |
| **Text Files** | 26,328 |
| **Summary Files (.md)** | 23,005 |
| **Formatted Text (no timestamp)** | 3,062 |

## 📁 Struktur File

```
YOUTUBE/
├── youtube_transcripts.db       # Database target
├── uploads/                      # File transkrip & summary
│   └── {channel}/
│       ├── text/                # Transcript dengan timestamp
│       ├── resume/              # Summary (.md)
│       └── text_formatted/      # Transcript tanpa timestamp
├── backups/                      # Backup database
│   └── youtube_transcripts_backup_*.db
└── flask_app/                    # Flask web interface
    └── (venv eksternal)          # /media/harry/DATA120B/venv_youtube
```

## 🔄 Script Sinkronisasi

### 1. Backup Database

```bash
python3 backup_db.py
```

**Fungsi:**
- Membuat backup otomatis sebelum migrasi
- Menyimpan di `backups/` dengan timestamp
- Verifikasi integrity backup
- Cleanup backup lama (simpan 5 terakhir)

### 2. Sync Database (Channels + Videos)

```bash
python3 sync_databases.py
```

**Fungsi:**
- Backup otomatis
- Migrasi 41 channels dari source
- Migrasi 27,127 videos
- Validasi data setelah migrasi
- Update statistik channels

**Options:**
```bash
# Dry run (simulasi)
python3 sync_databases.py --dry-run

# Skip backup (tidak direkomendasikan)
python3 sync_databases.py --no-backup
```

### 3. Sync Files (Transcript + Resume)

```bash
python3 sync_files.py
```

**Fungsi:**
- Copy 23,000+ text files dari source
- Copy 23,000+ resume files (.md)
- Update database dengan path file
- Handle file conflicts

### 4. Enrich Video Metadata

```bash
python3 enrich_videos.py
```

**Fungsi:**
- Extract metadata dari CSV files per channel
- Update title dari CSV
- Set default values untuk:
  - `description`
  - `view_count` (0)
  - `duration` (0)
  - `like_count` (0)
  - `comment_count` (0)

**Catatan:** Duration, view count, like count tidak tersedia di source data. Untuk mendapatkan data ini perlu YouTube API.

### 5. Sync Long Summary

```bash
python3 sync_long_summary.py
```

**Fungsi:**
- Sync summary lengkap dari `resume/*.md`
- Update path di database
- Support summary multi-paragraph

### 6. Sync Formatted Text (Tanpa Timestamp)

```bash
python3 sync_formatted_text.py
```

**Fungsi:**
- Sync transcript tanpa timestamp dari `text_formatted/`
- Format: plain text tanpa kode waktu
- Cocok untuk reading/prompt LLM

## 🚀 Flask Web Interface

### Setup Virtual Environment

```bash
cd flask_app
python3 -m venv /media/harry/DATA120B/venv_youtube
source /media/harry/DATA120B/venv_youtube/bin/activate
pip install -r requirements.txt
```

### Run Server

**Opsi 1: Script otomatis**
```bash
./run_flask.sh
```

**Opsi 2: Manual**
```bash
cd flask_app
source /media/harry/DATA120B/venv_youtube/bin/activate
python3 app.py
```

### Akses Web Interface

| Endpoint | URL |
|----------|-----|
| Homepage | http://localhost:5000 |
| Channels | http://localhost:5000/channels |
| Videos | http://localhost:5000/videos |
| Search | http://localhost:5000/search |
| API Stats | http://localhost:5000/api/statistics |

## 📝 Perbedaan Data Source vs Target

### Data yang Tersedia di Source

| Field | Source | Target | Status |
|-------|--------|--------|--------|
| channel_id | ✅ | ✅ | Mapped |
| video_id | ✅ | ✅ | Synced |
| title | ✅ | ✅ | Synced from CSV |
| upload_date | ✅ | ✅ | Synced |
| transcript_file | ✅ | ✅ | Copied |
| resume_file | ✅ | ✅ | Copied |
| text_formatted | ✅ | ✅ | Copied (3,062) |

### Data yang Tidak Tersedia di Source

| Field | Status | Notes |
|-------|--------|-------|
| duration | ❌ | Perlu YouTube API |
| view_count | ❌ | Perlu YouTube API |
| like_count | ❌ | Perlu YouTube API |
| comment_count | ❌ | Perlu YouTube API |
| description | ❌ | Perlu YouTube API/description files |
| thumbnail_url | ❌ | Generate dari video_id |

### Solusi untuk Data yang Hilang

#### 1. Duration & Statistics (YouTube API)

```python
from googleapiclient.discovery import build

api_key = "YOUR_API_KEY"
youtube = build('youtube', 'v3', developerKey=api_key)

video = youtube.videos().list(
    part='snippet,contentDetails,statistics',
    id='VIDEO_ID'
).execute()

duration = video['items'][0]['contentDetails']['duration']
view_count = video['items'][0]['statistics']['viewCount']
```

#### 2. Generate Thumbnail URL

```python
# High quality thumbnail
thumbnail = f"https://img.youtube.com/vi/{video_id}/hqdefault.jpg"
```

#### 3. Description dari File

Jika ada file description terpisah:
```bash
out/{channel}/description/{video_id}.txt
```

## 🔧 Troubleshooting

### File Tidak Ditemukan

**Masalah:** Transcript/summary tidak bisa dibaca

**Solusi:**
```bash
# Cek path di database
sqlite3 youtube_transcripts.db "SELECT video_id, transcript_file_path FROM videos LIMIT 5;"

# Cek file ada
ls uploads/{channel}/text/{video_id}.txt

# Fix path jika perlu
sqlite3 youtube_transcripts.db "UPDATE videos SET transcript_file_path = 'uploads/' || transcript_file_path WHERE transcript_file_path NOT LIKE 'uploads/%';"
```

### Flask App Error

**Masalah:** Module not found

**Solusi:**
```bash
cd flask_app
source /media/harry/DATA120B/venv_youtube/bin/activate
pip install -r requirements.txt
```

### Database Lock

**Masalah:** Database locked

**Solusi:**
```bash
# Kill proses yang menggunakan database
lsof youtube_transcripts.db

# Atau restart Flask server
```

## 📊 Workflow Lengkap

```bash
# 1. Backup
python3 backup_db.py

# 2. Sync database (channels + videos)
python3 sync_databases.py

# 3. Sync files (transcript + resume)
python3 sync_files.py

# 4. Enrich metadata
python3 enrich_videos.py

# 5. Sync long summaries
python3 sync_long_summary.py

# 6. Sync formatted text
python3 sync_formatted_text.py

# 7. Run Flask app
./run_flask.sh
```

## 📈 Statistik Setelah Enrichment

```
total_channels: 43
total_videos: 27,374
videos_with_transcript: 23,140
videos_without_transcript: 4,234
total_word_count: 476,160
total_duration_hours: 85.4
```

## 🎯 Next Steps

1. **YouTube API Integration** - Fetch duration, views, likes
2. **Auto Thumbnail** - Generate thumbnail URLs
3. **Batch Processing** - Process videos in batches
4. **Scheduled Sync** - Cron job untuk update berkala
5. **Search Optimization** - Full-text search untuk transcripts

---

**Last Updated:** 2026-03-25
**Database Version:** youtube_transcripts.db
**Source:** /media/harry/128NEW1/GIT/yt_channel/channels.db
