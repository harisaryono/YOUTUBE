# YouTube Transcript Framework

Framework Python untuk mengambil transkrip dari video YouTube, memformat hasil transkrip, dan membuat ringkasan otomatis. Framework ini mendukung pengambilan skala besar (channel) dan penyimpanan ke database terpusat.

## Dokumen Acuan

Dokumen acuan utama sekarang dipusatkan di `docs/`:
- [docs/README.md](docs/README.md)
- [docs/WORKFLOWS.md](docs/WORKFLOWS.md)
- [docs/PLAN.md](docs/PLAN.md)
- [docs/PROGRESS.md](docs/PROGRESS.md)
- [docs/VERIFY.md](docs/VERIFY.md)
- [docs/DEVELOPER_GUIDE.md](docs/DEVELOPER_GUIDE.md)
- [docs/CHANNEL_SOURCE_REPAIR.md](docs/CHANNEL_SOURCE_REPAIR.md)
- [AGENTS.md](AGENTS.md)

> **Catatan internal:** Dokumentasi coordinator internal dan konfigurasi lease ada di server operasional, tidak disertakan di repo publik.
Dokumen lama, parsial, migration note, setup sekali jalan, dan arsip hasil run dipindah ke [partial_docs/README.md](partial_docs/README.md).
Helper operasional non-utama seperti tunnel fallback dipindah ke [partial_ops/README.md](partial_ops/README.md).
Artefak ekspor, notebook, cookie lama, dan arsip file sementara dipindah ke [artifacts/README.md](artifacts/README.md).

## Fitur Utama

- ✅ **Pengambilan Skala Besar**: Download seluruh transkrip dari satu channel sekaligus.
- ✅ **Database Terpusat**: Menyimpan metadata video dan referensi file transkrip secara terorganisir.
- ✅ **Optimasi File**: Struktur folder `uploads/` yang rapi berdasarkan channel dan kategori (text/resume).
- ✅ **Web Interface**: Antarmuka berbasis Flask untuk pencarian dan pembacaan transkrip.
- ✅ **Summary Otomatis**: Membuat ringkasan dari setiap video yang didownload.

## Instalasi

### 1. Clonning & Persiapan
```bash
git clone <repository_url>
cd YOUTUBE
```

### 2. Setup Virtual Environment (Rekomendasi)
```bash
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows
pip install -r requirements.txt
```

## Penggunaan Utama

### Cara Menjalankan Script (PENTING)

Semua wrapper script di folder `scripts/` otomatis mendeteksi virtual environment:
1. Jika `EXTERNAL_VENV_DIR` diatur di `.env`, akan memakai path tersebut.
2. Jika tidak, akan memakai `.venv` di dalam repo.

> **Catatan untuk pengguna lokal:** Path venv eksternal (misal `/media/harry/DATA120B/venv_youtube/`) bisa diatur di `.env` lewat `EXTERNAL_VENV_DIR`. File `.env` sudah di-`.gitignore` sehingga aman untuk konfigurasi pribadi.

### 1. Wrapper Scripts (REKOMENDASI - Otomatis Pakai venv eksternal)

Folder `scripts/` berisi wrapper shell yang otomatis mengaktifkan virtual environment:

```bash
# Discovery - scan video baru dari channel
./scripts/discover.sh --help
./scripts/discover.sh --latest-only --recent-per-channel 50
./scripts/discover.sh --channel-id KenapaItuYa
./scripts/discover.sh --full-pipeline  # discovery + transcript + resume

# Transcript - download transcript
./scripts/transcript.sh --help
./scripts/transcript.sh --video-id dQw4w9WgXcQ
./scripts/transcript.sh --channel-id KenapaItuYa --limit 200
./scripts/transcript.sh --csv tasks.csv --run-dir runs/transcript_batch
./scripts/transcript.sh --webshare-only --limit 50  # paksa jalur proxy Webshare

# Resume - generate resume dari transcript
./scripts/resume.sh --help
./scripts/resume.sh --video-id dQw4w9WgXcQ
./scripts/resume.sh --tasks-csv tasks.csv --model openai/gpt-oss-120b

# Format - format transcript agar lebih mudah dibaca
./scripts/format.sh --help
./scripts/format.sh --video-id dQw4w9WgXcQ --model openai/gpt-oss-120b
./scripts/format.sh --limit 10 --provider nvidia

# Pipeline bertahap - discovery -> transcript -> resume -> format
./scripts/run_pipeline.sh --channel-id KenapaItuYa
./scripts/run_pipeline.sh --channel-name "Kok Bisa?"
./scripts/run_pipeline.sh --all-channels

# Mode parsial
./scripts/run_pipeline.sh --discovery-only --channel-id KenapaItuYa
./scripts/run_pipeline.sh --transcript-only --channel-id KenapaItuYa
./scripts/run_pipeline.sh --resume-only --channel-id KenapaItuYa
./scripts/run_pipeline.sh --format-only --channel-id KenapaItuYa
```

Default operasional pipeline bertahap:
- Discovery: auto per channel, `latest-only` untuk channel bersih dan `scan-all-missing` untuk backlog, plus `--rate-limit-safe`
- Transcript: `10` worker, direct-first
- Resume: `10` worker, `--nvidia-only`, model `openai/gpt-oss-120b`
- Format: `8` worker, provider plan `nvidia_only`

Mode pintas:
- `--discovery-only` menjalankan discovery saja
- `--transcript-only` menjalankan transcript saja
- `--resume-only` menjalankan resume saja
- `--format-only` menjalankan format saja

Discovery override:
- `--discover-auto` pakai aturan otomatis per channel
- `--discover-latest-only` paksa semua channel pakai jendela terbaru
- `--discover-scan-all-missing` paksa semua channel scan full history

Channel repair:
- `repair_channel_video_sources.py` memperbaiki channel yang salah ingest dari root handle page.
- `repair_channel_ranks.py` membangun rank eksplisit per channel untuk navigasi legacy video.
- Lihat detail di [docs/CHANNEL_SOURCE_REPAIR.md](docs/CHANNEL_SOURCE_REPAIR.md).

### Rate-limit / YouTube Block (Transcript)

`recover_transcripts.py` (dan pipeline `update_latest_channel_videos.py`) menerapkan pacing internal untuk mengurangi risiko IP block / HTTP 429 dari YouTube.

Env yang bisa di-tuning:
- `YT_TRANSCRIPT_INTER_VIDEO_DELAY_MIN` / `YT_TRANSCRIPT_INTER_VIDEO_DELAY_MAX` (default `8`–`15` detik)
- `YT_TRANSCRIPT_BACKOFF_CAP_SECONDS` (default `1800`)
- `YT_TRANSCRIPT_BACKOFF_START_429_SECONDS` (default `30`)
- `YT_TRANSCRIPT_BACKOFF_START_403_SECONDS` (default `60`)
- `YT_TRANSCRIPT_BACKOFF_START_IP_BLOCKED_SECONDS` (default `300`)

Mode operasional aman:
- `./scripts/transcript.sh --rate-limit-safe` menyalakan pacing, membatasi worker, dan memotong fallback mahal dulu.
- `./scripts/discover.sh --rate-limit-safe` menunda antar channel dan melewati lookup `upload_date` tambahan yang mahal.

### 2. Mengambil Transkrip Channel (Manual)
Gunakan versi lengkap (`complete`) untuk fitur database dan koleksi channel:
```bash
/media/harry/DATA120B/venv_youtube/bin/python3 partial_py/youtube_transcript_complete.py https://www.youtube.com/@KenapaItuYa --channel
```

### 3. Menjalankan Dashboard Web (Flask)
```bash
./scripts/run_flask.sh
```
Atau manual:
```bash
/media/harry/DATA120B/venv_youtube/bin/python3 flask_app/app.py
```
Akses melalui peramban di: `http://127.0.0.1:5000`

### 4. Manajemen Database
Gunakan script manajemen untuk melihat statistik atau memperbaiki data:
```bash
/media/harry/DATA120B/venv_youtube/bin/python3 manage_database.py stats
/media/harry/DATA120B/venv_youtube/bin/python3 manage_database.py retry
```

### 5. Pipeline Global Utama
Workflow utama repo ini sekarang adalah fase terpisah:
```bash
./scripts/run_pipeline.sh --all-channels
```

Untuk resume-only dengan antrean lintas akun:
```bash
/media/harry/DATA120B/venv_youtube/bin/python launch_resume_queue.py
```

Wrapper shell legacy (masih didukung):
```bash
./scripts/run_pipeline.sh --all-channels
./scripts/run_pipeline_one_channel.sh --channel-id KenapaItuYa
./scripts/run_pipeline_one_channel.sh --channel-name "Kok Bisa?"
./scripts/run_pipeline_all_channels.sh
```

## Struktur Project

```
YOUTUBE/
├── scripts/                           # Wrapper shell (otomatis pakai venv eksternal)
│   ├── discover.sh                    # Discovery video baru dari channel
│   ├── transcript.sh                  # Download transcript dari video
│   ├── resume.sh                      # Generate resume dari transcript
│   └── format.sh                      # Format transcript agar mudah dibaca
├── docs/                              # Dokumentasi utama
├── update_latest_channel_videos.py    # Pipeline global discovery -> transcript -> resume
├── recover_transcripts.py             # Recovery transcript inti
├── recover_transcripts_from_csv.py    # Recovery transcript berbasis task CSV
├── fill_missing_resumes_youtube_db.py # Worker resume inti untuk db/youtube_transcripts.db
├── launch_resume_queue.py             # Launcher resume lintas akun
├── local_services.py                  # Client coordinator + utilitas provider
├── manage_database.py                 # Alat manajemen database
├── database_optimized.py              # Modul DB untuk UI/web dan recovery
├── database.py                        # Modul DB legacy untuk jalur compat
├── partial_py/youtube_transcript_complete.py  # Jalur compat lengkap
├── format_transcripts_pool.py         # Format transcript dengan multi-worker pool
├── partial_py/                        # Script parsial / legacy / migration / eksperimen
├── flask_app/                         # Aplikasi Web Dashboard
├── db/                                # SQLite aktif (root symlink dipertahankan untuk kompatibilitas)
├── scripts/run_pipeline.sh            # Orchestrator pipeline utama
├── scripts/run_flask.sh               # Entrypoint Flask lokal
├── scripts/README.md                  # Indeks command dan utilitas
├── uploads/                           # Data hasil (Transkrip & Ringkasan)
│   └── Channel_ID/
│       ├── text/                      # File transkrip (.txt)
│       └── resume/                    # File ringkasan (.txt)
├── runs/                              # Output batch dan report
└── venv eksternal                     # /media/harry/DATA120B/venv_youtube/
```

## Catatan Struktur Baru

- Root repo sekarang hanya menyisakan prosedur global.
- Script yang sifatnya parsial, legacy, migration, repair satu kali, launcher lama, sync lama, atau channel-specific dipindah ke [`partial_py/README.md`](partial_py/README.md).
- `youtube_transcript.py` versi dasar juga dipindah ke `partial_py/`, karena tidak lagi menjadi jalur global utama.
- Database aktif sekarang disimpan di `db/` dan root filename tetap ada sebagai symlink kompatibilitas, jadi script lama tidak perlu langsung diubah satu per satu.
- Entrypoint shell resmi sekarang ada di `scripts/`; root `run_pipeline.sh` dan `run_flask.sh` hanya symlink kompatibilitas.
- Script parsial dijalankan dari root repo dengan format:

```bash
/media/harry/DATA120B/venv_youtube/bin/python -m partial_py.nama_script_tanpa_py
```

## Troubleshooting

- **Rate Limit (429)**: Script sudah dilengkapi *exponential backoff*. Jika masih terjadi, tunggu beberapa menit atau gunakan VPN/Proxy.
- **ModuleNotFoundError**: 
  - Jika pakai wrapper `scripts/*.sh`, pastikan `EXTERNAL_VENV_DIR` mengarah ke `/media/harry/DATA120B/venv_youtube`
  - Jika jalan manual, pastikan pakai python dari `/media/harry/DATA120B/venv_youtube/bin/python3`
- **Wrapper script tidak bisa dijalankan**: Beri permission executable:
  ```bash
  chmod +x scripts/*.sh
  ```
- **Virtualenv tidak ditemukan**: Jalankan:
  ```bash
  python3 -m venv /media/harry/DATA120B/venv_youtube
  source /media/harry/DATA120B/venv_youtube/bin/activate
  pip install -r requirements.txt
  ```
- **Database Error**: Jalankan `/media/harry/DATA120B/venv_youtube/bin/python3 manage_database.py stats` untuk memastikan skema database sudah sesuai.
- **Coordinator error**: Pastikan coordinator URL benar (default: `http://127.0.0.1:8788`). Set via env:
  ```bash
  export YT_PROVIDER_COORDINATOR_URL="http://127.0.0.1:8788"
  ```
