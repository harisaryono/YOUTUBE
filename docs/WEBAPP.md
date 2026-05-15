# Legacy Web Dashboard untuk `channels.db`

> Status: `webapp/` dipertahankan hanya sebagai compatibility layer.
> Jalur web resmi sekarang adalah [flask_app](../flask_app/app.py) via [scripts/run_flask.sh](../scripts/run_flask.sh).
> Kalau tidak sedang memelihara kompatibilitas lama, gunakan `flask_app/` saja.

## Install

```bash
python3 -m venv /media/harry/DATA120B/venv_youtube
source /media/harry/DATA120B/venv_youtube/bin/activate
pip install -r requirements-web.txt
```

Catatan:
- Untuk fitur update channel (job background), server butuh `yt-dlp` (sudah masuk `requirements-web.txt`).
- Disarankan Python `3.9+`.

## Jalankan Web Legacy

```bash
source /media/harry/DATA120B/venv_youtube/bin/activate
python3 run_web.py
```

Buka: `http://127.0.0.1:5000`

Catatan:
- Ini hanya untuk kompatibilitas lama.
- Untuk jalur resmi, pakai:

```bash
./scripts/run_flask.sh
```

## Fitur Yang Sudah Diimplementasikan

### 1) Dashboard channel (`/channels`)
- List channel dengan statistik video (`total`, `downloaded`, `pending`, `error`).
- Pagination + `per_page` agar load ringan untuk list besar.
- Filter channel berdasarkan `slug/url` dan kategori.
- Assign kategori channel langsung dari tabel (inline save).
- Tombol:
  - `Add channel`
  - `DB cleanup` (diblokir otomatis saat DB/job masih sibuk)
  - `Migrate resumes` (migrasi `resume_text` DB -> file `.md`)
  - `Update Stale (>7 days)` untuk queue update channel lama.
- Panel `Recent jobs` dengan status `queued/running/stopping/stopped/done/error`.

### 2) Manajemen channel
- Add channel baru (`/channels/add`) + opsi kategori channel.
- Bisa langsung queue update setelah channel ditambahkan.
- Update kategori channel lewat endpoint khusus:
  - `POST /channels/<channel_id>/category`

### 3) Detail channel (`/channels/<channel_id>`)
- List video per-channel dengan pagination.
- Filter video: keyword, status, kategori.
- Toggle tampilkan/sembunyikan `no_subtitle`.
- Aksi update:
  - Quick update (`stop-at-known`)
  - Full scan
  - Pending-only
- Bulk assign kategori ke video terpilih (mode `add`/`replace`).
- Retry pending untuk video tertentu.

### 4) Search global (`/search`)
- Cari video berdasarkan title/video_id.
- Filter status dan kategori.
- Opsi cari keyword di isi resume (`include_resume`).

### 5) Detail video + transcript + resume
- Halaman detail video (`/videos/<id>`) dengan:
  - Embed YouTube
  - Preview transcript
  - Kategori video
  - Resume editor
- Transcript:
  - View: `/videos/<id>/text`
  - Download raw TXT: `/videos/<id>/text?raw=1`
- Resume:
  - Save ke file Markdown (`out/<channel>/resume/*.md`), DB simpan `link_resume`.
  - Download raw `.md`: `/videos/<id>/resume.md`
  - Reader mode HTML: `/videos/<id>/resume/read` + prev/next.
- Mark video `read/unread`.

### 6) Kategori (`/categories`)
- CRUD kategori (nama + warna hex).
- Hapus kategori otomatis melepaskan relasi:
  - `video_categories`
  - `channels.category_id`

### 7) Job system background
- Update channel jalan sebagai proses terpisah (tetap lanjut walau browser ditutup).
- Queue otomatis: jika ada job running, job baru masuk `queued`.
- Bisa stop job dari UI (`/jobs/<job_id>/stop`).
- Deteksi stale PID agar job tidak terlihat “running” selamanya.
- Log job tersimpan di `.webapp_jobs/`.

### 8) DB safety & maintenance
- Inisialisasi schema + migrasi kolom opsional otomatis.
- Fallback `WAL -> DELETE` jika mode WAL tidak tersedia.
- Busy timeout untuk mengurangi `database is locked`.
- DB cleanup aman:
  - Cek blocker (job aktif / resume lock / DB busy)
  - checkpoint + cleanup file `-wal/-shm` saat aman.

## Operasional Resume Agent (CLI)

Script: `run_resume_agents.sh`

Perintah:

```bash
./run_resume_agents.sh run
./run_resume_agents.sh start
./run_resume_agents.sh status
./run_resume_agents.sh stop
./run_resume_agents.sh stop harry silfi
```

Catatan:
- `run`/`start` otomatis detach (tidak perlu `nohup` manual).
- `stop` bisa semua atau sebagian agent.
- State agent disimpan di `.resume_agents/`, log di `out/agent_logs/`.
- `status` menampilkan progress resume global (persentase selesai, done/total, remaining).

## Shard Storage (Hot/Cold)

Untuk mengurangi jumlah file + ukuran storage, file final bisa dipindah dari plain file ke shard `.zst` per channel:

- Hot storage: file plain (`out/<channel>/text/*.txt`, `out/<channel>/resume/*.md`) untuk in-progress.
- Cold storage: shard terkompresi (`out/<channel>/.shards/*.zst`) + index (`out/<channel>/.shards/index.json`).
- DB `link_file` / `link_resume` tetap path lama (tidak perlu migrasi schema DB).

Perintah:

```bash
# simulasi
python3 compact_out_to_shards.py --dry-run

# compact real (default: text+resume, shard max 128MB, min-age 60 menit)
python3 compact_out_to_shards.py

# hanya transcript, channel tertentu
python3 compact_out_to_shards.py --kind text --channel FirandaAndirjaOfficial
```

Catatan:
- Web app dan resume worker sudah bisa baca dari plain file maupun shard transparan.
- File yang masih baru/hot tidak dipindah (default `--min-age-minutes 60`).

## Konfigurasi (Opsional)

Default:
- DB: `channels.db`
- Output: `out/`

Override via env:

```bash
export CHANNELS_DB=channels.db
export OUT_ROOT=out
export SCRAPER_SCRIPT=scrap_all_channel.py
# Startup safety (default semuanya ON)
# export WEBAPP_STARTUP_INTEGRITY=1
# export WEBAPP_STARTUP_CLEANUP_WAL=1
# export WEBAPP_STARTUP_RECONCILE_FILES=1
# 0 = scan semua video; angka >0 = batasi scan startup
# export WEBAPP_STARTUP_RECONCILE_LIMIT=0
# Journal mode SQLite default sekarang DELETE (lebih aman untuk lock issue)
# export DB_JOURNAL_MODE=DELETE
python3 run_web.py
```

## Saran TODO Berikutnya

1. Tambah autentikasi (login + role admin/operator) sebelum akses aksi write/delete.
2. Tambah halaman kontrol resume agent di web (start/stop/status) agar tidak lewat shell.
3. Tambah progress bar job realtime (polling/SSE) termasuk ETA, rate, dan error terakhir.
4. Tambah retry policy yang lebih eksplisit untuk resume gagal (max retry, backoff, reason).
5. Tambah dashboard observability: jumlah resume kosong/truncated, timeout rate, biaya/token.
6. Tambah full-text search (SQLite FTS5) untuk transcript + resume agar search cepat di data besar.
7. Tambah endpoint healthcheck + readiness untuk deployment/monitoring.
8. Tambah test suite minimal:
   - route smoke test
   - schema migration test
   - job queue/stop behavior test
