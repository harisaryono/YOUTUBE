# Partial Python Scripts

`partial_py/` adalah area **non-canonical** untuk script Python yang:
- parsial,
- legacy,
- channel-specific,
- migration/repair sekali jalan,
- debug/eksperimen,
- atau utilitas operasional yang bukan jalur global utama repo.

Tujuan folder ini:
- menjaga root repo tetap bersih,
- memisahkan jalur aktif dari jalur one-off,
- dan membuat file yang rawan berubah tidak bercampur dengan entrypoint produksi.

## Batas Yang Jelas

Gunakan `partial_py/` hanya jika file yang Anda cari memang:
- bukan entrypoint harian,
- bukan jalur pipeline resmi,
- atau sengaja disimpan sebagai arsip kerja/migrasi.

Kalau Anda mencari jalur operasional utama, buka ini dulu:
- [scripts/README.md](/media/harry/DATA120B/GIT/YOUTUBE/scripts/README.md)
- [docs/README.md](/media/harry/DATA120B/GIT/YOUTUBE/docs/README.md)
- [docs/WORKFLOWS.md](/media/harry/DATA120B/GIT/YOUTUBE/docs/WORKFLOWS.md)

## Yang Aktif Bukan Di Sini

Script dan modul yang dipakai rutin tetap berada di lokasi canonical masing-masing:

- Entrypoint pipeline: `scripts/run_pipeline.sh`
- Entrypoint Flask: `scripts/run_flask.sh`
- Wrapper tugas: `scripts/discover.sh`, `scripts/transcript.sh`, `scripts/resume.sh`, `scripts/format.sh`, `scripts/asr.sh`
- Modul inti: `update_latest_channel_videos.py`, `recover_transcripts.py`, `recover_transcripts_from_csv.py`, `fill_missing_resumes_youtube_db.py`, `launch_resume_queue.py`, `database_optimized.py`, `local_services.py`, `provider_encryption.py`, `manage_database.py`, `partial_py/youtube_transcript_complete.py`

## Kategori Isi Folder

### 1) Legacy / Compat

Script di sini biasanya pernah jadi jalur utama, lalu diganti versi baru.
Contoh:
- `fill_missing_resumes.py`
- `fill_missing_resumes_simple_coordinator.py`
- `launch_universal_resume.py`
- `youtube_transcript.py`
- `provider_coordinator_server.py`

### 2) Repair / Migration / Backfill

Script yang dipakai untuk perbaikan data atau migrasi satu kali.
Contoh:
- `backup_db.py`
- `enrich_videos.py`
- `sync_databases.py`
- `sync_files.py`
- `sync_formatted_text.py`
- `sync_long_summary.py`
- `repair_member_only_state.py`
- `migrate_channels.py`
- `migrate_summaries_to_files.py`
- `fix_transcript_paths.py`

### 3) Channel-Specific / Scraper / Sandbox

Script yang hanya relevan untuk channel tertentu atau eksperimen terbatas.
Contoh:
- `scrape_kokbisa.py`
- `scrape_ilmuberlimpah.py`
- `scrape_ilmuberlimpah_manual.py`
- `scrape_ilmuberlimpah_auto.py`
- `scrape_ilmuberlimpah_rss.py`
- `fetch_kokbisa_videos.py`
- `import_kokbisa_videos.py`
- `update_kokbisa_paths.py`
- `compare_format_models.py`
- `test_recovery.py`

## Cara Menjalankan Script Parsial

Jalankan dari root repo dengan mode module:

```bash
/media/harry/DATA120B/venv_youtube/bin/python -m partial_py.nama_script_tanpa_py
```

Contoh:

```bash
/media/harry/DATA120B/venv_youtube/bin/python -m partial_py.migrate_summaries_to_files
/media/harry/DATA120B/venv_youtube/bin/python -m partial_py.scrape_kokbisa
/media/harry/DATA120B/venv_youtube/bin/python -m partial_py.compare_format_models --video-id i6xGvztL9ZE --video-id 8pau0LqikL8
```

## Aturan Pakai

- Jangan menganggap semua file di folder ini stabil.
- Jangan pakai file di sini untuk batch besar tanpa cek `README`, `PROGRESS.md`, atau log run terakhir.
- Jika butuh status operasional, cek `docs/PROGRESS.md` atau `docs/README.md` dulu.
- Kalau ada versi baru yang resmi, prioritasnya pindah ke `scripts/` atau root modul inti.
