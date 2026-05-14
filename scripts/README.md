# Scripts Index

`scripts/` adalah indeks command resmi repo `YOUTUBE`.

Gunakan direktori ini sebagai pintu masuk utama untuk entrypoint shell dan utilitas operasional yang masih aktif.

## Batas Dengan Root

- `scripts/` adalah sumber kebenaran utama.
- Jika ada wrapper/entrypoint lain di tempat lain, anggap itu kompatibilitas atau arsip, bukan jalur resmi.

## Entry Point Utama

- [run_pipeline.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/run_pipeline.sh)
  - Orchestrator utama: discovery, transcript, resume, format.
- [run_flask.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/run_flask.sh)
  - Menjalankan web lokal Flask.
- [supervisor.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/supervisor.sh)
  - Supervisor sadar-state untuk discovery, transcript, audio, ASR, resume, dan format.
- [preflight_orchestrator.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/preflight_orchestrator.sh)
  - Preflight runtime orchestrator: cek schema, wrapper stage, audio dir, dan yt-dlp sebelum daemon jalan panjang.
- [orchestrator_ctl.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/orchestrator_ctl.sh)
  - Kontrol pause/resume stage atau scope orchestrator, plus preflight dan janitor ad hoc.
- [run_pipeline_one_channel.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/run_pipeline_one_channel.sh)
  - Wrapper kompatibilitas untuk satu channel.
- [run_pipeline_all_channels.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/run_pipeline_all_channels.sh)
  - Wrapper kompatibilitas untuk semua channel.

## Wrapper Tugas

- [discover.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/discover.sh)
  - Jalankan discovery channel/video.
- [transcript.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/transcript.sh)
  - Jalankan recovery transcript.
- [manual_transcript_then_resume_format.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/manual_transcript_then_resume_format.sh)
  - Jalur web/manual: download transcript, lalu resume dan format otomatis.
- [audio.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/audio.sh)
  - Warm audio cache background via yt-dlp download-only.
- [resume.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/resume.sh)
  - Jalankan pembuatan resume.
- [format.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/format.sh)
  - Jalankan formatting transcript.
- [asr.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/asr.sh)
  - Jalankan ASR fallback via lease coordinator.
- [app.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/app.sh)
  - Entrypoint web legacy / compat.

## Utility Pendukung

- [generate_tasks.py](/media/harry/DATA120B/GIT/YOUTUBE/scripts/generate_tasks.py)
  - Membuat CSV task untuk transcript, resume, format, dan audio.
- [get_venv.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/get_venv.sh)
  - Resolver virtualenv eksternal.
- [repair_db_state.py](/media/harry/DATA120B/GIT/YOUTUBE/scripts/repair_db_state.py)
  - Repair state SQLite / metadata.
- [clear_shadow_text_columns.py](/media/harry/DATA120B/GIT/YOUTUBE/scripts/clear_shadow_text_columns.py)
  - Hapus duplikasi `transcript_text` / `summary_text` dari `videos` setelah blob migration, lalu opsi `VACUUM`.
- [refresh_stats.py](/media/harry/DATA120B/GIT/YOUTUBE/scripts/refresh_stats.py)
  - Refresh statistik database dan cache.
- [search.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/search.sh)
  - Bantuan pencarian metadata / discovery.
- [monitor_pipeline.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/monitor_pipeline.sh)
  - Monitor proses pipeline dan statistik.
- [test_orchestrator_rules.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/test_orchestrator_rules.sh)
  - Smoke-test rule orchestrator: YouTube cooldown, memory low, dan pause/resume stage.
- [sync_missing_rows_to_server.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/sync_missing_rows_to_server.sh)
  - Sinkronisasi delta ke server.
- [migrate_metadata.py](/media/harry/DATA120B/GIT/YOUTUBE/scripts/migrate_metadata.py)
- [migrate_metadata_to_blob.py](/media/harry/DATA120B/GIT/YOUTUBE/scripts/migrate_metadata_to_blob.py)
  - Backfill metadata video ke blob storage dan opsional kosongkan kolom teks lama.
- [migrate_search_cache.py](/media/harry/DATA120B/GIT/YOUTUBE/scripts/migrate_search_cache.py)
  - Pindahkan cache FTS blob-first ke `db/youtube_transcripts_search.db` lalu drop objek search legacy dari main DB.
- [backfill_text_blobs.py](/media/harry/DATA120B/GIT/YOUTUBE/scripts/backfill_text_blobs.py)
  - Mirror `transcript_text` dan `summary_text` ke blob storage tanpa menghapus kolom FTS.
- [backfill_transcript_files_to_blob.py](/media/harry/DATA120B/GIT/YOUTUBE/scripts/backfill_transcript_files_to_blob.py)
  - Backfill file transcript yang masih tersisa ke blob `transcript` lalu hapus file fisiknya.
- [cleanup_redundant_transcript_files.py](/media/harry/DATA120B/GIT/YOUTUBE/scripts/cleanup_redundant_transcript_files.py)
  - Hapus salinan fisik transcript yang sudah punya blob transcript dan kosongkan `transcript_file_path`.
- [cleanup_redundant_summary_files.py](/media/harry/DATA120B/GIT/YOUTUBE/scripts/cleanup_redundant_summary_files.py)
  - Hapus salinan fisik summary yang sudah punya blob resume.
- [migrate_to_blobs.py](/media/harry/DATA120B/GIT/YOUTUBE/scripts/migrate_to_blobs.py)
- [migrate_tar_to_blobs.py](/media/harry/DATA120B/GIT/YOUTUBE/scripts/migrate_tar_to_blobs.py)
- [update_db_from_tar.py](/media/harry/DATA120B/GIT/YOUTUBE/scripts/update_db_from_tar.py)
- [verify_blobs.py](/media/harry/DATA120B/GIT/YOUTUBE/scripts/verify_blobs.py)
- [verify_fix.py](/media/harry/DATA120B/GIT/YOUTUBE/scripts/verify_fix.py)
- [audit_no_subtitle_webshare.py](/media/harry/DATA120B/GIT/YOUTUBE/scripts/audit_no_subtitle_webshare.py)
- [sync_buffer_to_main.py](/media/harry/DATA120B/GIT/YOUTUBE/scripts/sync_buffer_to_main.py)
- [test_all_routes.py](/media/harry/DATA120B/GIT/YOUTUBE/scripts/test_all_routes.py)
- [archive_channels.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/archive_channels.sh)
- [deploy_exclude.txt](/media/harry/DATA120B/GIT/YOUTUBE/scripts/deploy_exclude.txt)

## Catatan

- Kalau bingung mau mulai dari mana, buka dulu [README.md](/media/harry/DATA120B/GIT/YOUTUBE/README.md) lalu kembali ke sini.
- Untuk ringkasan alur kerja, lihat juga [docs/WORKFLOWS.md](/media/harry/DATA120B/GIT/YOUTUBE/docs/WORKFLOWS.md).
