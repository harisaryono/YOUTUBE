# Documentation Index

Dokumen ini adalah pintu masuk utama untuk repo `YOUTUBE`. Gunakan ini saat ingin cepat mencari alur kerja, dokumen acuan, atau utilitas yang paling relevan.

## Baca Dulu

- [README.md](/media/harry/DATA120B/GIT/YOUTUBE/README.md)
- [WORKFLOWS.md](/media/harry/DATA120B/GIT/YOUTUBE/docs/WORKFLOWS.md)
- [STATE_MACHINE.md](/media/harry/DATA120B/GIT/YOUTUBE/docs/STATE_MACHINE.md)
- [PLAN.md](/media/harry/DATA120B/GIT/YOUTUBE/docs/PLAN.md)
- [PROGRESS.md](/media/harry/DATA120B/GIT/YOUTUBE/docs/PROGRESS.md)
- [VERIFY.md](/media/harry/DATA120B/GIT/YOUTUBE/docs/VERIFY.md)
- [DEVELOPER_GUIDE.md](/media/harry/DATA120B/GIT/YOUTUBE/docs/DEVELOPER_GUIDE.md)
- [DB_DIET_AUDIT.md](/media/harry/DATA120B/GIT/YOUTUBE/docs/DB_DIET_AUDIT.md)
- [FTS_MIGRATION_PLAN.md](/media/harry/DATA120B/GIT/YOUTUBE/docs/FTS_MIGRATION_PLAN.md)
- [AGENTS.md](/media/harry/DATA120B/GIT/YOUTUBE/AGENTS.md)

## Alur Kerja Utama

- Discovery, transcript, resume, dan format terpusat di [scripts/run_pipeline.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/run_pipeline.sh).
- Mode discovery auto dan full-history dijelaskan di [PLAN.md](/media/harry/DATA120B/GIT/YOUTUBE/docs/PLAN.md) dan [PROGRESS.md](/media/harry/DATA120B/GIT/YOUTUBE/docs/PROGRESS.md).
- Worker resume lintas akun ada di [launch_resume_queue.py](/media/harry/DATA120B/GIT/YOUTUBE/launch_resume_queue.py).
- Worker ASR berbasis lease coordinator ada di [recover_asr_transcripts.py](/media/harry/DATA120B/GIT/YOUTUBE/recover_asr_transcripts.py).
- Formatting transcript dengan GPT OSS 120B ada di [format_transcripts_pool.py](/media/harry/DATA120B/GIT/YOUTUBE/format_transcripts_pool.py).

## Utility Penting

- Perbaikan channel yang salah ingest dari root handle ada di [repair_channel_video_sources.py](/media/harry/DATA120B/GIT/YOUTUBE/repair_channel_video_sources.py).
- Perbaikan urutan video legacy per channel ada di [repair_channel_ranks.py](/media/harry/DATA120B/GIT/YOUTUBE/repair_channel_ranks.py).
- Database aktif tersimpan di [db/](/media/harry/DATA120B/GIT/YOUTUBE/db) dan root filename disisakan sebagai symlink kompatibilitas.
- Detail aturan perbaikan ada di [CHANNEL_SOURCE_REPAIR.md](/media/harry/DATA120B/GIT/YOUTUBE/docs/CHANNEL_SOURCE_REPAIR.md).
- Helper partial dan legacy ada di [partial_py/README.md](/media/harry/DATA120B/GIT/YOUTUBE/partial_py/README.md).
- Dokumen parsial/arsip ada di [partial_docs/README.md](/media/harry/DATA120B/GIT/YOUTUBE/partial_docs/README.md).
- Helper operasional non-utama ada di [partial_ops/README.md](/media/harry/DATA120B/GIT/YOUTUBE/partial_ops/README.md).
- Indeks command shell utama ada di [scripts/README.md](/media/harry/DATA120B/GIT/YOUTUBE/scripts/README.md).

## Cara Mencari Cepat

- Cari pipeline dan mode baru di [scripts/run_pipeline.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/run_pipeline.sh).
- Cari ringkasan workflow cepat di [WORKFLOWS.md](/media/harry/DATA120B/GIT/YOUTUBE/docs/WORKFLOWS.md).
- Cari perubahan status operasional di [PROGRESS.md](/media/harry/DATA120B/GIT/YOUTUBE/docs/PROGRESS.md).
- Cari standar validasi di [VERIFY.md](/media/harry/DATA120B/GIT/YOUTUBE/docs/VERIFY.md).
- Cari ringkasan repair channel di [CHANNEL_SOURCE_REPAIR.md](/media/harry/DATA120B/GIT/YOUTUBE/docs/CHANNEL_SOURCE_REPAIR.md).
- Cari rencana migrasi FTS di [FTS_MIGRATION_PLAN.md](/media/harry/DATA120B/GIT/YOUTUBE/docs/FTS_MIGRATION_PLAN.md).
