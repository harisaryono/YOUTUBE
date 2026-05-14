# Workflows

Dokumen ini adalah ringkasan operasional cepat untuk alur kerja repo `YOUTUBE`. Tujuannya supaya pencarian manual lebih cepat saat Anda ingin tahu:

- discovery berjalan lewat apa,
- transcript diambil dari mana,
- resume diproduksi bagaimana,
- format diproses di mana,
- repair channel dipakai kapan,
- dan ASR chunking berjalan bagaimana.

## Peta Cepat

- Orchestrator utama: [scripts/run_pipeline.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/run_pipeline.sh)
- Indeks dokumen: [docs/README.md](/media/harry/DATA120B/GIT/YOUTUBE/docs/README.md)
- Panduan umum: [README.md](/media/harry/DATA120B/GIT/YOUTUBE/README.md)
- Status operasional: [docs/PROGRESS.md](/media/harry/DATA120B/GIT/YOUTUBE/docs/PROGRESS.md)
- Standar validasi: [docs/VERIFY.md](/media/harry/DATA120B/GIT/YOUTUBE/docs/VERIFY.md)

## Discovery

Tujuan:
- menemukan channel/video baru atau memperbarui backlog channel yang sudah ada.

Entry point:
- [scripts/run_pipeline.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/run_pipeline.sh)
- [scripts/discover.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/discover.sh)
- [update_latest_channel_videos.py](/media/harry/DATA120B/GIT/YOUTUBE/update_latest_channel_videos.py)

Mode penting:
- `--discovery-only` untuk discovery saja.
- `--discover-auto` untuk split otomatis per channel.
- `--discover-latest-only` untuk jendela terbaru.
- `--discover-scan-all-missing` untuk full history.
- `--rate-limit-safe` untuk pacing lebih aman.

Output:
- report discovery di `runs/<run_id>/01_discovery/.../report.csv`
- plan discovery di `runs/<run_id>/01_discovery/discovery_plan.tsv`

Catatan:
- Channel backlog diproses dengan `scan-all-missing`.
- Channel bersih diproses dengan `latest-only`.

## Transcript

Tujuan:
- mengambil subtitle/transkrip video YouTube dan menyimpan ke DB + disk.

Entry point:
- [scripts/transcript.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/transcript.sh)
- [recover_transcripts.py](/media/harry/DATA120B/GIT/YOUTUBE/recover_transcripts.py)
- [recover_transcripts_from_csv.py](/media/harry/DATA120B/GIT/YOUTUBE/recover_transcripts_from_csv.py)
- [partial_py/youtube_transcript_complete.py](/media/harry/DATA120B/GIT/YOUTUBE/partial_py/youtube_transcript_complete.py)

Fallback umum:
- `youtube-transcript_api` / direct transcript
- `yt-dlp`
- SaveSubs
- proxy terakhir hanya kalau jalur non-paid gagal

Mode penting:
- `--transcript-only`
- `--rate-limit-safe`
- `--workers N`
- `--webshare-only` bila memang ingin memaksa jalur proxy

Output:
- transcript `.txt` di `uploads/<channel_id>/text/`
- report di `runs/<run_id>/.../recover_report.csv`

Catatan:
- Jalur web/manual untuk video publik yang belum punya transcript sekarang memakai chain `scripts/manual_transcript_then_resume_format.sh`, jadi setelah manual download sukses, resume dan format jalan otomatis.
- hard block harus ditandai jelas.
- batch harus berhenti lebih awal jika hard block berturut-turut sudah melewati threshold.
- logging kegagalan sekarang menampilkan class exception juga, supaya `RequestBlocked`, `TranscriptsDisabled`, dan bug internal gampang dibedakan saat audit.

## Resume

Tujuan:
- membuat ringkasan dari transcript yang sudah ada.

Entry point:
- [launch_resume_queue.py](/media/harry/DATA120B/GIT/YOUTUBE/launch_resume_queue.py)
- [fill_missing_resumes_youtube_db.py](/media/harry/DATA120B/GIT/YOUTUBE/fill_missing_resumes_youtube_db.py)
- [scripts/resume.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/resume.sh)

Mode penting:
- `--resume-only`
- `--nvidia-only` untuk batch besar yang stabil
- `--resume-model openai/gpt-oss-120b`

Prinsip:
- transcript yang sudah ada harus dipakai dari cache/DB dulu.
- jika provider Groq tidak dapat lease, worker harus fallback ke NVIDIA sesuai queue policy.
- timeout per item harus cukup ketat supaya satu task tidak mengunci batch lama.

Output:
- resume di `uploads/<channel_id>/resume/`
- status job di `runs/<run_id>/...`

## Format

Tujuan:
- merapikan transcript menjadi paragraf yang mudah dibaca.

Entry point:
- [format_transcripts_pool.py](/media/harry/DATA120B/GIT/YOUTUBE/format_transcripts_pool.py)
- [scripts/format.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/format.sh)

Model utama:
- `openai/gpt-oss-120b` sebagai baseline format
- fallback cadangan mengikuti konfigurasi provider yang tersedia

Mode penting:
- `--format-only`
- `--tasks-csv`
- `--provider nvidia`

Output:
- transcript formatted di disk dan path ter-update di SQLite

Catatan:
- transcript bahasa Inggris jangan diam-diam diterjemahkan.
- output formatting tidak boleh memunculkan tag reasoning seperti `<think>`.

## Audio

Tujuan:
- menyiapkan cache audio terpisah untuk video `no_subtitle` sebelum ASR berjalan.

Entry point:
- [scripts/audio.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/audio.sh)
- [recover_asr_transcripts.py](/media/harry/DATA120B/GIT/YOUTUBE/recover_asr_transcripts.py)

Mode penting:
- `--download-only`
- `--video-workers`
- `--limit`
- `--channel-id`

Prinsip:
- audio warmup hanya mengisi cache `source/` bersama.
- ASR consumer bisa dipaksa pakai cache lewat `--require-cached-audio`.
- jika cache belum ada, supervisor akan menaruh item itu ke retry cycle berikutnya.

Output:
- file audio cache di `runs/<run_id>/source/` atau cache shared sepadan
- report job di `runs/<run_id>/...`

## Repair

Tujuan:
- memperbaiki channel yang salah ingest dari root handle page atau state DB yang sudah tidak konsisten.

Entry point:
- [repair_channel_video_sources.py](/media/harry/DATA120B/GIT/YOUTUBE/repair_channel_video_sources.py)
- [docs/CHANNEL_SOURCE_REPAIR.md](/media/harry/DATA120B/GIT/YOUTUBE/docs/CHANNEL_SOURCE_REPAIR.md)
- Admin alias inspector: `/admin/data/channel/<channel_id>/aliases`
- Channel search API: `/api/channels/search?q=...`

Kapan dipakai:
- `video_count` terlalu kecil dan tidak masuk akal
- ada pseudo row seperti `"<name> - Videos"`
- source handle root ternyata harus diganti ke `/videos`
- navigasi legacy berhenti karena urutan per-channel belum punya rank eksplisit

Mode penting:
- scan-only untuk audit
- `--apply` untuk menulis perubahan
- `--channel-id` untuk perbaikan satu channel
- `repair_channel_ranks.py` untuk backfill rank video per channel

Output:
- backup state di `runs/repair_channel_sources_*`
- report repair di run directory
- rank eksplisit per channel di SQLite untuk legacy navigation

## ASR

Tujuan:
- transkrip audio/video yang tidak punya subtitle atau gagal di jalur transcript biasa.

Entry point:
- [recover_asr_transcripts.py](/media/harry/DATA120B/GIT/YOUTUBE/recover_asr_transcripts.py)
- [scripts/asr.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/asr.sh)

Lease/provider:
- ASR memakai lease coordinator.
- Provider aktif saat ini bisa Groq Whisper atau NVIDIA Whisper sesuai lease yang tersedia.
- Groq tetap lewat HTTP OpenAI-compatible, sedangkan NVIDIA sekarang memakai Riva gRPC offline recognition sesuai tutorial NVIDIA.
- NVIDIA ASR memakai model Riva terpisah dari Groq:
  - `ASR_MODEL_NVIDIA_RIVA=whisper-large-v3-multi-asr-offline`
  - `ASR_NVIDIA_FORCE_MODEL=0` kecuali ingin memaksa `RecognitionConfig.model`
- Variabel penting untuk NVIDIA:
  - `NVIDIA_RIVA_SERVER` default `grpc.nvcf.nvidia.com:443`
  - `NVIDIA_RIVA_FUNCTION_ID` default `b702f636-f60c-4a3d-a6f4-f3568c13bd7d`
  - `NVIDIA_RIVA_USE_SSL=1`
- Saat Groq kena `429`, worker boleh fallback ke NVIDIA untuk sisa run, lalu mengirim provider event dengan `reset_at` supaya coordinator mem-block Groq lintas run sampai cooldown habis.
- `audio_download` dan `ASR` tetap dipisah: download audio menyentuh YouTube, ASR hanya membaca audio lokal.

Chunking:
- audio panjang dipecah per chunk.
- chunk state disimpan agar job bisa resume.

Output:
- `transcript_raw.txt` untuk jejak timestamp mentah
- `transcript.txt` untuk output final
- `transcript_downloaded = 1` di DB
- report `recover_asr_report.csv`

Mode penting:
- `--video-id`
- `--channel-id`
- `--csv`
- `--video-workers 2`
- `--postprocess` bila ingin GPT OSS dipakai sebagai post-process
- `--download-only` untuk warm audio cache background
- `--require-cached-audio` untuk memaksa ASR hanya pakai cache audio yang sudah ada

## Supervisor

Tujuan:
- menjalankan mesin sadar-state yang memeriksa backlog dan mengeksekusi stage yang relevan.

Entry point:
- [scripts/supervisor.sh](/media/harry/DATA120B/GIT/YOUTUBE/scripts/supervisor.sh)
- [scripts/aware_supervisor.py](/media/harry/DATA120B/GIT/YOUTUBE/scripts/aware_supervisor.py)

Stage yang diorkestrasi:
- discovery
- transcript
- audio warmup
- ASR
- resume
- format

Prinsip:
- discovery tetap periodik.
- transcript, audio, resume, dan format dipilih dari backlog DB.
- ASR memakai `--require-cached-audio` agar tidak diam-diam download ulang.

## Urutan Praktis

Kalau tujuan Anda tidak spesifik, pakai urutan ini:

1. Discovery dulu.
2. Transcript untuk item baru atau backlog.
3. Audio warmup untuk target `no_subtitle`.
4. ASR hanya pakai cache audio yang sudah ada.
5. Resume setelah transcript ada.
6. Format setelah resume atau transcript final sudah stabil.
7. Repair hanya jika source/channel ternyata salah ingest.
