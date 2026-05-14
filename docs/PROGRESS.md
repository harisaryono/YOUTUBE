# RECOVERY PROGRESS

## Overall Backlog: 4,162 videos
- Status: **Phase 2 Completed (50 videos processed)**

## Current Local Run
- `runs/transcript_no_subtitle_webshare_audit_20260507_0355_rlsafe/` sedang berjalan ulang dengan `3200` target, `6` worker, pacing aktif, dan mode `rate-limit-safe`.
- Batch transcript lama yang memakai `20` worker sudah dihentikan supaya tekanan ke YouTube turun.
- Mode aman sekarang mematikan fallback mahal lebih awal untuk mengurangi request tambahan saat recovery transcript.
- Discovery full-history lama sudah dihentikan dan diganti run baru `--latest-only --rate-limit-safe` supaya scan channel besar tidak terus kena throttling.
- Resume summary backlog yang aktif sekarang berjalan di `runs/resume_resume_20260509_070000_nvidia_only/` dengan `318` target dari DB utama, `12` worker, dan `nvidia-only` mode supaya `clod` tidak dipakai.
- Jalur ASR baru untuk audio YouTube sudah ditambahkan:
  - `recover_asr_transcripts.py`
  - `scripts/asr.sh`
  - tabel `video_asr_chunks` untuk resume per chunk/provider
- ASR sekarang dipisah jadi dua stage:
  - `audio_download` untuk fetch audio lokal ke `video_audio_assets`
  - `asr` untuk membaca file audio lokal saja, tanpa YouTube download
- `scripts/audio.sh` / `scripts/audio_download.sh` sekarang jadi stage `audio_download`, sedangkan `scripts/asr.sh` hanya menjalankan ASR lokal.
- `recover_asr_transcripts.py` sekarang menyimpan `video_audio_assets.audio_file_path`, mendukung `--local-audio-only`, dan bisa menghapus audio lokal setelah ASR sukses.
- Policy orchestrator sekarang adaptif: stage lokal/provider tetap diprioritaskan saat aman, sedangkan stage YouTube-limited (`discovery`, `transcript`, `audio_download`) dinaikkan saat backlog menumpuk dan tetap ditahan kalau kena cooldown/block.
- Supervisor sadar-state baru sudah disiapkan:
  - `scripts/audio.sh`
  - `scripts/supervisor.sh`
  - `scripts/aware_supervisor.py`
- Database diet audit sudah dibuat di [docs/DB_DIET_AUDIT.md](/media/harry/DATA120B/GIT/YOUTUBE/docs/DB_DIET_AUDIT.md); WAL `youtube_transcripts.db` sudah di-truncate.
- Default chunk ASR sekarang `45 detik` dengan `2 detik` overlap supaya batas kata lebih aman.
- Smoke aman ASR sudah lolos: target palsu `DOESNOTEXIST` menghasilkan `Tidak ada target ASR yang diproses` tanpa download audio.
- Smoke wrapper `scripts/asr.sh --video-id DOESNOTEXIST` juga lolos dengan exit code `0`.
- ASR sekarang memakai lease coordinator untuk mengambil `api_key`/endpoint bundle per provider; smoke `iyo9VuY5dpg` selesai sukses via lease `groq/whisper-large-v3` dan transcript final tersimpan di DB + `uploads/asr/iyo9VuY5dpg/transcript.txt`.
- Timestamped ASR sekarang punya dua lapis output: `transcript_raw.txt` untuk hasil timestamp mentah dan `transcript.txt` untuk hasil akhir.
- GPT OSS 120B dipakai sebagai post-process hanya untuk transcript yang masih kecil; transcript panjang otomatis dilewati dan langsung memakai output timestamp mentah agar lebih cepat.
- Default ASR sekarang tidak memakai post-process; GPT OSS hanya aktif kalau `--postprocess` dipilih eksplisit.
- Mode `--video-workers 2` sudah lolos smoke parallel untuk 2 video nyata, dan report gabungan tetap menulis `postprocess_status=disabled` serta transcript raw timestamped ke DB + disk.

## Operational Notes
- Dokumentasi utama sekarang dipusatkan di `DOCS_INDEX.md`; gunakan itu sebagai pintu masuk cepat sebelum membuka file lain.
- Setiap mode baru di `run_pipeline.sh` dan setiap repair utility baru wajib punya jejak di `README.md`, `PROGRESS.md`, atau dokumen khusus seperti `CHANNEL_SOURCE_REPAIR.md`.
- `scripts/clear_shadow_text_columns.py` sudah dipakai untuk mengosongkan `videos.transcript_text` dan `videos.summary_text` pada row yang punya blob pasangan.
- `scripts/migrate_search_cache.py` sekarang memindahkan `videos_search_cache` + `videos_search_fts` ke `db/youtube_transcripts_search.db`; corpus search sudah diperkecil dengan menghapus `summary_search`. Hasil akhirnya: `youtube_transcripts.db` sekitar `51.6 MB`, `db/youtube_transcripts_search.db` sekitar `368.9 MB`, dan `idx_videos_upload_date` juga sudah dibuang.
- Orchestrator Stage 2 sekarang memisahkan `audio_download -> asr`; batch `audio_download` mencari `no_subtitle` yang belum punya aset audio lokal, sedangkan `asr` hanya memproses row yang sudah punya `video_audio_assets.status = downloaded`.
- Audit lanjutan menunjukkan `description` di search corpus tidak layak dibuang: simulasi title+transcript-only hanya menghemat sekitar `0.8 MB`, tapi sample query dari description kehilangan sekitar `1.42%` hit.
- Policy orchestrator final:
  - orchestrator bekerja terus selama target masih ada dan stage belum blocked
  - planner sekarang `work_conserving` / `available-work first`, bukan `local first`
  - stage sensitif YouTube hanya `discovery`, `transcript`, dan `audio_download`
  - stage lokal/provider tetap boleh jalan selama resource aman dan lease tersedia
  - `batch_limit` adalah ukuran potongan kerja, bukan batas total kerja harian
  - kalau batch habis dan masih ada backlog, orchestrator harus re-plan dan membuat batch baru
  - loop aggressiveness sekarang diturunkan ke `min_sleep_seconds: 5` agar re-plan cepat saat masih ada kerja yang aman
  - command `./scripts/orchestrator.sh explain` sekarang menampilkan inventori kerja, blocker, dan reason code defer aktif
- Ingest metadata-only terbaru: `@MentalCuann`, `@JurnalInvestasiku`, `@SiPalingLogis`, `@nalarlambat`, `@ilmulidi`, dan video `rn9-P466MWw` dari `@SeniMengaturGaji` dicatat di `runs/manual_channel_ingest_20260514_062000/report.json`.
- Ingest metadata-only berikutnya: `@KendatiDemikianStudio`, `@kayaalaceo`, `@FinansialMedia`, `@Jejolok`, `@RuangKaya`, serta normalisasi `@OasisCeritaUsaha` dicatat di `runs/manual_channel_ingest_20260514_062350/report.json`.
- Ingest channel baru `@NosTec.id1` sudah dilakukan lewat source `/videos`; hasil metadata awal yang tersimpan: `Nostalgia Technologi`, `37` video, `channel_db_id=648`.
- Discovery lanjutan untuk channel baru di atas sudah dijalankan di `runs/discovery_new_channels_20260514_065421/`; sebagian besar channel masuk `retry_incomplete` full-history, sementara `@SeniMengaturGaji` menghasilkan `97 new` dan `1 retry_incomplete`.
- Transcript batch untuk 12 channel baru sudah dijalankan di `runs/transcript_new_channels_20260514_072000/` dengan `2337` task dan `10` worker.
- Audit transcript terbaru menemukan bug nyata di jalur SaveSubs tmpdir cleanup, bukan di `youtube-transcript-api`; urutan fallback sekarang sudah dipindah ke `API -> yt-dlp -> SaveSubs -> Webshare`, dan log exception kini menyertakan class error supaya bedanya `blocked`, `retry_later`, `fatal`, dan bug internal lebih cepat dibaca.
- Normalisasi channel identity sekarang diperkuat: DB inti dan helper legacy sama-sama menyimpan `channel_handle`, menulis alias untuk `UC...`, `@handle`, dan display name, lalu lookup channel bisa lewat satu entri canonical yang sama.
- Admin web sekarang punya halaman alias per channel di `/admin/data/channel/<channel_id>/aliases`, tombol rebuild alias, serta endpoint `GET /api/channels/search` untuk mencari channel berdasarkan `UC...`, `@handle`, alias, atau nama tampilan.
- Halaman detail video `/video/<video_id>` sekarang bypass cache jika request membawa `manual_transcript_job` atau `manual_transcript_message`, supaya sukses manual-download tidak terjebak polling/reload berulang dari HTML lama.
- Contoh video yang memang tampak source-side gagal penuh setelah semua fallback: `oG0TlBzmVGU`, `El6B2D-JTvM`; contoh video yang dulu false-fail karena bug SaveSubs tapi API sukses sesudahnya: `5fnnRhq3HsQ`, `NlYkz6zsN1A`, `BX_sIPWZQ0A`.
- Smoke tambahan pada `OefC5OGyrvM`, `s418LF-9oW4`, dan `9rEqVgmrMBc` sekarang semuanya `ok` lewat API langsung; ini menegaskan bahwa beberapa item yang sebelumnya tampak seperti hard block memang hanya tertahan oleh urutan fallback lama.
- Default discovery wrapper sekarang diarahkan ke `latest-only` + `rate-limit-safe` agar tidak full-history tanpa sengaja.
- Orchestrator utama sekarang ada di `run_pipeline.sh` dengan mode `--discovery-only`, `--transcript-only`, `--resume-only`, dan `--format-only`.
- Discovery auto sekarang memisahkan channel backlog ke `scan-all-missing` dan channel bersih ke `latest-only`.
- Repair source channel sudah dilakukan untuk `Fuel of Legends`, `Topi Merah`, `Jeda Jajan`, dan `BINCANG FINANSIAL` dengan source `/videos`.
- Utility repair baru `repair_channel_video_sources.py` sudah tersedia untuk scan/apply batch pada channel yang kena pseudo entry dari handle root.
- `scripts/transcript.sh --rate-limit-safe` sekarang membatasi worker paralel ke `2` dan menghentikan batch lebih cepat saat hard block berulang.
- Webshare proxy sekarang diperlakukan sebagai fallback terakhir untuk transcript, bukan jalur utama.
- Jika fallback non-paid terlalu sering pada job yang sama, recoverer sekarang bisa menaikkan Webshare lebih awal sebagai mode sementara; daftar proxy juga di-cache dan dirotasi per video.
- Discovery keyword search tersedia lagi via `youtube_search_util.py` dan `scripts/search.sh`.
- Jalur compat lama `partial_py/youtube_transcript_complete.py` juga sudah disamakan: direct/non-paid dulu, Webshare hanya last-resort.
- `scripts/transcript.sh` sekarang mendukung `--workers N` untuk sharding paralel; Webshare tetap fallback terakhir per worker.
- Jalur manual web untuk transcript sekarang memakai `scripts/manual_transcript_then_resume_format.sh`, sehingga tombol manual download tidak perlu ditekan dua kali dan setelah transcript sukses pipeline lanjut ke resume lalu format otomatis.
- Search FTS sudah dipindah ke `videos_search_cache` + `videos_search_fts` dan dibackfill dari blob-first readers lewat `scripts/migrate_search_cache.py`; legacy `videos_fts` sudah dihapus dari DB aktif.
- Constraint operator: pertanyaan status di tengah proses tidak boleh ditafsirkan sebagai perintah menghentikan job background. Cek status, laporkan snapshot, lalu biarkan job tetap berjalan kecuali user eksplisit meminta stop atau ada alasan fatal yang jelas.
- Coordinator produksi untuk repo `YOUTUBE` harus dibaca dari `YT_PROVIDER_COORDINATOR_URL`; jangan hardcode host coordinator di docs atau runtime.
- Coordinator `acquire lease` sekarang harus menjadi satu-satunya jalur distribusi credential worker: bundle lease wajib sudah berisi `api_key` plaintext + `usage_method` + `endpoint_url` + `extra_headers`.
- Bundle lease sekarang juga membawa `model_limits` dari katalog `provider_model_limits`, supaya sizing/chunking worker tidak perlu lookup tambahan.
- Smoke test kecil `smoke_test_coordinator.py` sudah divalidasi ke live coordinator: status/accounts -> acquire lease -> release berhasil pada `nvidia/openai/gpt-oss-120b`.
- Preset smoke test `smoke_test_coordinator.py --preset all` juga sudah lolos untuk NVIDIA, Groq (`moonshotai/kimi-k2-instruct`), dan Cerebras (`qwen-3-235b-a22b-instruct-2507`).
- Akun provider `clod` sudah ditambahkan ke server sebagai `clod 1 | harry`, tetapi request CLōD yang persis seperti dokumentasi resmi tetap balas `403 / error code 1010` untuk `DeepSeek V3`, `GPT OSS 120B`, `Free GPT OSS 120B`, `Trinity Mini`, `Free Trinity Mini`, dan `Arcee Free Trinity Mini`, jadi akun tetap `inactive` sampai akses key-nya dikonfirmasi.
- Coordinator admin test helper untuk CLōD sekarang memakai `max_completion_tokens` agar mengikuti sample resmi, dan `provider_account_models` untuk akun `clod` sudah dipulihkan ke default `GPT OSS 120B` supaya state legacy tetap konsisten.
- Helper `coordinator_status_accounts()` sekarang URL-encode `model_name` dan mengirim `include_inactive` sebagai `1/0`, supaya model dengan spasi seperti `DeepSeek V3` bisa dipakai sebagai filter.
- Endpoint `GET /v1/accounts/{id}/api-key` dianggap legacy/debug-only dan tidak boleh jadi jalur worker normal.
- Admin UI sekarang punya tabel `admin_jobs` yang persisten; job background dari Flask/wrapper tidak lagi hanya bergantung pada `_bg_jobs` in-memory.
- Log job CLI sekarang ditulis ke `logs/<job_id>.log` dan bisa dibuka dari `/admin/data/jobs/<job_id>/log`, jadi admin data tidak lagi bergantung pada daftar file legacy saja.
- Selection account di coordinator sekarang harus berputar antar akun idle (`least recently used`), bukan terus menempel ke satu akun.
- Kebijakan lease terbaru: default TTL `300` detik, inactivity `300` detik berarti expired/release, dan worker panjang sebaiknya heartbeat sekitar `TTL/3`.
- Provider dengan reset harian seperti Groq harus dipulihkan oleh server melalui cleanup block expired, bukan menunggu clear manual.
- Struktur repo sudah dirapikan: script parsial/legacy/channel-specific/migration dipindah ke `partial_py/`, sedangkan root hanya menyisakan prosedur global.
- Dokumen root juga sudah dipersempit: `.md` yang parsial/legacy/setup/migration/arsip dipindah ke `partial_docs/`, sedangkan root hanya menyisakan dokumen acuan utama.
- Helper tunnel coordinator tidak lagi dianggap jalur utama dan sudah dipindah ke `partial_ops/`.
- Kebijakan resume yang harus dipegang: semua akun aktif yang cocok model harus dipakai; `Groq` jalan paralel selama quota masih ada; `NVIDIA` menjadi fallback paling stabil; sisa task `Groq` yang berhenti karena quota harus masuk queue `NVIDIA`, bukan hilang.
- Saat pool resume penuh sementara, worker harus menunggu lease tersedia lagi; jangan fail cepat hanya karena `No accounts for provider/model`.
- `parkerprompts` ditandai `scan_enabled = 0` di table `channel_runtime_state` pada `db/youtube_transcripts.db`.
- Alasan skip permanen: sumber YouTube sudah banned / unavailable (`source_status = source_banned_404`).
- Repair targeted untuk item incomplete pasca shutdown lokal sudah selesai untuk `BPSDMJATIMTV / 39J_SPg_TAY`.
- Sisa row tanpa resume yang masih ada hanyalah item `transcript_language = 'no_subtitle'`, bukan backlog resume normal.
- Batch `discovery-only` untuk cek video terbaru seluruh channel dipisahkan dari transcript/resume agar objective tetap sempit dan hasilnya bisa diaudit.
- Discovery-only sekarang juga meng-*insert* video baru ke DB, jadi halaman `/` dan `/videos` langsung berubah setelah discovery tanpa perlu menjalankan transcript/resume.
- Default discovery UI sekarang diarahkan ke `scan-all-missing` / full history, dan `--latest-only` tetap ada sebagai mode cek cepat yang harus dipilih eksplisit.
- Full-history discovery memakai timeout fetch yang lebih longgar supaya channel besar tidak gampang terpotong oleh crawl yt-dlp yang lambat.
- Tambah channel dari admin UI sekarang langsung memicu pipeline per-channel: discovery → import → transcript → resume → format.
- Tambah multiple channel dari admin UI juga langsung menjadwalkan pipeline per channel setelah insert berhasil.
- Admin summary `Ada Resume` sekarang dihitung hanya untuk row yang benar-benar punya transcript aktif, supaya tidak lebih besar dari `Ada Transkrip` karena row stale/orphan.
- Admin summary `Sudah Terformat` sekarang menghitung `transcript_formatted_path` **atau** `link_file_formatted`, lalu state DB bisa disinkronkan ulang lewat `scripts/repair_db_state.py`.
- Homepage `Latest Videos` sekarang menampilkan video terbaru per channel, bukan global recent list.
- Audit smoke menemukan dua jalur yang sebelumnya bermasalah dan sudah diperbaiki:
  - coordinator worker format sekarang memprioritaskan `YT_PROVIDER_COORDINATOR_URL` dari repo `.env` sehingga tidak lagi jatuh ke host hardcoded lama
  - `format_transcripts_pool.py` kini tetap menulis ke SQLite saat dijalankan dengan `--tasks-csv`, jadi `transcript_formatted_path` dan `link_file_formatted` benar-benar persisten
- `database_optimized.py` sekarang membaca `transcript_formatted_path` yang tersimpan di DB dulu sebelum fallback legacy, sehingga endpoint `/api/formatted/<video_id>` kembali bisa membaca file yang sudah diformat.
- Run `discovery_latest_channels_20260327_full` selesai: 42 channel discan, `140` video `new`, `24` channel `no_actionable`, `1` channel `channel_skipped` (`parkerprompts`).
- Report batch discovery terbaru: `runs/discovery_latest_channels_20260327_full/report.csv`.
- Import hasil discovery selesai: `128` video baru benar-benar diinsert, `12` video ternyata sudah ada di DB. Report: `runs/import_new_videos_20260327_01/import_report.csv`.
- Batch transcript untuk target discovery selesai: `136` target berakhir `transcript_ok`, `4` target `no_subtitle`, `0` fatal error. Report: `runs/transcript_new_videos_20260327_full/recover_report.csv`.
- Smoke resume `NVIDIA-first` selesai `3/3 ok` pada `Sekilas_AjaID`. Report: `runs/resume_new_videos_20260327_smoke/report.csv`.
- Batch resume penuh untuk target transcript baru sedang berjalan pada `runs/resume_new_videos_20260327_full/` dengan task sisa `121` item dan rotasi `NVIDIA` dulu lalu `Groq` fallback.
- Audit `latest 50` pada `2026-03-28` membuktikan backlog baru di luar batch `latest 12`: `451 new`, `27 retry_incomplete`, `20 no_actionable`, `1 channel_skipped`. Report: `runs/discovery_latest50_20260328_full/report.csv`.
- Import backlog `latest 50` selesai: `401` row benar-benar diinsert, `50` sudah ada. Report: `runs/import_latest50_20260328_01/import_report.csv`.
- Batch transcript backlog `latest 50` sedang berjalan pada `runs/transcript_latest50_20260328_01/` untuk `478` actionable item.
- `update_latest_channel_videos.py` sekarang punya mode `--scan-all-missing` untuk scan seluruh riwayat channel dan menangkap semua `missing_in_db` / `retry_incomplete`, bukan hanya jendela `latest N`.
- Smoke test `--scan-all-missing --discovery-only` untuk 5 channel selesai pada `runs/discovery_fullhistory_smoke_20260328_05/`: `896` actionable row dari full history (`735 new`, `159 retry_incomplete`, `2 no_actionable`), semuanya bertanda `scan_scope=full_history`.
- Audit root Python dirapikan lagi: `youtube_transcript.py` dan `youtube_transcript_complete.py` dipindah ke `partial_py/`; `manage_database.py` sekarang mengimpor `partial_py.youtube_transcript_complete`.
- Root repo sekarang menyisakan 12 file Python inti/global.
- Smoke compare formatting transcript selesai pada `runs/format_compare_20260328_120607/` untuk `nvidia/openai/gpt-oss-120b` vs `z.ai/glm-4.7`.
- Temuan kualitas awal: `gpt-oss-120b` lebih dekat ke transcript sumber dan lebih stabil untuk tugas formatting; `z.ai/glm-4.7` cenderung drift ke terjemahan/rewrite yang lebih agresif.
- Utility pembanding formatting disimpan di `partial_py/compare_format_models.py` karena masih bersifat eksperimen terukur, belum jalur global.
- Endpoint status coordinator sekarang sudah sinkron dengan acquire lease:
  - `GET /v1/status/accounts` mengembalikan `leaseable`, `lease_block_reason`, `raw_state`, `model_registered`, dan `model_is_deprecated`.
  - Preflight worker harus membaca `leaseable`, bukan sekadar `state=idle`.
- Matrix compare formatting yang lebih lengkap selesai pada `runs/format_compare_20260328_124131/`.
- Catatan penting: ranking formatting lama yang hanya menyebut `z.ai` dan `mistral-small` sudah superseded oleh matrix compare setelah coordinator runtime dibetulkan ke `provider_models`.
- Coordinator runtime sudah dipatch lagi agar eligibility model mengikuti katalog `provider_models` level provider, bukan `provider_account_models` lama.
- Setelah patch itu, kandidat Groq yang sekarang leaseable meliputi:
  - `llama-3.3-70b-versatile`
  - `meta-llama/llama-4-scout-17b-16e-instruct`
  - `moonshotai/kimi-k2-instruct`
  - `moonshotai/kimi-k2-instruct-0905`
  - `openai/gpt-oss-20b`
  - `qwen/qwen3-32b`
- Kandidat Cerebras yang leaseable saat ini:
  - `qwen-3-235b-a22b-instruct-2507`
  - `llama3.1-8b`
- Smoke compare lanjutan untuk kandidat Groq/Cerebras ada di `runs/format_compare_20260328_groq_cerebras_matrix_v2/`.
- Hasil compare lanjutan Groq/Cerebras:
  - `groq/llama-3.3-70b-versatile`: usable, tetapi cenderung menerjemahkan transcript English ke Bahasa Indonesia.
  - `groq/meta-llama/llama-4-scout-17b-16e-instruct`: usable, cepat, tetapi juga cenderung menerjemahkan transcript English.
  - `groq/moonshotai/kimi-k2-instruct`: usable dan paling menjanjikan dari kandidat Groq baru; struktur rapi dan tetap dekat ke sumber.
  - `groq/moonshotai/kimi-k2-instruct-0905`: usable, tetapi terlalu padat/lebih ringkas dari baseline.
  - `groq/openai/gpt-oss-20b`: usable, tetapi lebih lambat dan lebih pendek dari baseline `gpt-oss-120b`.
  - `groq/qwen/qwen3-32b`: bisa jalan pada transcript pendek, tetapi tidak stabil untuk transcript lebih besar (`413 Request too large` / TPM), jadi bukan jalur utama.
  - `cerebras/qwen-3-235b-a22b-instruct-2507`: usable, cepat, dan hasil cukup baik; layak jadi opsi cadangan yang kuat.
- `cerebras/llama3.1-8b`: usable, tetapi lebih lemah dan lebih ringkas daripada `cerebras qwen`.
- Coordinator sekarang punya tabel `provider_model_limits` sebagai katalog batas operasional per `provider:model`.
- Bundle `acquire lease` sekarang sudah membawa `model_limits`, jadi worker tidak perlu request tambahan untuk sizing/chunking saat task aktif.
- Jalur resume global (`update_latest_channel_videos.py`) dan worker `fill_missing_resumes_youtube_db.py` sekarang membaca `model_limits` untuk:
  - `chunk_chars`
  - `chunk_max_tokens`
  - `chunk_retry_tokens`
  - budget single-pass/final-pass
- Seed limits awal yang sudah dimasukkan:
  - `nvidia/openai/gpt-oss-120b`
  - `groq/openai/gpt-oss-20b`
  - `groq/qwen/qwen3-32b`
  - `groq/llama-3.3-70b-versatile`
  - `groq/meta-llama/llama-4-scout-17b-16e-instruct`
  - `groq/moonshotai/kimi-k2-instruct`
  - `groq/moonshotai/kimi-k2-instruct-0905`
  - `cerebras/llama3.1-8b`
  - `cerebras/qwen-3-235b-a22b-instruct-2507`
  - `z.ai/glm-4.7`
- Smoke adaptive sizing:
  - `groq/qwen/qwen3-32b` sekarang lolos pada sampel `8pau0LqikL8` tanpa `413`; report di `runs/format_compare_20260328_adaptive_smoke/report.csv`.
  - `cerebras/llama3.1-8b` juga lolos dua sampel; report di `runs/format_compare_20260328_adaptive_cerebras_only/report.csv`.
- Catatan kualitas:
  - adaptive sizing memperbaiki kelayakan request,
  - tetapi `groq/qwen/qwen3-32b` masih bisa bocor `<think>`; validator compare sekarang menolak `reasoning_leak`.
- Urutan operasional formatting yang harus dipakai sekarang:
  - baseline / parameter kebenaran: `nvidia/openai/gpt-oss-120b`
  - fallback non-NVIDIA utama: `groq/moonshotai/kimi-k2-instruct`
  - fallback non-NVIDIA berikutnya: `cerebras/qwen-3-235b-a22b-instruct-2507`
  - fallback yang masih bisa diterima tetapi lebih agresif rewrite: `z.ai/glm-4.7`
  - usable tapi bukan jalur utama: `nvidia/mistralai/mistral-small-24b-instruct`, `groq/openai/gpt-oss-20b`, `cerebras/llama3.1-8b`
  - jangan dipakai saat ini:
    - `gemini/gemini-2.5-flash`
    - `groq/openai/gpt-oss-120b`
    - `groq/qwen/qwen3-32b`
    - `groq/llama-3.3-70b-versatile` untuk transcript English
    - `groq/meta-llama/llama-4-scout-17b-16e-instruct` untuk transcript English

- **2026-04-17 (Local)**: Meluncurkan pipeline penuh untuk seluruh channel (video terbaru).
    - Memperbaiki script `discover.sh`, `format.sh`, `transcript.sh`, dan `resume.sh` agar menggunakan `get_venv.sh` dan path REPO_DIR yang benar pada mesin lokal.
    - Batch discovery `latest-only` seluruh channel sedang berjalan: `runs/discovery_latest_channels_20260417_033754_4776/` (46 channel).
    - Batch formatting seluruh transcript pending sedang berjalan: `runs/format_20260417_033836_5502/` (4827 target).

- **2026-05-09 (Local + tafsir-server)**: Menambahkan delta sync tool `scripts/sync_missing_rows_to_server.py` + wrapper `scripts/sync_missing_rows_to_server.sh`.
  - Tool ini membandingkan key unik lokal vs server, lalu hanya mengirim row yang belum ada di server untuk `channels`, `videos`, `video_asr_chunks`, `channels_meta`, `channel_runtime_state`, dan `content_blobs`.
  - Bundle juga membawa file `uploads/...` yang direferensikan oleh video baru, tanpa menyalin database full.
  - Sync real ke `tafsir-server` sudah selesai dan melewatkan 638 file upload referensi serta delta row yang belum ada di server.

- **2026-05-09 (tafsir-server)**: ASR server untuk backlog `no_subtitle` sudah dijalankan.
  - Run dir: `runs/asr_server_no_subtitle_20260509_163725`
  - Mode: `--video-workers 2 --providers groq,nvidia`
  - Post-process GPT OSS tetap off.

- **2026-05-14 (Local)**: Memperbaiki navigasi legacy video detail agar tidak berhenti di video yang `upload_date`-nya kosong.
  - `get_adjacent_videos_by_video_id()` sekarang memakai urutan penuh `upload_date DESC -> created_at DESC -> id DESC` dengan fallback untuk `NULL`/string kosong.
  - Tombol `Prev` di halaman legacy sekarang lanjut ke video yang lebih lama, jadi channel dengan ribuan video tidak terputus setelah grup kecil yang punya `upload_date`.
  - Kasus yang dicek manual: `6TAIG7usqEU` di `FirandaAndirjaOfficial`.

- **2026-05-14 (Local)**: Menambahkan rank eksplisit per channel untuk legacy navigation.
  - Kolom SQLite `videos.channel_rank` dibackfill untuk `55,003` video.
  - Rank `1` berarti video visible paling baru di channel.
  - Navigasi legacy dan daftar video channel sekarang memakai rank itu dulu, lalu fallback ke urutan timestamp jika rank belum ada.
  - Skrip repair yang dipakai: [repair_channel_ranks.py](/media/harry/DATA120B/GIT/YOUTUBE/repair_channel_ranks.py)

- **2026-05-14 (Local)**: Menyetel ulang fallback ASR agar tidak membuang waktu saat provider bermasalah.
  - Groq yang kena `ASPH 429` sekarang masuk cooldown lebih lama untuk sisa run, bukan diulang cepat pada chunk/video berikutnya.
  - NVIDIA yang memunculkan error TLS/SSL sekarang dimatikan untuk sisa run supaya tidak mengulang request yang pasti gagal.
  - Jika semua provider sedang cooldown/disabled, target langsung ditandai `retry_later` tanpa download audio lagi.

- **2026-05-14 (Local)**: Menambahkan preflight lease untuk ASR supaya batch tidak download audio kalau lease kosong.
  - `recover_asr_transcripts.py` sekarang mencoba acquire lease sebelum `yt-dlp` download audio.
  - Kalau lease coordinator tidak menyediakan slot Groq/NVIDIA, video langsung ditandai `retry_later`.
  - Ini menghindari pemborosan bandwidth/storage saat coordinator sedang kosong.

- **2026-05-14 (Local)**: Menyetel cooldown Groq `ASPH 429` menjadi 24 jam.
  - Jika Groq mengembalikan `seconds of audio per hour` limit, worker menahan provider itu selama satu hari penuh.
  - Ini mengikuti sifat limit audio per jam, bukan retry cepat beberapa menit.

- **2026-05-14 (Local)**: Mengalihkan NVIDIA Whisper ke jalur Riva gRPC sesuai tutorial resmi NVIDIA.
  - `nvidia-riva-client` dan `grpcio` sudah dipasang di venv.
  - Worker NVIDIA sekarang memakai `grpc.nvcf.nvidia.com:443` dengan metadata `function-id` dan `authorization: Bearer <API_KEY>` dari lease coordinator.
  - Input audio dipakai sebagai WAV PCM mono 16-bit per chunk, lalu dikirim sebagai raw audio bytes ke `offline_recognize`.

- **2026-05-14 (Local)**: Menambahkan preflight akses YouTube sebelum download audio di ASR.
  - Jika yt-dlp mendeteksi `private` atau `members-only`, video langsung ditandai `skip_access_blocked`.
  - Video akses-terblokir tidak lagi download audio, tidak memakan lease provider, dan batch lanjut ke item berikutnya.
  - Jika file audio cached sudah ada di `runs/.../source/`, ASR memakai file itu langsung dan melewati probe akses.
  - Cache audio sekarang dibaca dari shared run root dan worker source lama, jadi worker lain bisa reuse file yang sama tanpa probe ulang.

- **2026-05-14 (Local)**: Mengetatkan selector audio yt-dlp untuk ASR.
  - Download audio sekarang memakai selector bitrate bertingkat `ba[abr<=96]/ba[abr<=128]/ba[abr<=160]/ba/b`.
  - Tujuannya agar file yang ditarik tidak terlalu besar, tapi tetap ada fallback kalau bitrate rendah tidak tersedia.

- **2026-05-14 (Local)**: Diet database untuk metadata dan formatted path.
  - `videos.metadata` dibackfill ke blob `metadata` dan kolom lama dikosongkan untuk row yang sudah dimigrasi.
  - Writer aktif sekarang menyimpan `metadata` ke blob dan tidak lagi bergantung pada salinan teks besar di kolom utama.
  - `transcript_formatted_path` menjadi sumber tulis canonical; `link_file_formatted` tetap tersedia sebagai fallback baca legacy.
  - Tabel legacy `transcripts` dan `summaries` yang kosong sudah di-drop dari schema aktif.

- **2026-05-14 (Local)**: Menahan diet `transcript_text` / `summary_text` karena FTS masih bergantung ke kolom itu.
  - Audit menunjukkan trigger `videos_fts` masih membaca `transcript_text` dan `summary_text` secara langsung.
  - Jalur baca runtime sudah blob-first, dan write baru untuk transcript/resume ikut mirror ke blob.
  - Kolom teks lama belum dikosongkan supaya search FTS tetap aman sampai redesign indeks dilakukan.

- **2026-05-14 (Local)**: Menghapus kolom formatted legacy yang sudah benar-benar redundant.
  - `link_file_formatted` sudah tidak dipakai aktif lagi, dan kolom fisiknya di `videos` sudah di-drop.
  - `transcript_formatted_path` sekarang menjadi satu-satunya kolom formatted path yang canonical.
  - Runtime/monitor/sync path yang tersisa sudah membaca `transcript_formatted_path` langsung.

- **2026-05-14 (Local)**: Backfill blob untuk transcript/resume agar jalur baca blob-first lengkap.
  - `transcript_text` dan `summary_text` yang masih dipertahankan untuk FTS sudah disalin ke blob storage sebagai mirror.
  - Jalur baca runtime tetap mengambil dari blob dulu; kolom teks lama hanya dipertahankan untuk trigger/search FTS.

## Batch Report
| Batch | Date | Limit | Success | Failed | Status |
|---|---|---|---|---|---|
| Phase 1 | 2026-03-27 | 5 | 0 | 5 | Done |
| Phase 2 | 2026-03-27 | 50 | 0 | 50 | Done (Confirmed no subtitles exist for these) |
| Phase 4 | 2026-03-27 | 5000 | - | - | **Restarting with logic fix** (Distinguish NoSubtitle vs FatalError) |
- **2026-05-14 (Local)**: Rediscovery channel kecil yang sempat nol video.
  - `Ancestral Yields` naik ke `109` video.
  - `Backyard Bill` naik ke `57` video setelah rerun tunggal; error trigger yang muncul hanya efek run paralel sebelumnya.
  - `Nostalgia Technologi` naik ke `37` video.

- **2026-05-14 (Local)**: Audit ulang channel kecil di DB.
  - `@AncestralYields` tetap `109` video setelah rerun ulang.
  - `@nalarlambat` tetap `5` video setelah rerun ulang.
  - `CozyPedia-93` tetap `7` video setelah rerun ulang.
  - `@BackyardBill_YT` naik dari `9` ke `66` video, jadi sebelumnya belum terdiscovery penuh.

- **2026-05-14 (Local)**: Membersihkan transcript palsu dari SaveSubs Cloudflare HTML.
  - 503 row di `videos.transcript_text` berisi halaman `Access denied | savesubs.com used Cloudflare to restrict access`.
  - Root cause: SaveSubs lama mengembalikan HTML error, lalu hasilnya ikut disimpan sebagai transcript mentah.
  - Sudah dipasang guard baru di `savesubs_playwright.py`, `partial_py/savesubs_direct.py`, dan `recover_transcripts.py`.
  - Sudah dijalankan repair script untuk menghapus transcript palsu dari DB, blob transcript, dan file fisiknya.

- **2026-05-14 (Local)**: Membersihkan salinan fisik transcript yang sudah redundant.
  - Folder `uploads/ilmulidi/text/` berisi 110 file dan semuanya sudah ada di DB/blob.
  - `transcript_file_path` untuk row terkait sudah dikosongkan setelah file fisiknya dihapus.
  - Cleanup lanjutan menghapus 7,473 row/path yang sudah punya blob transcript dan menghapus 3,252 file fisik redundan.

- **2026-05-14 (Local)**: Membersihkan summary fisik yang sudah redundant.
  - Satu-satunya file summary fisik yang tersisa sudah dihapus.
  - `summary_file_path` legacy dipertahankan sebagai penanda status karena worker resume masih memakainya untuk seleksi backlog.
  - Rencana migrasi FTS untuk melepaskan `transcript_text` dan `summary_text` dipisahkan ke [FTS_MIGRATION_PLAN.md](/media/harry/DATA120B/GIT/YOUTUBE/docs/FTS_MIGRATION_PLAN.md).

- **2026-05-14 (Local)**: Audit sisa file transcript fisik.
  - Dari 4,007 file transcript yang tersisa di `uploads/*/text/`, 4,006 belum punya blob transcript sehingga dipertahankan.
  - Satu file Oasis (`xvhVFQtfiwA`) sudah punya `transcript_text` di DB tetapi belum punya blob; sudah dipindah ke blob lalu file fisiknya dihapus.

- **2026-05-14 (Local)**: Backfill final transcript fisik ke blob.
  - 3,919 file transcript aktif dibackfill ke blob `transcript`, lalu file fisiknya dihapus setelah commit batch.
  - 88 orphan file transcript yang sudah tidak punya `transcript_file_path` di DB juga dibackfill dari disk, lalu dihapus.
  - Folder `uploads/*/text/` sekarang kosong.

- **2026-05-14 (Local)**: Resume NVIDIA dibuat fail-fast.
  - Jalur `fill_missing_resumes_youtube_db.py` sekarang memberi timeout khusus yang lebih pendek untuk NVIDIA daripada timeout generasi default.
  - Retry internal OpenAI client untuk NVIDIA dimatikan agar request tidak macet terlalu lama di `chat.completions.create()`.
  - Jalur NVIDIA sekarang memakai streaming chat completion ala contoh resmi, lalu mengumpulkan `delta.content` menjadi hasil final.
  - Kalau NVIDIA kena timeout-like error, provider itu ditandai disabled untuk sisa run supaya worker pindah ke backlog lain alih-alih mengulang macet yang sama.

- **2026-05-14 (Local)**: Resume launcher tidak lagi abort saat status coordinator timeout.
  - `launch_resume_queue.py` sekarang retry lookup status coordinator secara singkat sebelum menyerah.
  - Jika status pool tetap tidak bisa dibaca, launcher turun ke direct NVIDIA fallback worker alih-alih menghentikan seluruh run.

- **2026-05-14 (Local)**: Work-conserving daemon dispatch path diperbaiki.
  - Indentasi lock/dispatch di `orchestrator/daemon.py` sudah dibetulkan supaya `RUN` branch benar-benar acquire lock, dispatch job, dan release lock dalam satu blok yang sama.
  - Duplikasi mode `explain` dihapus; `explain` sekarang memakai inventory snapshot langsung.
  - Stage status report kini menampilkan `Audio Download` sebagai stage YouTube-limited yang terpisah.

- **2026-05-14 (Local)**: Dispatch runner hardening ditambahkan.
  - `run_once()` sekarang punya fallback `result` saat `dispatch_job()` melempar exception sebelum return.
  - Lock tetap dilepas di `finally`, dan exception dispatch dicatat sebagai failure biasa agar cycle tidak mati mendadak.
