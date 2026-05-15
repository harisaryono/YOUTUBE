# VERIFICATION STANDARDS

## Objective
Verify that transcripts are correctly downloaded, formatted, and stored in the database.

## Standards
- Table `videos` must have `transcript_downloaded = 1`.
- `transcript_path` must exist and point to a valid `.txt` file in `uploads/<channel_id>/text/`.
- File content must have timestamps `[HH:MM:SS.000]`.
- Unrecoverable videos must be marked `transcript_language = 'no_subtitle'`.
- Permanently banned / unavailable channels may be excluded from normal scans via `channel_runtime_state.scan_enabled = 0`.
- For banned / unavailable channels, verification should confirm `skip_reason` and `source_status` are persisted in `channel_runtime_state`.
- Transcript scraper wajib punya pacing internal (minimal delay antar video) supaya caller seperti `update_latest_channel_videos.py` tidak meng-hammer YouTube tanpa sengaja. Konfigurasi via env: `YT_TRANSCRIPT_INTER_VIDEO_DELAY_MIN/MAX` dan backoff `YT_TRANSCRIPT_BACKOFF_*`.
- Webshare proxy hanya boleh dipakai sebagai fallback terakhir ketika semua jalur direct/non-paid gagal.
- Jika fallback non-paid terlalu sering, recoverer boleh menaikkan Webshare lebih awal untuk job yang sama; proxy list harus di-cache dan dirotasi per video.
- `scripts/discover.sh` default sekarang harus berada di mode lebih aman: latest-only + rate-limit-safe, kecuali `--scan-all-missing` dipilih eksplisit.
- Channel yang belum punya `full_history_scanned_at` harus diprioritaskan ke `scan-all-missing` dulu, lalu masuk `latest-only` setelah full crawl pertama selesai.
- `scripts/transcript.sh --rate-limit-safe` harus membatasi worker paralel ke maksimum `2` dan tidak meneruskan batch yang sudah kena hard block berulang.
- Jalur compat `partial_py/youtube_transcript_complete.py` juga harus mengikuti urutan yang sama: direct -> yt-dlp -> Webshare last.
- `scripts/transcript.sh --workers N` harus membuat shard CSV per worker, menjalankan worker paralel, dan menggabungkan report hasilnya ke run dir utama.
- `scripts/transcript.sh --rate-limit-safe` harus menyalakan pacing, membatasi worker, dan memotong fallback mahal sebelum `yt-dlp`/Webshare.
- `scripts/discover.sh --rate-limit-safe` harus menahan channel pacing dan melewati lookup `upload_date` tambahan yang mahal.
- `scripts/asr.sh` harus bisa menjalankan batch ASR chunked untuk `--video-id`, `--channel-id`, atau `--csv` tanpa mencampur logika subtitle lama.
- `recover_asr_transcripts.py` harus menulis chunk state ke tabel `video_asr_chunks` per provider/model/chunk index agar batch bisa resume.
- `scripts/audio.sh` / `scripts/audio_download.sh` harus menulis status local audio ke tabel `video_audio_assets`.
- `scripts/asr.sh --local-audio-only` harus membaca audio lokal dari `video_audio_assets.audio_file_path` dan tidak lagi memanggil yt-dlp untuk download.
- `orchestrator/daemon.py` harus menjaga lock/dispatch tetap satu blok saat `decision.verdict == "RUN"`, tanpa indentasi yang membuat dispatch unreachable.
- `./scripts/orchestrator.sh explain` harus menampilkan inventory snapshot, stage decisions ringkas, dan stage `Audio Download` di status report.
- `./scripts/preflight_orchestrator.sh` harus memeriksa schema utama, wrapper stage, audio dir, yt-dlp, dan coordinator opsional tanpa mengubah state kerja normal.
- `./scripts/orchestrator_ctl.sh pause <target>` harus membuat safety gate menahan target tersebut, dan `resume` harus melepasnya lagi.
- `orchestrator_inventory_snapshots` harus bertambah setiap cycle report agar trend backlog bisa diaudit.
- `adaptive_batch_limit:<stage>` harus berubah dari cycle ke cycle ketika stage sukses berulang atau kena blok.
- `./scripts/orchestrator_ctl.sh janitor` harus bisa menjalankan maintenance ringan tanpa error, dan cycle runtime boleh memanggil janitor periodik lewat `janitor_last_run_at`.
- Final ASR sukses harus menulis `videos.transcript_text`, `videos.transcript_file_path`, `transcript_downloaded = 1`, dan file `.txt` final di `uploads/asr/<video_id>/`.
- Final ASR juga harus menyimpan `transcript_raw.txt` sebagai jejak timestamp mentah, lalu memakai GPT OSS 120B hanya untuk transcript yang masih kecil; transcript panjang boleh otomatis dilewati dan langsung pakai raw timestamped output.
- Default ASR sekarang harus menyimpan transcript raw timestamped tanpa GPT OSS post-process, kecuali `--postprocess` dipilih eksplisit.
- `recover_asr_transcripts.py --video-workers 2` harus mampu membagi 2 video ke worker subprocess terpisah, lalu menggabungkan report run utama kembali ke satu `recover_asr_report.csv`.
- Batch ASR harus menghasilkan `recover_asr_report.csv` di run dir utama dan menyimpan `tasks.csv` sebagai target snapshot.
- Jika satu chunk ASR gagal pada semua provider, job harus berhenti pada video itu, menyimpan state chunk yang sudah sukses, dan menandai retry later.
- ASR worker harus acquire lease dari coordinator, memakai `api_key` plaintext dari bundle lease, lalu release lease setelah selesai.
- Smoke 1 video `no_subtitle` dianggap lolos jika `recover_asr_report.csv` menunjukkan `status=done`, `transcript_downloaded=1`, dan transcript final tersimpan di `uploads/asr/<video_id>/transcript.txt`.
- Smoke 2 video paralel dianggap lolos jika parent report berisi 2 row `done`, `postprocess_status=disabled`, dan masing-masing transcript final tersimpan di DB + disk.
- Untuk bahasa campur Indo/English/Arabic, `language=multi` harus diperlakukan sebagai auto-detect dan tidak dikirim mentah sebagai `language=multi` ke Groq.
- Outcome `blocked` harus dicatat sebagai hard block/member-only, bukan jatuh ke `no_subtitle` tanpa penanda.
- Batch discovery/transcript harus berhenti lebih awal setelah hard block berturut-turut melewati threshold operasional.
- `youtube_search_util.py` / `scripts/search.sh` harus bisa menampilkan hasil discovery keyword dari `yt-dlp` tanpa error sintaks.

## Stage 8 Control Plane Validation

- [ ] `python -m compileall orchestrator` berhasil.
- [ ] `bash -n scripts/orchestrator.sh` berhasil.
- [ ] `./scripts/orchestrator.sh validate` berhasil.
- [ ] `./scripts/orchestrator.sh explain` menampilkan inventory tanpa exception.
- [ ] `./scripts/orchestrator.sh once --dry-run --max-jobs 7` tidak meluncurkan job nyata.
- [ ] `scripts/orchestrator.sh run` menyimpan PID daemon Python.
- [ ] `scripts/orchestrator.sh stop` menghentikan daemon dan tidak meninggalkan child process.
- [ ] File `AI_CONTEXT/*.md` terbaca rapi dan tidak collapse menjadi satu baris panjang.

## Stage 9 Observability Validation

- [ ] `./scripts/orchestrator.sh doctor` menampilkan daemon, backlog, cooldown, dan rekomendasi tanpa error.
- [ ] `./scripts/orchestrator.sh doctor --json` mengeluarkan JSON valid.
- [ ] `./scripts/orchestrator.sh doctor` menampilkan backlog per stage dan usage per group.
- [ ] `./scripts/orchestrator.sh doctor` menampilkan recent failures dari event store.
- [ ] `scripts/orchestrator.sh` help text memasukkan `doctor` sebagai control-plane command.

## Stage 10 Web Orchestrator Validation

- [ ] `/admin/orchestrator` dapat dibuka setelah login admin.
- [ ] `/admin/orchestrator?format=json` mengembalikan snapshot JSON yang valid.
- [ ] Tombol `Run Doctor`, `Explain`, `Validate`, `Reconcile`, dan `Once Dry-Run` di dashboard bekerja.
- [ ] Form `Pause` / `Resume` pada dashboard mengubah gate target yang dipilih.
- [ ] Form `Cancel Job`, `Cancel Stage`, dan `Cancel Group` di dashboard memanggil control-plane tanpa error sintaks.
- [ ] Dashboard menampilkan backlog, group usage, stage usage, recent failures, dan recommendations.
- [ ] Link log job pada dashboard membuka log orchestrator yang benar.

## Stage 11 Safe Actions Validation

- [ ] `./scripts/orchestrator.sh pause-stage transcript --minutes 5 --reason test --json` berhasil dan mencatat event control.
- [ ] `./scripts/orchestrator.sh resume-stage transcript --json` menghapus pause stage tadi.
- [ ] `./scripts/orchestrator.sh pause-group youtube --minutes 5 --reason test --json` berhasil dan terlihat di doctor.
- [ ] `./scripts/orchestrator.sh resume-group youtube --json` menghapus pause group tadi.
- [ ] `./scripts/orchestrator.sh quarantine-channel UC_TEST --reason test --json` berhasil dan terlihat di doctor.
- [ ] `./scripts/orchestrator.sh unquarantine-channel UC_TEST --json` menghapus quarantine tadi.
- [ ] `./scripts/orchestrator.sh retry-failed --stage transcript --limit 5 --dry-run --json` mengeluarkan kandidat retry tanpa requeue nyata.
- [ ] `./scripts/orchestrator.sh doctor` menampilkan active pauses, quarantined channels, policy blockers, recent control actions, dan cycle failure summary.
- [ ] `/admin/orchestrator` menampilkan pause/quarantine/control-action snapshot tanpa error.

## Batch Validations
- **Phase 1 Validation**:
  - [ ] 5 videos processed.
  - [ ] Log shows no 403 errors.
  - [ ] DB entries updated.
  - [ ] Files exist on disk.

- **Discovery-only Validation**:
  - [ ] `report.csv` tersedia di run directory.
  - [ ] Setiap channel aktif menghasilkan row `new`, `retry_incomplete`, `no_actionable`, `channel_skipped`, atau `channel_error`.
  - [ ] Tidak ada file transcript/resume baru yang ditulis oleh batch discovery.

- **Full-history Discovery Validation**:
  - [ ] `report.csv` tersedia di run directory.
  - [ ] Kolom `scan_scope` berisi `full_history`.
  - [ ] Kolom `scanned_entries` terisi jumlah entry channel yang benar-benar dipindai.
  - [ ] Row actionable mencakup seluruh `new` dan `retry_incomplete` dari seluruh feed channel yang berhasil diambil, bukan hanya jendela `N` terbaru.

- **Coordinator Validation**:
  - [ ] Batch produksi menampilkan URL coordinator yang benar dari `YT_PROVIDER_COORDINATOR_URL`.
  - [ ] Jangan menganggap `localhost:8788` valid tanpa health check eksplisit.
  - [ ] Default `lease_ttl_seconds` coordinator adalah `300`.
  - [ ] Lease tanpa heartbeat/status update selama `300` detik menjadi expired.
- [ ] `POST /v1/leases/acquire` mengembalikan `api_key` plaintext, bukan ciphertext `ENC:...`.
- [ ] Response acquire juga memuat `usage_method`, `endpoint_url`, dan `extra_headers`.
- [ ] Response acquire juga memuat `model_limits` untuk model yang sedang dipakai.
- [ ] Smoke test coordinator kecil dapat membaca `GET /v1/status/accounts`, acquire lease satu account aktif, lalu release lagi tanpa error.
- [ ] `smoke_test_coordinator.py --preset all` berhasil untuk NVIDIA, Groq, dan Cerebras berurutan.
- [ ] Test koneksi CLōD di coordinator memakai `max_completion_tokens` agar sesuai sample resmi, lalu tetap menolak akun yang benar-benar tidak punya akses model.
- [ ] Acquire berulang untuk provider/model yang sama berputar ke beberapa akun idle, tidak terus kembali ke satu akun yang sama.
- [ ] Server membersihkan block expired provider harian seperti Groq sehingga account tersedia lagi setelah window reset lewat.
- [ ] Resume worker tidak gagal cepat saat semua akun sementara sibuk; acquisition harus menunggu sampai lease tersedia lagi atau timeout yang dikonfigurasi.
- [ ] Admin job tracking persisten di tabel `admin_jobs`, dan `/admin/data` menampilkan job terbaru walau `_bg_jobs` sudah kosong.
- [ ] Job wrapper harus menulis `log_path` nyata ke `logs/<job_id>.log`, dan `/admin/data/jobs/<job_id>/log` harus bisa menampilkan isi log yang sama.
- [ ] `./scripts/orchestrator.sh active` menampilkan job running beserta umur, slot, group, dan status hidup/stale.
- [ ] `./scripts/orchestrator.sh logs --job-id <JOB_ID> --tail 100` menampilkan tail `stdout_stderr.log` dari job yang dipilih.
- [ ] `./scripts/orchestrator.sh cancel --job-id <JOB_ID>` bisa menandai job running sebagai `cancelled` dan melepaskan lock.
- [ ] `./scripts/orchestrator.sh cancel-stage <stage>` dan `cancel-group <group>` hanya menarget job running yang cocok.
- [ ] `./scripts/orchestrator.sh reconcile` menandai job yang PID-nya sudah mati berdasarkan `exit_code.txt` atau log yang tersedia.
- [ ] `./scripts/orchestrator.sh validate` lolos dan memeriksa `parallel`, `timeouts`, serta file konteks `AI_CONTEXT/` yang wajib ada.
- [ ] `./scripts/orchestrator.sh run` menulis PID proses daemon Python ke pidfile, bukan PID shell wrapper.
- [ ] Stage YouTube sensitif (`transcript`, `audio_download`) yang scope/channel sama tidak boleh berjalan paralel jika scope lock masih aktif.
- [ ] Claim per video aktif: row yang sudah di-claim punya `processing_stage` / `processing_owner` / `processing_until`, dan wrapper transcript/resume/format me-release claim setelah job selesai.
- [ ] `timeouts:` di [orchestrator.yaml](/media/harry/DATA120B/GIT/YOUTUBE/orchestrator.yaml) memuat timeout stage dasar untuk discovery, transcript, audio download, resume, ASR, dan format.
- [ ] `timeouts:` juga bisa dioverride lewat `.env` dengan `ORCH_TIMEOUT_*` keys yang relevan.
- [ ] `./scripts/orchestrator.sh active` menampilkan kolom `TIMEOUT` dan `REMAIN` untuk tiap job running.
- [ ] `poll_active_jobs()` menandai job yang melewati timeout stage sebagai `timeout`, melepaskan lock, dan menulis event timeout.
- [ ] discovery hanya ikut cooldown `youtube` global atau `youtube:discovery`; cooldown `youtube:content` tidak boleh memblok discovery.
- [ ] `recover_asr_transcripts.py --help` dan `scripts/asr.sh --help` berjalan tanpa error sintaks.
- [ ] `iyo9VuY5dpg` smoke ASR selesai via lease coordinator dan menulis transcript final ke DB + disk.
- [ ] Transcript panjang yang melewati threshold post-process harus tercatat `postprocess_status=skipped_long` dan tidak memanggil GPT OSS 120B.
- [ ] `scripts/transcript.sh` menerima `--video-id` dan `--channel-id`, lalu membangun `tasks.csv` internal sebelum memanggil `recover_transcripts_from_csv.py`.
- [ ] `scripts/resume.sh` menerima `--video-id` dan `--channel-id`, lalu membangun `tasks.csv` internal sebelum memanggil `launch_resume_queue.py`.
- [ ] `scripts/format.sh` tetap menulis `transcript_formatted_path` dan `link_file_formatted` ke SQLite saat dijalankan dengan `--tasks-csv`.
- [ ] `GET /api/formatted/<video_id>` mengembalikan `200` untuk video yang memang sudah punya formatted transcript di DB.
- [ ] `coordinator_base_url()` memprioritaskan `YT_PROVIDER_COORDINATOR_URL` dari repo `.env` sehingga worker format tidak lagi jatuh ke host coordinator lama.
- [ ] Tambah channel dari admin UI langsung menjadwalkan pipeline per-channel dan menghasilkan run directory berisi stage discovery, transcript, resume, dan format.
- [ ] Tambah multiple channel dari admin UI juga langsung menjadwalkan pipeline per channel setelah insert berhasil.

- **Repo Structure Validation**:
  - [ ] Root repo hanya berisi script global.
  - [ ] `youtube_transcript.py` tidak lagi berada di root; versi compat dasar ada di `partial_py/`.
  - [ ] `partial_py/youtube_transcript_complete.py` berada di `partial_py/` dan dipakai oleh `manage_database.py` lewat import package.
  - [ ] Script parsial/legacy/channel-specific/migration berada di `partial_py/`.
  - [ ] `partial_py/README.md` menjelaskan cara menjalankan script parsial via `python -m partial_py.<nama>`.
  - [ ] Root repo hanya menyisakan `.md` yang masih jadi acuan utama.
  - [ ] Dokumen parsial/legacy/setup/migration/arsip dipindah ke `partial_docs/`.
  - [ ] `partial_docs/README.md` menjelaskan fungsi folder dokumen parsial.
  - [ ] Helper operasional non-utama seperti fallback tunnel berada di `partial_ops/`.

- **Documentation Traceability**:
  - [ ] Semua mode utama `run_pipeline.sh` punya contoh pemakaian di `README.md` atau `DOCS_INDEX.md`.
  - [ ] Perubahan workflow baru wajib dicatat di `PROGRESS.md` sebelum dianggap selesai.
  - [ ] Utility repair atau backfill baru wajib punya halaman khusus atau entri jelas di `DOCS_INDEX.md`.
  - [ ] Link markdown di `README.md`, `PROGRESS.md`, `VERIFY.md`, dan `CHANNEL_SOURCE_REPAIR.md` menunjuk ke path repo aktif, bukan path lama.
  - [ ] `DOCS_INDEX.md` menjadi satu pintu masuk utama untuk mencari file acuan, pipeline, dan utility penting.

- **New Video Transcript Batch Validation**:
  - [ ] `runs/import_new_videos_20260327_01/import_report.csv` tersedia.
  - [ ] `runs/transcript_new_videos_20260327_full/recover_report.csv` tersedia.
  - [ ] Seluruh `140` target discovery berakhir sebagai `transcript_ok` atau `no_subtitle`.
  - [ ] Tidak ada target discovery yang tertinggal dalam state kosong/pending.
  - [ ] Discovery-only tetap meng-insert video baru ke DB sehingga halaman homepage `/` dan `/videos` ikut terbarui.
  - [ ] `Ada Resume` di admin summary tidak boleh melebihi `Ada Transkrip`; row stale/orphan harus dikecualikan dari hitungan.
  - [ ] `Sudah Terformat` di admin summary menghitung path terformat aktif, dan `scripts/repair_db_state.py` dapat backfill row yang file-nya sudah ada di disk.
  - [ ] Homepage menampilkan video terbaru per channel, bukan daftar global yang bisa bias ke channel yang lebih aktif.

- **Formatted Transcript Compare Validation**:
  - [ ] Smoke compare dijalankan via `partial_py/compare_format_models.py`.
  - [ ] Semua provider/model candidate wajib lewat preflight coordinator sebelum acquire lease.
  - [ ] Preflight membaca `leaseable=true` dari `GET /v1/status/accounts`, bukan hanya `state=idle`.
  - [ ] `lease_block_reason` dipakai untuk menjelaskan kenapa model tidak boleh masuk rotasi.
  - [ ] Eligibility runtime model mengikuti `provider_models` level provider, bukan `provider_account_models` lama.
  - [ ] Bundle lease membawa `model_limits` untuk model yang sedang dipakai.
  - [ ] Worker formatting/resume membaca `model_limits` untuk menentukan chunk/input sizing.
  - [ ] `GET /v1/model-limits` mengembalikan katalog limit operasional yang bisa diaudit ulang.
  - [ ] `413` / `Request too large` tidak lagi diatasi dengan retry ukuran lama; harus memicu split yang lebih kecil.
  - [ ] Output formatting tidak boleh meminta transcript lain, URL lain, atau konteks tambahan.
  - [ ] Untuk transcript berbahasa Inggris, hasil formatting tidak boleh diam-diam diterjemahkan ke Bahasa Indonesia.
  - [ ] Output formatting tidak boleh bocor tag reasoning seperti `<think>`.
  - [ ] Jalur `z.ai` wajib mengirim `thinking.type="disabled"` jika parser worker hanya membaca `message.content`.
  - [ ] Backbone formatting default saat ini adalah `nvidia/openai/gpt-oss-120b`.
  - [ ] Fallback non-NVIDIA utama saat ini adalah `groq/moonshotai/kimi-k2-instruct`.
  - [ ] `cerebras/qwen-3-235b-a22b-instruct-2507` boleh dipakai sebagai fallback berikutnya jika smoke tetap lolos validator.
  - [ ] `z.ai/glm-4.7` hanya boleh dipakai sebagai fallback tambahan jika hasil smoke masih lolos validator.

- **Content Filtering Validation**:
  - [ ] Query `SELECT COUNT(*) FROM videos WHERE COALESCE(is_short, 0) = 1 OR COALESCE(is_member_only, 0) = 1` mengembalikan jumlah yang sesuai ( Shorts + Member-only).
  - [ ] Web UI halaman `/` dan `/videos` tidak menampilkan video yang ditandai `is_short` atau `is_member_only`.
  - [ ] Statistik global di dashboard tidak menghitung durasi/jumlah video dari kategori yang difilter.

- **Streaming Reasoning Validation (Nvidia)**:
  - [ ] Log worker (`runs/.../workers/run_nvidia_*.log`) menampilkan blok `[AI REASONING]` sebelum `[AI CONTENT]`.
- [ ] Teks `reasoning_content` mengalir masuk secara real-time ke stdout/stderr selama proses pembentukan resume.
- [ ] Resume akhir tersimpan dalam format Markdown yang benar di disk dan DB.

- Delta sync database:
  - [ ] `./scripts/sync_missing_rows_to_server.sh --dry-run` membandingkan key lokal vs `tafsir-server` tanpa transfer data.
  - [ ] `python3 scripts/sync_missing_rows_to_server.py apply --bundle <bundle.sqlite3> --db <target.db> --blob-db <target_blob.db> --uploads-dir uploads` dapat diterapkan ke DB temp tanpa error.
  - [ ] Sync real hanya mengirim row/file yang belum ada di server, bukan menyalin `db/youtube_transcripts.db` full.
