# PROJECT PLAN - YouTube Transcript Recovery

## Purpose
Recover missing transcripts for 4,217 videos to enable resume generation.

## Strategy
1. Use `recover_transcripts.py` which leverages cookies, direct transcript paths, and Webshare only as the last-resort fallback.
2. Run in controlled batches to avoid rate-limiting.
3. Mark unrecoverable videos as `no_subtitle`.
4. Use `youtube_search_util.py` / `scripts/search.sh` for yt-dlp discovery search before transcript processing when needed.

## Pipeline Architecture
1. Discovery dan import database harus selesai dulu, baru transcript/resume/format dijalankan.
2. Transcript, resume, dan format harus tetap bisa dijalankan sebagai fase independen kalau discovery sudah selesai sebelumnya.
3. Satu worker pool tidak boleh memegang semua fase sekaligus; gunakan run directory terpisah per fase.
4. Default operasional yang dipakai sekarang:
   - discovery: auto per channel
     - channel bersih: `latest-only`
     - channel backlog: `scan-all-missing`
     - selalu dengan `rate-limit-safe`
   - transcript: `10` worker
   - resume: `10` worker, `--nvidia-only`, model `openai/gpt-oss-120b`
   - format: `8` worker, provider plan `nvidia_only`
5. Entry point yang direkomendasikan:
   - `./run_pipeline.sh --channel-id <CHANNEL_ID>`
   - `./run_pipeline.sh --channel-name <CHANNEL_NAME>`
   - `./run_pipeline.sh --all-channels`
   - mode parsial tersedia:
     - `./run_pipeline.sh --discovery-only`
     - `./run_pipeline.sh --transcript-only`
     - `./run_pipeline.sh --resume-only`
     - `./run_pipeline.sh --format-only`

## ASR Transcript Strategy
1. Use `scripts/audio.sh` / `scripts/audio_download.sh` to fetch local audio files for `no_subtitle` videos.
2. Use `recover_asr_transcripts.py` / `scripts/asr.sh` only for local-audio ASR, never for YouTube download.
3. Download audio once, store `video_audio_assets.audio_file_path`, split into fixed-size chunks, and transcribe per chunk with Whisper on Groq or NVIDIA.
4. Persist every chunk result to `video_asr_chunks` so a failed batch can resume from the first missing chunk.
5. Merge chunk text only after all chunks succeed, then write the final transcript text back into `videos.transcript_text` and the final `.txt` file under `uploads/asr/`.

## Operator Constraint
- PERTANYAAN STATUS DI TENGAH PROSES BUKAN INSTRUKSI UNTUK MENGHENTIKAN JOB.
- Jika user bertanya `bagaimana?`, `status?`, `progress sampai mana?`, atau pertanyaan serupa saat batch masih berjalan, agen hanya boleh:
  - mengecek status,
  - melaporkan snapshot,
  - membiarkan job tetap berjalan di background.
- Job background hanya boleh dihentikan jika:
  - user secara eksplisit meminta stop / cancel / hentikan,
  - objective batch memang sudah selesai,
  - ada bukti kuat batch salah arah atau merusak target.

## Coordinator Source Of Truth
- URL coordinator yang benar untuk repo ini dibaca dari `YT_PROVIDER_COORDINATOR_URL` dan harus diset eksplisit di environment.
- Default fallback lokal tetap `http://127.0.0.1:8788` jika environment memang mengarah ke coordinator lokal.
- Saat start batch produksi, log harus menampilkan URL coordinator yang benar sebelum pekerjaan transcript/resume berjalan.
- Sebelum membuat program baru yang menyentuh coordinator/provider/runtime lease, wajib baca source of truth operasional di server:
  - `ssh yt-server 'sed -n "1,260p" /root/services/COORDINATOR_GUIDE.md'`
- Jangan menulis script atau utility coordinator baru hanya dari tebakan file lokal.
- Lease default coordinator harus `300` detik.
- Jika tidak ada heartbeat/status update selama `300` detik, lease harus dianggap expired dan dilepas.
- Untuk worker panjang, heartbeat aman dikirim sekitar `TTL/3`.
- Jika user meminta `perbaiki coordinator`, agen harus:
  - stop proses yang terdampak,
  - patch coordinator dan worker terkait,
  - restart coordinator,
  - verifikasi health + acquire path,
  - baru melapor.
- Jangan menunggu instruksi kedua untuk langkah stop/patch/restart jika objective yang diminta memang perbaikan coordinator.

## Resume Queue Policy
- Untuk resume `openai/gpt-oss-120b`, gunakan SEMUA akun aktif yang cocok, bukan satu akun saja.
- Setiap key = satu akun. Jangan perlakukan banyak key sebagai satu jalur tunggal.
- `Groq` dipakai sebagai jalur utama paralel selama quota masih ada.
- `NVIDIA` adalah fallback paling stabil untuk task resume yang gagal di `Groq` karena rate limit/quota.
- Sediakan minimal 1 akun `NVIDIA` sebagai fallback queue untuk melanjutkan task `Groq` yang berhenti di tengah jalan.
- Task yang belum selesai pada worker `Groq` harus bisa dipindahkan ke antrian `NVIDIA` idle, bukan dibiarkan hangus.
- **Model DeepSeek-R1 (Nvidia)**: Gunakan mode `stream=True` dan tampilkan `reasoning_content` di log untuk memantau proses "berpikir" AI secara real-time.

## Content Filtering Policy
- **Video Shorts**: Semua video dengan durasi < 60 detik (`is_short = 1`) harus disembunyikan dari UI web dan perhitungan statistik.
- **Member-only Videos**: Video yang ditandai sebagai `is_member_only = 1` (berdasarkan metadata `member_only`) diperlakukan sama seperti Shorts: tidak ditampilkan dan tidak dihitung dalam statistik publik.
- Filter ini harus diterapkan secara konsisten di level query database (e.g., `database_optimized.py`) dan pipeline pemrosesan (e.g., resume generation).

## Goals
- [ ] Recover entire 4,217 backlog.
- [ ] Ensure `no_subtitle` marking is accurate.
- [ ] Update `db/youtube_transcripts.db` with correct paths/metadata.
- [ ] Jalankan discovery-only scan seluruh channel untuk mendeteksi video baru tanpa mengunduh transcript/resume.

## Phase 1: Smoke Test
- Run limit 5.
- Status: Pending approval.

## Discovery Batch: Latest Video Check
- Objective: cek seluruh channel aktif dan hasilkan report video `new` atau `retry_incomplete`.
- Scope: discovery only, tanpa download transcript dan tanpa generate resume.
- Exit condition: `report.csv` tersedia dan setiap channel menghasilkan status `new`, `retry_incomplete`, `no_actionable`, `channel_skipped`, atau `channel_error`.

## Discovery Batch: All Missing Per Channel
- Objective: scan seluruh riwayat channel untuk menemukan SEMUA video yang belum ada di DB atau masih incomplete, bukan hanya jendela `N` terbaru.
- Scope: mode `--scan-all-missing` pada `update_latest_channel_videos.py`.
- Exit condition: `report.csv` tersedia, `scan_scope=full_history`, dan setiap row actionable berasal dari seluruh entry channel yang berhasil dipindai.

## Formatted Transcript
- Objective: menambah transcript yang sudah diformat agar lebih mudah dibaca TANPA merangkum atau menerjemahkan isi.
- Tahap awal wajib smoke compare lintas provider/model sebelum dibuat pipeline global.
- Kandidat compare yang sudah diuji: `nvidia/openai/gpt-oss-120b`, `nvidia/mistralai/mistral-small-24b-instruct`, `z.ai/glm-4.7`, `gemini/gemini-2.5-flash`, beberapa model `groq`, dan beberapa model `cerebras`.
- Exit condition tahap compare: ada run directory, output contoh bisa dibaca, dan ada keputusan provider/model mana yang paling dekat ke transcript sumber.
- Keputusan operasional saat ini:
  - baseline / parameter kebenaran formatting: `nvidia/openai/gpt-oss-120b`
  - fallback non-NVIDIA yang paling menjanjikan: `groq/moonshotai/kimi-k2-instruct`
  - fallback non-NVIDIA berikutnya: `cerebras/qwen-3-235b-a22b-instruct-2507`
  - fallback yang masih bisa diterima tetapi lebih agresif rewrite: `z.ai/glm-4.7`
  - usable tapi bukan jalur utama:
    - `nvidia/mistralai/mistral-small-24b-instruct`
    - `groq/openai/gpt-oss-20b`
    - `cerebras/llama3.1-8b`
  - jangan dipakai saat ini:
    - `gemini/gemini-2.5-flash` karena `429 RESOURCE_EXHAUSTED`
    - `groq/openai/gpt-oss-120b` karena `blocked_model`
    - `groq/qwen/qwen3-32b` karena walau request sekarang bisa lolos dengan adaptive sizing, kualitas masih bocor `<think>`
    - `groq/llama-3.3-70b-versatile` dan `groq/meta-llama/llama-4-scout-17b-16e-instruct` untuk transcript English karena cenderung menerjemahkan
