# Repo Operating Principles

Dokumen ini berlaku untuk seluruh repo `yt_channel`.

Tujuan utamanya:
- mencegah drift,
- mencegah loop sia-sia,
- mempersempit objective,
- mempercepat hasil yang benar,
- memaksa klarifikasi jika tugas masih terlalu umum.

Dokumen operasional yang wajib dianggap sebagai state repo:
- `PLAN.md`
- `PROGRESS.md`
- `VERIFY.md`

## Prinsip Inti

1. Satu worker, satu tujuan.
- Satu proses hanya boleh punya objective yang jelas.
- Jangan campur scraping, formatting, resume, import, dan repair ke dalam satu loop yang sama.

2. Validasi kecil dulu, scale kemudian.
- Semua provider/model baru wajib lewat smoke test atau batch kecil.
- Batch besar hanya boleh jalan setelah kualitas dan stabilitas lolos.

3. Hentikan loop jelek secepat mungkin.
- Jika output generik, salah arah, auth gagal, transcript hilang, atau throughput buruk, hentikan.
- Jangan membiarkan proses “tetap jalan” jika jelas tidak memperbaiki hasil.

4. State harus persisten dan bisa dibaca lintas program.
- Block provider/model/account, auth disable, dan state operasional lain harus disimpan di SQLite atau state yang bisa dibaca program lain.
- Jangan simpan keputusan penting hanya di log sementara.

5. Provider error fatal tidak boleh diulang.
- `401`, `403`, `User not found`, `Invalid API Key`, `Missing Authentication header`, dan error autentikasi sejenis adalah fatal.
- Akun seperti itu harus dikeluarkan dari rotasi dan dinonaktifkan sampai diperbaiki manual.

6. Objective harus terukur.
- “Jadikan lebih baik” terlalu kabur.
- Objective harus bisa diperiksa, misalnya:
  - transcript berhasil diunduh,
  - resume tertulis ke `link_resume`,
  - formatted text tertulis ke `link_file_formatted`,
  - error rate turun,
  - backlog berkurang.

7. Jangan optimalkan hal lain selain target.
- Jika targetnya download transcript, jangan mengubah struktur resume.
- Jika targetnya kualitas resume, jangan mengutak-atik scraper tanpa alasan kuat.

8. Gunakan fallback hanya jika lebih baik daripada gagal.
- Fallback boleh dipakai jika menjaga throughput atau kualitas.
- Fallback tidak boleh dipakai jika hanya menyamarkan kegagalan dengan output jelek.

9. Hasil buruk tidak dihitung sebagai sukses.
- Output generik, meta, ngawur, atau terlalu rusak tidak boleh dianggap “done”.
- Lebih baik `skip`, `retry`, atau `failed` daripada menyimpan file sampah.

10. Batch harus bisa dihentikan, dilanjutkan, dan diaudit.
- Setiap batch harus punya:
  - task file,
  - log,
  - report CSV,
  - run directory yang jelas.
  - referensi ke objective di `PLAN.md`
  - validasi yang bisa dijalankan ulang dari `VERIFY.md`

## Aturan Saat Tugas Terlalu Umum

Jika permintaan user terlalu umum, ambigu, atau berisiko membuat agen drift, jangan langsung improvisasi luas.

Sebaliknya, keluarkan daftar pertanyaan klarifikasi yang pendek, tajam, dan operasional.

Gunakan pertanyaan yang mempersempit:
- target akhir,
- scope,
- prioritas,
- definisi kualitas,
- batas resource,
- exit condition.

Contoh pola pertanyaan yang benar:

1. Target akhirnya apa:
- download transcript,
- formatting transcript,
- resume,
- perbaikan DB,
- atau web UI?

2. Scope-nya apa:
- satu channel,
- satu provider,
- satu model,
- atau seluruh backlog?

3. Yang diprioritaskan apa:
- kualitas,
- kecepatan,
- biaya/quota,
- atau stabilitas?

4. Output dianggap berhasil jika seperti apa:
- file tersimpan,
- DB terupdate,
- error rate turun,
- atau hasil teks memenuhi standar tertentu?

5. Batas eksperimen apa:
- berapa worker,
- provider mana,
- boleh fallback atau tidak,
- stop setelah berapa gagal?

6. Jika ada tradeoff, mana yang dipilih:
- kualitas lebih tinggi tapi lambat,
- atau lebih cepat tapi lebih generik?

Aturan keras:
- jangan bertanya umum seperti “Anda maunya bagaimana?”
- jangan bertanya terlalu banyak
- cukup pertanyaan yang membuat task menjadi sempit dan executable

## Kapan Harus Bertanya, Kapan Harus Jalan

Default: jalan.

Namun harus bertanya jika:
- objective belum jelas,
- scope belum jelas,
- ada beberapa arah yang sama-sama valid tetapi hasilnya berbeda,
- ada risiko merusak data besar,
- ada risiko membuang quota besar untuk eksperimen kabur.

Tidak perlu bertanya jika:
- tinggal menjalankan batch lanjutan yang jelas,
- tinggal restart dengan konfigurasi yang lebih baik,
- tinggal mematikan provider yang jelas rusak,
- tinggal memperkecil task untuk validasi.

## Checklist Sebelum Menjalankan Batch Besar

Sebelum scale up:
- objective jelas
- task source jelas
- provider/model valid
- transcript/file input benar-benar ada
- error fatal sudah di-filter
- report CSV tersedia
- run directory jelas
- exit condition jelas

Jika salah satu belum jelas, jangan scale.

## Checklist Kualitas Resume/Formatting

Resume atau formatted text harus ditolak jika:
- terlalu meta
- terlalu global
- meminta transcript lain
- seperti balasan chatbot
- mengarang detail penting
- terlalu pendek untuk dianggap bermakna
- jelas lebih buruk daripada transcript sumber

## Prinsip Operasional untuk Provider

- NVIDIA: backbone utama jika stabil.
- Groq: pakai hanya dengan guard quota dan fallback yang jelas.
- Gemini/OpenRouter/provider lain: jangan jadi jalur utama jika auth/quota tidak stabil.
- Provider yang bermasalah harus diperlakukan sebagai data operasional:
  - disable,
  - block sampai waktu tertentu,
  - atau batasi hanya untuk eksperimen kecil.

## Prinsip Implementasi Teknis

- Simpan keputusan operasional di DB jika perlu dibaca lintas proses.
- Jangan duplikasi katalog model per akun jika sebenarnya level provider.
- Gunakan skema yang mencerminkan realitas operasional.
- Jangan pertahankan kompatibilitas lama jika itu membuat sistem terus salah arah.
- Sebelum membuat program baru yang menyentuh provider, coordinator, lease, block, preflight, queue, formatting transcript, atau resume generation, baca dulu guide coordinator yang aktif di server:
  - `ssh yt-server 'sed -n "1,260p" /root/services/COORDINATOR_GUIDE.md'`
- Copy server di `/root/services/COORDINATOR_GUIDE.md` adalah source of truth operasional.
- Jangan membuat script atau utility coordinator baru hanya dari asumsi file lokal jika guide server belum dibaca.

## Prinsip Akhir

Yang paling penting:
- model pintar tidak cukup,
- prompt bagus tidak cukup,
- key banyak tidak cukup.

Yang menentukan hasil adalah:
- loop yang sempit,
- validasi yang keras,
- state yang jelas,
- dan keputusan cepat saat hasil buruk.

---

# Skill Reference (from .codex/home/skills/)

Skill berikut di-copy dari `.codex/home/skills/` sebagai referensi operasional untuk agen yang bekerja di repo ini.

## youtube-pipeline

Pipeline utama: discovery → transcript → resume → format.

**Workflow:**
1. Discovery: cache-first → scrapetube → invidious → innertube → yt-dlp → Webshare (last resort)
2. Transcript: `youtube_transcript_api` → direct subtitle fetch → yt-dlp → savesubs_direct → Webshare
3. Resume: cached transcript_items → synthesize timestamps → offline by default di `itc-server`
4. Cooldown: saat 403, captcha, bot-check, rate-limit → catat cooldown, jangan retry dalam tight loop

**Aturan:**
- Discovery default pasif: `scrapetube → invidious → innertube`
- Channel discovery bisa `full` mode untuk enumerasi seluruh channel
- Transcript stop setelah stage pertama sukses, ingat stage yang bekerja
- Resume retry provider timeout dengan chunk lebih kecil sebelum mark failed
- Worker queues: `worker_jobs` (discovery/transcript/resume), `refresh_jobs` (backfill)

## youtube-provider-fallback-layer

Urutan fallback untuk sumber data YouTube (bukan fallback LLM).

**Search:** `yt_dlp_search → innertube_search → invidious_search`
**Metadata:** `yt_dlp_metadata → invidious_metadata`
**Transcript:** `youtube_transcript_api → direct subtitle track fetch → yt-dlp subtitle download → Webshare proxy fallback`

**Prinsip:**
- Gunakan sumber direct/default paling murah dan stabil dulu
- Webshare hanya sebagai opsi terakhir
- Jika pipeline gagal terlalu sering di non-paid, Webshare boleh dinaikkan sementara
- Semua provider harus return `ProviderResult(ok=True, provider="...", data=[...], status="ready")`

## api-lease-coordinator

Mengatur peminjaman API provider untuk worker paralel.

**Konsep:**
1. Worker minta lease
2. Terima provider/model/base_url/api_key
3. Pakai lease selama TTL
4. Lapor sukses/gagal
5. Coordinator beri cooldown bila error retryable

**Gunakan saat:** banyak worker, banyak provider/model, API key terbatas, perlu cooldown 429/timeout.

## asr-lease-coordinator

Transkripsi audio/MP3 via lease coordinator.

**Workflow:**
1. Baca source list audio
2. Acquire lease dari coordinator
3. Prefer `groq` dulu, lalu `nvidia` sebagai fallback
4. Pakai `whisper-large-v3` default
5. Download audio sekali, transcribe, save per item
6. Long job: heartbeat; short job: release setelah selesai
7. Success → release dengan `ok=true`
8. 429/timeout → mark retryable, release cleanly
9. Auth error → stop, report blocker

**Default coordinator:** baca dari `YT_PROVIDER_COORDINATOR_URL` (fallback lokal `http://127.0.0.1:8788`)

## webshare-proxy-rotation

Rotasi proxy Webshare untuk HTTP request.

**Aturan:**
- Ambil `WEBSHARE_API_KEY` dari env atau `.env.local`
- Fetch daftar proxy dari `https://proxy.webshare.io/api/v2/proxy/list/`
- Cache daftar proxy (jangan fetch tiap request)
- Rotasi deterministik per `request_id` atau key stabil
- Coba proxy satu per satu sebelum fallback direct
- Format: `http://username:password@proxy.host:port`

## sqlite-batch-commit

Untuk long-running SQLite writes (import, audit, backfill).

**Default:** commit setiap 25 rows.
**Pola:**
```python
batch = []
for row in rows:
    batch.append(row)
    if len(batch) >= 25:
        cur.executemany(SQL, batch)
        conn.commit()
        batch.clear()
if batch:
    cur.executemany(SQL, batch)
    conn.commit()
```

## sqlite-zstd-cache

Simpan teks panjang (transcript, resume, OCR) ke SQLite sebagai zstd blob.

## sqlite-delta-journal

Sinkronkan perubahan SQLite besar dengan JSONL event (emit/apply), bukan upload/download DB penuh.

**Cocok untuk:** kerja lintas komputer, perubahan kecil di DB besar.

## ai-context-generator

Generate `AI_CONTEXT/` package agar repo bisa dibaca AI/ChatGPT via Google Drive.

**Output:** `LATEST_HANDOFF.md`, `FILE_INDEX.md`, `FILE_JOURNAL.md`, `PROJECT_STATE.md`, `REPO_CHANGELOG.md`, `GDRIVE_HANDOFF.md`, `repo_snapshot.json`

## audit-trail-watch

Watch AI context outputs, auto-regenerate saat file berubah.

## gdrive-project-sync

Sync repo ke Google Drive via `rclone` dengan exclude rules.

**Exclude default:** `data/*.sqlite3*`, `__pycache__/`, `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`, `.journal_state/`, `.git/`

## prompt-template-externalization

Pisahkan prompt dari source code ke file `.md` agar mudah diedit dan diaudit.

**Format:**
```md
## nama_template
Isi prompt dengan ${variable}
```

## portable-project-layout

Buat repo portable antar komputer/server tanpa path absolut.

**Komponen:** `app_config.py` (resolver env/path), `bootstrap_portable_layout.sh` (symlink .env dan folder eksternal)

## data-rework-backfill

Buat job migrasi/perbaikan data lama secara idempotent, batch, dan resumable.

**Pola:** Audit → Buat job → Claim batch → Proses → Finalize → Retry failed

## bundle-first

Meta-skill: kerja dalam bundled chunks, bukan micro-step.

**Aturan:**
- Mulai dengan 2-4 opsi besar
- Rekomendasi satu
- Eksekusi tanpa pause tiap substep
- Compact context saat terlalu panjang
- Jangan ubah satu request jadi chain approval kecil
