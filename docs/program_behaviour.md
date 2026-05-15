# Program Behaviour

Dokumen ini mencatat perilaku program di repo `yt_channel`: apa yang dibaca, diproses, ditulis, dan diubah di DB.

## 1. Gambaran Umum Sistem

Program ini mengelola pipeline data YouTube berbasis SQLite + filesystem:

1. Ambil daftar video channel dan subtitle.
2. Simpan transcript sebagai `text/*.txt`.
3. Simpan metadata video/channel di `channels.db`.
4. Generate resume markdown `resume/*.md`.
5. Simpan file plain ke shard terkompresi (`.shards/*.zst`) tanpa ubah path link di DB.
6. Sediakan UI web Flask untuk manajemen channel, video, job, category, transcript, resume.

## 2. Kontrak Data Utama

Lokasi default:

- DB: `channels.db`
- Output root: `out/`
- Per channel: `out/<slug>/`
- Transcript: `out/<slug>/text/*.txt`
- Resume: `out/<slug>/resume/*.md`
- Shard index: `out/<slug>/.shards/index.json`
- Shard blob: `out/<slug>/.shards/text-*.zst`, `resume-*.zst`

Kolom penting tabel `videos`:

- `status_download`: `downloaded`, `pending`, `no_subtitle`, `error`
- `link_file`: path relatif transcript (contoh `text/0123_<video_id>.txt`)
- `link_resume`: path relatif resume (contoh `resume/0123_<video_id>.md`)
- `seq_num`: urutan video per channel (umumnya tua->baru; angka besar = video lebih baru)

## 3. Behaviour Per Script (CLI)

### 3.1 `scrap_all_channel.py` (scraper utama)

Tujuan:

- Scan video channel YouTube.
- Download subtitle (manual/auto) via `yt-dlp`.
- Konversi VTT -> TXT.
- Simpan metadata ke `channels.db`.

Input:

- Argumen wajib: `channel_url`
- Opsi: `--update`, `--pending-only`, `--video-id`, `--stop-at-known`, `--db`, `--out`, dsb.

Proses:

1. Resolve URL channel ke URL `/videos`.
2. Ambil playlist flat (`yt-dlp --flat-playlist`).
3. Ambil `upload_date`, hitung/rapikan `seq_num`.
4. Pilih bahasa subtitle terbaik (prioritas `en-orig`, `id-orig`, dst).
5. Download VTT ke folder sementara.
6. Konversi VTT jadi plain text.
7. Simpan TXT final sebagai nama kanonik `####_<video_id>.txt`.
8. Update row DB: status, `link_file`, error handling.

Output:

- File transcript di `out/<slug>/text/`
- CSV ringkas `out/<slug>/videos_text.csv`
- Update/insert row `channels`, `videos`

Efek DB:

- Upsert channel dan video.
- Set `status_download` sesuai hasil (`downloaded/pending/no_subtitle/error`).
- Set `link_file` jika transcript tersedia.

### 3.2 `import_local_channels.py` (import data lokal)

Tujuan:

- Masukkan transcript yang sudah ada ke `channels.db`.

Sumber input:

- Prioritas 1: `out/<slug>/videos_text.csv`
- Fallback: scan `out/<slug>/text/*.txt`

Perilaku:

- Upsert channel + video.
- Set `status_download='downloaded'`.
- Isi `link_file`.
- Isi/rekalkulasi `seq_num` bila belum lengkap.

### 3.3 `migrate_naming_seqid.py` (normalisasi nama file)

Tujuan:

- Rename transcript/resume ke format kanonik `####_<video_id>.(txt|md)`.
- Sinkronkan `link_file` dan `link_resume` di DB.

Perilaku:

- Cari kandidat file berdasarkan `video_id`, `seq_num`, `link_*` lama.
- Rename jika perlu.
- Update DB link ke path baru.
- Mendukung `--dry-run`.

### 3.4 `reconcile_missing_link_files.py` (rekonsiliasi transcript)

Tujuan:

- Validasi bahwa `videos.link_file` benar-benar ada (plain atau shard).
- Jika hilang: set `status_download='pending'`.

Catatan:

- Script ini hanya mengecek `link_file` (transcript), bukan `link_resume`.
- Opsi `--skip-null` akan mengabaikan baris `link_file` kosong.
- Default membuat backup DB sebelum update.

### 3.5 `reconcile_resume_links.py` (rekonsiliasi resume, langkah 2)

Tujuan:

- Clear `link_resume` yang menunjuk file resume hilang.
- Relink `link_resume` kosong ke path default resume jika file resume default ada.
- (Opsional) canonicalize link resume existing ke nama default.

Perilaku:

1. Scan video (default hanya `status_download='downloaded'`).
2. Jika `link_resume` terisi tetapi file tidak ditemukan (plain/shard) -> set `link_resume=NULL`.
3. Jika `link_resume` kosong:
 - hitung path default `resume/####_<video_id>.md` dari `seq_num`/`link_file`
 - jika file default ada, set `link_resume` ke path itu.
4. Opsi penting:
 - `--dry-run` untuk simulasi.
 - `--all-status` untuk proses semua status.
 - `--relink-without-text` untuk relink tanpa syarat transcript ada.
 - `--no-canonicalize` untuk mempertahankan path non-default yang masih valid.

### 3.6 `fill_missing_resumes.py` (generator resume modern)

Tujuan:

- Buat resume untuk video yang belum punya resume.
- Relink resume existing bila file sudah ada.
- Validasi kualitas resume.

Perilaku penting:

1. Startup preflight:
 - Clear `link_resume` yang mengarah ke file hilang/tidak valid.
 - Relink ke nama default resume jika file valid ada.
2. Loop video target:
 - Cek transcript tersedia (plain/shard).
 - Kalau transcript hilang, bisa set `status_download='pending'`.
 - Jika resume valid sudah ada, skip/relink.
 - Jika belum ada, panggil model API, generate markdown, simpan file, update `link_resume`.
3. Locking:
 - Lock file `.lock` per resume untuk menghindari race antar agent.

### 3.7 `generate_resumes.py` (generator resume lama/per-range)

Tujuan:

- Generate draft resume untuk rentang `seq_num` tertentu.

Ciri:

- Bekerja per channel + range seq.
- Engine bisa `heuristic`, `openai`, atau `codex`.
- Cocok untuk operasi terarah, bukan rekonsiliasi massal harian.

### 3.8 `compact_out_to_shards.py` (compactor shard)

Tujuan:

- Pindahkan file plain `text|resume` ke shard `.zst`.

Perilaku:

1. Ambil kandidat path dari DB (`link_file`/`link_resume`).
2. Untuk file plain valid dan cukup tua (`--min-age-minutes`), kompres zstd.
3. Append ke shard channel (`text-xxxx.zst` / `resume-xxxx.zst`).
4. Tulis/ubah entri index `out/<slug>/.shards/index.json`.
5. Hapus file plain yang sudah berhasil masuk shard.

Catatan:

- Path di DB tetap sama (`text/...`, `resume/...`).
- Reader akan resolve otomatis ke plain jika ada, jika tidak ke shard index.

### 3.9 `run_resume_agents.sh` (orchestrator multi-agent)

Tujuan:

- Menjalankan banyak worker `fill_missing_resumes.py` secara detached.

Perintah:

- `run/start`: jalankan agent
- `status`: cek status PID + progress
- `stop`: hentikan agent

State dan log:

- State PID: `.resume_agents/`
- Log agent: `out/agent_logs/`

### 3.10 `run_web.py`, `wsgi.py`, `passenger_wsgi.py` (entrypoint web legacy)

Perilaku:

- `run_web.py`: start Flask lokal cepat, startup check berat dimatikan default.
- `wsgi.py`: bootstrap untuk deployment WSGI/Passenger, set env safety, load external venv bila ada.
- `passenger_wsgi.py`: loader wrapper untuk Passenger.

Catatan:

- Jalur web resmi saat ini ada di [flask_app/](../flask_app/app.py).
- `webapp/` hanya dipertahankan untuk kompatibilitas lama.

### 3.11 `down_all.py` dan `clean.py` (legacy utilities)

`down_all.py`:

- Download subtitle dari CSV kategori ke folder `subs/`.
- Tidak terhubung langsung ke `channels.db` modern.

`clean.py`:

- Hapus file `-orig.vtt` dan konversi VTT ke TXT dari `subs/` ke `txt/`.
- Utility pra-pipeline lama.

### 3.12 `test.py` (manual API test)

Perilaku:

- Script ad-hoc untuk uji streaming chat completion.
- Bukan bagian pipeline produksi.

## 4. Storage Behaviour (`shard_storage.py`)

Modul ini dipakai lintas script untuk akses file yang transparan antara plain dan shard.

Fungsi utama:

- `link_exists`: true jika file plain ada atau entry shard valid.
- `read_link_bytes` / `read_link_text`: baca dari plain dulu, fallback shard.
- `link_size`, `link_mtime`, `link_source_label`.
- `choose_append_shard`, `append_blob` untuk write shard.

Safety behaviour:

- Validasi path relatif aman (`normalize_rel_path`, `safe_resolve`).
- Validasi offset/length terhadap ukuran shard agar tidak baca data korup.
- Fallback dekompresi zstd via python bridge jika modul tidak tersedia.

## 5. Behaviour Web App Legacy (`webapp/`)

`webapp/` dipertahankan sebagai jalur legacy/compatibility saja. Jalur web resmi saat ini ada di `flask_app/`.

Aplikasi Flask legacy (`webapp/app.py`) melakukan:

1. Dashboard channel/video/search.
2. CRUD kategori.
3. Detail video + transcript viewer + resume editor.
4. Queue job background untuk update channel.
5. Startup maintenance opsional:
 - integrity check DB
 - cleanup WAL/SHM
 - reconcile link resume awal
6. Integrasi storage transparan via `shard_storage`.

`webapp/jobs.py` + `webapp/job_runner.py`:

- Queue tunggal job update channel (`queued`, `running`, `stopping`, `stopped`, `done`, `error`).
- Spawn subprocess scraper.
- Deteksi PID stale dan auto recovery status.
- Lanjutkan job antrean berikutnya setelah job selesai.

`webapp/db.py`:

- Koneksi SQLite + busy timeout.
- Ensure schema/migrasi kolom opsional.
- Maintenance util (`integrity_check`, cleanup WAL/SHM, blocker detection).

## 6. Log, Lock, dan Artefak Runtime

- Log resume agent: `out/agent_logs/`
- Lock resume per file: `*.md.lock`
- Preflight lock resume: `out/.resume_preflight.lock`
- Job web log: `.webapp_jobs/`
- SQLite sidecar aktif saat WAL: `channels.db-wal`, `channels.db-shm`

## 7. Urutan Operasional Rekomendasi

1. Ambil/update transcript:
 - `scrap_all_channel.py` atau `import_local_channels.py`
2. Rekonsiliasi transcript missing:
 - `reconcile_missing_link_files.py --skip-null`
3. Rekonsiliasi link resume (langkah 2):
 - `reconcile_resume_links.py` (gunakan `--dry-run` dulu untuk verifikasi)
4. Rekonsiliasi/generate resume:
 - `fill_missing_resumes.py` (single) atau `run_resume_agents.sh run` (multi-agent)
5. Compact ke shard:
 - `compact_out_to_shards.py`
6. Validasi:
 - Pastikan `downloaded` tidak punya transcript missing.
 - Pastikan `link_resume` tidak menunjuk file missing.

## 8. Prinsip Idempoten yang Dipakai

- Banyak operasi bersifat idempoten: import/upsert, relink, reconcile, compact (akan skip yang sudah benar).
- Komponen write-heavy (resume generation, compaction) memakai lock/guard untuk mengurangi race.
- Reader selalu membaca dari plain dulu, lalu shard, sehingga migrasi storage bisa bertahap.
