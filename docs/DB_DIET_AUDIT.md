# Database Diet Audit

Audit ini merangkum area database yang boros, redundant, atau masih menyimpan data ganda.

## Langkah Aman Yang Sudah Dijalankan

- `youtube_transcripts.db` sudah di-checkpoint dan `WAL` sudah di-truncate.
- Hasilnya, `youtube_transcripts.db-wal` turun ke `0` bytes.
- Shadow columns `videos.transcript_text` dan `videos.summary_text` sudah dikosongkan pada row yang punya blob pasangan, lalu `VACUUM` dijalankan ulang.
- Search cache/FTS sudah dipindah ke `youtube_transcripts_search.db`, lalu objek search lama di main DB di-drop.
- Corpus search sekarang tidak lagi menyimpan `summary_search`; hanya `title`, `description`, dan `transcript_search`.

## Ukuran Saat Audit

- `youtube_transcripts.db`: sekitar `51.6 MB`
- `youtube_transcripts_search.db`: sekitar `368.9 MB`
- `youtube_transcripts_blobs.db`: sekitar `469 MB`
- `youtube_transcripts.db-wal` sebelum truncate: sekitar `65 MB`

## Temuan Utama

### 1. `videos.metadata` besar dan masih plain text

- Total ukuran mentah sekitar `147 MB`.
- Rata-rata panjang sekitar `2.7 KB` per row untuk `54,393` row.
- Ini kandidat paling jelas untuk kompresi atau eksternalisasi.

### 2. `videos.summary_text` redundant dengan blob store

- Ada `8,007` row `summary_text` non-empty.
- Semua `8,007` row itu overlap dengan blob `resume`.
- Artinya resume sudah tersimpan di dua tempat.

### 3. `videos.link_file_formatted` redundant dengan `transcript_formatted_path`

- Ada `27,722` row formatted.
- Semua row itu nilainya sama persis antara dua kolom tersebut.
- Ini kolom dobel yang seharusnya cukup satu sumber kebenaran.
- Kolom fisik `link_file_formatted` sudah di-drop dari schema aktif; `transcript_formatted_path` sekarang canonical.

### 4. `videos.transcript_text` sebagian besar masih ganda

- Ada `10,201` row `transcript_text` non-empty.
- Overlap dengan blob `transcript` sudah `10,201` row.
- Artinya content transcript aktif sudah ada di blob store juga, dan `videos.transcript_text` sekarang berfungsi sebagai shadow column untuk FTS/search.

### 5. Legacy tables kosong

- `transcripts`: `0` row
- `summaries`: `0` row

Tabel ini sudah tidak berisi data aktif dan sudah di-drop dari schema aktif.

### 6. Search cache/FTS sudah dipisah

- `videos_search_cache` dan `videos_search_fts` tidak lagi tinggal di main DB.
- Keduanya sekarang ada di `youtube_transcripts_search.db`.
- Ini menurunkan ukuran file utama secara drastis tanpa mengorbankan search.
- Korpus search sekarang tidak lagi menyimpan `summary_search`; hanya `title`, `description`, dan `transcript_search`.

### 7. Kandidat audit lanjutan

- `idx_videos_duration` dan `idx_videos_word_count` tetap dipertahankan karena query planner memang memakainya.
- `idx_videos_upload_date` sudah di-drop karena tidak terpakai di query planner yang saya benchmark.
- `idx_videos_title` sudah di-drop karena search title sudah dipindah ke DB search terpisah.
- `description` di search corpus tidak layak dibuang: simulasi title+transcript-only cuma menghemat sekitar `0.8 MB` pada `youtube_transcripts_search.db`, tetapi pada sample 100 query yang diambil dari description, total hit turun sekitar `1.42%` (`85,528` → `84,313`).

## Rekomendasi Diet

### Sudah dilakukan

- Pertahankan WAL checkpoint rutin saat batch sudah berhenti.
- `videos.metadata` dibackfill ke blob `metadata` dan kolom lama dikosongkan untuk row yang sudah dimigrasi.
- Jalur tulis aktif untuk formatted transcript sekarang hanya memakai `transcript_formatted_path`.
- `videos.transcript_text` dan `videos.summary_text` sudah dikosongkan untuk row yang punya blob pasangan.
- Search cache/FTS dipindah ke `youtube_transcripts_search.db`.
- Tabel legacy `transcripts` dan `summaries` yang kosong sudah di-drop.
- Kolom fisik `link_file_formatted` sudah di-drop dari tabel `videos`.
- File transcript fisik yang sudah sepenuhnya ada di DB/blob sudah dibuang; folder `uploads/*/text/` sekarang kosong.
- Cleanup bertahap sudah menghapus 7,473 row/path transcript yang punya blob, lalu 3,919 file transcript yang masih aktif, lalu 88 orphan file transcript yang belum tercatat di DB.
- File summary fisik yang sudah punya blob `resume` juga sudah dibuang; hanya `summary_file_path` legacy yang dipertahankan sebagai penanda status.

### Bisa dilakukan berikutnya

- Jalur baca runtime sudah blob-first, dan write baru sekarang juga mirror ke blob.
- Kalau search mau dipindah penuh ke blob atau dipisah ke DB search terpisah, perlu migrasi/search redesign dulu sebelum cache bisa diperkecil tanpa menurunkan kualitas search.

## Catatan

- Audit ini sengaja konservatif: hanya jalur yang sudah aman dipindahkan yang dibersihkan.
- Data yang masih dipakai web/app/legacy script tetap dipertahankan di fallback sampai ada migrasi yang jelas.
