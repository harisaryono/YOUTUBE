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
