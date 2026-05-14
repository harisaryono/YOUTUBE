# YouTube Transcript Framework Complete Version

Framework Python lengkap untuk mengambil transkrip dari video YouTube dengan database dan kemampuan batch processing. Versi ini dirancang untuk penggunaan skala besar dan manajemen koleksi channel secara efisien.

## ✨ Fitur Utama

### 🗄️ Integrasi Database
- ✅ SQLite database untuk menyimpan metadata video, transkrip, dan ringkasan.
- ✅ Dukungan kolom `metadata` untuk menyimpan info tambahan dari YouTube.
- ✅ Sinkronisasi kolom file path (`transcript_file_path`, `summary_file_path`) dengan Flask App.
- ✅ Pencarian teks lengkap (Full-text search) pada judul dan deskripsi.

### 📺 Pemrosesan Channel
- ✅ Download seluruh video dalam satu channel (Support hingga 200 video per request).
- ✅ Mekanisme skip video yang sudah ada untuk efisiensi waktu dan kuota.
- ✅ *Rate limiting* cerdas dengan *exponential backoff* dan *user-agent rotation*.
- ✅ Penyimpanan terorganisir di folder `uploads/<channel_id>/text/` dan `uploads/<channel_id>/resume/`.

### 🛠️ Manajemen Database
- ✅ Alat baris perintah (`manage_database.py`) untuk kontrol penuh.
- ✅ Statistik lengkap, pencarian cepat, dan retry download yang gagal.
- ✅ Optimasi database otomatis dengan perintah `vacuum`.

## 📦 Struktur Proyek

```
YOUTUBE/
├── venv/                            # Environment terisolasi (Rekomendasi)
├── youtube_transcript_complete.py   # Script utama dengan integrasi DB & Channel
├── database.py                      # Modul skema dan koneksi database
├── manage_database.py               # Utilitas manajemen database
├── flask_app/                       # Aplikasi Web Dashboard
├── uploads/                         # Direktori penyimpanan hasil (Terpusat)
│   └── <Safe_Channel_ID>/
│       ├── text/                    # File transkrip video
│       └── resume/                  # File ringkasan video
├── youtube_transcripts.db           # SQLite database utama
└── requirements.txt                # Daftar dependensi
```

## 🚀 Penggunaan Cepat

Selalu gunakan Python dari virtual environment (`./venv/bin/python3`) untuk menghindari masalah dependensi.

### 1. Download Koleksi Channel
```bash
# Download semua video dari channel (skip yang sudah ada)
./venv/bin/python3 youtube_transcript_complete.py https://www.youtube.com/@KenapaItuYa --channel

# Gunakan limit untuk pengetesan
./venv/bin/python3 youtube_transcript_complete.py https://www.youtube.com/@KenapaItuYa --channel --max 10
```

### 2. Download Video Tunggal
```bash
./venv/bin/python3 youtube_transcript_complete.py https://www.youtube.com/watch?v=VIDEO_ID --video
```

### 3. Manajemen Koleksi
```bash
# Tampilkan statistik download
./venv/bin/python3 manage_database.py stats

# Tampilkan video yang belum memiliki transkrip
./venv/bin/python3 manage_database.py videos-without

# Coba lagi download yang sebelumnya gagal
./venv/bin/python3 manage_database.py retry
```

## 🗄️ Skema Database (Tabel Utama)

### `videos`
Tabel ini sekarang menggunakan kolom yang kompatibel dengan aplikasi Flask:
- `transcript_file_path`: Lokasi path file teks transkrip.
- `summary_file_path`: Lokasi path file teks ringkasan.
- `metadata`: Data JSON mentah dari YouTube (untuk analisis lanjut).

## 🛡️ Error Handling
- **Rate Limit (429)**: Script akan otomatis menunggu dan mencoba lagi.
- **Missing Columns**: Jika database lama tidak cocok, jalankan `python3 manage_database.py stats` untuk verifikasi.
- **Path Issues**: Pastikan folder `uploads/` memiliki izin tulis (biasanya otomatis jika dijalankan sebagai user biasa).

---
Panduan ini mencerminkan versi sistem terbaru dengan optimasi penyimpanan dan integrasi dashboard web. 🚀
