# Ringkasan Perubahan yang Diperlukan

Repository ini adalah copy dari server production. Berikut adalah **satu-satunya** perubahan yang WAJIB dilakukan untuk menjalankannya secara lokal.

## 🔴 SATU PERUBAHAN KRITIS - WAJIB

### Update `.env` File

Edit file `.env` dan ubah baris ini:

```bash
# DARI:
EXTERNAL_VENV_DIR=/media/harry/DATA120B/venv_youtube

# KE:
EXTERNAL_VENV_DIR=/media/harry/DATA120B/venv_youtube
```

**Itu saja!** Setelah mengubah ini, sistem akan berjalan dengan menggunakan virtual environment yang sudah ada.

## ✅ Perubahan Opsional (Disarankan tapi Tidak Wajib)

### 1. Update Path di Dokumentasi

Jalankan perintah ini untuk memperbarui semua path di file markdown:

```bash
cd /media/harry/DATA120B/GIT/YOUTUBE
find . -name "*.md" -type f -exec sed -i 's|/media/harry/128NEW1/GIT/YOUTUBE/|./|g' {} \;
find . -name "*.md" -type f -exec sed -i 's|/root/YOUTUBE/|./|g' {} \;
find . -name "*.md" -type f -exec sed -i 's|/media/harry/128NEW1/GIT/yt_channel/|./|g' {} \;
```

Ini hanya mengubah tampilan di dokumentasi, tidak mempengaruhi fungsi sistem.

### 2. Update Partial Scripts

```bash
sed -i 's|/media/harry/128NEW1/GIT/yt_channel/channels.db|./channels.db|g' partial_py/sync_databases.py
sed -i 's|/media/harry/128NEW1/GIT/yt_channel/out|./out|g' partial_py/sync_files.py
sed -i 's|/media/harry/128NEW1/GIT/YOUTUBE/uploads|./uploads|g' partial_py/sync_files.py
```

Ini hanya diperlukan jika Anda akan menjalankan script `sync_databases.py` atau `sync_files.py`.

## ⚠️ Yang Tidak Perlu Diubah

### Virtual Environment
**JANGAN** recreate virtual environment baru! Gunakan yang sudah ada di `/media/harry/DATA120B/venv_youtube/`

### Coordinator
Biarkan `YT_PROVIDER_COORDINATOR_URL=http://8.215.77.132:8788` di `.env` - ini menggunakan production coordinator yang sudah jalan.

### Database
Database aktif sekarang disimpan di `db/` dan root filename dipertahankan sebagai symlink kompatibilitas.

### Cookie Files
Cookie files sudah di root repository (`cookies.txt`, `cookies_2.txt`) dan tidak perlu diubah.

## 🚀 Quick Start

Setelah mengubah `.env`, langsung bisa digunakan:

```bash
# 1. Update .env
nano .env
# Ubah: EXTERNAL_VENV_DIR=/media/harry/DATA120B/venv_youtube

# 2. Test venv
/media/harry/DATA120B/venv_youtube/bin/python3 --version

# 3. Test database
/media/harry/DATA120B/venv_youtube/bin/python3 manage_database.py stats

# 4. Jalankan aplikasi
./run_flask.sh
# Buka http://127.0.0.1:5000
```

## 📋 File yang Perlu Diupdate

### Wajib (1 file):
- ✅ `.env` - **SATA-SATUNYA yang wajib diubah**

### Opsional (3 file):
- ⚠️ `partial_py/sync_databases.py` - hanya jika pakai sync
- ⚠️ `partial_py/sync_files.py` - hanya jika pakai sync
- ⚠️ Dokumentasi `*.md` - hanya untuk tampilan

### Tidak perlu diubah:
- ✅ Semua script di `scripts/`
- ✅ `launch_resume_queue.py`
- ✅ `flask_app/app.py`
- ✅ Database files
- ✅ Cookie files
- ✅ Logs (historical)

## 🔍 Verifikasi

Setelah mengubah `.env`, jalankan test ini:

```bash
# Test venv
/media/harry/DATA120B/venv_youtube/bin/python3 --version

# Test database
/media/harry/DATA120B/venv_youtube/bin/python3 manage_database.py stats

# Test coordinator
curl -s http://8.215.77.132:8788/health

# Test script
./scripts/discover.sh --help
```

## 📖 Dokumentasi Lengkap

Untuk detail lebih lengkap:
- `CRITICAL_CHANGES.md` - Ringkasan perubahan prioritas
- `LOCAL_SETUP_GUIDE.md` - Panduan setup lengkap
- `README.md` - Dokumentasi umum sistem

---

**Kesimpulan:** Hanya ubah 1 baris di `.env` dan sistem siap digunakan!
