---
{
  "id": "file_s4urb9bg",
  "filetype": "document",
  "filename": "DEVELOPER_GUIDE",
  "created_at": "2026-04-11T02:06:11.463Z",
  "updated_at": "2026-04-11T04:36:08.765Z",
  "meta": {
    "location": "/",
    "tags": [],
    "categories": [],
    "description": "",
    "source": "markdown"
  }
}
---
# Developer & Operations Guide: YouTube Resume System

Panduan ini ditujukan bagi programmer berikutnya untuk memahami, menjalankan, dan memelihara sistem koordinasi API serta pembuatan resume otomatis.

## 0. Struktur Repo

- Root repo hanya untuk prosedur global.
- Script yang parsial, legacy, migration, repair satu kali, channel-specific, atau eksperimen dipindah ke `partial_py/`.
- Dokumen yang parsial, legacy, setup sekali jalan, migration note, atau arsip hasil lama dipindah ke `partial_docs/`.
- Helper operasional non-utama seperti fallback tunnel dipindah ke `partial_ops/`.
- Sebelum membuat program baru yang menyentuh provider, lease, preflight, block, model, queue, transcript formatting, atau resume generation, programmer **wajib** membaca guide coordinator yang aktif di server:

```bash
ssh yt-server 'sed -n "1,260p" /root/services/COORDINATOR_GUIDE.md'
```

- Copy server di `/root/services/COORDINATOR_GUIDE.md` adalah source of truth operasional. Jangan membuat script baru dengan asumsi lokal jika belum membaca guide server itu.
- Jika memang perlu menjalankan script parsial, jalankan dari root repo dengan:

```bash
/media/harry/DATA120B/venv_youtube/bin/python -m partial_py.nama_script_tanpa_py
```

## 1. Arsitektur Sistem

Sistem ini bekerja dengan skema Client‑Coordinator:

- **Coordinator (**`yt-server`**)**: Menyimpan seluruh API Key secara terenkripsi. Mengelola antrean (leasing) akun agar tidak terjadi tabrakan kuota/rate-limit.
- **Worker (**`harry-pc`**)**: Menjalankan script pemrosesan resume secara paralel yang mengambil API Key dari coordinator sesuai kebutuhan.

## 2. Sinkronisasi & Koneksi Server

Coordinator berjalan di `yt-server` (IP: `8.215.77.132`) pada port `8788`.

### Memeriksa Status Server

Login ke `yt-server` dan cek apakah layanan aktif:

```bash
# Di server (yt-server)
ps aux | grep provider_coordinator_server.py
curl -s http://localhost:8788/v1/status/accounts
```

### SSH Tunneling (Akses Lokal)

Jika port `8788` terblokir firewall, jalankan tunnel dari terminal lokal:

```bash
ssh -L 8788:localhost:8788 yt-server
```

Lalu pastikan `.env` lokal Anda mengarah ke: `YT_PROVIDER_COORDINATOR_URL=http://localhost:8788`

Catatan: ini fallback darurat saja, bukan jalur utama.

## 3. Manajemen API Key

API Key disimpan di `provider_accounts.sqlite3` dalam kolom `api_key` dengan awalan `ENC:`.

- **Enkripsi**: Menggunakan Fernet (AES-128).
- **Key**: Membutuhkan `PROVIDER_ENCRYPTION_KEY` di environment variable untuk dekripsi.
- **Leasing**: Client harus melakukan `acquire_accounts` untuk mendapatkan `lease_token` sebelum bisa mengambil `api_key`.

## 4. Pembuatan Resume Massal

Jalur utama sekarang:

- `launch_resume_queue.py` untuk antrean resume lintas akun
- `fill_missing_resumes_youtube_db.py` sebagai worker resume utama
- `update_latest_channel_videos.py` untuk pipeline global discovery -&gt; transcript -&gt; resume

`launch_universal_resume.py` dipindah ke `partial_py/` karena itu launcher lama yang statis.

### Cara Menjalankan

```bash
# Di harry-pc (direktori repo)
/media/harry/DATA120B/venv_youtube/bin/python3 launch_resume_queue.py
```

### Konfigurasi Launcher

Di dalam `launch_resume_queue.py`, pola utama sekarang adalah:

- gunakan semua akun aktif yang cocok model
- `Groq` sebagai jalur utama selama quota masih ada
- `NVIDIA` sebagai fallback stabil
- sisa task `Groq` yang berhenti dipindah ke antrean `NVIDIA`

### Pemantauan (Monitoring)

- **Logs**: Periksa `out/agent_logs/resume_*.log` untuk melihat aktivitas leasing dan API call.
- **Progress**: Cek `tmp/reports/report_*.csv` untuk status keberhasilan tiap video.

## 5. Tips Debugging & Troubleshooting

- **Target Rows: 0**: Pastikan sumber task memang berasal dari DB dan file transcript yang benar.
- **Path Mismatch**: Terkadang ada prefix `001_` di nama file fisik tapi tidak ada di database. Sesuaikan `link_file` di database jika worker melaporkan `transcript_not_found`.
- **Timeout**: Jika coordinator lambat merespon (timeout), pastikan server tidak sedang overload.

## 6. Provider API Blocking Policy

### ✅ NVIDIA - NO BLOCKING (Primary Provider)

**NVIDIA TIDAK memiliki TPD (Tokens Per Day) blocking:**

- Unlimited / quota sangat besar
- Selalu aktif 24/7
- Tidak perlu auto-block saat error
- Jadikan **primary provider** untuk batch processing

```bash
# NVIDIA API Keys - Always Active
NVIDIA_API_KEY_1="nvapi-..."  # ✅ No blocking
NVIDIA_API_KEY_2="nvapi-..."  # ✅ No blocking
NVIDIA_API_KEY_3="nvapi-..."  # ✅ No blocking
```

### ⚠️ Groq, Cerebras, OpenRouter - WITH TPD BLOCKING (Secondary)

**Provider ini memiliki TPD limit dan akan auto-block:**

| Provider | TPD Limit | Block Duration | Reset Time |
| --- | --- | --- | --- |
| Groq | \~10K-30K tokens/day | Until midnight PT | 00:00 Pacific |
| Cerebras | \~100K tokens/day | Until midnight PT | 00:00 Pacific |
| OpenRouter | Credit-based | Until top-up | Manual |

**Auto-Block Logic:**

```python
# Block jika TPD >= 95% used
if used >= int(limit * 0.95) or (used + requested) >= int(limit * 0.95):
    blocked_until = next_local_midnight().isoformat()
    coordinator_report_model_block(
        provider_account_id=account.id,
        provider=account.provider,  # groq, cerebras, etc.
        model_name=account.model_name,
        blocked_until=blocked_until,
        reason="TPD limit exceeded"
    )
```

### 🎯 Provider Priority Strategy

```python
worker_counts = {
    "nvidia": 7,      # ✅ PRIMARY - no blocking, unlimited
    "groq": 5,        # ⚠️ SECONDARY - TPD blocking enabled
    "cerebras": 1     # ⚠️ TERTIARY - TPD blocking enabled
}
```

**Best Practice:**

1. Gunakan NVIDIA untuk batch processing besar (27K+ videos)
2. Groq/Cerebras sebagai fallback atau untuk testing
3. Monitor TPD usage Groq/Cerebras via coordinator dashboard
4. Ja
5. an block NVIDIA - selalu retry pada error

---

**Penting**: Selalu rujuk ke `AGENTS.md` untuk prinsip operasional repo ini.
