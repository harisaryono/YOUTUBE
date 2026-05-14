# YouTube Metadata Auto-Fetch - Cron Setup

## 📋 Setup Cron Job untuk Auto-Fetch Harian

### 1. Buat Cron Job

```bash
# Edit crontab
crontab -e

# Tambahkan baris berikut (fetch setiap jam 2 pagi):
0 2 * * * /media/harry/128NEW1/GIT/YOUTUBE/auto_fetch_daily.sh

# Save dan exit
```

### 2. Jadwal Fetch

Script akan jalan otomatis:
- **Waktu**: 02:00 pagi setiap hari
- **Quota**: 5,000 videos/hari (setengah dari limit 10,000)
- **Log**: `/media/harry/128NEW1/GIT/YOUTUBE/logs/youtube_api_YYYYMMDD.log`

### 3. Cek Status

```bash
# Lihat log hari ini
tail -f /media/harry/128NEW1/GIT/YOUTUBE/logs/youtube_api_$(date +%Y%m%d).log

# Lihat log kemarin
cat /media/harry/128NEW1/GIT/YOUTUBE/logs/youtube_api_$(date -d yesterday +%Y%m%d).log

# Cek cron job yang aktif
crontab -l

# Cek cron log
grep CRON /var/log/syslog | tail -20
```

### 4. Manual Trigger (Test)

```bash
# Jalankan manual untuk test
cd /media/harry/128NEW1/GIT/YOUTUBE
./auto_fetch_daily.sh
```

### 5. Monitoring Dashboard

```bash
# Buat script monitoring
cat > /media/harry/128NEW1/GIT/YOUTUBE/check_status.sh << 'EOF'
#!/bin/bash
echo "=== YouTube API Status ==="
echo ""
echo "📊 Database Stats:"
sqlite3 youtube_transcripts.db "
SELECT 
  'Total Videos: ' || COUNT(*) ||
  ' | With Duration: ' || SUM(CASE WHEN duration > 0 THEN 1 ELSE 0 END) ||
  ' | With Views: ' || SUM(CASE WHEN view_count > 0 THEN 1 ELSE 0 END)
FROM videos;
"
echo ""
echo "📝 Recent Logs:"
ls -lt logs/*.log | head -5
echo ""
echo "📅 Last Fetch:"
grep "completed successfully" logs/*.log | tail -1
EOF

chmod +x /media/harry/128NEW1/GIT/YOUTUBE/check_status.sh

# Jalankan monitoring
./check_status.sh
```

### 6. Estimate Waktu Selesai

Dengan 27,374 videos dan fetch 5,000/hari:

```
Total videos: 27,374
Already fetched: ~10,000
Remaining: ~17,000

Days needed: 17,000 / 5,000 = ~3-4 hari
```

### 7. Pause/Resume Auto-Fetch

```bash
# Pause (comment out di crontab)
crontab -e
# Tambahkan # di depan line: #0 2 * * * /path/to/auto_fetch_daily.sh

# Resume (hapus #)
crontab -e
# Hapus #: 0 2 * * * /path/to/auto_fetch_daily.sh
```

### 8. Change Schedule

```bash
# Edit crontab
crontab -e

# Pilihan jadwal:

# Setiap hari jam 2 pagi (default)
0 2 * * * /media/harry/128NEW1/GIT/YOUTUBE/auto_fetch_daily.sh

# Setiap hari jam 1 pagi
0 1 * * * /media/harry/128NEW1/GIT/YOUTUBE/auto_fetch_daily.sh

# Setiap 12 jam (2x sehari)
0 */12 * * * /media/harry/128NEW1/GIT/YOUTUBE/auto_fetch_daily.sh

# Senin-Jumat jam 3 pagi
0 3 * * 1-5 /media/harry/128NEW1/GIT/YOUTUBE/auto_fetch_daily.sh
```

---

## 🔧 Troubleshooting

### Script tidak jalan?

```bash
# Cek cron service
sudo systemctl status cron

# Start jika tidak aktif
sudo systemctl start cron
sudo systemctl enable cron
```

### Permission denied?

```bash
# Fix permission
chmod +x /media/harry/128NEW1/GIT/YOUTUBE/auto_fetch_daily.sh
chmod 755 /media/harry/128NEW1/GIT/YOUTUBE/logs
```

### API Key expired?

```bash
# Update API key di script
nano /media/harry/128NEW1/GIT/YOUTUBE/auto_fetch_daily.sh
# Ganti API_KEY="..." dengan key yang baru
```

---

## 📊 Monitoring via Web

Akses Flask app untuk lihat progress:
- Homepage: http://localhost:5000
- Statistics API: http://localhost:5000/api/statistics

---

**Setup selesai! Auto-fetch akan mulai besok pagi jam 2.**
