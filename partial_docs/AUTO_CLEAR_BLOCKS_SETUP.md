# Auto-Clear Expired Blocks Setup

Panduan lengkap untuk auto-clear expired model blocks di provider coordinator.

## 📋 Overview

Sistem ini otomatis menghapus block model yang sudah expired (blocked_until < now) sehingga:
- Groq/Cerebras/OpenRouter accounts otomatis tersedia kembali setelah midnight PT
- Tidak perlu manual clear blocks
- Workers bisa langsung acquire lease tanpa hambatan

## 🏗️ Arsitektur

### 1. **Cron Job (Auto-Clear Berkala)**

File: `/root/services/auto_clear_expired_blocks.py`

Jalan setiap 5 menit via cron:
```bash
*/5 * * * * python3 /root/services/auto_clear_expired_blocks.py
```

**Cara kerja:**
- Check database setiap 5 menit
- Hapus blocks dengan `blocked_until < now (UTC)`
- Log ke `/root/services/auto_clear_blocks.log`

### 2. **Coordinator Server (Auto-Clear on Request)**

File: `/root/services/provider_coordinator_server.py`

Fungsi baru: `cleanup_expired_blocks(con)`

Dipanggil otomatis saat:
- `handle_acquire()` - saat worker request lease
- `handle_status_accounts()` - saat check status

**Cara kerja:**
- Clear expired blocks sebelum acquire lease
- Log event "block_cleared" ke audit table
- Return jumlah blocks yang di-clear

## 📊 Provider Blocking Policy

| Provider | Blocking? | Reset Time | Auto-Clear? |
|----------|-----------|------------|-------------|
| **NVIDIA** | ❌ NO | N/A | N/A |
| **Groq** | ⚠️ YES | 00:00 PT | ✅ YES |
| **Cerebras** | ⚠️ YES | 00:00 PT | ✅ YES |
| **OpenRouter** | ⚠️ YES | Manual | ✅ YES |

## 🔧 Setup

### 1. Copy Scripts ke yt-server

```bash
# Auto-clear script
scp auto_clear_expired_blocks.py yt-server:/root/services/

# Patch coordinator (jika belum)
scp patch_coordinator_auto_clear.py yt-server:/root/services/
ssh yt-server "cd /root/services && python3 patch_coordinator_auto_clear.py"
```

### 2. Setup Cron Job

```bash
ssh yt-server "crontab -l | grep -v auto_clear; echo '*/5 * * * * python3 /root/services/auto_clear_expired_blocks.py' | crontab -"
```

### 3. Restart Coordinator Server

```bash
ssh yt-server "pkill -9 -f provider_coordinator_server"
ssh yt-server "cd /root/services && nohup python3 provider_coordinator_server.py --port 8788 > coordinator_server.log 2>&1 &"
```

### 4. Verify Setup

```bash
# Check cron job
ssh yt-server "crontab -l"

# Check coordinator patch
ssh yt-server "grep -c 'cleanup_expired_blocks' /root/services/provider_coordinator_server.py"

# Check health
ssh yt-server "curl -s http://localhost:8788/health"

# Check logs
ssh yt-server "tail -20 /root/services/auto_clear_blocks.log"
```

## 📝 Monitoring

### Check Active Blocks

```bash
ssh yt-server "sqlite3 /root/services/provider_accounts.sqlite3 \"SELECT provider, COUNT(*) as blocks FROM provider_account_model_blocks GROUP BY provider;\""
```

### Check Auto-Clear Logs

```bash
ssh yt-server "tail -50 /root/services/auto_clear_blocks.log"
```

### Check Coordinator Logs

```bash
ssh yt-server "tail -50 /root/services/coordinator_server.log"
```

### Manual Trigger

```bash
ssh yt-server "python3 /root/services/auto_clear_expired_blocks.py"
```

## 🎯 Expected Behavior

### Scenario: Groq TPD Exceeded

1. **Worker mendapat error TPD:**
   ```
   Error 429: Rate limit reached - TPD: Limit 200000, Used 199922
   ```

2. **Worker report block:**
   ```python
   coordinator_report_model_block(
       provider="groq",
       model_name="openai/gpt-oss-120b",
       blocked_until="2026-03-26T00:05:00+07:00"  # Midnight PT
   )
   ```

3. **Block stored in database:**
   ```sql
   INSERT INTO provider_account_model_blocks
   (provider, model_name, blocked_until)
   VALUES ('groq', 'openai/gpt-oss-120b', '2026-03-26T00:05:00+07:00')
   ```

4. **Auto-clear saat waktu tiba:**
   - Cron job (setiap 5 menit) check expired blocks
   - Coordinator (saat acquire) check expired blocks
   - Block dihapus otomatis saat `blocked_until < now`

5. **Worker bisa acquire lagi:**
   ```
   ✅ Lease granted - groq/openai/gpt-oss-120b
   ```

## 📈 Statistics

### View Summary

```bash
ssh yt-server "python3 -c \"
import sqlite3, json
c = sqlite3.connect('/root/services/provider_accounts.sqlite3')
print('Active Blocks:')
for row in c.execute('SELECT provider, COUNT(*) FROM provider_account_model_blocks GROUP BY provider'):
    print(f'  {row[0]}: {row[1]}')
print('\\nAccounts by Provider:')
for row in c.execute('SELECT provider, COUNT(*), SUM(is_active) FROM provider_accounts GROUP BY provider'):
    print(f'  {row[0]}: {row[1]} total, {row[2]} active')
\""
```

## ⚠️ Troubleshooting

### Blocks Tidak Clear Otomatis

1. Check cron job:
   ```bash
   ssh yt-server "crontab -l"
   ```

2. Check cron log:
   ```bash
   ssh yt-server "grep CRON /var/log/syslog | tail -10"
   ```

3. Manual run:
   ```bash
   ssh yt-server "python3 /root/services/auto_clear_expired_blocks.py"
   ```

### Coordinator Tidak Clear Blocks

1. Check patch applied:
   ```bash
   ssh yt-server "grep 'cleanup_expired_blocks' /root/services/provider_coordinator_server.py"
   ```

2. Restart coordinator:
   ```bash
   ssh yt-server "pkill -f provider_coordinator_server && cd /root/services && nohup python3 provider_coordinator_server.py --port 8788 > coordinator_server.log 2>&1 &"
   ```

3. Test acquire:
   ```bash
   curl -s http://localhost:8788/v1/status/accounts | python3 -m json.tool
   ```

### Timezone Issues

Blocked_until format: ISO8601 dengan timezone (`2026-03-26T00:05:00+07:00`)

Comparison: UTC time (`datetime.now(timezone.utc).isoformat()`)

ISO8601 strings adalah lexicographically sortable, jadi comparison langsung works.

## 📚 Files

| File | Location | Purpose |
|------|----------|---------|
| `auto_clear_expired_blocks.py` | `/root/services/` | Cron job script |
| `auto_clear_blocks.log` | `/root/services/` | Auto-clear log |
| `provider_coordinator_server.py` | `/root/services/` | Coordinator (patched) |
| `provider_accounts.sqlite3` | `/root/services/` | Database |
| `patch_coordinator_auto_clear.py` | `/media/harry/.../YOUTUBE/` | Patch script (local) |

## ✅ Checklist

- [x] Auto-clear script copied to yt-server
- [x] Cron job installed (*/5 * * * *)
- [x] Coordinator patched with cleanup_expired_blocks
- [x] Coordinator restarted
- [x] Logs verified
- [x] Manual test passed

---

**Last Updated:** 2026-03-26
**Status:** ✅ OPERATIONAL
