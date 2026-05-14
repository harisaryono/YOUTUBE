# Provider Model Migration Summary

## 📅 Date: 2026-03-26

---

## ✅ Migration Completed

### **NVIDIA Accounts (13 total)**

**Before:**
- `mistralai/mistral-small-24b-instruct`: 11 accounts
- `openai/gpt-oss-120b`: 2 accounts

**After:**
- `openai/gpt-oss-120b`: **13 accounts** ✅

**Migrated Accounts:**
```
ID  | Account Name                        | Old Model                        | New Model
----|-------------------------------------|----------------------------------|---------------------
3   | nvidia 10 | unmapped.x0_c1we3       | mistral-small-24b                | gpt-oss-120b ✅
4   | nvidia 2 | silfi                    | mistral-small-24b                | gpt-oss-120b ✅
5   | nvidia 3 | albert                   | mistral-small-24b                | gpt-oss-120b ✅
6   | nvidia 11 | unmapped.he_uum         | mistral-small-24b                | gpt-oss-120b ✅
7   | nvidia 4 | budi                     | mistral-small-24b                | gpt-oss-120b ✅
8   | nvidia 5 | noah                     | mistral-small-24b                | gpt-oss-120b ✅
9   | nvidia 6 | affiliationcenter@gmail  | mistral-small-24b                | gpt-oss-120b ✅
23  | nvidia 7 | freddy.king2009          | mistral-small-24b                | gpt-oss-120b ✅
24  | nvidia 8 | bambangsukempit          | mistral-small-24b                | gpt-oss-120b ✅
25  | nvidia 9 | realmrkt                 | mistral-small-24b                | gpt-oss-120b ✅
26  | nvidia 1 | harry                    | mistral-small-24b                | gpt-oss-120b ✅
28  | nvidia 12 | mil42                   | gpt-oss-120b (already)           | gpt-oss-120b ✅
29  | nvidia 13 | materikursus2014        | gpt-oss-120b (already)           | gpt-oss-120b ✅
```

---

### **Groq Accounts (12 total)**

**Before:**
- `llama-3.3-70b-versatile`: 12 accounts
- `openai/gpt-oss-120b`: 0 accounts

**After:**
- `openai/gpt-oss-120b`: **12 accounts** ✅

**Migrated Accounts:**
```
ID  | Account Name              | Old Model               | New Model
----|---------------------------|-------------------------|---------------------
10  | groq 1 | hari.saryoo      | llama-3.3-70b           | gpt-oss-120b ✅
11  | groq 2 | silfi.anggraini  | llama-3.3-70b           | gpt-oss-120b ✅
12  | groq 3 | albumkenangan    | llama-3.3-70b           | gpt-oss-120b ✅
13  | groq 4 | kailin           | llama-3.3-70b           | gpt-oss-120b ✅
14  | groq 5 | amdal.dlhkp      | llama-3.3-70b           | gpt-oss-120b ✅
15  | groq 6 | labforce         | llama-3.3-70b           | gpt-oss-120b ✅
16  | groq 7 | wadicasa         | llama-3.3-70b           | gpt-oss-120b ✅
17  | groq 8 | albertprk        | llama-3.3-70b           | gpt-oss-120b ✅
18  | groq 9 | noahjohnson      | llama-3.3-70b           | gpt-oss-120b ✅
19  | groq 10 | ituaja          | llama-3.3-70b           | gpt-oss-120b ✅
20  | groq 11 | affcenter       | llama-3.3-70b           | gpt-oss-120b ✅
22  | groq 12 | mil42undip      | llama-3.3-70b           | gpt-oss-120b ✅
```

---

### **Cerebras Accounts (1 total)**

**No changes:**
```
ID  | Account Name       | Model                          | Status
----|--------------------|--------------------------------|--------
21  | CEREBRAS_API_KEY1  | qwen-3-235b-a22b-instruct-2507 | ✅ Kept
```

---

## 🚫 TPD Blocking Status

### **Historical Blocks (Cleared)**

**Old Groq Blocks (from 2026-03-24):**
- All blocks were for `llama-3.3-70b-versatile` and `openai/gpt-oss-120b`
- TPD Limit: 200,000 tokens/day
- Blocked until: 2026-03-25T00:05:00+07:00
- **Status**: ✅ ALL CLEARED (expired + auto-clear script running)

### **Current Status:**

| Provider | Active Blocks? | TPD Blocking Still Applies? |
|----------|----------------|----------------------------|
| **NVIDIA** | ❌ NO | ❌ NO (unlimited) |
| **Groq** | ❌ NO | ⚠️ YES (but using different model now) |
| **Cerebras** | ❌ NO | ⚠️ YES (but quota not reached) |

---

## ⚠️ Important Notes

### **Groq TPD Blocking:**

1. **Old blocks were for `llama-3.3-70b-versatile`**
   - TPD: 200,000 tokens/day per account
   - Blocks expired on 2026-03-25

2. **New model is `openai/gpt-oss-120b`**
   - Different model = separate TPD quota
   - Need to monitor new quota usage

3. **TPD blocking logic still active:**
   ```python
   PROVIDERS_WITH_TPD_BLOCKING = {"groq", "cerebras", "openrouter"}
   ```

### **Auto-Clear System:**

✅ **Active on yt-server:**
- Cron job: `*/5 * * * *` (every 5 minutes)
- Coordinator: Auto-clear on acquire/status
- Script: `/root/services/auto_clear_expired_blocks.py`

---

## 📊 Current Provider Summary

| Provider | Accounts | Model | Blocking? | Priority |
|----------|----------|-------|-----------|----------|
| **nvidia** | 13 | openai/gpt-oss-120b | ❌ NO | PRIMARY ✅ |
| **groq** | 12 | openai/gpt-oss-120b | ⚠️ YES | SECONDARY |
| **cerebras** | 1 | qwen-3-235b-a22b-instruct | ⚠️ YES | TERTIARY |
| **gemini** | 1 | gemini-2.5-flash | ⚠️ YES | FALLBACK |

---

## 🎯 Recommended Usage

### **For Resume Generation:**

```bash
# Primary (NVIDIA - no blocking)
/media/harry/DATA120B/venv_youtube/bin/python3 fill_missing_resumes_youtube_db.py \
  --db youtube_transcripts.db \
  --provider nvidia \
  --model openai/gpt-oss-120b \
  --limit 250

# Secondary (Groq - with TPD monitoring)
/media/harry/DATA120B/venv_youtube/bin/python3 fill_missing_resumes_youtube_db.py \
  --db youtube_transcripts.db \
  --provider groq \
  --model openai/gpt-oss-120b \
  --limit 50

# Fallback (Cerebras)
/media/harry/DATA120B/venv_youtube/bin/python3 fill_missing_resumes_youtube_db.py \
  --db youtube_transcripts.db \
  --provider cerebras \
  --model qwen-3-235b-a22b-instruct-2507 \
  --limit 20
```

### **Worker Configuration (.env):**

```bash
# Provider priority
RESUME_PROVIDER_PRIORITY="nvidia,groq,cerebras"

# Blocking policy
RESUME_NVIDIA_NO_BLOCKING=1
RESUME_GROQ_TPD_BLOCKING=1
RESUME_CEREBRAS_TPD_BLOCKING=1

# Model
RESUME_MODEL="openai/gpt-oss-120b"
```

---

## 🔍 Monitoring Commands

### **Check Provider Usage:**

```bash
# Local database
sqlite3 /media/harry/128NEW1/services/provider_accounts.sqlite3 \
  "SELECT provider, model_name, COUNT(*) FROM provider_accounts GROUP BY provider, model_name;"

# Coordinator API
curl -s http://localhost:8788/v1/status/accounts | python3 -m json.tool
```

### **Check Active Blocks:**

```bash
sqlite3 /media/harry/128NEW1/services/provider_accounts.sqlite3 \
  "SELECT provider, model_name, blocked_until FROM provider_account_model_blocks WHERE blocked_until > datetime('now');"
```

### **Check Auto-Clear Logs:**

```bash
ssh yt-server "tail -20 /root/services/auto_clear_blocks.log"
```

---

## ✅ Migration Verification

```bash
# Verify NVIDIA (should be 13 with gpt-oss-120b)
sqlite3 /media/harry/128NEW1/services/provider_accounts.sqlite3 \
  "SELECT COUNT(*) FROM provider_accounts WHERE provider='nvidia' AND model_name='openai/gpt-oss-120b';"
# Expected: 13

# Verify Groq (should be 12 with gpt-oss-120b)
sqlite3 /media/harry/128NEW1/services/provider_accounts.sqlite3 \
  "SELECT COUNT(*) FROM provider_accounts WHERE provider='groq' AND model_name='openai/gpt-oss-120b';"
# Expected: 12

# Verify no old models remain
sqlite3 /media/harry/128NEW1/services/provider_accounts.sqlite3 \
  "SELECT COUNT(*) FROM provider_accounts WHERE model_name LIKE '%mistral-small%' OR model_name LIKE '%llama-3.3%';"
# Expected: 0
```

---

**Last Updated:** 2026-03-26
**Status:** ✅ ALL MIGRATED TO `openai/gpt-oss-120b`
