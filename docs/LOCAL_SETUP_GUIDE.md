# Local Setup Guide - YouTube Transcript Repository

This repository was copied from a production server. To run it locally, set the coordinator URL explicitly through `YT_PROVIDER_COORDINATOR_URL` and keep server-specific paths out of the runtime config.

## Critical Changes Required

### 1. Environment Variables (`.env`)

**Current (Server):**
```bash
EXTERNAL_VENV_DIR=/media/harry/DATA120B/venv_youtube
YT_PROVIDER_COORDINATOR_URL=<production-coordinator-url>
```

**Required (Local):**
```bash
EXTERNAL_VENV_DIR=/media/harry/DATA120B/venv_youtube
# Option 1: Use the configured coordinator URL
YT_PROVIDER_COORDINATOR_URL=http://localhost:8788

# Option 2: Point to a remote coordinator via the same env var
# YT_PROVIDER_COORDINATOR_URL=<remote-coordinator-url>
```

### 2. Virtual Environment

**IMPORTANT:** Gunakan virtual environment yang sudah ada di `/media/harry/DATA120B/venv_youtube/`

Virtual environment ini sudah dikonfigurasi dan siap digunakan. Tidak perlu recreate atau update path.

**Verify virtual environment exists:**
```bash
/media/harry/DATA120B/venv_youtube/bin/python3 --version
```

### 3. Cookie File Paths

**Current (Server):**
- `/root/YOUTUBE/cookies.txt`
- `/root/cookies.txt`
- `/root/YOUTUBE/cookies_2.txt`

**Required (Local):**
Update these references in `.env` and code:
```bash
# In .env, ensure cookies point to local paths
# The scripts will look for cookies.txt in the repo root by default
```

**Files with hardcoded cookie paths:**
- `recover_transcripts.py`
- `update_latest_channel_videos.py`
- `partial_py/youtube_transcript_complete.py`

### 4. Documentation Links

**Files with hardcoded server paths:**
- `README.md`
- `DEVELOPER_GUIDE.md`
- `PLAN.md`
- `AGENTS.md`

**Current paths to update:**
```
/media/harry/128NEW1/GIT/YOUTUBE/ → /media/harry/DATA120B/GIT/YOUTUBE/
/root/YOUTUBE/ → /media/harry/DATA120B/GIT/YOUTUBE/
```

**Example in README.md:**
```markdown
# Change from:
- [AGENTS.md](/media/harry/128NEW1/GIT/YOUTUBE/AGENTS.md)

# To:
- [AGENTS.md](./AGENTS.md)  # Use relative paths
```

### 5. Python Scripts with Hardcoded Paths

**Files that need updates:**

#### `launch_resume_queue.py`
```python
# Current:
if os.environ.get("EXTERNAL_VENV_DIR"):
    VENV_DIR = Path(os.environ.get("EXTERNAL_VENV_DIR"))
    
# This is fine as long as .env is updated correctly
```

#### `partial_py/sync_databases.py`
```python
# Current default:
default="/media/harry/128NEW1/GIT/yt_channel/channels.db"

# Should be updated or use environment variable
```

#### `partial_py/sync_files.py`
```python
# Current paths:
# Source: /media/harry/128NEW1/GIT/yt_channel/out/{channel}/text/
# Target: /media/harry/128NEW1/GIT/YOUTUBE/uploads/{channel}/text/

# Update to your actual paths
```

### 6. Script Files in `scripts/` and `partial_ops/`

**Shell scripts using hardcoded paths:**
- `scripts/transcript.sh`
- `scripts/discover.sh`
- `scripts/resume.sh`
- `scripts/format.sh`
- `run_flask.sh`
- `partial_ops/run_all_batches_server.sh`

These scripts already use environment variables, so updating `.env` should be sufficient. However, verify the logic:

```bash
# In scripts/get_venv.sh (helper for other scripts)
# VENV_DIR resolves from EXTERNAL_VENV_DIR and defaults to /media/harry/DATA120B/venv_youtube
```

### 7. Database Files

**Current (Server):**
- Database references in logs point to `/root/YOUTUBE/db/youtube_transcripts.db`
- Provider DB at `/root/services/provider_accounts.sqlite3`

**Required (Local):**
```bash
# Main database is already in db/:
/media/harry/DATA120B/GIT/YOUTUBE/db/youtube_transcripts.db

# Provider database (if using local coordinator):
/media/harry/DATA120B/GIT/YOUTUBE/provider_accounts.sqlite3
# Or use remote coordinator's DB
```

### 8. Log Files

**Server logs in `logs/` directory:**
These contain references to server paths but are historical logs. You can:
1. Keep them as-is (they won't affect operation)
2. Archive them to a separate folder
3. Delete them if not needed

**New logs will use current paths.**

### 9. Run Directories

**Existing run directories contain server paths:**
- `runs/pipeline_all_channels_20260421_103155/`
- `runs/pipeline_all_channels_20260421_103302/`

These are historical. New runs will create fresh directories with correct paths.

## Step-by-Step Setup

### Step 1: Update `.env`
```bash
cd /media/harry/DATA120B/GIT/YOUTUBE
nano .env
```

Update these lines:
```bash
EXTERNAL_VENV_DIR=/media/harry/DATA120B/venv_youtube
OUT_ROOT=out
RESUME_OUT_ROOT=out
```

### Step 2: Verify Virtual Environment
```bash
# Verify venv exists and is accessible
/media/harry/DATA120B/venv_youtube/bin/python3 --version
```

### Step 3: Update Documentation Paths
Run this command to update most documentation files:
```bash
find . -name "*.md" -type f -exec sed -i 's|/media/harry/128NEW1/GIT/YOUTUBE/|./|g' {} \;
find . -name "*.md" -type f -exec sed -i 's|/root/YOUTUBE/|./|g' {} \;
find . -name "*.md" -type f -exec sed -i 's|/media/harry/128NEW1/GIT/yt_channel/|./|g' {} \;
```

### Step 4: Update Partial Scripts
```bash
# Update sync_databases.py
sed -i 's|/media/harry/128NEW1/GIT/yt_channel/channels.db|./channels.db|g' partial_py/sync_databases.py

# Update sync_files.py
sed -i 's|/media/harry/128NEW1/GIT/yt_channel/out|./out|g' partial_py/sync_files.py
sed -i 's|/media/harry/128NEW1/GIT/YOUTUBE/uploads|./uploads|g' partial_py/sync_files.py
```

### Step 5: Test the Setup
```bash
# Test virtual environment
/media/harry/DATA120B/venv_youtube/bin/python3 --version

# Test database access
/media/harry/DATA120B/venv_youtube/bin/python3 manage_database.py stats

# Test coordinator connection (if using remote)
curl -s http://localhost:8788/health

# Test a simple script
./scripts/discover.sh --help
```

### Step 6: Verify Flask App
```bash
# Test Flask app
./run_flask.sh
# Open http://127.0.0.1:5000 in browser
```

## Coordinator Options

### Option A: Use Remote Coordinator (Recommended)
Keep `YT_PROVIDER_COORDINATOR_URL` set in `.env` so the coordinator source of truth stays explicit

**Pros:**
- No additional setup
- Uses existing API keys and quota management
- Seamless with production setup

**Cons:**
- Requires network connectivity
- Depends on server availability

### Option B: Run Local Coordinator (Advanced)
1. Copy coordinator server code from server
2. Set up local database
3. Configure local API keys
4. Update `.env`: set `YT_PROVIDER_COORDINATOR_URL` to the correct coordinator URL

**See:** `ssh yt-server 'cat /root/services/COORDINATOR_GUIDE.md'` for setup instructions

## Troubleshooting

### Issue: "Virtualenv tidak ditemukan"
**Solution:** Ensure `.env` has correct `EXTERNAL_VENV_DIR` pointing to `/media/harry/DATA120B/venv_youtube`

### Issue: "Coordinator tidak bisa dihubungi"
**Solution:** 
- Check network connectivity to the configured coordinator URL
- Or set up local coordinator
- Or temporarily disable coordinator-dependent features

### Issue: "Cookies not found"
**Solution:**
- Place `cookies.txt` in repo root
- Update cookie file paths in relevant scripts

### Issue: "Database not found"
**Solution:**
- Ensure `db/youtube_transcripts.db` exists in repo root
- Check database path in `.env` if customized

## Files Summary

### Files to Update:
1. ✅ `.env` - **CRITICAL**
2. ⚠️ `README.md` - **UPDATE PATHS**
3. ⚠️ `DEVELOPER_GUIDE.md` - **UPDATE PATHS**
4. ⚠️ `PLAN.md` - **UPDATE PATHS**
5. ⚠️ `AGENTS.md` - **UPDATE PATHS**
6. ⚠️ `partial_py/sync_databases.py` - **UPDATE PATHS**
7. ⚠️ `partial_py/sync_files.py` - **UPDATE PATHS**

### Files That Should Work with `.env` Update:
- `scripts/*.sh` (use environment variables)
- `launch_resume_queue.py` (reads from .env)
- `update_latest_channel_videos.py` (uses Path(__file__).parent)
- `flask_app/app.py` (uses relative paths)

### Historical Files (Can Ignore or Archive):
- `logs/*.log` - contains server paths but won't affect operation
- `runs/pipeline_all_channels_*` - historical run directories
- `recover_transcripts.log` - historical log

## Verification Checklist

After completing the setup, verify:

- [ ] `.env` has correct `EXTERNAL_VENV_DIR=/media/harry/DATA120B/venv_youtube`
- [ ] `.env` has correct `YT_PROVIDER_COORDINATOR_URL`
- [ ] Virtual environment exists and is accessible: `/media/harry/DATA120B/venv_youtube/bin/python3 --version`
- [ ] All dependencies installed: `/media/harry/DATA120B/venv_youtube/bin/pip list`
- [ ] Database accessible: `/media/harry/DATA120B/venv_youtube/bin/python3 manage_database.py stats`
- [ ] Coordinator accessible (if using remote): `curl "$YT_PROVIDER_COORDINATOR_URL/health"`
- [ ] Documentation paths updated (no more `/root/YOUTUBE/` in README)
- [ ] Cookie files exist in correct location
- [ ] Flask app starts: `./run_flask.sh`
- [ ] Wrapper scripts work: `./scripts/discover.sh --help`

## Next Steps

Once setup is complete:
1. Run a smoke test: `./scripts/discover.sh --latest-only --channel-id <test_channel>`
2. Verify logs show correct paths (not `/root/YOUTUBE/`)
3. Test transcript download: `./scripts/transcript.sh --video-id <test_video>`
4. Test resume generation (if coordinator available)

---

**Last Updated:** 2026-04-25  
**Repository Root:** `/media/harry/DATA120B/GIT/YOUTUBE`  
**Server Origin:** production coordinator server
