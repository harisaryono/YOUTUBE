# Critical Changes Required - Summary

This document summarizes the **MOST CRITICAL** changes needed to run this repository locally.

## 🔴 URGENT - Must Change Before Running Anything

### 1. Update `.env` File

Edit `.env` and change:
```bash
# FROM:
EXTERNAL_VENV_DIR=/media/harry/DATA120B/venv_youtube

# TO:
EXTERNAL_VENV_DIR=/media/harry/DATA120B/venv_youtube
```

**IMPORTANT:** Gunakan virtual environment yang sudah ada di `/media/harry/DATA120B/venv_youtube/` - jangan recreate venv baru!

## 🟡 HIGH PRIORITY - Should Change Soon

### 2. Update Documentation Paths

Run these commands to fix documentation links:

```bash
cd /media/harry/DATA120B/GIT/YOUTUBE
find . -name "*.md" -type f -exec sed -i 's|/media/harry/128NEW1/GIT/YOUTUBE/|./|g' {} \;
find . -name "*.md" -type f -exec sed -i 's|/root/YOUTUBE/|./|g' {} \;
find . -name "*.md" -type f -exec sed -i 's|/media/harry/128NEW1/GIT/yt_channel/|./|g' {} \;
```

### 3. Update Partial Scripts

```bash
# Fix sync_databases.py
sed -i 's|/media/harry/128NEW1/GIT/yt_channel/channels.db|./channels.db|g' partial_py/sync_databases.py

# Fix sync_files.py
sed -i 's|/media/harry/128NEW1/GIT/yt_channel/out|./out|g' partial_py/sync_files.py
sed -i 's|/media/harry/128NEW1/GIT/YOUTUBE/uploads|./uploads|g' partial_py/sync_files.py
```

## 🟢 LOW PRIORITY - Optional but Recommended

### 4. Cookie Files

Ensure you have:
- `cookies.txt` in repository root
- Or update paths in scripts if using different location

### 5. Clean Up Historical Data

These can be archived or deleted (won't affect operation):
- `logs/*.log` files (historical server logs)
- `runs/pipeline_all_channels_*` directories (historical run data)

## Quick Start Command

After updating `.env`, run this one-liner to fix everything:

```bash
cd /media/harry/DATA120B/GIT/YOUTUBE && \
find . -name "*.md" -type f -exec sed -i 's|/media/harry/128NEW1/GIT/YOUTUBE/|./|g' {} \; && \
find . -name "*.md" -type f -exec sed -i 's|/root/YOUTUBE/|./|g' {} \; && \
find . -name "*.md" -type f -exec sed -i 's|/media/harry/128NEW1/GIT/yt_channel/|./|g' {} \; && \
sed -i 's|/media/harry/128NEW1/GIT/yt_channel/channels.db|./channels.db|g' partial_py/sync_databases.py && \
sed -i 's|/media/harry/128NEW1/GIT/yt_channel/out|./out|g' partial_py/sync_files.py && \
sed -i 's|/media/harry/128NEW1/GIT/YOUTUBE/uploads|./uploads|g' partial_py/sync_files.py && \
echo "✅ Setup complete!"
```

## Test After Setup

```bash
# Test 1: Virtual environment
/media/harry/DATA120B/venv_youtube/bin/python3 --version

# Test 2: Database
/media/harry/DATA120B/venv_youtube/bin/python3 manage_database.py stats

# Test 3: Flask app
./run_flask.sh
# Then open http://127.0.0.1:5000

# Test 4: Discovery script
./scripts/discover.sh --help
```

## What Works Out of the Box

These scripts will work automatically once `.env` is updated:
- ✅ `scripts/transcript.sh`
- ✅ `scripts/discover.sh`
- ✅ `scripts/resume.sh`
- ✅ `scripts/format.sh`
- ✅ `launch_resume_queue.py`
- ✅ `update_latest_channel_videos.py`
- ✅ `flask_app/app.py`
- ✅ `manage_database.py`

## What Still Points to Server

These can be ignored or updated later:
- 📝 Historical log files (won't affect new logs)
- 📝 Historical run directories (won't affect new runs)
- 📝 Documentation examples (cosmetic only)
- 📝 Cookie paths in some scripts (optional)

## Coordinator Decision

**Keep as-is (Recommended):**
```bash
YT_PROVIDER_COORDINATOR_URL=http://127.0.0.1:8788
```

This uses the production coordinator. You can run the system locally without setting up a local coordinator.

---

**See `LOCAL_SETUP_GUIDE.md` for detailed instructions and troubleshooting.**
