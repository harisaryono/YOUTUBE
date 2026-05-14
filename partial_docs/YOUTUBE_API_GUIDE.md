# YouTube API Metadata Fetcher

## 📋 Prerequisites

### 1. Google Cloud Project

1. Buka https://console.cloud.google.com/
2. Buat project baru atau pilih project yang ada
3. Enable **YouTube Data API v3**
4. Buat API Key di **APIs & Services > Credentials**

### 2. Install Dependencies

```bash
pip install google-api-python-client
```

Atau jika menggunakan venv Flask:

```bash
cd flask_app
source /media/harry/DATA120B/venv_youtube/bin/activate
pip install google-api-python-client
```

## 🚀 Cara Menggunakan

### Basic Usage

```bash
python3 fetch_youtube_metadata.py --api-key YOUR_API_KEY
```

### Options

```bash
# Process only 100 videos (test)
python3 fetch_youtube_metadata.py --api-key YOUR_API_KEY --limit 100

# Custom batch size
python3 fetch_youtube_metadata.py --api-key YOUR_API_KEY --batch-size 25

# Dry run (see how many videos need metadata)
python3 fetch_youtube_metadata.py --api-key YOUR_API_KEY --dry-run
```

## 📊 Quota Usage

YouTube Data API v3 free tier: **10,000 units/day**

| Operation | Quota Cost |
|-----------|------------|
| videos.list | 1 unit per video |

**Example:**
- 1,000 videos = 1,000 units
- 10,000 videos = 10,000 units (full daily quota)

### Strategy for Large Databases

Untuk database dengan 27,000+ videos:

```bash
# Day 1: Process 10,000 videos
python3 fetch_youtube_metadata.py --api-key YOUR_API_KEY --limit 10000

# Day 2: Process next 10,000 videos
python3 fetch_youtube_metadata.py --api-key YOUR_API_KEY --limit 10000

# Day 3: Process remaining videos
python3 fetch_youtube_metadata.py --api-key YOUR_API_KEY
```

## 📈 What Data Gets Fetched

| Field | Source | Updates |
|-------|--------|---------|
| duration | YouTube API | ✅ Yes |
| view_count | YouTube API | ✅ Yes |
| like_count | YouTube API | ✅ Yes |
| comment_count | YouTube API | ✅ Yes |
| description | YouTube API | ⚠️ Only if empty |
| thumbnail_url | YouTube API | ⚠️ Only if empty |

## 🔍 Check Progress

```bash
sqlite3 youtube_transcripts.db "
SELECT 
  COUNT(*) as total,
  SUM(CASE WHEN duration > 0 THEN 1 ELSE 0 END) as with_duration,
  SUM(CASE WHEN view_count > 0 THEN 1 ELSE 0 END) as with_views
FROM videos;
"
```

## ⚠️ Troubleshooting

### Quota Exceeded

```
Error 403: The request cannot be completed because you have 
exceeded your quota.
```

**Solution:**
- Wait for quota to reset (midnight Pacific Time)
- Request quota increase from Google Cloud Console
- Use multiple API keys (rotate)

### API Key Invalid

```
Error: Invalid API key.
```

**Solution:**
- Check API key is correct
- Ensure YouTube Data API v3 is enabled
- Check API key has no restrictions

### Video Not Found

Some videos may be:
- Deleted
- Private
- Unlisted (still accessible via API)

These will be counted in "Videos not found" statistic.

## 📝 Example Output

```
============================================================
📺 FETCH YOUTUBE METADATA
============================================================
📊 Found 27019 videos needing metadata
📉 Estimated quota usage: 27019 units

⏳ Processing batch 1... (50 videos)
   📊 Processed: 100/27019 videos
   📉 Quota used: 100 units
   ✅ Updated: 98 videos

📈 Fetch Summary:
   Videos processed: 27019
   Videos updated: 26543
   Videos not found: 476
   API errors: 0
   Quota used: 27019 units
```

## 🎯 Alternative: Free Thumbnail Only

If you don't want to use API quota, thumbnail URLs can be generated:

```python
# Update all thumbnails without API
sqlite3 youtube_transcripts.db "
UPDATE videos 
SET thumbnail_url = 'https://img.youtube.com/vi/' || video_id || '/hqdefault.jpg'
WHERE thumbnail_url IS NULL;
"
```

---

**Note:** Script ini **opsional**. Database sudah berfungsi tanpa YouTube API metadata.
