# YouTube Transcript Manager - Flask Web Application

Aplikasi web Flask untuk menampilkan dan mengelola transkrip YouTube dengan database optimized.

## ✨ Fitur Utama

### 🗄️ Database Optimized
- **Database Ringkas**: Hanya menyimpan metadata, transkrip disimpan di file terpisah
- **File Terpisah**: Transkrip dan ringkasan di folder `uploads/transcripts/` dan `uploads/summaries/`
- **Skalabilitas**: Database tidak membengkak meski ribuan transkrip
- **Query Cepat**: Indexed tables untuk performa optimal

### 🌐 Web Interface
- **Dashboard**: Statistik real-time dan video terbaru
- **Channel Browser**: Jelajah channel dan video
- **Video Detail**: Tampilkan video embed, transkrip, dan ringkasan
- **Search**: Pencarian video berdasarkan judul dan deskripsi
- **Filter**: Filter videos dengan/without transcript

### 📱 Responsive Design
- **Mobile-Friendly**: UI responsive untuk semua device sizes
- **Modern UI**: Bootstrap 5 dengan custom styling
- **Icons**: Bootstrap Icons untuk visual appeal
- **Interaktif**: JavaScript features untuk user experience

## 📦 Struktur Project

```
YOUTUBE/
├── database_optimized.py          # Optimized database module
├── youtube_transcripts.db        # Main database (metadata only)
├── uploads/                      # File storage directory
│   ├── transcripts/              # Transcript files (.txt)
│   └── summaries/               # Summary files (.txt)
├── flask_app/                    # Flask application
│   ├── app.py                   # Main Flask application
│   ├── requirements.txt          # Python dependencies
│   ├── static/                  # Static files
│   │   ├── css/                # Custom CSS (optional)
│   │   └── js/                 # Custom JavaScript (optional)
│   └── templates/               # HTML templates
│       ├── base.html            # Base template
│       ├── index.html           # Homepage
│       ├── channels.html        # Channels listing
│       ├── channel_detail.html  # Channel detail page
│       ├── videos.html          # Videos listing
│       ├── video_detail.html    # Video detail page
│       ├── search.html          # Search page
│       └── error.html          # Error page
├── migrate_to_optimized.py      # Migration tool
└── youtube_transcript_complete.py # YouTube downloader
```

## 🚀 Quick Start

### 1. Install Dependencies

```bash
# Create virtual environment (if not exists)
python3 -m venv venv
source venv/bin/activate

# Install required packages
pip install Flask flask WTForms email-validator
```

### 2. Migrate Existing Data (Optional)

```bash
# Jika punya data dari database lama
python migrate_to_optimized.py

# Atau specific database paths
python migrate_to_optimized.py youtube_transcripts.db youtube_transcripts_new.db uploads
```

### 3. Start Flask Application

```bash
# Navigate to flask_app directory
cd flask_app

# Start development server
python app.py

# Atau dengan specific host/port
python app.py --host 0.0.0.0 --port 8080 --debug false
```

### 4. Access Web Application

- **Development**: http://127.0.0.1:5000
- **Network**: http://192.168.x.x:5000 (if using 0.0.0.0)

## 🎯 Fitur dan Penggunaan

### Dashboard (Homepage)

**Features:**
- 🔢 Statistik real-time (channels, videos, transcripts)
- 📺 Popular channels dengan video counts
- 📹 Latest videos dengan thumbnails
- 🔍 Quick search bar

**Access:**
```
http://127.0.0.1:5000/
```

### Channel Browser

**Features:**
- 📊 List semua channels
- 📺 Channel information (videos, transcripts count)
- 🔗 Direct links ke YouTube
- 🔍 Pagination

**Access:**
```
http://127.0.0.1:5000/channels
```

### Channel Detail

**Features:**
- 📊 Channel statistics
- 📹 List semua videos dalam channel
- ✅ Status transcript untuk setiap video
- 🔗 Thumbnail dan video preview

**Access:**
```
http://127.0.0.1:5000/channel/<channel_id>
```

### Videos Browser

**Features:**
- 🔍 Filter: All videos / With transcript / Without transcript
- 📹 Video cards dengan thumbnails
- ✅ Transcript status indicators
- 📊 Statistics (views, word count)
- 🔍 Pagination

**Access:**
```
http://127.0.0.1:5000/videos
http://127.0.0.1:5000/videos?transcript=with
http://127.0.0.1:5000/videos?transcript=without
```

### Video Detail

**Features:**
- 📺 Embedded video player
- 📝 Transcript dengan timestamps
- 📄 Auto-generated summary
- 🔗 Copy/Download transcript
- ⏱️  Click timestamps untuk seek video
- 📊 Video statistics dan metadata

**Access:**
```
http://127.0.0.1:5000/video/<video_id>
```

### Search

**Features:**
- 🔍 Search video titles dan descriptions
- 📹 Real-time results dengan thumbnails
- ✅ Transcript status indicators
- 📊 Result count

**Access:**
```
http://127.0.0.1:5000/search
http://127.0.0.1:5000/search?q=keyword
```

## 🔧 Configuration

### Database Configuration

Edit `flask_app/app.py`:
```python
# Database paths
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'youtube_transcripts.db')
BASE_DIR = os.path.join(os.path.dirname(__file__), '..', 'uploads')
```

### Flask Configuration

Edit `flask_app/app.py`:
```python
# Server configuration
app.secret_key = 'your-secret-key-here'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB
```

### Custom Styling

Add custom CSS to `flask_app/static/css/custom.css`:
```css
/* Your custom styles */
.custom-class {
    background-color: #ff0000;
}
```

Reference in templates:
```html
<link href="{{ url_for('static', filename='css/custom.css') }}" rel="stylesheet">
```

## 🌐 API Endpoints

### Public APIs

**Get Statistics:**
```bash
GET /api/statistics
```

**Get Videos List:**
```bash
GET /api/videos?page=1
```

**Get Video Detail:**
```bash
GET /api/video/<video_id>
```

**Get Transcript:**
```bash
GET /api/transcript/<video_id>
```

**Get Summary:**
```bash
GET /api/summary/<video_id>
```

**Search Videos:**
```bash
GET /api/search?q=keyword&page=1
```

### API Response Format

```json
{
  "success": true,
  "data": { ... }
}
```

## 📊 Database Schema

### Tables Overview

#### `channels`
```sql
- id (PRIMARY KEY)
- channel_id (UNIQUE)
- channel_name
- channel_url
- subscriber_count
- video_count
- thumbnail_url
- last_updated
- created_at
```

#### `videos`
```sql
- id (PRIMARY KEY)
- video_id (UNIQUE)
- channel_id (FOREIGN KEY)
- title
- description
- duration
- upload_date
- view_count
- like_count
- comment_count
- video_url
- thumbnail_url
- transcript_file_path
- summary_file_path
- transcript_downloaded
- transcript_language
- word_count
- line_count
- created_at
- updated_at
```

### File Storage

```
uploads/
├── transcripts/
│   ├── VIDEO_ID_transcript_TIMESTAMP.txt
│   └── ...
└── summaries/
    ├── VIDEO_ID_summary_TIMESTAMP.txt
    └── ...
```

## 🔄 Data Management

### Add New Videos with Transcripts

```bash
# Download single video
python youtube_transcript_complete.py https://youtube.com/watch?v=VIDEO_ID --video

# Download entire channel
python youtube_transcript_complete.py https://youtube.com/@ChannelName --channel --max 10
```

### Migration from Old Database

```bash
# Automated migration
python migrate_to_optimized.py

# Custom migration
python migrate_to_optimized.py old.db new.db uploads
```

### Database Backup

```bash
# Backup database
cp youtube_transcripts.db youtube_transcripts_backup_$(date +%Y%m%d).db

# Backup uploads
tar -czf uploads_backup_$(date +%Y%m%d).tar.gz uploads/
```

### Database Maintenance

```python
# Vacuum database (optimize)
from database_optimized import OptimizedDatabase
db = OptimizedDatabase()
db.vacuum_database()
db.close()
```

## 🚨 Troubleshooting

### Common Issues

#### **Database Locked Error**
```bash
# Solution: Close all database connections
# Check for open connections
lsof | grep youtube_transcripts.db

# Restart Flask application
```

#### **File Not Found Error**
```bash
# Solution: Check file paths
ls -la uploads/transcripts/
ls -la uploads/summaries/

# Verify database paths
python -c "from database_optimized import OptimizedDatabase; db = OptimizedDatabase(); print(db.get_statistics())"
```

#### **Flask Won't Start**
```bash
# Solution: Check dependencies
pip install -r flask_app/requirements.txt

# Check Python version (3.8+ required)
python3 --version

# Check port availability
netstat -tlnp | grep 5000
```

#### **Empty Videos in Database**
```bash
# Solution: Download new videos
python youtube_transcript_complete.py https://youtube.com/@Channel --channel

# Check database statistics
python -c "from database_optimized import OptimizedDatabase; db = OptimizedDatabase(); print(db.get_statistics())"
```

## 📈 Performance Optimization

### Database Optimization

```python
# Vacuum database regularly
from database_optimized import OptimizedDatabase
db = OptimizedDatabase()
db.vacuum_database()
db.close()
```

### File System Optimization

```bash
# Clean up old files
find uploads/transcripts/ -type f -mtime +90 -delete
find uploads/summaries/ -type f -mtime +90 -delete
```

### Caching (Future Enhancement)

```python
# Implement Redis caching
import redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)
```

## 🔒 Security Considerations

### Production Deployment

1. **Change Secret Key:**
```python
app.secret_key = os.environ.get('SECRET_KEY', 'generate-strong-secret-key')
```

2. **Use HTTPS:**
```bash
# Use gunicorn with SSL
gunicorn --certfile=cert.pem --keyfile=key.pem app:app
```

3. **Rate Limiting:**
```python
from flask_limiter import Limiter
limiter = Limiter(app)
```

4. **Input Validation:**
```python
# Sanitize all user inputs
from bleach import clean
clean(user_input)
```

## 🚀 Production Deployment

### Using Gunicorn

```bash
# Install gunicorn
pip install gunicorn

# Start production server
cd flask_app
gunicorn -w 4 -b 0.0.0.0:5000 app:app
```

### Using Docker

```dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
EXPOSE 5000
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:5000", "app:app"]
```

### Nginx Configuration

```nginx
location / {
    proxy_pass http://127.0.0.1:5000;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
}
```

## 📚 API Documentation

### Using APIs with Python

```python
import requests

# Get statistics
response = requests.get('http://127.0.0.1:5000/api/statistics')
stats = response.json()

# Search videos
response = requests.get('http://127.0.0.1:5000/api/search?q=python')
results = response.json()['results']

# Get transcript
response = requests.get(f'http://127.0.0.1:5000/api/transcript/{video_id}')
transcript = response.json()['transcript']
```

### Using APIs with JavaScript

```javascript
// Search videos
async function searchVideos(query) {
    const response = await fetch(`/api/search?q=${query}`);
    const data = await response.json();
    return data.results;
}

// Get video details
async function getVideoDetails(videoId) {
    const response = await fetch(`/api/video/${videoId}`);
    return await response.json();
}
```

## 🎨 Customization

### Theme Colors

Edit `flask_app/templates/base.html`:
```css
:root {
    --primary-color: #ff0000;      /* YouTube red */
    --secondary-color: #282828;    /* Dark gray */
    --accent-color: #065fd4;       /* Blue */
    --bg-light: #f9f9f9;          /* Light background */
}
```

### Logo and Branding

Edit `flask_app/templates/base.html`:
```html
<a class="navbar-brand" href="{{ url_for('index') }}">
    <img src="{{ url_for('static', filename='images/logo.png') }}" alt="Logo" height="30">
    Your Brand Name
</a>
```

## 📞 Support dan Resources

### Documentation
- Flask Documentation: https://flask.palletsprojects.com/
- Bootstrap 5: https://getbootstrap.com/docs/5.3/
- SQLite: https://www.sqlite.org/docs.html

### Common Issues
- Database locked → Check connections
- File not found → Verify paths
- Empty results → Download new content

---

Framework siap digunakan dengan web interface Flask yang modern dan database optimized! 🚀
