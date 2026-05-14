# Channel-Based File Structure

Framework sekarang menggunakan struktur file berbasis channel untuk organisasi yang lebih baik.

## 📂 Struktur Baru

```
uploads/
├── @ChannelName1/
│   ├── text/              # Transcript files (.txt)
│   │   ├── video1_transcript.txt
│   │   ├── video2_transcript.txt
│   │   └── ...
│   └── resume/             # Summary files (.txt)
│       ├── video1_summary.txt
│       ├── video2_summary.txt
│       └── ...
├── @ChannelName2/
│   ├── text/
│   │   └── ...
│   └── resume/
│       └── ...
└── ...
```

## 🎯 Keuntungan Struktur Baru

### ✅ **Organisasi Lebih Baik**
- Semua files dari satu channel dalam satu folder
- Mudah untuk backup dan restore per channel
- Simple untuk browse dan manage files

### 📁 **Manajemen File Mudah**
- Browse transcripts dan summaries per channel
- Download semua files dari satu channel
- Delete atau archive seluruh channel dengan mudah

### 🚀 **Skalabilitas**
- Ratusan channels dapat di-organize rapi
- Tidak ada file conflict antara channels
- Growth linear dengan jumlah channels

### 🔍 **Pencarian Lebih Cepat**
- File paths mengandung channel ID untuk filtering
- Query database dapat dengan mudah filter by channel
- File access lebih efficient

## 📋 Cara Menggunakan

### **1. Browse Files per Channel**

**Via Web Interface:**
```
http://127.0.0.1:5000/channel_files/<channel_id>
```

**Example:**
```
http://127.0.0.1:5000/channel_files/@KenapaItuYa
```

**Via Command Line:**
```bash
# List transcript files
ls uploads/@ChannelName/text/

# List summary files
ls uploads/@ChannelName/resume/

# Read specific file
cat uploads/@ChannelName/text/video_transcript.txt
```

### **2. Download Semua Files Channel**

**Via Web Interface:**
1. Navigate ke channel detail page
2. Click "Browse Files" button
3. Use "Download All Text" atau "Download All Resumes"

**Via Command Line:**
```bash
# Download all transcripts from channel
wget -r -nH --cut-dirs=1 -A "*.txt" \
     http://127.0.0.1:5000/file/@ChannelName/text/

# Download all summaries from channel
wget -r -nH --cut-dirs=1 -A "*.txt" \
     http://127.0.0.1:5000/file/@ChannelName/resume/
```

### **3. Backup per Channel**

```bash
# Backup entire channel
tar -czf @ChannelName_backup.tar.gz uploads/@ChannelName/

# Backup only transcripts
tar -czf @ChannelName_transcripts.tar.gz uploads/@ChannelName/text/

# Backup only summaries
tar -czf @ChannelName_summaries.tar.gz uploads/@ChannelName/resume/
```

### **4. Archive atau Delete Channel**

```bash
# Archive channel
mv uploads/@ChannelName uploads/archived_channels/

# Delete channel (careful!)
rm -rf uploads/@ChannelName
```

## 🔧 Modifikasi Database Module

### **New Functions in `database_optimized.py`:**

```python
# Get channel folder path
db.get_channel_folder_path(channel_id)

# Get channel transcripts directory
db.get_channel_transcripts_dir(channel_id)

# Get channel summaries directory
db.get_channel_summaries_dir(channel_id)

# Sanitize channel ID for folder name
db._sanitize_channel_id(channel_id)
```

### **File Path Storage:**

Database sekarang menyimpan paths dalam format:
```
uploads/@ChannelName/text/VIDEO_ID_transcript.txt
uploads/@ChannelName/resume/VIDEO_ID_summary.txt
```

## 🌐 Flask Routes Baru

### **Channel Files Browser:**
```python
@app.route('/channel_files/<channel_id>')
def channel_files(channel_id):
    # Browse files in channel
    # Returns: channel_files.html
```

### **File Serving:**
```python
@app.route('/file/<channel_id>/<file_type>/<filename>')
def serve_channel_file(channel_id, file_type, filename):
    # Serve files from channel directories
    # file_type: 'text' atau 'resume'
    # Returns: File download
```

## 🔄 Migration dari Struktur Lama

### **Automated Migration:**

```bash
# Jalankan migration script
python reorganize_files.py youtube_transcripts.db uploads

# Atau dengan specific paths
python reorganize_files.py old.db new_uploads
```

### **Manual Migration:**

```bash
# Create channel directories
mkdir -p "uploads/@ChannelName/text"
mkdir -p "uploads/@ChannelName/resume"

# Move files
mv uploads/transcripts/VIDEO_ID_transcript.txt "uploads/@ChannelName/text/"
mv uploads/summaries/VIDEO_ID_summary.txt "uploads/@ChannelName/resume/"

# Update database paths
sqlite3 youtube_transcripts.db "UPDATE videos SET transcript_file_path = 'uploads/@ChannelName/text/VIDEO_ID_transcript.txt' WHERE video_id = 'VIDEO_ID'"
sqlite3 youtube_transcripts.db "UPDATE videos SET summary_file_path = 'uploads/@ChannelName/resume/VIDEO_ID_summary.txt' WHERE video_id = 'VIDEO_ID'"
```

## 📊 API Endpoint Baru

### **Get Channel Files:**
```bash
GET /channel_files/@ChannelName
```

### **Download Specific File:**
```bash
GET /file/@ChannelName/text/VIDEO_ID_transcript.txt
GET /file/@ChannelName/resume/VIDEO_ID_summary.txt
```

## 🎨 Template Baru

### **`channel_files.html`:**
- Browse transcripts dan summaries per channel
- Download individual atau all files
- File statistics (size, modified date)
- Quick actions (download all, back to channel)

### **Updated `channel_detail.html`:**
- Added "Browse Files" button
- Added "Download All" button
- Links to channel file browser

## 🔍 Sanitasi Channel ID

Channel IDs disanitize untuk nama folder yang valid:

```python
def _sanitize_channel_id(self, channel_id: str) -> str:
    # Replace special characters
    safe_id = channel_id.replace('/', '_').replace('\\', '_')
    safe_id = safe_id.replace(':', '_').replace('*', '_')
    safe_id = safe_id.replace(' ', '_')
    
    # Limit length
    if len(safe_id) > 100:
        safe_id = safe_id[:100]
    
    return safe_id
```

**Examples:**
```
@KenapaItuYa          → uploads/@KenapaItuYa/
UC123456789        → uploads/UC123456789/
Channel Name          → uploads/Channel_Name/
```

## 🚀 Performance Benefits

### **Database Size:**
- **Sebelum**: Database bisa membengkak dengan ribuan transkrip
- **Sesudah**: Database hanya menyimpan metadata (kecil dan cepat)

### **File Access:**
- **Sebelum**: Satu directory dengan ribuan files
- **Sesudah**: Terdistribusi dalam channel folders (fast file lookup)

### **Backup & Restore:**
- **Sebelum**: Harus backup seluruh uploads directory
- **Sesudah**: Bisa backup per channel atau per tipe file

### **Disk Usage:**
- **Sebelum**: Harder untuk track storage per channel
- **Sesudah**: Easy untuk monitor storage per channel

## 📝 Contoh Penggunaan

### **Python Script untuk Download Channel:**

```python
import requests
from pathlib import Path

def download_channel_files(channel_id, output_dir="."):
    """Download semua files dari channel tertentu"""
    
    # Download transcripts
    text_url = f"http://127.0.0.1:5000/file/{channel_id}/text/"
    transcripts = requests.get(text_url).json()
    
    for file in transcripts['files']:
        file_url = f"http://127.0.0.1:5000/file/{channel_id}/text/{file['name']}"
        response = requests.get(file_url)
        
        output_path = Path(output_dir) / "text" / file['name']
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'wb') as f:
            f.write(response.content)
    
    # Download summaries
    resume_url = f"http://127.0.0.1:5000/file/{channel_id}/resume/"
    summaries = requests.get(resume_url).json()
    
    for file in summaries['files']:
        file_url = f"http://127.0.0.1:5000/file/{channel_id}/resume/{file['name']}"
        response = requests.get(file_url)
        
        output_path = Path(output_dir) / "resume" / file['name']
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'wb') as f:
            f.write(response.content)

# Usage
download_channel_files("@KenapaItuYa", "downloaded_channel")
```

### **Shell Script untuk Backup Channels:**

```bash
#!/bin/bash
# Backup semua channels

cd uploads

for channel_dir in */; do
    channel_name="${channel_dir%/}"
    echo "Backing up: $channel_name"
    
    # Create backup
    tar -czf "${channel_name}_backup_$(date +%Y%m%d).tar.gz" "$channel_dir"
    
    # Move to backup folder
    mv "${channel_name}_backup_$(date +%Y%m%d).tar.gz" backups/
done

echo "Backup complete!"
```

## ⚠️ Perhatian

### **File Renaming:**
- Channel ID sanitization mungkin mengubah nama folder
- Selalu gunakan database paths untuk accurate references
- Jangan rename folder manually

### **Path Updates:**
- Migration script otomatis update database paths
- Jangan move files secara manual tanpa update database
- Selalu gunakan `reorganize_files.py` untuk perubahan struktur

### **Cross-Platform:**
- Sanitization menggunakan underscores untuk compatibility
- Paths selalu menggunakan forward slashes (/)
- Windows dan Linux compatible

## 📚 Additional Resources

- **Database Module**: `database_optimized.py`
- **Migration Tool**: `reorganize_files.py`
- **Flask App**: `flask_app/app.py`
- **Main Documentation**: `README_FLASK.md`

---

Framework sekarang menggunakan struktur file berbasis channel yang lebih mudah untuk manage dan organize! 🎉
