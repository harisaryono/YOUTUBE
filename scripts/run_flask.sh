#!/bin/bash
# Script untuk menjalankan Flask YouTube Transcript Manager

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do
    DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
    SOURCE="$(readlink "$SOURCE")"
    [[ "$SOURCE" != /* ]] && SOURCE="$DIR/$SOURCE"
done
SCRIPT_DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
if [ -f "$REPO_DIR/.env" ]; then
    set -a
    . "$REPO_DIR/.env"
    set +a
fi
VENV_PYTHON="$("$REPO_DIR/scripts/get_venv.sh")"
ROOT_VENV="$(dirname "$(dirname "$VENV_PYTHON")")"
FLASK_DIR="$REPO_DIR/flask_app"

echo "============================================================"
echo "🚀 YouTube Transcript Manager - Flask Server"
echo "============================================================"
echo ""

# Check if virtualenv exists
if [ ! -x "$VENV_PYTHON" ]; then
    echo "❌ Virtual environment not found or not executable." >&2
    echo "Check .env EXTERNAL_VENV_DIR or /media/harry/DATA120B/venv_youtube." >&2
    exit 1
fi

echo "✅ Starting Flask server using external venv..."
echo ""
echo "📊 Database Statistics:"
cd "$REPO_DIR"
YOUTUBE_DB_PATH="${YOUTUBE_DB_PATH:-$REPO_DIR/db/youtube_transcripts.db}"
"$VENV_PYTHON" -c "
import sys
import os
sys.path.insert(0, '.')
from database_optimized import OptimizedDatabase
db = OptimizedDatabase(os.environ.get('YOUTUBE_DB_PATH', '$YOUTUBE_DB_PATH'))
stats = db.get_statistics()
print(f'   📺 Channels: {stats[\"total_channels\"]:,}')
print(f'   📹 Videos: {stats[\"total_videos\"]:,}')
print(f'   ✅ With Transcript: {stats[\"videos_with_transcript\"]:,}')
print(f'   ❌ Without Transcript: {stats[\"videos_without_transcript\"]:,}')
" 

echo ""
echo "🌐 Server running at: http://localhost:5000"
echo "📊 API Statistics: http://localhost:5000/api/statistics"
echo "📹 All Videos: http://localhost:5000/videos"
echo "📺 All Channels: http://localhost:5000/channels"
echo "📋 Admin Panel: http://localhost:5000/admin/data"
echo ""
echo "Press Ctrl+C to stop the server"
echo "============================================================"

# Run Flask app using external venv Python
exec "$VENV_PYTHON" -m flask --app flask_app.app run --host 0.0.0.0 --port 5000 --debug
