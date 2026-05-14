#!/bin/bash
# Pipeline Monitoring Script
# Monitor discover, transcript, audio/ASR, resume, format, and supervisor processes

clear
echo "=============================================="
echo "  🚀 YOUTUBE PIPELINE MONITOR"
echo "=============================================="
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to check process status
check_process() {
    local name=$1
    local pattern=$2
    local pid=$(ps aux | grep "$pattern" | grep -v grep | awk '{print $2}' | head -1)
    
    if [ -n "$pid" ]; then
        local cpu=$(ps aux | grep "$pattern" | grep -v grep | awk '{print $3}' | head -1)
        local mem=$(ps aux | grep "$pattern" | grep -v grep | awk '{print $4}' | head -1)
        local time=$(ps aux | grep "$pattern" | grep -v grep | awk '{print $10}' | head -1)
        echo -e "  ${GREEN}✓${NC} $name"
        echo "     PID: $pid | CPU: ${cpu}% | MEM: ${mem}% | TIME: $time"
    else
        echo -e "  ${RED}✗${NC} $name (not running)"
    fi
}

# Function to get database stats
db_stats() {
    /media/harry/DATA120B/venv_youtube/bin/python3 -c "
import sqlite3
from datetime import datetime

conn = sqlite3.connect('youtube_transcripts.db')
cursor = conn.cursor()

cursor.execute('SELECT COUNT(*) FROM videos')
total = cursor.fetchone()[0]

cursor.execute('SELECT COUNT(*) FROM videos WHERE transcript_downloaded = 1')
transcript = cursor.fetchone()[0]

cursor.execute('SELECT COUNT(*) FROM videos WHERE summary_file_path IS NOT NULL AND summary_file_path != \"\"')
resume = cursor.fetchone()[0]

cursor.execute('SELECT COUNT(*) FROM videos WHERE transcript_formatted_path IS NOT NULL AND transcript_formatted_path != \"\"')
formatted = cursor.fetchone()[0]

print(f'{total},{transcript},{resume},{formatted}')
conn.close()
"
}

# Function to get log info
log_info() {
    local name=$1
    local pattern=$2
    local log_file=$(ls -t logs/${pattern}_*.log 2>/dev/null | head -1)
    
    if [ -n "$log_file" ]; then
        local size=$(stat -c%s "$log_file" 2>/dev/null || echo "0")
        local mtime=$(stat -c%y "$log_file" 2>/dev/null | cut -d'.' -f1)
        echo "     Log: $(basename $log_file)"
        echo "     Size: $size bytes | Modified: $mtime"
    else
        echo "     Log: Not found"
    fi
}

# Infinite loop
while true; do
    clear
    echo "=============================================="
    echo "  🚀 YOUTUBE PIPELINE MONITOR"
    echo "=============================================="
    echo ""
    
    # Get current time
    echo "📅 $(date '+%Y-%m-%d %H:%M:%S')"
    echo ""
    
    # Check processes
    echo "🔄 RUNNING PROCESSES:"
    echo ""
    check_process "Discover (Video Discovery)" "update_latest_channel_videos.py"
    log_info "Discover" "discover_full"
    echo ""
    
    check_process "Transcript Download" "recover_transcripts_from_csv.py"
    log_info "Transcript" "transcript_batch"
    echo ""

    check_process "Audio / ASR" "recover_asr_transcripts.py"
    log_info "ASR" "asr_"
    echo ""

    check_process "Resume Generation" "launch_resume_queue.py"
    log_info "Resume" "resume_batch"
    echo ""

    check_process "Transcript Formatting" "format_transcripts_pool.py"
    log_info "Format" "format_batch"
    echo ""

    check_process "Supervisor" "aware_supervisor.py"
    log_info "Supervisor" "supervisor"
    echo ""
    
    # Database stats
    echo "📊 DATABASE STATISTICS:"
    echo ""
    stats=$(db_stats)
    IFS=',' read -r total transcript resume formatted <<< "$stats"
    
    echo "  Total Videos: $total"
    echo "  With Transcript: $transcript ($(echo "scale=1; $transcript * 100 / $total" | bc)%)"
    echo "  With Resume: $resume ($(echo "scale=1; $resume * 100 / $total" | bc)%)"
    echo "  Formatted: $formatted ($(echo "scale=1; $formatted * 100 / $total" | bc)%)"
    echo ""
    
    # Calculate missing
    missing_transcript=$((total - transcript))
    missing_resume=$((transcript - resume))
    missing_format=$((transcript - formatted))
    
    echo "  Missing Transcript: $missing_transcript"
    echo "  Missing Resume: $missing_resume"
    echo "  Missing Format: $missing_format"
    echo ""
    
    # System info
    echo "💻 SYSTEM:"
    echo ""
    echo "  CPU Load: $(uptime | awk -F'load average:' '{print $2}')"
    echo "  Memory: $(free -h | awk '/Mem:/ {print $3 "/" $2}')"
    echo "  Disk Usage: $(df -h . | awk 'NR==2 {print $3 "/" $2 " (" $5 ")"}')"
    echo ""
    
    echo "=============================================="
    echo "  Press Ctrl+C to exit"
    echo "  Refreshing every 10 seconds..."
    echo "=============================================="
    
    sleep 10
done
