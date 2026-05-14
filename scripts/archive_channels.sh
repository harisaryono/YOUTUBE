#!/bin/bash
# Script to archive uploads per channel

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UPLOADS_DIR="$REPO_DIR/uploads"
ARCHIVE_DIR="$REPO_DIR/uploads_tar"

echo "📂 Archiving uploads per channel..."
echo "Source: $UPLOADS_DIR"
echo "Target: $ARCHIVE_DIR"

mkdir -p "$ARCHIVE_DIR"

# Loop through each directory in uploads/
# Use find to list only top-level directories
find "$UPLOADS_DIR" -maxdepth 1 -mindepth 1 -type d | while read -r channel_dir; do
    channel_name=$(basename "$channel_dir")
    
    # Skip hidden directories if any
    if [[ "$channel_name" == .* ]]; then
        continue
    fi
    
    archive_file="$ARCHIVE_DIR/${channel_name}.tar.gz"
    
    echo "📦 Archiving channel: $channel_name..."
    tar -czf "$archive_file" -C "$UPLOADS_DIR" "$channel_name"
done

echo "✅ Archiving completed!"
ls -lh "$ARCHIVE_DIR"
