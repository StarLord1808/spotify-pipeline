#!/bin/bash

set -e

SOURCE_DIR="data/spotify-mpd/spotify_million_playlist_dataset/data_json"
DEST_DIR="$SOURCE_DIR/first-100"
BATCH_SIZE=100
TRACK_FILE="codebase/Proj_Spotify/scripts/processed_files.txt"

echo "[$(date)] 🚀 Preparing batch of unprocessed files..."

# Ensure tracking file exists
touch "$TRACK_FILE"

# Get up to $BATCH_SIZE unprocessed files
mapfile -t files < <(
    ls "${SOURCE_DIR}"/mpd.slice.*.json \
    | sed -E 's/.*mpd.slice.([0-9]+)-[0-9]+\\.json/\\1 \\0/' \
    | sort -n \
    | cut -d' ' -f2- \
    | comm -23 - <(sort "$TRACK_FILE") \
    | head -n $BATCH_SIZE
)

if [ ${#files[@]} -eq 0 ]; then
    echo "✅ No new files to process. Exiting."
    exit 0
fi

# Prepare destination directory
rm -rf "$DEST_DIR" && mkdir -p "$DEST_DIR"

# Copy files and update tracker
for file in "${files[@]}"; do
    cp "$file" "$DEST_DIR"
    echo "$file" >> "$TRACK_FILE"
done

echo "✅ Copied ${#files[@]} files to $DEST_DIR"
