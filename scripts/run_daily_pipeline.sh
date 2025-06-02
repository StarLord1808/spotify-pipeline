#!/bin/bash

# === Configuration ===
SOURCE_DIR="data/spotify-mpd/spotify_million_playlist_dataset/data_json"
DEST_DIR="data/spotify-mpd/spotify_million_playlist_dataset/data_json/first-100"
BATCH_SIZE=1000
TRACK_FILE="codebase/Proj_Spotify/scripts/processed_files.txt"
PARQUET_OUTPUT_DIR="data/spotify-mpd/spotify_million_playlist_dataset/data_parquet/next_batch"
LOG_DIR="codebase/Proj_Spotify/logs"

# Generate timestamp for log filename
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Ensure log directory exists
mkdir -p "$LOG_DIR/batch_loader_logs"
mkdir -p "$LOG_DIR/json_to_parquet_logs"
mkdir -p "$LOG_DIR/load_playlist_logs"
mkdir -p "$LOG_DIR/load_tracks_logs"

# Start logging everything to the timestamped file
exec > >(tee -a "$LOG_DIR/batch_loader_logs/batch_loader_$TIMESTAMP.log") 2>&1

echo "[$(date)] 🚀 Pipeline started."

# Activate virtual environment
source ~/codebase/codebase_env/bin/activate
if [ $? -ne 0 ]; then
    echo "❌ Failed to activate virtual environment."
    exit 1
fi
echo "✅ Virtual environment activated."

# Ensure tracking file exists
touch "$TRACK_FILE"
echo "✅ Tracking file ensured."

# Get list of unprocessed files
mapfile -t files < <(
    ls "${SOURCE_DIR}"/mpd.slice.*.json \
    | sed -E 's/.*mpd.slice.([0-9]+)-[0-9]+\.json/\1 \0/' \
    | sort -n \
    | cut -d' ' -f2- \
    | comm -23 - <(sort "$TRACK_FILE") \
    | head -n $BATCH_SIZE
)

if [ ${#files[@]} -eq 0 ]; then
    echo "✅ All files processed!"
    echo "🎉 Pipeline completed successfully (no new files)."
    exit 0
fi

# Prepare batch directory
rm -rf "$DEST_DIR" && mkdir -p "$DEST_DIR"
if [ $? -ne 0 ]; then
    echo "❌ Failed to prepare destination directory."
    exit 1
fi
echo "✅ Batch directory prepared."

# Copy files
for file in "${files[@]}"; do
    cp "$file" "$DEST_DIR"
    if [ $? -ne 0 ]; then
        echo "❌ Failed to copy file: $file"
        exit 1
    fi
done

echo "✅ Copied ${#files[@]} files to $DEST_DIR"

# Update tracking file
for file in "${files[@]}"; do
    echo "$file" >> "$TRACK_FILE"
done
echo "✅ Tracking file updated."

# Run PySpark job in isolated shell
echo "🚀 Converting JSON to Parquet..."
PYSPARK_LOG="$LOG_DIR/json_to_parquet_logs/Json_to_parquet_run_$TIMESTAMP.log"
mkdir -p "$(dirname "$PYSPARK_LOG")"
(
  exec spark-submit \
    --master local[4] \
    --driver-memory 4g \
    --executor-memory 4g \
    codebase/Proj_Spotify/scripts/spotify-json-parquet.py >> "$PYSPARK_LOG" 2>&1
)
if [ $? -ne 0 ]; then
    echo "❌ PySpark job failed. Check log: $PYSPARK_LOG"
    exit 1
fi
echo "✅ PySpark job completed successfully."

# Clear cache without sudo
if [ -w /proc/sys/vm/drop_caches ]; then
  sync && echo 3 > /proc/sys/vm/drop_caches
else
  echo "⚠️ Skipping drop_caches: requires root"
fi

# Upload playlists to Snowflake
echo "📅 Uploading Playlists Parquet files to Snowflake..."
PLAYLIST_LOG="$LOG_DIR/load_playlist_logs/load_playlists_$TIMESTAMP.log"
mkdir -p "$(dirname "$PLAYLIST_LOG")"
(
  exec python3 codebase/Proj_Spotify/scripts/load_parquet_to_snowflake_playlists.py >> "$PLAYLIST_LOG" 2>&1
)
if [ $? -ne 0 ]; then
    echo "❌ Playlists upload failed. Check log: $PLAYLIST_LOG"
    exit 1
fi
echo "✅ Playlists uploaded to Snowflake successfully."

# Clear cache without sudo
if [ -w /proc/sys/vm/drop_caches ]; then 
  sync && echo 3 > /proc/sys/vm/drop_caches
else
  echo "⚠️ Skipping drop_caches: requires root"
fi

# Upload tracks to Snowflake
echo "📅 Uploading Tracks Parquet files to Snowflake..."
TRACKS_LOG="$LOG_DIR/load_tracks_logs/load_tracks_$TIMESTAMP.log"
mkdir -p "$(dirname "$TRACKS_LOG")"
(
  exec spark-submit \
    --master local[2] \
    --driver-memory 4g \
    --executor-memory 4g \
    --jars /home/jiraiya/snowflake-jdbc-3.13.22.jar,/home/jiraiya/spark-snowflake_2.13-2.13.0-spark_3.4.jar \
    codebase/Proj_Spotify/scripts/load_parquet_to_snowflake_tracks.py >> "$TRACKS_LOG" 2>&1
)
if [ $? -ne 0 ]; then
    echo "❌ Tracks upload failed. Check log: $TRACKS_LOG"
    exit 1
fi
echo "✅ Tracks uploaded to Snowflake successfully."

# Run dbt models
cd codebase/Proj_Spotify/spotify_dbt || { echo "❌ Could not change to project directory"; exit 1; }

DBT_STAGING_LOG="$LOG_DIR/dbt/dbt_run_logs/dbt_staging_$TIMESTAMP.log"
mkdir -p "$(dirname "$DBT_STAGING_LOG")"
dbt run --select stg_processed_playlists stg_processed_tracks > "$DBT_STAGING_LOG" 2>&1
if [ $? -ne 0 ]; then
    echo "❌ dbt staging models failed. Log: $DBT_STAGING_LOG"
    exit 1
fi
echo "✅ dbt staging models completed."

DBT_DIM_LOG="$LOG_DIR/dbt/dbt_run_logs/dbt_dimensions_$TIMESTAMP.log"
mkdir -p "$(dirname "$DBT_DIM_LOG")"
dbt run --select dim_tracks dim_albums dim_artists dim_playlists > "$DBT_DIM_LOG" 2>&1
if [ $? -ne 0 ]; then
    echo "❌ dbt dimension models failed. Log: $DBT_DIM_LOG"
    exit 1
fi
echo "✅ dbt dimension models completed."

DBT_FACT_LOG="$LOG_DIR/dbt/dbt_run_logs/dbt_fact_$TIMESTAMP.log"
mkdir -p "$(dirname "$DBT_FACT_LOG")"
dbt run --select fct_playlist_tracks > "$DBT_FACT_LOG" 2>&1
if [ $? -ne 0 ]; then
    echo "❌ dbt fact model failed. Log: $DBT_FACT_LOG"
    exit 1
fi
echo "✅ dbt fact model completed."

DBT_TEST_DIM_LOG="$LOG_DIR/dbt/dbt_test_logs/dbt_dim_tests_$TIMESTAMP.log"
mkdir -p "$(dirname "$DBT_TEST_DIM_LOG")"
dbt test --select dim_tracks dim_albums dim_artists dim_playlists > "$DBT_TEST_DIM_LOG" 2>&1
if [ $? -ne 0 ]; then
    echo "❌ dbt dimension tests failed. Log: $DBT_TEST_DIM_LOG"
    exit 1
fi
echo "✅ dbt dimension tests passed."

DBT_TEST_FACT_LOG="$LOG_DIR/dbt/dbt_test_logs/dbt_fact_test_$TIMESTAMP.log"
mkdir -p "$(dirname "$DBT_TEST_FACT_LOG")"
dbt test --select fct_playlist_tracks > "$DBT_TEST_FACT_LOG" 2>&1
if [ $? -ne 0 ]; then
    echo "❌ dbt fact table tests failed. Log: $DBT_TEST_FACT_LOG"
    exit 1
fi
echo "✅ dbt fact table tests passed."

DBT_ROW_COUNT_LOG="$LOG_DIR/dbt/dbt_count_logs/dbt_row_count_$TIMESTAMP.log"
mkdir -p "$(dirname "$DBT_ROW_COUNT_LOG")"
dbt run-operation print_row_counts > "$DBT_ROW_COUNT_LOG" 2>&1
if [ $? -ne 0 ]; then
    echo "❌ Failed to print row counts. Log: $DBT_ROW_COUNT_LOG"
    exit 1
fi
echo "✅ Row counts printed and logged."

echo "🎉 Pipeline completed successfully at $(date)"
