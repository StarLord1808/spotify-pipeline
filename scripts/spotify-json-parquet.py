from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
import os
import gc

# ⚙️ Paths (modify as needed)
INPUT_PATH = "data/spotify-mpd/spotify_million_playlist_dataset/data_json/first-100"
OUTPUT_PATH = "data/spotify-mpd/spotify_million_playlist_dataset/data_parquet/first_100"

# ✅ Spark Session (tune memory via spark-submit)
spark = SparkSession.builder \
    .appName("Spotify JSON to Parquet") \
    .getOrCreate()

# ✅ Read JSON with multiLine enabled
df = spark.read.option("multiLine", True).json(INPUT_PATH)

# 🧠 Validate structure
if "playlists" not in df.columns:
    print("❌ 'playlists' key not found. Check your JSON formatting.")
    df.show(truncate=False)
    spark.stop()
    exit(1)

# ✅ Explode playlists
playlists_df = df.select(explode(col("playlists")).alias("playlist"))

# ✅ Extract playlist-level data
playlist_data = playlists_df.select(
    col("playlist.pid").alias("playlist_id"),
    col("playlist.name").alias("playlist_name"),
    col("playlist.num_tracks"),
    col("playlist.num_albums"),
    col("playlist.num_artists"),
    col("playlist.num_followers"),
    col("playlist.num_edits"),
    col("playlist.duration_ms"),
    col("playlist.modified_at")
)

# ✅ Extract track-level data
track_data = playlists_df.select(
    col("playlist.pid").alias("playlist_id"),
    explode(col("playlist.tracks")).alias("track")
).select(
    col("playlist_id"),
    col("track.track_name"),
    col("track.artist_name"),
    col("track.track_uri"),
    col("track.album_name"),
    col("track.album_uri"),
    col("track.artist_uri"),
    col("track.duration_ms").alias("track_duration_ms")
)

# ✅ Write output to Parquet
playlist_data.write.mode("overwrite").parquet(os.path.join(OUTPUT_PATH, "playlists"))
track_data.write.mode("overwrite").parquet(os.path.join(OUTPUT_PATH, "tracks"))

print("✅ Conversion complete!")

# 🧹 Explicit memory and cache cleanup
playlist_data.unpersist(blocking=True)
track_data.unpersist(blocking=True)
spark.catalog.clearCache()
gc.collect()
print("🧹 Cache and memory cleaned up.")

spark.stop()
print("✅ Spark session stopped.")
