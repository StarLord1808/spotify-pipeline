import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# CONFIGURE THESE
PARQUET_DIR = "data/spotify-mpd/spotify_million_playlist_dataset/data_parquet/first_100/playlists"
SNOWFLAKE_CONFIG = {
    "user": "******", # Username for Snowflake
    "password": "******", # Password for Snowflake
    "account": "******",  # e.g., "xy12345.ap-southeast-1"
    "warehouse": "******",
    "role": "******",
    "database": "*****",
    "schema": "*******",
}
TABLE_NAME = "raw_playlists"

# Step 1: Read all Parquet files from the directory
def read_all_parquet_files(directory):
    dfs = []
    for filename in os.listdir(directory):
        if filename.endswith(".parquet"):
            filepath = os.path.join(directory, filename)
            print(f"Reading {filepath} ...")
            df = pd.read_parquet(filepath)
            dfs.append(df)
    if not dfs:
        raise FileNotFoundError("No Parquet files found in directory.")
    return pd.concat(dfs, ignore_index=True)

# Step 2: Upload to Snowflake (with truncate)
def upload_to_snowflake(df, config, table_name):
    conn = snowflake.connector.connect(**config)
    cursor = conn.cursor()
    try:
        print(f"Truncating table {table_name} before upload...")
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        print("Table truncated successfully.")
        
        success, nchunks, nrows, _ = write_pandas(conn, df, table_name, quote_identifiers=False)
        print(f"Upload successful: {success}, Rows written: {nrows}, Chunks: {nchunks}")
    finally:
        cursor.close()
        conn.close()

# Main
if __name__ == "__main__":
    df = read_all_parquet_files(PARQUET_DIR)
    
    # Rename columns to match Snowflake table schema
    df = df.rename(columns={
        "playlist_id": "PID",
        "playlist_name": "NAME",
        "num_tracks": "NUM_TRACKS",
        "num_albums": "NUM_ALBUMS",
        "num_artists": "NUM_ARTISTS",
        "num_followers": "NUM_FOLLOWERS",
        "num_edits": "NUM_EDITS",
        "duration_ms": "DURATION_MS",
        "modified_at": "MODIFIED_AT",
    })

    print(f"Total rows to upload: {len(df)}")
    upload_to_snowflake(df, SNOWFLAKE_CONFIG, TABLE_NAME)
