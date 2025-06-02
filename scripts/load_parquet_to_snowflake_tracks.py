from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Load Parquet to Snowflake") \
    .getOrCreate()

# Input Parquet Path
PARQUET_PATH = "data/spotify-mpd/spotify_million_playlist_dataset/data_parquet/first_100/tracks"

# Read Parquet Files
df = spark.read.parquet(PARQUET_PATH)

# Snowflake Config
sfOptions = {
    "sfURL": "********.snowflakecomputing.com",  # Replace with your Snowflake account URL
    "sfUser": "********",  # Replace with your Snowflake username
    "sfPassword": "********",  # Replace with your Snowflake password
    "sfDatabase": "********",  # Replace with your Snowflake database name
    "sfSchema": "********",  # Replace with your Snowflake schema name
    "sfWarehouse": "********",  # Replace with your Snowflake warehouse name
}

SNOWFLAKE_TABLE = "raw_tracks"

# Write DataFrame to Snowflake
df.write \
    .format("net.snowflake.spark.snowflake") \
    .options(**sfOptions) \
    .option("dbtable", SNOWFLAKE_TABLE) \
    .mode("overwrite") \
    .save()