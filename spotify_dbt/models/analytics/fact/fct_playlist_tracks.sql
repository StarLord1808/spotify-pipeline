{{ config(
    alias='fct_playlist_tracks',
    materialized='incremental',
    unique_key=['playlist_id', 'track_sk'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    tags=['fact', 'analytics'],
    description='Fact table for Spotify playlist tracks, capturing unique track details and their current status.',
    schema='analytics',
    post_hook=[
        log('Post-hook for fct_playlist_tracks completed successfully', info=True)
    ]
) }}

WITH stg_playlist_tracks AS (
  SELECT 
    playlist_id,
    TRACK_NAME,
    ARTIST_NAME,
    TRACK_URI,
    ALBUM_NAME,
    ALBUM_URI,
    ARTIST_URI,
    duration_mins,
    track_id,
    album_id,
    artist_id,
    cr_db_dt,
    updt_db_dt,
    ROW_NUMBER() OVER (PARTITION BY playlist_id ORDER BY playlist_id) AS track_position
  FROM {{ ref('stg_processed_tracks') }}
),

-- Join with dimension SKs
final_facts AS (
  SELECT
    s.playlist_id,
    dt.prnt_sk track_sk,
    da.prnt_sk album_sk,
    dar.prnt_sk artist_sk,
    s.TRACK_NAME,
    s.ARTIST_NAME,
    s.ALBUM_NAME,
    s.duration_mins,
    s.track_position,
    s.cr_db_dt,
    s.updt_db_dt
  FROM stg_playlist_tracks s
  LEFT JOIN {{ ref('dim_tracks') }} dt ON s.TRACK_URI = dt.track_uri
  LEFT JOIN {{ ref('dim_albums') }} da ON s.ALBUM_URI = da.album_uri
  LEFT JOIN {{ ref('dim_artists') }} dar ON s.ARTIST_URI = dar.artist_uri
)

-- Deduplicate before merge
, deduplicated AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY playlist_id, track_sk
           ORDER BY updt_db_dt DESC
         ) AS rn
  FROM final_facts
  {% if is_incremental() %}
  WHERE updt_db_dt > COALESCE((SELECT MAX(updt_db_dt) FROM {{ this }}), '1900-01-01')
  {% endif %}
)

SELECT
  playlist_id,
  track_sk,
  album_sk,
  artist_sk,
  TRACK_NAME,
  ARTIST_NAME,
  ALBUM_NAME,
  duration_mins,
  track_position,
  cr_db_dt,
  updt_db_dt
FROM deduplicated
WHERE rn = 1
ORDER BY playlist_id