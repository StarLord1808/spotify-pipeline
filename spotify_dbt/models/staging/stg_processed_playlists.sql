{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    incremental_strategy='append',
) }}

WITH PLAYLISTS AS (
  SELECT
    pid AS Playlist_ID,
    TRIM(NULLIF(name, '')) AS name,
    num_tracks AS no_of_tracks,
    num_albums AS no_of_albums,
    num_artists AS no_of_artists,
    num_followers AS no_of_followers,
    num_edits AS no_of_edits,
    duration_ms AS total_duration_ms,
    TO_TIMESTAMP(modified_at / 1000) AS modified_at_raw,
    DATEADD(day, UNIFORM(0, 365, RANDOM()), TO_TIMESTAMP('2024-01-01')) AS modified_at_fixed
  FROM {{source('spotify_db', 'playlists')}}
)
SELECT DISTINCT
  Playlist_ID,
  name,
  no_of_tracks,
  no_of_albums,
  no_of_artists,
  no_of_followers,
  no_of_edits,
  ROUND(total_duration_ms / 1000 / 60.0 / 60.0, 2) AS duration_hrs,
  modified_at_fixed,
  CURRENT_TIMESTAMP() AS CR_DB_DT,
  CURRENT_TIMESTAMP() AS UPDT_DB_DT
FROM PLAYLISTS
WHERE
  NOT no_of_tracks IS NULL AND no_of_tracks > 0
  order by Playlist_ID