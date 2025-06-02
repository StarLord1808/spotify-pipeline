{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    incremental_strategy='append',
) }}

WITH TRACKS AS(
SELECT
     PLAYLIST_ID , 
     TRACK_NAME ,
     ARTIST_NAME ,
     TRACK_URI , 
     ALBUM_NAME , 
     ALBUM_URI , 
     ARTIST_URI , 
     TRACK_DURATION_MS 
FROM {{ source('spotify_db', 'tracks') }}
)
SELECT playlist_id AS Playlist_ID,
       TRACK_NAME,
       ARTIST_NAME,
       TRACK_URI,
       ALBUM_NAME,
       ALBUM_URI,
       ARTIST_URI,
       ROUND(TRACK_DURATION_MS / 1000 / 60.0 , 2) AS duration_mins,
       SPLIT(track_uri, ':')[2] AS track_id,
       SPLIT(album_uri, ':')[2] AS album_id,
       SPLIT(artist_uri, ':')[2] AS artist_id,
       CONCAT('https://open.spotify.com/track/', track_id) AS track_url,
       CONCAT('https://open.spotify.com/album/', album_id) AS album_url,
       CONCAT('https://open.spotify.com/artist/', artist_id) AS artist_url,
       CURRENT_TIMESTAMP() AS CR_DB_DT,
       CURRENT_TIMESTAMP() AS UPDT_DB_DT
FROM TRACKS
order by playlist_id
