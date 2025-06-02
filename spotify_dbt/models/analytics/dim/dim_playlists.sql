{{ config(
    alias='dim_playlists',
    materialized='incremental',
    unique_key='playlist_sk',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    tags=['dim', 'analytics'],
    description='Dimension table for Spotify playlists, capturing versioned details.',
    schema='analytics',
    post_hook=[
        log('Post-hook for dim_playlists completed successfully', info=True)
    ]
) }}

WITH stg_playlists AS (
    SELECT DISTINCT
        Playlist_ID,
        name,
        no_of_tracks,
        no_of_albums,
        no_of_artists,
        no_of_followers,
        no_of_edits,
        duration_hrs,
        modified_at_fixed,
        CR_DB_DT,
        UPDT_DB_DT
    FROM {{ ref('stg_processed_playlists') }}
),

-- Only try to read from existing table if running incrementally
existing_current_playlists AS (
  {% if is_incremental() %}
    SELECT *
    FROM {{ this }}
    WHERE is_current = TRUE
  {% else %}
    -- Dummy CTE to avoid breaking the query on first run
    SELECT
      NULL::INT AS playlist_sk,
      NULL::INT AS prnt_sk,
      NULL::INT AS Playlist_ID,
      NULL::VARCHAR AS name,
      NULL::INT AS no_of_tracks,
      NULL::INT AS no_of_albums,
      NULL::INT AS no_of_artists,
      NULL::INT AS no_of_followers,
      NULL::INT AS no_of_edits,
      NULL::FLOAT AS duration_hrs,
      NULL::TIMESTAMP AS modified_at_fixed,
      NULL::TIMESTAMP AS CR_DB_DT,
      NULL::TIMESTAMP AS UPDT_DB_DT
    WHERE FALSE
  {% endif %}
),

-- Detect changes between staging and existing dimension
changed_playlists AS (
    SELECT
        s.Playlist_ID,
        s.name,
        s.no_of_tracks,
        s.no_of_albums,
        s.no_of_artists,
        s.no_of_followers,
        s.no_of_edits,
        s.duration_hrs,
        s.modified_at_fixed,
        s.CR_DB_DT,
        s.UPDT_DB_DT,
        e.playlist_sk AS previous_playlist_sk
    FROM stg_playlists s
    LEFT JOIN existing_current_playlists e
        ON s.Playlist_ID = e.Playlist_ID
    WHERE e.playlist_sk IS NULL
       OR s.name != e.name
       OR s.no_of_tracks != e.no_of_tracks
       OR s.no_of_followers != e.no_of_followers
       OR s.no_of_albums != e.no_of_albums
        OR s.no_of_artists != e.no_of_artists
        OR s.no_of_edits != e.no_of_edits
),

-- Expire old versions of changed playlists
expire_old_versions AS (
    SELECT
        e.playlist_sk,
        e.prnt_sk,
        e.Playlist_ID,
        e.name,
        e.no_of_tracks,
        e.no_of_albums,
        e.no_of_artists,
        e.no_of_followers,
        e.no_of_edits,
        e.duration_hrs,
        e.modified_at_fixed,
        e.CR_DB_DT,
        e.UPDT_DB_DT,
        FALSE AS is_current
    FROM existing_current_playlists e
    INNER JOIN changed_playlists c
        ON e.Playlist_ID = c.Playlist_ID
),

-- Compute max SKs for new inserts
max_sk AS (
    {% if is_incremental() %}
        SELECT
            COALESCE(MAX(playlist_sk), 0) AS max_playlist_sk,
            COALESCE(MAX(prnt_sk), 0) AS max_prnt_sk
        FROM {{ this }}
    {% else %}
        SELECT 0 AS max_playlist_sk, 0 AS max_prnt_sk
    {% endif %}
),

-- Assign row numbers for new/changed playlists
new_rows AS (
    SELECT *,
           ROW_NUMBER() OVER (ORDER BY Playlist_ID) AS row_index
    FROM changed_playlists
),

-- Final assignment of new SKs and set is_current = TRUE
new_versioned_playlists AS (
    SELECT
        m.max_playlist_sk + r.row_index AS playlist_sk,
        CASE
            WHEN r.previous_playlist_sk IS NOT NULL THEN p.prnt_sk
            ELSE m.max_prnt_sk + r.row_index
        END AS prnt_sk,
        r.Playlist_ID,
        r.name,
        r.no_of_tracks,
        r.no_of_albums,
        r.no_of_artists,
        r.no_of_followers,
        r.no_of_edits,
        r.duration_hrs,
        r.modified_at_fixed,
        r.CR_DB_DT,
        r.UPDT_DB_DT,
        TRUE AS is_current
    FROM new_rows r
    CROSS JOIN max_sk m
    {% if is_incremental() %}
    LEFT JOIN {{ this }} p
        ON r.previous_playlist_sk = p.playlist_sk
    {% else %}
    LEFT JOIN (
        SELECT NULL AS playlist_sk, NULL AS prnt_sk
    ) p
        ON FALSE
    {% endif %}
)

-- Combine expired old versions and new current versions
SELECT * FROM expire_old_versions
UNION ALL
SELECT * FROM new_versioned_playlists
ORDER BY playlist_sk