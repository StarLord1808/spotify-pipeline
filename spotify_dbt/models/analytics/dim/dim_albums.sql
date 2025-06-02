{{config(
    alias='dim_albums',
    materialized='incremental',
    unique_key='album_sk',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    tags=['dim', 'analytics'],
    description='Dimension table for Spotify albums, capturing unique album details and their current status.',
    schema='analytics',
    post_hook=[
               log('Post-hook for dim_albums completed successfully', info=True)
    ]
)}}

WITH stg_albums AS (
  SELECT DISTINCT
    trim(album_uri) AS album_uri,
    album_name,
    SPLIT(album_uri, ':')[2] AS album_id,
    CONCAT('https://open.spotify.com/album/', album_id) AS album_url,
    cr_db_dt,
    updt_db_dt
  FROM {{ ref('stg_processed_tracks') }}
),

-- Only try to read from existing table if running incrementally
existing_current_albums AS (
  {% if is_incremental() %}
    SELECT *
    FROM {{ this }}
    WHERE is_current = TRUE
  {% else %}
    -- Dummy CTE to avoid breaking the query on first run
    SELECT
      NULL::VARCHAR AS album_sk,
      NULL::VARCHAR AS prnt_sk,
      NULL::VARCHAR AS album_uri,
      NULL::VARCHAR AS album_name,
      NULL::VARCHAR AS album_id,
      NULL::VARCHAR AS album_url,
      NULL::TIMESTAMP AS cr_db_dt,
      NULL::TIMESTAMP AS updt_db_dt
    WHERE FALSE
  {% endif %}
),

-- Detect changes between staging and existing dimension
changed_albums AS (
  SELECT
    s.album_uri,
    s.album_name,
    s.album_id,
    s.album_url,
    s.cr_db_dt,
    s.updt_db_dt,
    e.album_sk AS previous_album_sk
  FROM stg_albums s
  LEFT JOIN existing_current_albums e
    ON s.album_uri = e.album_uri
  WHERE e.album_sk IS NULL
     OR s.album_name != e.album_name
     OR s.album_id != e.album_id
  ),

-- Expire old versions of changed albums
expire_old_versions AS (
  SELECT
    e.album_sk,
    e.prnt_sk,
    e.album_uri,
    e.album_name,
    e.album_id,
    e.album_url,
    e.cr_db_dt,
    e.updt_db_dt,
    FALSE AS is_current
  FROM existing_current_albums e
  INNER JOIN changed_albums c
    ON e.album_uri = c.album_uri
),

-- Compute max SKs for new inserts
max_sk AS (
  {% if is_incremental() %}
    SELECT
      COALESCE(MAX(album_sk), 0) AS max_album_sk,
      COALESCE(MAX(prnt_sk), 0) AS max_prnt_sk
    FROM {{ this }}
  {% else %}
    SELECT 0 AS max_album_sk, 0 AS max_prnt_sk
  {% endif %}
),

-- Assign row numbers for new/changed albums
new_rows AS (
  SELECT *,
         ROW_NUMBER() OVER (ORDER BY album_uri) AS row_index
  FROM changed_albums
),

-- Final assignment of new SKs and set is_current = TRUE
new_versioned_albums AS (
  SELECT
    m.max_album_sk + r.row_index AS album_sk,
    CASE
      WHEN r.previous_album_sk IS NOT NULL THEN p.prnt_sk
      ELSE m.max_prnt_sk + r.row_index
    END AS prnt_sk,
    r.album_uri,
    r.album_name,
    r.album_id,
    r.album_url,
    r.cr_db_dt,
    r.updt_db_dt,
    TRUE AS is_current
  FROM new_rows r
  CROSS JOIN max_sk m
  {% if is_incremental() %}
  LEFT JOIN {{ this }} p
    ON r.previous_album_sk = p.album_sk
  {% else %}
  LEFT JOIN (
    SELECT NULL AS album_sk, NULL AS prnt_sk
  ) p
    ON FALSE
  {% endif %}
)

-- Combine expired old versions and new current versions
SELECT * FROM expire_old_versions
UNION ALL
SELECT * FROM new_versioned_albums
ORDER BY album_sk