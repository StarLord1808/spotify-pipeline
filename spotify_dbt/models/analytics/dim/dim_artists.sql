{{config(
    alias='dim_artists',
    materialized='incremental',
    unique_key='artist_sk',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    tags=['dim', 'analytics'],
    description='Dimension table for Spotify artists, capturing unique artist details and their current status.',
    schema='analytics',
    post_hook=[
               log('Post-hook for dim_artists completed successfully', info=True)
    ]
)}}

WITH stg_artists AS (
  SELECT DISTINCT
    trim(artist_uri) AS artist_uri,
    artist_name,
    SPLIT(artist_uri, ':')[2] AS artist_id,
    CONCAT('https://open.spotify.com/artist/', artist_id) AS artist_url,
    cr_db_dt,
    updt_db_dt
  FROM {{ ref('stg_processed_tracks') }}
),

-- Only try to read from existing table if running incrementally
existing_current_artists AS (
  {% if is_incremental() %}
    SELECT *
    FROM {{ this }}
    WHERE is_current = TRUE
  {% else %}
    -- Dummy CTE to avoid breaking the query on first run
    SELECT
      NULL::VARCHAR AS artist_sk,
      NULL::VARCHAR AS prnt_sk,
      NULL::VARCHAR AS artist_uri,
      NULL::VARCHAR AS artist_name,
      NULL::FLOAT AS artist_id,
      NULL::VARCHAR AS artist_url,
      NULL::TIMESTAMP AS cr_db_dt,
      NULL::TIMESTAMP AS updt_db_dt
    WHERE FALSE
  {% endif %}
),

-- Detect changes between staging and existing dimension
changed_artists AS (
  SELECT
    s.artist_uri,
    s.artist_name,
    s.artist_id,
    s.artist_url,
    s.cr_db_dt,
    s.updt_db_dt,
    e.artist_sk AS previous_artist_sk
  FROM stg_artists s
  LEFT JOIN existing_current_artists e
    ON s.artist_uri = e.artist_uri
  WHERE e.artist_sk IS NULL
     OR s.artist_name != e.artist_name
     OR s.artist_id != e.artist_id
  ),

-- Expire old versions of changed artists
expire_old_versions AS (
  SELECT
    e.artist_sk,
    e.prnt_sk,
    e.artist_uri,
    e.artist_name,
    e.artist_id,
    e.artist_url,
    e.cr_db_dt,
    e.updt_db_dt,
    FALSE AS is_current
  FROM existing_current_artists e
  INNER JOIN changed_artists c
    ON e.artist_uri = c.artist_uri
),

-- Compute max SKs for new inserts
max_sk AS (
  {% if is_incremental() %}
    SELECT
      COALESCE(MAX(artist_sk), 0) AS max_artist_sk,
      COALESCE(MAX(prnt_sk), 0) AS max_prnt_sk
    FROM {{ this }}
  {% else %}
    SELECT 0 AS max_artist_sk, 0 AS max_prnt_sk
  {% endif %}
),

-- Assign row numbers for new/changed artists
new_rows AS (
  SELECT *,
         ROW_NUMBER() OVER (ORDER BY artist_uri) AS row_index
  FROM changed_artists
),

-- Final assignment of new SKs and set is_current = TRUE
new_versioned_artists AS (
  SELECT
    m.max_artist_sk + r.row_index AS artist_sk,
    CASE
      WHEN r.previous_artist_sk IS NOT NULL THEN p.prnt_sk
      ELSE m.max_prnt_sk + r.row_index
    END AS prnt_sk,
    r.artist_uri,
    r.artist_name,
    r.artist_id,
    r.artist_url,
    r.cr_db_dt,
    r.updt_db_dt,
    TRUE AS is_current
  FROM new_rows r
  CROSS JOIN max_sk m
  {% if is_incremental() %}
  LEFT JOIN {{ this }} p
    ON r.previous_artist_sk = p.artist_sk
  {% else %}
  LEFT JOIN (
    SELECT NULL AS artist_sk, NULL AS prnt_sk
  ) p
    ON FALSE
  {% endif %}
)

-- Combine expired old versions and new current versions
SELECT * FROM expire_old_versions
UNION ALL
SELECT * FROM new_versioned_artists
ORDER BY artist_sk