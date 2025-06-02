{{config(
    alias='dim_tracks',
    materialized='incremental',
    unique_key='track_sk',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    tags=['dim', 'analytics'],
    description='Dimension table for Spotify tracks, capturing unique track details and their current status.',
    schema='analytics',
    post_hook=[
               log('Post-hook for dim_tracks completed successfully', info=True)
    ]
)}}

WITH stg_tracks AS (
  SELECT DISTINCT
    trim(track_uri) AS track_uri,
    track_name,
    duration_mins,
    SPLIT(track_uri, ':')[2] AS track_id,
    CONCAT('https://open.spotify.com/track/ ', track_id) AS track_url,
    cr_db_dt,
    updt_db_dt
  FROM {{ ref('stg_processed_tracks') }}
),

-- Only try to read from existing table if running incrementally
existing_current_tracks AS (
  {% if is_incremental() %}
    SELECT *
    FROM {{ this }}
    WHERE is_current = TRUE
  {% else %}
    -- Dummy CTE to avoid breaking the query on first run
    SELECT
      NULL::VARCHAR AS track_sk,
      NULL::VARCHAR AS prnt_sk,
      NULL::VARCHAR AS track_uri,
      NULL::VARCHAR AS track_name,
      NULL::FLOAT AS duration_mins,
      NULL::VARCHAR AS track_id,
      NULL::VARCHAR AS track_url,
      NULL::TIMESTAMP AS cr_db_dt,
      NULL::TIMESTAMP AS updt_db_dt
    WHERE FALSE
  {% endif %}
),

-- Detect changes between staging and existing dimension
changed_tracks AS (
  SELECT
    s.track_uri,
    s.track_name,
    s.duration_mins,
    s.track_id,
    s.track_url,
    s.cr_db_dt,
    s.updt_db_dt,
    e.track_sk AS previous_track_sk
  FROM stg_tracks s
  LEFT JOIN existing_current_tracks e
    ON s.track_uri = e.track_uri
  WHERE e.track_sk IS NULL
     OR s.track_name != e.track_name
     OR s.duration_mins != e.duration_mins
  ),

-- Expire old versions of changed tracks
expire_old_versions AS (
  SELECT
    e.track_sk,
    e.prnt_sk,
    e.track_uri,
    e.track_name,
    e.duration_mins,
    e.track_id,
    e.track_url,
    e.cr_db_dt,
    e.updt_db_dt,
    FALSE AS is_current
  FROM existing_current_tracks e
  INNER JOIN changed_tracks c
    ON e.track_uri = c.track_uri
),

-- Compute max SKs for new inserts
max_sk AS (
  {% if is_incremental() %}
    SELECT
      COALESCE(MAX(track_sk), 0) AS max_track_sk,
      COALESCE(MAX(prnt_sk), 0) AS max_prnt_sk
    FROM {{ this }}
  {% else %}
    SELECT 0 AS max_track_sk, 0 AS max_prnt_sk
  {% endif %}
),

-- Assign row numbers for new/changed tracks
new_rows AS (
  SELECT *,
         ROW_NUMBER() OVER (ORDER BY track_uri) AS row_index
  FROM changed_tracks
),

-- Final assignment of new SKs and set is_current = TRUE
new_versioned_tracks AS (
  SELECT
    m.max_track_sk + r.row_index AS track_sk,
    CASE
      WHEN r.previous_track_sk IS NOT NULL THEN p.prnt_sk
      ELSE m.max_prnt_sk + r.row_index
    END AS prnt_sk,
    r.track_uri,
    r.track_name,
    r.duration_mins,
    r.track_id,
    r.track_url,
    r.cr_db_dt,
    r.updt_db_dt,
    TRUE AS is_current
  FROM new_rows r
  CROSS JOIN max_sk m
  {% if is_incremental() %}
  LEFT JOIN {{ this }} p
    ON r.previous_track_sk = p.track_sk
  {% else %}
  LEFT JOIN (
    SELECT NULL AS track_sk, NULL AS prnt_sk
  ) p
    ON FALSE
  {% endif %}
)

-- Combine expired old versions and new current versions
SELECT * FROM expire_old_versions
UNION ALL
SELECT * FROM new_versioned_tracks
ORDER BY track_sk