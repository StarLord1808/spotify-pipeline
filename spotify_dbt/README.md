# Spotify Analytics dbt Project

## Overview
This dbt project implements a data warehouse solution for Spotify playlist and track analytics using a dimensional modeling approach. The project transforms raw Spotify data into a structured analytics-ready format with proper data versioning and incremental processing capabilities.

## Architecture Overview

### Data Flow Architecture
```
Raw Spotify Data (Source)
        ↓
Staging Layer (Data Cleaning & Preparation)
        ↓
Analytics Layer (Dimensional Model)
        ↓
Fact & Dimension Tables (Star Schema)
```

## Project Structure

### 1. Source Layer
**Database:** `spotify_db`
- `playlists` - Raw playlist metadata
- `tracks` - Raw track information with playlist associations

### 2. Staging Layer (`models/staging/`)
The staging layer performs data cleaning, standardization, and basic transformations.

#### `stg_processed_playlists.sql`
**Purpose:** Cleans and standardizes playlist data
**Key Transformations:**
- Converts duration from milliseconds to hours
- Standardizes timestamp handling with fixed modified dates
- Filters out playlists with no tracks
- Adds audit timestamps (CR_DB_DT, UPDT_DB_DT)

**Configuration:**
- Materialized as incremental table
- Uses append strategy for new data
- Auto-syncs schema changes

#### `stg_processed_tracks.sql`
**Purpose:** Processes track data and extracts Spotify URIs
**Key Transformations:**
- Converts track duration from milliseconds to minutes
- Extracts Spotify IDs from URIs (track_id, album_id, artist_id)
- Generates Spotify URLs for tracks, albums, and artists
- Maintains playlist-track relationships

**Configuration:**
- Materialized as incremental table
- Uses append strategy
- Ordered by playlist_id for consistent processing

### 3. Analytics Layer (`models/analytics/`)
Implements a star schema with versioned dimensions and fact tables.

#### Dimension Tables (`models/analytics/dim/`)
All dimension tables implement **Slowly Changing Dimension Type 2 (SCD2)** pattern:

##### `dim_playlists.sql`
**Purpose:** Versioned playlist dimension
**Key Features:**
- Tracks playlist metadata changes over time
- Maintains current and historical versions
- Uses surrogate keys (playlist_sk) and parent keys (prnt_sk)
- Incremental merge strategy with change detection

**Schema:**
- `playlist_sk` - Surrogate key (unique for each version)
- `prnt_sk` - Parent key (groups all versions of same playlist)
- `Playlist_ID` - Business key
- `name`, `no_of_tracks`, `no_of_albums`, etc. - Playlist attributes
- `is_current` - Flag indicating active version

##### `dim_tracks.sql`
**Purpose:** Versioned track dimension
**Key Features:**
- Maintains track metadata with version history
- Tracks changes in track names and duration
- Links to Spotify track URLs

**Schema:**
- `track_sk` - Surrogate key
- `prnt_sk` - Parent key for version grouping
- `track_uri`, `track_name`, `duration_mins` - Track attributes
- `track_id`, `track_url` - Spotify identifiers

##### `dim_albums.sql`
**Purpose:** Versioned album dimension
**Key Features:**
- Maintains album metadata with SCD2 versioning
- Extracted from track data (distinct albums)
- Links to Spotify album URLs

##### `dim_artists.sql`
**Purpose:** Versioned artist dimension
**Key Features:**
- Maintains artist metadata with version history
- Extracted from track data (distinct artists)
- Links to Spotify artist URLs

#### Fact Tables (`models/analytics/fact/`)

##### `fct_playlist_tracks.sql`
**Purpose:** Central fact table linking playlists to tracks
**Key Features:**
- Many-to-many relationship between playlists and tracks
- References dimension tables via surrogate keys
- Includes track position within playlist
- Incremental processing with deduplication

**Schema:**
- `playlist_id` - Business key reference
- `track_sk`, `album_sk`, `artist_sk` - Foreign keys to dimensions
- `track_position` - Order of track in playlist
- Descriptive attributes for easy querying

## Data Processing Strategy

### Incremental Processing
All models use incremental materialization for efficient processing:
- **Staging tables:** Append new records only
- **Dimension tables:** Merge strategy with SCD2 implementation
- **Fact tables:** Merge with deduplication logic

### Change Detection
Dimension tables implement sophisticated change detection:
1. Compare staging data with current dimension records
2. Identify new records and changed attributes
3. Expire old versions (set `is_current = FALSE`)
4. Insert new versions with updated surrogate keys
5. Maintain parent-child relationships via `prnt_sk`

### Data Quality & Testing

#### Custom Tests (`macros/custom_tests/`)
- `no_duplicates.sql` - Ensures unique values in specified columns
- `no_nulls.sql` - Validates required fields are not null

#### Utility Macros (`macros/`)
- `print_row_counts.sql` - Debug macro for monitoring table sizes
- `generate_schema_name.sql` - Custom schema naming convention

## Key Design Decisions

### 1. Versioning Strategy
- **SCD Type 2** for all dimensions to maintain historical context
- Surrogate keys (`_sk`) for stable references
- Parent keys (`prnt_sk`) to group versions of same entity

### 2. Incremental Strategy
- **Append** for staging (raw data ingestion)
- **Merge** for analytics layer (handles updates and new records)
- Automatic schema evolution with `sync_all_columns`

### 3. Data Relationships
- **Star Schema** design for optimal query performance
- Fact table maintains both surrogate key references and descriptive attributes
- Denormalized approach in fact table for easier business user access

### 4. Performance Optimizations
- Incremental processing reduces processing time
- Strategic indexing via unique keys
- Efficient change detection algorithms
- Proper ordering for consistent results

## Usage Scenarios

### 1. Playlist Analytics
Query playlist trends, growth patterns, and metadata changes over time using `dim_playlists` with version history.

### 2. Track Popularity Analysis
Analyze track appearances across playlists using `fct_playlist_tracks` joined with track dimensions.

### 3. Artist/Album Insights
Examine artist and album distribution across playlists using the star schema relationships.

### 4. Historical Reporting
Leverage SCD2 implementation to analyze how playlist compositions and metadata have changed over time.

## Deployment Configuration

### Target Schemas
- **Staging:** Default target schema
- **Analytics:** Custom 'analytics' schema for dimensional model

### Dependencies
- `dbt-labs/dbt_utils` (version 1.1.1) for additional utility functions

### Post-hooks
All analytics models include logging post-hooks for monitoring successful completion.

## Monitoring & Maintenance

### Data Quality Checks
- Built-in uniqueness validation on surrogate keys
- Custom tests for null value detection
- Row count monitoring via debug macros

### Performance Monitoring
- Track incremental processing performance
- Monitor dimension table growth
- Validate fact table referential integrity

## SCD Type 2 Implementation Example

### Sample Data Snapshot: `dim_playlists`

The following example demonstrates how SCD Type 2 versioning works in practice:

#### Scenario: A playlist "My Chill Mix" gets updated with new tracks

**Initial Load (Day 1):**
```sql
playlist_sk | prnt_sk | Playlist_ID | name          | no_of_tracks | no_of_followers | is_current | cr_db_dt            | updt_db_dt
1          | 1       | 12345       | My Chill Mix  | 25           | 150             | TRUE       | 2024-01-01 10:00:00 | 2024-01-01 10:00:00
```

**After Update (Day 5) - User adds 5 more tracks and gains followers:**
```sql
playlist_sk | prnt_sk | Playlist_ID | name          | no_of_tracks | no_of_followers | is_current | cr_db_dt            | updt_db_dt
1          | 1       | 12345       | My Chill Mix  | 25           | 150             | FALSE      | 2024-01-01 10:00:00 | 2024-01-01 10:00:00
2          | 1       | 12345       | My Chill Mix  | 30           | 175             | TRUE       | 2024-01-05 14:30:00 | 2024-01-05 14:30:00
```

**After Another Update (Day 10) - Playlist renamed and more tracks added:**
```sql
playlist_sk | prnt_sk | Playlist_ID | name              | no_of_tracks | no_of_followers | is_current | cr_db_dt            | updt_db_dt
1          | 1       | 12345       | My Chill Mix      | 25           | 150             | FALSE      | 2024-01-01 10:00:00 | 2024-01-01 10:00:00
2          | 1       | 12345       | My Chill Mix      | 30           | 175             | FALSE      | 2024-01-05 14:30:00 | 2024-01-05 14:30:00
3          | 1       | 12345       | Ultimate Chill Vibes | 35        | 200             | TRUE       | 2024-01-10 09:15:00 | 2024-01-10 09:15:00
```

#### Key SCD Type 2 Features Demonstrated:

1. **Surrogate Keys (`playlist_sk`)**: Each version gets a unique identifier (1, 2, 3)
2. **Parent Keys (`prnt_sk`)**: All versions share the same parent key (1) to group them
3. **Business Key (`Playlist_ID`)**: Remains constant (12345) across all versions
4. **Version Control (`is_current`)**: Only the latest version is marked as current
5. **Historical Preservation**: All previous versions are maintained with `is_current = FALSE`
6. **Audit Trail**: Each version has its own creation and update timestamps

#### Querying Patterns:

**Get Current State:**
```sql
SELECT * FROM dim_playlists 
WHERE Playlist_ID = 12345 AND is_current = TRUE;
-- Returns: Ultimate Chill Vibes with 35 tracks
```

**Get Full History:**
```sql
SELECT * FROM dim_playlists 
WHERE Playlist_ID = 12345 
ORDER BY playlist_sk;
-- Returns: All 3 versions showing evolution over time
```

**Point-in-Time Query (as of Day 6):**
```sql
SELECT * FROM dim_playlists 
WHERE Playlist_ID = 12345 
  AND cr_db_dt <= '2024-01-06'
  AND (updt_db_dt > '2024-01-06' OR is_current = TRUE)
ORDER BY playlist_sk DESC 
LIMIT 1;
-- Returns: Version 2 (My Chill Mix with 30 tracks)
```

This approach allows for comprehensive historical analysis while maintaining referential integrity in the fact table through stable surrogate key relationships.

## Future Enhancements
- Add data quality tests in `schema.yml` files
- Implement data freshness checks
- Add documentation for business users
- Consider partitioning strategies for large datasets
- Implement data lineage documentation