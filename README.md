# 🎧 Spotify Million Playlist Data Warehouse

## 🧱 Overview

This project builds a fully automated, end-to-end data pipeline and analytics warehouse using the **Spotify Million Playlist Dataset (MPD)**. The solution covers data ingestion, batch preparation, PySpark transformation, Snowflake loading, DBT-based modeling, and orchestration using Apache Airflow — all within a local WSL development environment.

---

## 🛠 Tech Stack

| Layer             | Tool/Service                                |
|------------------|----------------------------------------------|
| **Ingestion**     | Local batch files (simulating raw zone)     |
| **Processing**    | PySpark (JSON → Parquet conversion)         |
| **Storage**       | Local FS + Snowflake (cloud warehouse)       |
| **Transformation**| dbt (staging, dimensions, facts)            |
| **Validation**    | dbt tests (built-in + custom)               |
| **Orchestration** | Apache Airflow (DAG automation)             |
| **Monitoring**    | Airflow + email alerts (success/failure)    |


---

## 📌 Architecture

```text
[Raw JSON Files (MPD)]
        ↓
[Batch Loader Shell Script]
        ↓
[PySpark JSON Parser → Parquet]
        ↓
[Local Parquet Folder (Staging)]
        ↓
[PySpark + Python → Snowflake Upload]
        ↓
[Snowflake Raw Tables]
        ↓
[DBT Staging & Modeling (Dim/Fct)]
        ↓
[Airflow DAG Orchestration + Email]
        ↓
[BI Dashboards in Superset/Tableau]
```

---

## 🧱 Data Warehouse Architecture

```text
                            +----------------------+
                            |   Raw Zone (MPD)     |
                            |  JSON Slices (Local) |
                            +----------+-----------+
                                       ↓
                             Batch Loader Script (Shell)
                                       ↓
                        +--------------v-----------------+
                        |     PySpark Transformation     |
                        |  (Normalize JSON to Parquet)   |
                        +--------------+-----------------+
                                       ↓
                              Parquet Files (Staging)
                                       ↓
                            Spark Snowflake Connector
                                       ↓
                      +----------------v-----------------+
                      |         Snowflake: Raw           |
                      |    Tables: raw_tracks, ...       |
                      +----------------+-----------------+
                                       ↓
                                DBT Transformations
                          (staging → dim/fct models)
                                       ↓
                        +--------------v----------------+
                        |     Snowflake: Analytics      |
                        |  dim_*, fct_playlist_tracks   |
                        +--------------+----------------+
                          
```


---

## 🚀 Pipeline Steps (Orchestrated by Airflow)

### 1. **Prepare JSON Batch (Shell Script)**
- Selects unprocessed MPD slice JSON files
- Moves batch to processing folder `first-100/`
- Appends filenames to `processed_files.txt` for tracking

### 2. **Convert JSON → Parquet (PySpark)**
- Converts JSON playlists and tracks to flattened Parquet files:
  ```bash
  /home/jiraiya/data/spotify-mpd/.../data_parquet/first_100/
  ```
- Handles nested `tracks[]` and exploded playlist metadata

### 3. **Upload to Snowflake (PySpark & JDBC)**
- Tracks and playlists written to `raw_tracks` and `raw_playlists` using Spark Snowflake connector
- JDBC jars: `snowflake-jdbc`, `spark-snowflake`

### 4. **DBT Transformations**
- Project: `spotify_dbt`
- Models:
  - `staging/` – flatten raw tables
  - `analytics/` – `dim_tracks`, `dim_albums`, `dim_artists`, `dim_playlists`
  - `fct/` – `fct_playlist_tracks`
- Includes:
  - `dbt run`
  - `dbt test`
  - `dbt run-operation print_row_counts`

### 5. **Validation & Alerts**
- Email sent on success:
  - Includes output from `dbt_row_counts` log
- Email on failure if any task fails
- Log files persisted in:
  ```bash
  /home/jiraiya/codebase/Proj_Spotify/logs/
  ```

---

## 📆 Scheduling Logic

- Airflow DAG: `spotify_data_pipeline`
- Triggered manually
- Controlled to run **exactly 10 times** using Airflow Variable:
  ```bash
  Variable: spotify_pipeline_run_count
  Max: 10 runs
  ```
- DAG re-triggers itself via `TriggerDagRunOperator` until 10 runs complete

---



## 🧪 Data Quality & Testing

- All dbt models tested with:
  - `not_null`
  - `unique`
  - `relationships`
  - Custom tests:
    - `no_nulls.sql`
    - `no_duplicates.sql`
- SCD Type2 was implemented for Dimension tables to track the history.

---

## 🗂 Project Folder Structure

```plaintext
spotify-data-app/
├── dags/
│   └── spotify_data_pipeline.py          # Full DAG logic (PySpark → Snowflake → DBT)
├── spotify_dbt/
│   ├── models/
│   │   ├── staging/
│   │   └── analytics/
│   └── tests/
│       ├── no_nulls.sql
│       └── no_duplicates.sql
├── scripts/
│   ├── prepare_batch.sh
│   ├── spotify-json-parquet.py
│   ├── load_parquet_to_snowflake_playlists.py
│   └── load_parquet_to_snowflake_tracks.py
├── logs/
│   └── ... (Airflow log directories)
├── data/
│   └── spotify_million_playlist_dataset/
└── README.md
```

---

## 📎 References

- [Spotify MPD Dataset](https://research.spotify.com/datasets/)
- [Snowflake Docs](https://docs.snowflake.com/)
- [Apache Airflow](https://airflow.apache.org/)
- [DBT Docs](https://docs.getdbt.com/)
- [Superset](https://superset.apache.org/)

---

## 🙋 Need Help?

Raise an issue or connect if you need help with:
- Extending the Airflow DAG
- Building more dbt models or tests

