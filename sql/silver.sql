-- SILVER: flatten raw API arrays into typed rows, keep lineage, dedupe to latest ingestion.
CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE TABLE silver.ingest_manifest AS
SELECT run_id, location_id, CAST(ingested_at AS TIMESTAMP) AS ingested_at,
       request_url, http_status, filename AS source_file,
       len(response.hourly.time) AS hourly_rows
FROM read_json('{bronze}/**/*.json', filename = true, union_by_name = true);

-- Every version of every hour, across all runs (used for forecast revision analysis)
CREATE OR REPLACE TABLE silver.weather_hourly_history AS
SELECT location_id,
       CAST(ingested_at AS TIMESTAMP) AS ingested_at,
       filename AS source_file,
       CAST(unnest(response.hourly.time) AS TIMESTAMP) AS ts_utc,
       CAST(unnest(response.hourly.temperature_2m) AS DOUBLE) AS temperature_c,
       CAST(unnest(response.hourly.relative_humidity_2m) AS DOUBLE) AS relative_humidity_pct,
       CAST(unnest(response.hourly.apparent_temperature) AS DOUBLE) AS apparent_temperature_c,
       CAST(unnest(response.hourly.precipitation) AS DOUBLE) AS precipitation_mm,
       CAST(unnest(response.hourly.wind_speed_10m) AS DOUBLE) AS wind_speed_kmh,
       CAST(unnest(response.hourly.wind_gusts_10m) AS DOUBLE) AS wind_gusts_kmh,
       CAST(unnest(response.hourly.cloud_cover) AS DOUBLE) AS cloud_cover_pct,
       CAST(unnest(response.hourly.weather_code) AS INTEGER) AS weather_code
FROM read_json('{bronze}/**/*.json', filename = true, union_by_name = true);

-- Current best view: one row per (location, hour), latest ingestion wins
CREATE OR REPLACE TABLE silver.weather_hourly AS
SELECT * EXCLUDE (rn), ts_utc > ingested_at AS is_forecast
FROM (
    SELECT *, row_number() OVER (PARTITION BY location_id, ts_utc ORDER BY ingested_at DESC) AS rn
    FROM silver.weather_hourly_history
)
WHERE rn = 1;

CREATE OR REPLACE TABLE silver.weather_daily AS
SELECT *
FROM (
    SELECT location_id,
           CAST(ingested_at AS TIMESTAMP) AS ingested_at,
           CAST(unnest(response.daily.time) AS DATE) AS date,
           CAST(unnest(response.daily.temperature_2m_max) AS DOUBLE) AS temp_max_c,
           CAST(unnest(response.daily.temperature_2m_min) AS DOUBLE) AS temp_min_c,
           CAST(unnest(response.daily.precipitation_sum) AS DOUBLE) AS precipitation_sum_mm,
           CAST(unnest(response.daily.wind_speed_10m_max) AS DOUBLE) AS wind_speed_max_kmh,
           CAST(unnest(response.daily.uv_index_max) AS DOUBLE) AS uv_index_max
    FROM read_json('{bronze}/**/*.json', filename = true, union_by_name = true)
)
QUALIFY row_number() OVER (PARTITION BY location_id, date ORDER BY ingested_at DESC) = 1;
