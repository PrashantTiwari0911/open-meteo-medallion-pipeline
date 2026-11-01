-- GOLD: business-ready tables.
CREATE SCHEMA IF NOT EXISTS gold;

-- Grain: one row per city per day
CREATE OR REPLACE TABLE gold.daily_city_summary AS
SELECT l.name AS city, d.date,
       d.temp_min_c, d.temp_max_c,
       round(h.avg_temp_c, 1) AS avg_temp_c,
       round(h.avg_humidity_pct, 1) AS avg_humidity_pct,
       d.precipitation_sum_mm, d.wind_speed_max_kmh,
       h.max_gust_kmh, d.uv_index_max,
       d.temp_max_c >= 30 AS is_hot_day,
       d.precipitation_sum_mm >= 1 AS is_wet_day,
       h.forecast_hours > 0 AS is_forecast
FROM silver.weather_daily d
JOIN silver.dim_location l ON l.location_id = d.location_id
JOIN (
    SELECT location_id, CAST(ts_utc AS DATE) AS date,
           avg(temperature_c) AS avg_temp_c, avg(relative_humidity_pct) AS avg_humidity_pct,
           max(wind_gusts_kmh) AS max_gust_kmh, count(*) FILTER (WHERE is_forecast) AS forecast_hours
    FROM silver.weather_hourly GROUP BY ALL
) h ON h.location_id = d.location_id AND h.date = d.date;

-- Grain: one row per city, last 7 observed (non-forecast) days
CREATE OR REPLACE TABLE gold.city_comparison_7d AS
SELECT city,
       round(avg(temp_max_c), 1) AS avg_daily_max_c,
       round(avg(temp_min_c), 1) AS avg_daily_min_c,
       round(sum(precipitation_sum_mm), 1) AS total_rain_mm,
       count(*) FILTER (WHERE is_wet_day) AS wet_days,
       count(*) FILTER (WHERE is_hot_day) AS hot_days,
       rank() OVER (ORDER BY avg(temp_max_c) DESC) AS warmest_rank
FROM gold.daily_city_summary
WHERE NOT is_forecast
GROUP BY city;

-- Grain: one row per city per hour; how the forecast moved across pipeline runs
CREATE OR REPLACE TABLE gold.forecast_revisions AS
SELECT l.name AS city, h.ts_utc,
       count(DISTINCT h.ingested_at) AS versions,
       round(min(h.temperature_c), 1) AS min_temp_forecast_c,
       round(max(h.temperature_c), 1) AS max_temp_forecast_c,
       round(max(h.temperature_c) - min(h.temperature_c), 1) AS temp_spread_c
FROM silver.weather_hourly_history h
JOIN silver.dim_location l ON l.location_id = h.location_id
GROUP BY ALL;
