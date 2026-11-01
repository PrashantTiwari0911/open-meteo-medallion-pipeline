"""Data quality checks. Each query returns the number of offending rows; any non-zero fails the run."""
import logging

import duckdb

log = logging.getLogger(__name__)

CHECKS = {
    "silver hourly PK unique": """SELECT count(*) FROM (SELECT location_id, ts_utc FROM silver.weather_hourly
                                  GROUP BY ALL HAVING count(*) > 1)""",
    "silver hourly keys not null": "SELECT count(*) FROM silver.weather_hourly WHERE location_id IS NULL OR ts_utc IS NULL",
    "temperature in [-60, 60]": "SELECT count(*) FROM silver.weather_hourly WHERE temperature_c NOT BETWEEN -60 AND 60",
    "humidity in [0, 100]": "SELECT count(*) FROM silver.weather_hourly WHERE relative_humidity_pct NOT BETWEEN 0 AND 100",
    "precipitation >= 0": "SELECT count(*) FROM silver.weather_hourly WHERE precipitation_mm < 0",
    "every location has data": """SELECT count(*) FROM silver.dim_location l
                                  WHERE NOT EXISTS (SELECT 1 FROM silver.weather_hourly h WHERE h.location_id = l.location_id)""",
    "gold daily PK unique": """SELECT count(*) FROM (SELECT city, date FROM gold.daily_city_summary
                               GROUP BY ALL HAVING count(*) > 1)""",
    "bronze rows match manifest": """SELECT count(*) FROM silver.ingest_manifest m
                                     WHERE hourly_rows <> (SELECT count(*) FROM silver.weather_hourly_history h
                                                           WHERE h.source_file = m.source_file)""",
}


def run_checks(warehouse) -> dict[str, int]:
    with duckdb.connect(str(warehouse), read_only=True) as con:
        results = {name: con.execute(sql).fetchall()[0][0] for name, sql in CHECKS.items()}
    for name, bad in results.items():
        log.info("DQ %-30s %s", name, "PASS" if bad == 0 else f"FAIL ({bad} rows)")
    failed = [n for n, bad in results.items() if bad]
    if failed:
        raise RuntimeError(f"data quality failed: {failed}")
    return results
