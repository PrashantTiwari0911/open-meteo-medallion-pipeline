"""Offline tests: synthetic API responses -> bronze -> silver/gold -> DQ. No network."""
import duckdb
import pytest

from pipeline.extract import build_params, write_bronze
from pipeline.quality import run_checks
from pipeline.transform import transform

LOC = {"id": "melbourne", "name": "Melbourne", "latitude": -37.8, "longitude": 144.9}
HOURS = ["2026-11-01T00:00", "2026-11-01T01:00", "2026-11-01T02:00"]


def fake_response(temp: float) -> dict:
    n = len(HOURS)
    return {
        "hourly": {"time": HOURS, "temperature_2m": [temp] * n, "relative_humidity_2m": [50] * n,
                   "apparent_temperature": [temp] * n, "precipitation": [0.0] * n,
                   "wind_speed_10m": [10.0] * n, "wind_gusts_10m": [20.0] * n,
                   "cloud_cover": [30] * n, "weather_code": [1] * n},
        "daily": {"time": ["2026-11-01"], "temperature_2m_max": [temp], "temperature_2m_min": [temp],
                  "precipitation_sum": [0.0], "wind_speed_10m_max": [10.0], "uv_index_max": [5.0]},
    }


def land(bronze, ingested_at, temp, run_id="run-0000"):
    write_bronze(bronze, {"run_id": run_id, "location_id": "melbourne", "ingested_at": ingested_at,
                          "request_url": "x", "http_status": 200, "response": fake_response(temp)})


@pytest.fixture
def cfg(tmp_path):
    return {"bronze_dir": tmp_path / "bronze", "warehouse": tmp_path / "wh.duckdb", "locations": [LOC]}


def test_build_params_uses_utc_and_config():
    cfg = {"hourly": ["a", "b"], "daily": ["c"], "past_days": 7, "forecast_days": 7}
    p = build_params(cfg, LOC)
    assert p["timezone"] == "UTC" and p["hourly"] == "a,b" and p["latitude"] == -37.8


def test_silver_dedupes_to_latest_ingestion(cfg):
    land(cfg["bronze_dir"], "2026-11-01T00:30:00", 15.0, "run-aaaa")
    land(cfg["bronze_dir"], "2026-11-01T06:30:00", 18.0, "run-bbbb")
    transform(cfg)
    with duckdb.connect(str(cfg["warehouse"])) as con:
        assert con.execute("SELECT count(*) FROM silver.weather_hourly_history").fetchone()[0] == 6
        rows = con.execute("SELECT temperature_c FROM silver.weather_hourly").fetchall()
        spread = con.execute("SELECT max(temp_spread_c) FROM gold.forecast_revisions").fetchone()[0]
    assert len(rows) == 3 and all(r[0] == 18.0 for r in rows)
    assert spread == 3.0
    run_checks(cfg["warehouse"])


def test_rerun_is_idempotent(cfg):
    land(cfg["bronze_dir"], "2026-11-01T00:30:00", 15.0)
    transform(cfg)
    transform(cfg)
    with duckdb.connect(str(cfg["warehouse"])) as con:
        assert con.execute("SELECT count(*) FROM silver.weather_hourly").fetchone()[0] == 3


def test_dq_fails_on_bad_data(cfg):
    land(cfg["bronze_dir"], "2026-11-01T00:30:00", 999.0)
    transform(cfg)
    with pytest.raises(RuntimeError, match="temperature"):
        run_checks(cfg["warehouse"])
