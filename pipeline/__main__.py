"""CLI: python -m pipeline [run|extract|transform|dq|query]"""
import argparse
import logging

import duckdb

from pipeline.config import load_config
from pipeline.extract import extract
from pipeline.quality import run_checks
from pipeline.transform import transform


def query(cfg: dict) -> None:
    with duckdb.connect(str(cfg["warehouse"]), read_only=True) as con:
        for table in ["silver.weather_hourly", "gold.daily_city_summary",
                      "gold.city_comparison_7d", "gold.forecast_revisions"]:
            n = con.execute(f"SELECT count(*) FROM {table}").fetchall()[0][0]
            print(f"\n== {table} ({n} rows)")
            con.sql(f"SELECT * FROM {table} LIMIT 5").show()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(prog="pipeline")
    parser.add_argument("stage", nargs="?", default="run", choices=["run", "extract", "transform", "dq", "query"])
    stage = parser.parse_args().stage
    cfg = load_config()
    if stage in ("run", "extract"):
        extract(cfg)
    if stage in ("run", "transform"):
        transform(cfg)
    if stage in ("run", "dq"):
        run_checks(cfg["warehouse"])
    if stage in ("run", "query"):
        query(cfg)


if __name__ == "__main__":
    main()
