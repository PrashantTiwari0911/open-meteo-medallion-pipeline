"""Silver and gold layers: run the SQL files against the DuckDB warehouse."""
import logging
from pathlib import Path

import duckdb

from pipeline.config import ROOT

log = logging.getLogger(__name__)


def run_sql_file(con: duckdb.DuckDBPyConnection, name: str, **params) -> None:
    sql = (ROOT / "sql" / name).read_text().format(**params)
    con.execute(sql)
    log.info("ran %s", name)


def load_dim_location(con: duckdb.DuckDBPyConnection, locations: list[dict]) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("""CREATE OR REPLACE TABLE silver.dim_location (
        location_id VARCHAR PRIMARY KEY, name VARCHAR, latitude DOUBLE, longitude DOUBLE)""")
    con.executemany("INSERT INTO silver.dim_location VALUES (?, ?, ?, ?)",
                    [(l["id"], l["name"], l["latitude"], l["longitude"]) for l in locations])


def transform(cfg: dict) -> None:
    Path(cfg["warehouse"]).parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(cfg["warehouse"])) as con:
        load_dim_location(con, cfg["locations"])
        run_sql_file(con, "silver.sql", bronze=Path(cfg["bronze_dir"]).as_posix())
        run_sql_file(con, "gold.sql")
