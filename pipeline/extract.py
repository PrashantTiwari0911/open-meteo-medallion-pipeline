"""Bronze layer: call the Open-Meteo API and land raw responses untouched, append-only."""
import datetime as dt
import json
import logging
import uuid
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)


def make_session() -> requests.Session:
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def build_params(cfg: dict, loc: dict) -> dict:
    return {
        "latitude": loc["latitude"],
        "longitude": loc["longitude"],
        "hourly": ",".join(cfg["hourly"]),
        "daily": ",".join(cfg["daily"]),
        "past_days": cfg["past_days"],
        "forecast_days": cfg["forecast_days"],
        "timezone": "UTC",
    }


def write_bronze(bronze_dir: Path, envelope: dict) -> Path:
    ingested = dt.datetime.fromisoformat(envelope["ingested_at"])
    out_dir = Path(bronze_dir) / f"ingest_date={ingested.date()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{envelope['location_id']}_{ingested:%Y%m%dT%H%M%S}_{envelope['run_id'][:8]}.json"
    path.write_text(json.dumps(envelope))
    return path


def extract(cfg: dict, session: requests.Session | None = None) -> list[Path]:
    session = session or make_session()
    run_id = str(uuid.uuid4())
    written, failures = [], []
    for loc in cfg["locations"]:
        params = build_params(cfg, loc)
        try:
            resp = session.get(cfg["api_url"], params=params, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as exc:
            log.error("extract failed for %s: %s", loc["id"], exc)
            failures.append(loc["id"])
            continue
        envelope = {
            "run_id": run_id,
            "location_id": loc["id"],
            "ingested_at": dt.datetime.now(dt.UTC).replace(tzinfo=None).isoformat(timespec="seconds"),
            "request_url": resp.url,
            "http_status": resp.status_code,
            "response": resp.json(),
        }
        path = write_bronze(cfg["bronze_dir"], envelope)
        log.info("bronze: wrote %s", path.name)
        written.append(path)
    if failures:
        raise RuntimeError(f"extract failed for: {failures}")
    return written
