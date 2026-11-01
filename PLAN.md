# Implementation Plan: Open-Meteo Medallion Pipeline

**Brief:** Build an end-to-end data pipeline on the Open-Meteo API using a Medallion architecture, runnable locally. Deliver: (1) a repo with run instructions, (2) screenshots of the AI conversations.

**Total realistic effort: ~6 hours** (core 4.5 h + docs/evidence 1.5 h). Stretch items are listed separately. Ship the core first.

---

## 1. Stack (one choice, kept small)

| Concern | Choice | Why |
|---|---|---|
| Language / env | Python 3.12 via `uv` (pinned in `.python-version`) | `uv` is already installed; 3.12 avoids library wheel gaps on 3.14 |
| HTTP | `requests` + `urllib3.Retry` | Retries, backoff and timeouts with no extra framework |
| Storage | Raw JSON files (bronze) + **DuckDB** file `warehouse.duckdb` (silver/gold) | Zero-server, runs anywhere, fast SQL, reads JSON/Parquet natively |
| Transforms | Plain SQL files run by a small Python runner | Easy to review; layer logic is visible as SQL |
| Data quality | SQL assertions run after each layer, fail the run on breach | Shows DQ thinking without adding Great Expectations |
| Orchestration | CLI `python -m pipeline run [--stage ...]` + `Makefile` | Runs locally with one command; Dagster/Airflow is a "next step" |
| Tests | `pytest` with a saved API response fixture (no network) | Deterministic CI-style tests |
| Lint | `ruff` | One tool, fast |

Docker is not installed on this machine, so the run path is `uv` only. Add a Dockerfile only if time allows (stretch).

---

## 2. Architecture

```
Open-Meteo Forecast API (hourly + daily, past_days=7, forecast_days=7)
        │  extract.py  (retries, timeout, one request per location)
        ▼
BRONZE  data/bronze/open_meteo/ingest_date=YYYY-MM-DD/<location>_<run_ts>.json
        raw response, untouched + manifest row (url, params, status, run_id, ingested_at)
        │  silver/*.sql
        ▼
SILVER  silver.weather_hourly   one row per (location_id, ts_utc), typed, deduped
        silver.weather_daily    one row per (location_id, date)
        silver.dim_location     from config/locations.yaml
        │  gold/*.sql
        ▼
GOLD    gold.daily_city_summary      temp min/max/avg, rain total, max wind, hot/wet-day flags
        gold.city_comparison_7d      rolling 7-day stats per city, ranked
        gold.forecast_revisions      how a given hour's forecast changed across runs
```

### Layer rules

**Bronze (raw, append-only, immutable)**
- Save the exact JSON response; never edit or overwrite.
- Partition by ingest date; file name includes location and run timestamp.
- `bronze.ingest_manifest` table: `run_id, location_id, request_url, http_status, row_count, ingested_at, file_path`.
- Request `timezone=UTC` so all timestamps are UTC from the source.

**Silver (clean, conformed, deduped)**
- Flatten the API's column arrays (`hourly.time[]`, `hourly.temperature_2m[]`, …) into rows with DuckDB `UNNEST`.
- Cast types, rename to snake_case, attach `location_id`, `ingested_at`, `source_file`.
- `is_forecast = ts_utc > ingested_at` (past hours are model "actuals", future hours are forecasts).
- **Dedupe:** the same hour arrives in many runs (past_days overlaps). Keep the latest `ingested_at` per `(location_id, ts_utc)`. This makes reruns idempotent.
- Full rebuild from bronze every run (data is small; simpler than MERGE and always correct).

**Gold (business-ready)**
- Aggregates and flags an analyst would use directly. Every gold table documents its grain in the README.

### Variables to pull
- Hourly: `temperature_2m, relative_humidity_2m, apparent_temperature, precipitation, wind_speed_10m, wind_gusts_10m, weather_code, cloud_cover`
- Daily: `temperature_2m_max, temperature_2m_min, precipitation_sum, wind_speed_10m_max, sunrise, sunset, uv_index_max`

### Locations (`config/locations.yaml`)
Melbourne, Sydney, Brisbane, Perth, Adelaide (id, name, lat, lon). Adding a city = one YAML line.

---

## 3. Repo layout

```
Loop/
├── README.md                 # run steps, architecture diagram, design decisions, trade-offs
├── PLAN.md                   # this file
├── pyproject.toml / uv.lock / .python-version
├── Makefile                  # setup, run, test, lint, clean, query
├── config/
│   ├── locations.yaml
│   └── pipeline.yaml         # variables, past_days, forecast_days, paths
├── pipeline/
│   ├── __main__.py           # CLI: run / extract / silver / gold / dq
│   ├── config.py
│   ├── extract.py            # API client + bronze writer + manifest
│   ├── transform.py          # runs sql/ files in order against DuckDB
│   ├── quality.py            # DQ checks, raises on failure
│   └── logging_setup.py
├── sql/
│   ├── silver/01_dim_location.sql, 02_weather_hourly.sql, 03_weather_daily.sql
│   ├── gold/01_daily_city_summary.sql, 02_city_comparison_7d.sql, 03_forecast_revisions.sql
│   └── dq/silver_checks.sql, gold_checks.sql
├── tests/
│   ├── fixtures/melbourne_forecast.json
│   ├── test_extract.py       # URL/params built right, retries, bronze file written
│   ├── test_silver.py        # flattening row counts, dedupe keeps latest
│   └── test_end_to_end.py    # mocked API → gold tables exist and pass DQ
├── data/                     # gitignored (bronze files + warehouse.duckdb)
└── docs/
    ├── ai-evidence/          # screenshots of AI conversations
    └── ai-usage.md           # what AI did, what you checked/changed
```

---

## 4. Milestones

| # | Milestone | Tasks | Done when | Effort |
|---|---|---|---|---|
| **M0** | Scaffold | `uv init`, pin 3.12, add deps (`requests duckdb pyyaml pytest ruff`), folders, `.gitignore`, `git init`, Makefile skeleton, config YAMLs | `make setup` works on a clean clone | 20 min |
| **M1** | Bronze ingestion | API client with retries/timeout; one call per location; write raw JSON to partitioned path; write manifest row; structured logging; `run_id` per run | `make extract` creates 5 JSON files + 5 manifest rows; rerun adds new files, never overwrites | 50 min |
| **M2** | Silver layer | SQL to read bronze JSON with `read_json`, `UNNEST` arrays to rows, cast, add lineage cols, dedupe by latest `ingested_at`; daily table; dim_location | `silver.weather_hourly` has 5 cities × 14 days × 24 h = **1,680 rows** after one run, and the same count after a second run (idempotent) | 60 min |
| **M3** | Gold layer | daily_city_summary, city_comparison_7d, forecast_revisions (empty-safe if only one run) | Gold tables queryable; `make query` prints a sample per table | 40 min |
| **M4** | Data quality | Checks: PK uniqueness, not-null keys, temp in [-60, 60], humidity in [0, 100], precip ≥ 0, every location present, row-count drift vs manifest | A deliberately broken fixture makes `make dq` fail with a clear message | 30 min |
| **M5** | Tests + CLI polish | pytest fixture-based tests (no network); `make run` = extract → silver → dq → gold → dq; `ruff` clean | `make test` green; `make run` end to end in < 30 s | 40 min |
| **M6** | README + diagram | Quickstart (3 commands), Mermaid architecture diagram, layer contracts/grain, design decisions, trade-offs, "what I'd do in production" | A reviewer can clone and run without asking you anything | 40 min |
| **M7** | AI evidence | Screenshot key AI conversations (this planning chat, build sessions, any debugging); write `docs/ai-usage.md`: prompts used, what you accepted, what you corrected and why | Folder has 5–10 labelled screenshots + short write-up | 30 min |
| **M8** | Final check + submit | Fresh clone into `/tmp`, run `make setup run test`; push to GitHub; send link | Clean-clone run passes | 20 min |

**Core total: ~5 h 30 min.** Add ~30 min buffer for surprises → plan for 6 h.

### Order if time runs out
M0 → M1 → M2 → M3 → M6 (README) → M7 (evidence) → M8. Cut M4/M5 depth before cutting README or evidence; the brief explicitly asks for run instructions and AI screenshots.

---

## 5. Stretch (only after M8 is shippable)

| Item | Value to reviewer | Effort |
|---|---|---|
| Historical backfill from Archive API (`archive-api.open-meteo.com`) into the same bronze → silver flow | Shows backfill vs incremental handling | 45 min |
| Forecast accuracy gold table (forecast at T-24h vs later actual) | Strong analytical story; needs runs on 2+ days | 30 min |
| Parquet export of gold (`COPY ... TO 'gold/*.parquet'`) | Shows lakehouse-style output | 10 min |
| Dagster or Prefect wrapper with a daily schedule | Production orchestration story | 60 min |
| Dockerfile + `docker compose run pipeline` | "Runs anywhere" | 30 min (needs Docker installed) |
| dbt-duckdb instead of raw SQL runner | Lineage + built-in tests | 90 min |
| Small Streamlit page over gold | Visual demo | 45 min |

---

## 6. Design decisions to state in the README

1. **Why DuckDB:** local, serverless, columnar, reads JSON directly. Production equivalent: Snowflake/BigQuery/Databricks with the same SQL.
2. **Why raw JSON in bronze:** replayable. If silver logic changes, rebuild from bronze without calling the API again.
3. **Why full rebuild of silver:** data is tiny; rebuild is simpler and always consistent. At scale: incremental MERGE on `(location_id, ts_utc)` partitioned by date.
4. **Why `past_days=7`:** overlapping windows make dedupe/idempotency real and give "actuals" to compare forecasts against.
5. **UTC everywhere:** request `timezone=UTC`; convert to local only in gold if needed.
6. **Failure handling:** retries with exponential backoff on 429/5xx; one failed city logs an error and is recorded in the manifest; the run fails if any city fails (no partial silent loads).
7. **Production next steps:** scheduler (Airflow/Dagster), object storage (S3/ADLS) for bronze, Delta/Iceberg tables, alerting on DQ failures, secrets/config per environment, CI running `make test`.

---

## 7. Risks

| Risk | Mitigation |
|---|---|
| Python 3.14 wheel gaps (duckdb etc.) | Pin 3.12 with `uv python pin 3.12` |
| API rate limit / outage during demo | Retries + tests use fixtures, never the live API |
| Reviewer has no `make` (Windows) | README also lists the raw `uv run python -m pipeline run` commands |
| Running out of time | Follow the cut order in section 4 |

---

## 8. AI evidence checklist

- [ ] Screenshot this planning conversation (prompt + plan)
- [ ] Screenshot the scaffold/build prompts and results per milestone
- [ ] Screenshot at least one case where you corrected or rejected an AI suggestion (reviewers look for judgement, not just output)
- [ ] Screenshot debugging sessions (failing test → fix)
- [ ] `docs/ai-usage.md`: tools used, how you used them, what you verified manually
