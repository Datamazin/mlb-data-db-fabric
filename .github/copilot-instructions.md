# Copilot Instructions

## Commands

Use `uv`; do not install with `pip` or hand-edit `requirements.txt`.

```bash
make install            # uv sync --all-groups
make migrate            # apply T-SQL schema migrations → Fabric Warehouse
make transform          # run silver SQL transforms
make aggregate          # run gold SQL aggregations
make test               # full pytest suite
make test-unit          # unit tests only
make test-integration   # integration tests only (requires FABRIC_CONNECTION_STRING)
make lint               # ruff check
make fmt                # ruff check --fix + ruff format
make typecheck          # mypy
make backfill           # extract 2022–2025 historical data to OneLake
make scheduler          # start APScheduler daemon
make run-nightly        # trigger nightly_incremental once (yesterday)
make streamlit          # launch the Streamlit frontend
```

For targeted pytest runs, use `uv run pytest` directly:

```bash
uv run pytest tests/unit/test_writer.py
uv run pytest tests/unit/test_writer.py::TestBronzeWriterGames::test_writes_to_correct_path
uv run pytest tests/integration/test_jobs.py::TestStandingsSnapshot::test_snapshot_inserts_rows
```

## High-level architecture

This repository is a medallion-style MLB data pipeline targeting **Azure Microsoft Fabric** (Fabric Warehouse + OneLake):

1. **Extraction (`src/extractor/`)** calls the public MLB Stats API with `httpx` + asyncio, validates responses with Pydantic models, and writes partitioned bronze Parquet to **OneLake** via the custom `OneLakeFileSystem` wrapper in `src/connections.py`. `MLBClient` enforces the token-bucket rate limit (8 req/s) and tenacity retry policy. Game feeds use `/v1.1/game/{gamePk}/feed/live`; most other endpoints are `/v1/...`. `game_batting.py` and `game_pitching.py` extract per-player stats from game feed JSON and MERGE them directly into `silver.game_batting` / `silver.game_pitching`.
2. **Transformation (`src/transformer/` + `sql/silver/`)** loads bronze Parquet from OneLake into ephemeral `staging.*` tables in Fabric Warehouse (via pyodbc), then runs numbered T-SQL MERGE scripts to upsert into `silver.*`. The `STAGING_REGISTRY` dict maps script filenames to their Python loader. Staging tables are always dropped in `finally` blocks. Script checksums are recorded in `meta._silver_transforms`.
3. **Aggregation (`src/aggregator/` + `sql/gold/`)** builds the consumer-facing `gold` layer from `silver`. Gold scripts are checksum-tracked in `meta._gold_aggregations`. Downstream consumers (Power BI, Streamlit) query `gold` only — never `bronze`, `silver`, or `staging`.
4. **State and idempotency (`src/run_tracker/`)** live in Fabric Warehouse. `meta.pipeline_runs` tracks job lifecycle; `meta.entity_checksums` is used to skip already-extracted entities and support safe re-runs.
5. **Orchestration (`src/scheduler/jobs.py`)** wires three APScheduler jobs: `nightly_incremental` (extract + transform + aggregate), `roster_sync`, `standings_snapshot`.
6. **Frontend (`app.py`, `pages/`)** is a Streamlit app that reads the gold layer via `get_warehouse_conn()`. All pages import `get_conn` and `query_df` from `app.py`.

## Key conventions

- All connections go through `src/connections.py`: `get_warehouse_conn()` (pyodbc → Fabric Warehouse), `get_onelake_fs()` (custom `OneLakeFileSystem` → OneLake), `get_bronze_root()` (OneLake path prefix).
- Bronze Parquet stores **both** typed columns and the full `raw_json` blob so staging loaders can recover nested fields without re-extracting.
- Silver and gold loading is fully idempotent via T-SQL `MERGE ... WHEN MATCHED ... WHEN NOT MATCHED`. No `INSERT OR REPLACE`.
- Integration tests use **real Fabric Warehouse connections**, not mocks. Set `FABRIC_CONNECTION_STRING` and run `make migrate` before running them. Tests skip automatically when no connection is configured.
- SQL scripts are numbered and run in filename order. Keep the sequencing model.
- Scheduled jobs pass `force=True` to transform/aggregate runs so data updates propagate even when the SQL checksum is unchanged.
- All timestamps are UTC: `SYSDATETIMEOFFSET()` in T-SQL, `datetime.now(timezone.utc)` in Python.
- T-SQL dialect: `CREATE OR ALTER VIEW`, `DATETIMEOFFSET`, `BIT`, `SYSDATETIMEOFFSET()`, `+` for string concat, `GO` batch separator, `OFFSET n ROWS FETCH NEXT n ROWS ONLY` for pagination.
