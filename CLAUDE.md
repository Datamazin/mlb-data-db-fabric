# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

An enterprise-grade ETL pipeline that ingests data from the official MLB Stats API, transforms it into analytics-ready datasets, and persists in **Azure Microsoft Fabric** (Fabric Warehouse + OneLake). Covers **seasons 2022–2026**. Downstream consumers are the 30 MLB club data science departments who query the gold layer via Power BI or the Fabric Warehouse T-SQL endpoint.

---

## Environment Setup

**Package manager:** [uv](https://docs.astral.sh/uv/) — do not use `pip` directly.  
**Python version:** 3.12 (pinned in `.python-version`).  
**Lockfile:** `uv.lock` is committed — always run `uv sync` after pulling.

```bash
uv sync --all-groups     # first-time or after pulling
```

Copy `.env.example` to `.env` and fill in Fabric credentials before running anything that touches the warehouse or OneLake.

### Common commands

```bash
make install            # uv sync --all-groups
make migrate            # apply T-SQL schema migrations → Fabric Warehouse
make transform          # run silver SQL transforms (staging → silver)
make aggregate          # run gold SQL aggregations (silver → gold)
make test               # full pytest suite (unit + integration)
make test-unit          # unit tests only (offline, no DB needed)
make test-integration   # integration tests only (requires Fabric connection)
make lint               # ruff check
make fmt                # ruff check --fix + ruff format
make typecheck          # mypy
```

### Targeted pytest runs

```bash
uv run pytest tests/unit/test_writer.py
uv run pytest tests/unit/test_writer.py::TestBronzeWriterGames::test_writes_to_correct_path
uv run pytest tests/integration/test_aggregate.py::TestStandingsSnap
```

### Running modules directly

```bash
uv run python -m src.transformer.transform [--scripts 007_games.sql] [--force] [--dry-run]
uv run python -m src.aggregator.aggregate  [--scripts 008_standings_snap.sql] [--force] [--dry-run]
uv run python migrations/migrate.py
uv run python -m src.scheduler.jobs [--run nightly_incremental] [--date YYYY-MM-DD]
uv run python scripts/explore_api.py
```

---

## Architecture: Medallion Pattern

```
MLB Stats API  →  EXTRACTION (Python/httpx)  →  OneLake bronze/ (raw Parquet)
                                                         ↓
                                     STAGING (Python/pandas)  →  staging.* tables
                                                         ↓
                                       TRANSFORMATION (T-SQL MERGE)  →  silver schema
                                                         ↓
                                        AGGREGATION (T-SQL)  →  gold schema
                                                         ↓
                                     Power BI semantic model (Fabric Warehouse endpoint)
```

### Layers

1. **Extraction (`src/extractor/`)** — calls the public MLB Stats API with `httpx` + asyncio, validates responses with Pydantic models, writes partitioned bronze Parquet to **OneLake** via `adlfs.AzureBlobFileSystem`. `MLBClient` enforces the token-bucket rate limit (8 req/s) and tenacity retry policy.

2. **Staging (`src/transformer/staging.py`)** — for each silver script that needs bronze data, a Python loader reads Parquet from OneLake, applies JSON extraction / type coercion / deduplication in pandas, then creates a `staging.*` table in Fabric Warehouse via pyodbc and bulk-inserts the cleaned rows. `STAGING_REGISTRY` maps script filenames to their loader function.

3. **Transformation (`src/transformer/` + `sql/silver/`)** — pure T-SQL MERGE scripts that read from `staging.*` and upsert into `silver.*` tables. The `Transformer` runner executes numbered scripts in alphabetical order and records checksums in `meta._silver_transforms`.

4. **Aggregation (`src/aggregator/` + `sql/gold/`)** — builds the consumer-facing `gold` layer from `silver`. Includes `CREATE OR ALTER VIEW` for dimension/fact views and MERGE-based upserts for materialized tables (`standings_snap`, `league_averages`, `head_to_head`). Checksum-tracked in `meta._gold_aggregations`.

5. **State and idempotency (`src/run_tracker/`)** — `meta.pipeline_runs` tracks job lifecycle; `meta.entity_checksums` prevents duplicate extraction.

6. **Orchestration (`src/scheduler/jobs.py`)** — three APScheduler jobs: `nightly_incremental` (extract + transform + aggregate), `roster_sync`, `standings_snapshot`.

---

## Connections and Credentials

All connection helpers live in `src/connections.py`:

- `get_warehouse_conn()` — returns a `pyodbc.Connection` to Fabric Warehouse. Uses `FABRIC_CONNECTION_STRING` env var directly, or builds from `FABRIC_SERVER` + `FABRIC_DATABASE` + `FABRIC_AUTH`. Supports `ActiveDirectoryMsi` (Fabric notebooks / Managed Identity) and `ActiveDirectoryServicePrincipal` (local dev / CI).
- `get_onelake_fs()` — returns an `adlfs.AzureBlobFileSystem` pointed at OneLake (`account_name="onelake"`, `DefaultAzureCredential`).
- `get_bronze_root()` — returns the OneLake path prefix (`{ONELAKE_WORKSPACE_ID}/{ONELAKE_LAKEHOUSE_NAME}.Lakehouse/Files/bronze`).

---

## Fabric Warehouse Schema Layout

```
Fabric Warehouse
├── schema: bronze       -- Raw API JSON as typed columns (pipeline-internal, loaded by Python)
├── schema: staging      -- Ephemeral load tables (created/dropped per transform run)
├── schema: silver       -- Cleaned, typed, deduplicated entities
├── schema: gold         -- Aggregated & fan-ready (PRIMARY consumer layer)
│   ├── dim_player, dim_team, dim_venue           (views)
│   ├── fact_game, fact_batting, fact_pitching    (views)
│   ├── fact_fielding, fact_pitch_mix             (views)
│   ├── leaderboards, player_season_summary       (views)
│   ├── standings_snap, league_averages, head_to_head  (materialized tables)
└── schema: meta         -- Pipeline run tracking & data lineage
    ├── pipeline_runs, entity_checksums
    ├── _silver_transforms, _gold_aggregations
```

**Rule:** Club data scientists query `gold` schema only. Never expose `bronze`, `silver`, or `staging`.

---

## MLB Stats API

- **Base URL:** `https://statsapi.mlb.com/api`
- **Auth:** None required
- **Rate limit:** ~10 req/s per IP (pipeline uses 8 req/s with token bucket)
- **User-Agent:** `MLB-DataPipeline/1.0 (MLB Data Engineering; dataeng@mlb.com)`
- **Note:** The live game feed uses `/v1.1/game/{gamePk}/feed/live`; most other endpoints are `/v1/...`

### Key Endpoints

| Endpoint | Description |
|---|---|
| `/v1/seasons` | Season metadata |
| `/v1/schedule` | Schedule with gamePk identifiers |
| `/v1.1/game/{gamePk}/feed/live` | Full game feed (play-by-play, stats) |
| `/v1/teams` | Team/division/league/venue info |
| `/v1/people/{personId}` | Player biographical details |
| `/v1/stats` | Aggregated leaderboard stats |
| `/v1/standings` | Division and wildcard standings |
| `/v1/sports/1/players` | Full player universe for a season |

---

## Key Design Rules

- **No mocks in integration tests** — use a real Fabric Warehouse connection. Set `FABRIC_CONNECTION_STRING` and run `make migrate` before running integration tests. Tests skip automatically when no DB is configured.
- Club teams query `gold` schema only; never expose `bronze` or `silver` directly.
- All timestamps stored as UTC (`SYSDATETIMEOFFSET()` in T-SQL, `datetime.now(timezone.utc)` in Python).
- Identifiers are integer surrogate keys from the MLB Stats API unless noted.
- Pipeline is fully idempotent — safe to re-run or replay any date range.
- T-SQL upserts use `MERGE ... WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT`. No `INSERT OR REPLACE`.
- Schema changes go through numbered migration scripts in `migrations/` applied via `migrations/migrate.py`. The migrate runner splits files on `GO` before executing batches via pyodbc.
- SQL scripts are numbered and run in filename order. Keep this sequencing model.
- Scheduled jobs pass `force=True` to transform/aggregate runs so data updates propagate even when the SQL checksum is unchanged.
- Bronze writers store flattened columns **and** the full `raw_json` blob so later staging loaders can recover nested fields without re-extracting.
- `staging.*` tables are always dropped in `finally` blocks — never leave them behind.

---

## T-SQL / Fabric Dialect Notes

Key differences from DuckDB SQL that appear throughout this codebase:

| DuckDB | T-SQL |
|---|---|
| `CREATE OR REPLACE VIEW` | `CREATE OR ALTER VIEW` |
| `INSERT OR REPLACE` | `MERGE ... WHEN MATCHED ... WHEN NOT MATCHED` |
| `BOOLEAN` | `BIT` |
| `TIMESTAMPTZ` | `DATETIMEOFFSET` |
| `current_timestamp` | `SYSDATETIMEOFFSET()` |
| `current_date` | `CAST(GETUTCDATE() AS DATE)` |
| `QUALIFY ROW_NUMBER() = 1` | Subquery with `WHERE rn = 1` |
| `COUNT(*) FILTER (WHERE ...)` | `SUM(CASE WHEN ... THEN 1 ELSE 0 END)` |
| `\|\|` string concat | `+` |
| `read_parquet('{path}')` | Python staging loader + T-SQL MERGE |

---

## Season Coverage & Game Type Codes

| Season | Date Range |
|---|---|
| 2022 | April 7 – Nov 5 |
| 2023 | March 30 – Nov 4 |
| 2024 | March 20 – Oct 30 |
| 2025 | March 27 – Oct 29 |
| 2026 | March 26 – TBD |

`S` = Spring Training, `R` = Regular Season, `F` = Wild Card, `D` = Division Series, `L` = Championship Series, `W` = World Series

Spring Training is captured but excluded from official stats by default.

---

## Security & Compliance

- API requests honour `Retry-After` headers; exponential backoff with jitter on HTTP 429 and 5xx (max 5 retries)
- Bronze Parquet and gold layer stored in OneLake with AES-256 SSE
- Fabric Warehouse access controlled via Entra ID (Managed Identity in notebooks, Service Principal in CI)
- No PII beyond publicly disclosed player biography data
- Service credentials via Azure Managed Identities — no long-lived secrets in code or environment files
