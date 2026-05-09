# MLB Pipeline Jobs

This document describes every scheduled and on-demand job in the pipeline, what each one does, when it runs, and how to trigger it manually.

---

## Architecture Reminder

All jobs follow the same medallion path:

```
MLB Stats API  →  Bronze (OneLake Parquet)  →  Silver (Fabric Warehouse)  →  Gold (Fabric Warehouse)
```

- **Bronze** — raw API responses stored as partitioned Parquet on OneLake. Never modified after write.
- **Silver** — cleaned, typed, deduplicated tables populated via T-SQL MERGE from staging.
- **Gold** — consumer-facing dimension and fact tables queried by Power BI.

Run tracking for every job is recorded in `meta.pipeline_runs`.

---

## Scheduled Jobs

Three jobs run on Windows Task Scheduler with `StartWhenAvailable = true` — if the machine was asleep at the scheduled time, each job fires as soon as the machine wakes.

| Job | Schedule | Window |
|---|---|---|
| `nightly_incremental` | 2:00 AM ET daily | Mar – Nov |
| `standings_snapshot` | 3:00 AM ET daily | Apr – Oct |
| `roster_sync` | 6:00 AM ET daily | Year-round |

---

## Job: nightly_incremental

**Source:** [src/scheduler/jobs.py](../src/scheduler/jobs.py)  
**Schedule:** 2:00 AM ET, daily (March – November)  
**Purpose:** Ingest the previous day's game results and push them through all three medallion layers.

### Steps

**1. Extract schedule** (`/v1/schedule`)  
Fetches the full list of game PKs for `target_date` (defaults to yesterday). Game types: R, F, D, L, W (no Spring Training).

**2. Extract game feeds** (`/v1.1/game/{gamePk}/feed/live`)  
Re-fetches every game from the target date unconditionally — game data changes until a game reaches Final status, so the checksum gate is bypassed. Checksums are recorded in `meta.entity_checksums` after extraction.

**3. Populate batting and pitching stats**  
Reads the day's bronze Parquet file and calls the `game_batting` and `game_pitching` loaders directly into silver, bypassing the staging loader pattern.

**4. Transform (all silver scripts)**  
Runs every `sql/silver/*.sql` script in order with `force=True` so silver always reflects the latest data regardless of SQL checksum.

**5. Aggregate (all gold scripts)**  
Runs every `sql/gold/*.sql` script in order with `force=True`, refreshing all dimension and fact views and materialized tables.

### Manual trigger

```powershell
# Yesterday's games (default)
uv run python -m src.scheduler.jobs --run nightly_incremental

# Specific date
uv run python -m src.scheduler.jobs --run nightly_incremental --date 2026-05-07
```

---

## Job: standings_snapshot

**Source:** [src/scheduler/jobs.py](../src/scheduler/jobs.py)  
**Schedule:** 3:00 AM ET, daily (April – October)  
**Purpose:** Recompute the materialized standings table from scratch using all Final regular-season games accumulated to date.

### Steps

**1. Run `008_standings_snap.sql`**  
Executes a single gold aggregation script that rebuilds `gold.standings_snap` — win/loss records, run differentials, and standings position per division. Reads entirely from silver; no API calls.

This job is intentionally cheap: no extraction, no staging, one SQL script. It runs an hour after `nightly_incremental` so standings always reflect the previous night's completed games.

### Manual trigger

```powershell
uv run python -m src.scheduler.jobs --run standings_snapshot
```

---

## Job: roster_sync

**Source:** [src/scheduler/jobs.py](../src/scheduler/jobs.py)  
**Schedule:** 6:00 AM ET, daily (year-round)  
**Purpose:** Keep dimension data fresh — team rosters, player bios, venue details, and season metadata.

### Steps

**1. Extract teams** (`/v1/teams`)  
Fetches all 30 MLB teams for the active season and writes to `bronze/teams/season={year}/`.

**2. Extract players** (`/v1/sports/1/players` + `/v1/people?personIds=...`)  
Fetches the full player universe for the season (~1,200 IDs). Checks which player IDs are already in bronze and skips them — only new players (call-ups, signings) are hydrated via batched `/v1/people?personIds=...` calls (50 players per request). On most days this results in 0–2 API calls for players.

**3. Extract seasons** (`/v1/seasons`)  
Fetches season metadata (start/end dates, games per team) for 2022 through the active season.

**4. Extract venues** (`/v1/venues?venueIds=...&hydrate=location,fieldInfo`)  
Queries `silver.venues` for known venue IDs, then batch-fetches location and field detail (city, state, country, capacity, surface type, roof type) in groups of 50.

**5. Transform (4 silver scripts)**

| Script | What it updates |
|---|---|
| `004_venues.sql` | City, state, country, capacity, surface, roof type |
| `005_teams.sql` | Team name, abbreviation, division, league, venue |
| `006_players.sql` | Player bio: name, DOB, height, weight, position, bats/throws |
| `012_seasons.sql` | Season date ranges and game counts |

**6. Aggregate (6 gold scripts)**

| Script | What it updates |
|---|---|
| `001_dim_player.sql` | `gold.dim_player` materialized table |
| `002_dim_team.sql` | `gold.dim_team` materialized table |
| `003_dim_venue.sql` | `gold.dim_venue` materialized table |
| `010_dim_home_team.sql` | `gold.dim_home_team` (role-playing copy for Power BI) |
| `011_dim_away_team.sql` | `gold.dim_away_team` (role-playing copy for Power BI) |
| `012_dim_season.sql` | `gold.dim_season` materialized table |

### Manual trigger

```powershell
uv run python -m src.scheduler.jobs --run roster_sync
```

---

## Job: backfill (on-demand)

**Source:** [src/extractor/backfill.py](../src/extractor/backfill.py)  
**Schedule:** Manual only  
**Purpose:** Load historical game data for one or more past seasons (2022–2026) into bronze. Run once per season to populate history; does not run nightly.

### Steps

For each season (processed sequentially):

**1. Extract teams and players**  
Pulls dimension data for the season before processing games, so silver joins have the required foreign keys.

**2. Extract schedule + game feeds (parallelised by month)**  
Splits the season date range into months and runs up to 3 months concurrently. Within each month:
- Fetches the schedule to get game PKs
- Checks `meta.entity_checksums` to skip games already extracted (idempotent)
- Fetches remaining game feeds at up to 8 concurrent requests
- Records checksums for successfully extracted games

A full five-season backfill (2022–2026) completes in under four hours.

### Manual trigger

```powershell
# All default seasons (2022–2025)
uv run python -m src.extractor.backfill

# Specific seasons
uv run python -m src.extractor.backfill --seasons 2024 2025

# Dry run (prints what would be extracted, no API calls)
uv run python -m src.extractor.backfill --seasons 2026 --dry-run
```

---

## Checking Job History

Query `meta.pipeline_runs` to see recent job execution history:

```sql
SELECT TOP 20
    job_name,
    status,
    started_at,
    completed_at,
    records_extracted,
    records_loaded,
    error_message
FROM meta.pipeline_runs
ORDER BY started_at DESC;
```

---

## Re-registering the Task Scheduler Tasks

If the Windows Task Scheduler tasks need to be recreated (e.g. after a machine rebuild):

```powershell
# Run as Administrator
powershell -ExecutionPolicy Bypass -File scripts\setup_task_scheduler.ps1
```

This registers `MLB-Nightly-Incremental`, `MLB-Roster-Sync`, and `MLB-Standings-Snapshot` with `StartWhenAvailable = true`.
