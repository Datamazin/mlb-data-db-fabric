"""
Scheduled pipeline jobs — Fabric Warehouse version.

Three recurring jobs:

  nightly_incremental   02:00 ET daily (Mar–Nov)
      Extract prior-day games, transform bronze→silver, aggregate silver→gold.

  standings_snapshot    03:00 ET daily (Apr–Oct)
      Recompute gold.standings_snap from all Final regular-season games.

  roster_sync           06:00 ET daily
      Pull 40-man rosters + player bios; re-run silver teams/players transforms.

Legacy file-based publish is removed; delivery is now via the shared Fabric
Warehouse endpoint + Power BI semantic model.

Scheduler entry point:
    uv run python -m src.scheduler.jobs
    uv run python -m src.scheduler.jobs --run nightly_incremental
    uv run python -m src.scheduler.jobs --run roster_sync
    uv run python -m src.scheduler.jobs --run standings_snapshot

PowerShell audit one-liner (daemon mode + timestamped log):
    $ts=Get-Date -Format yyyyMMdd_HHmmss; $log="logs/scheduler_$ts.log"; New-Item -ItemType Directory -Force logs | Out-Null; uv run python -m src.scheduler.jobs *>&1 | Tee-Object -FilePath $log -Append
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date, timedelta
from pathlib import Path

import pyodbc
import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
from src.logging_config import configure_logging

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from connections import get_warehouse_conn, get_onelake_fs, get_bronze_root
from extractor.client import MLBClient
from extractor.extract import extract_game_feeds, extract_players, extract_schedule, extract_teams
from extractor.writer import BronzeWriter
from run_tracker.tracker import RunTracker
from transformer.game_batting import populate_from_files as populate_batting
from transformer.game_pitching import populate_from_files as populate_pitching
from transformer.transform import Transformer
from aggregator.aggregate import Aggregator

log = structlog.get_logger(__name__)
PROJECT_ROOT = Path(__file__).parent.parent.parent

ACTIVE_SEASON = 2026
INCREMENTAL_GAME_TYPES = "R,F,D,L,W"


def _open_conn() -> pyodbc.Connection:
    return get_warehouse_conn()


# ── Job: Nightly Incremental ───────────────────────────────────────────────────

async def nightly_incremental(
    target_date: date | None = None,
) -> None:
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    log.info("nightly_incremental_start", target_date=str(target_date))

    fs = get_onelake_fs()
    bronze_root = get_bronze_root()
    conn = _open_conn()
    tracker = RunTracker(conn)
    writer = BronzeWriter(fs, bronze_root)
    run_id = tracker.start_run("nightly_incremental", target_date=target_date)

    try:
        async with MLBClient() as client:
            season_year = target_date.year
            game_pks = await extract_schedule(
                client, writer,
                start_date=target_date,
                end_date=target_date,
                season_year=season_year,
                game_types=INCREMENTAL_GAME_TYPES,
            )

            new_pks = tracker.filter_unextracted("game_feed", [str(pk) for pk in game_pks])
            skipped = len(game_pks) - len(new_pks)
            if skipped:
                log.info("nightly_incremental_skip", skipped=skipped)

            extracted_count = 0
            if new_pks:
                extracted_pks = await extract_game_feeds(
                    client, writer, [int(pk) for pk in new_pks]
                )
                extracted_count = len(extracted_pks)
                tracker.record_checksums_bulk(
                    "game_feed",
                    [
                        {
                            "entity_key": str(pk),
                            "raw_json": f'{{"gamePk":{pk}}}',
                            "source_url": f"/v1.1/game/{pk}/feed/live",
                        }
                        for pk in extracted_pks
                    ],
                )

        # Populate per-game batting and pitching from today's Parquet file
        game_file = (
            f"{bronze_root}/games"
            f"/year={target_date.year}"
            f"/month={target_date.month:02d}"
            f"/games_{target_date.strftime('%Y%m%d')}.parquet"
        )
        if fs.exists(game_file):
            batting_rows = populate_batting(conn, fs, [game_file])
            pitching_rows = populate_pitching(conn, fs, [game_file])
            log.info(
                "nightly_incremental_game_stats",
                batting_rows=batting_rows,
                pitching_rows=pitching_rows,
            )

        transformer = Transformer(
            conn=conn,
            fs=fs,
            bronze_root=bronze_root,
            year_glob=str(target_date.year),
        )
        transform_result = transformer.run(force=True)
        if not transform_result.success:
            raise RuntimeError(f"Transform failed: {transform_result.errors}")

        aggregator = Aggregator(conn)
        agg_result = aggregator.run(force=True)
        if not agg_result.success:
            raise RuntimeError(f"Aggregate failed: {agg_result.errors}")

        tracker.complete_run(
            run_id,
            records_extracted=extracted_count,
            records_loaded=transform_result.total_rows_loaded,
        )
        log.info(
            "nightly_incremental_done",
            target_date=str(target_date),
            games_extracted=extracted_count,
            rows_loaded=transform_result.total_rows_loaded,
        )

    except Exception as exc:
        tracker.fail_run(run_id, str(exc))
        log.error("nightly_incremental_failed", error=str(exc))
        raise
    finally:
        conn.close()


# ── Job: Roster Sync ───────────────────────────────────────────────────────────

async def roster_sync(season_year: int = ACTIVE_SEASON) -> None:
    log.info("roster_sync_start", season_year=season_year)

    fs = get_onelake_fs()
    bronze_root = get_bronze_root()
    conn = _open_conn()
    tracker = RunTracker(conn)
    writer = BronzeWriter(fs, bronze_root)
    run_id = tracker.start_run("roster_sync", season_year=season_year)

    try:
        async with MLBClient() as client:
            team_ids = await extract_teams(client, writer, season_year)
            player_ids = await extract_players(client, writer, season_year)
            log.info("roster_sync_extracted", teams=len(team_ids), players=len(player_ids))

        transformer = Transformer(conn=conn, fs=fs, bronze_root=bronze_root)
        result = transformer.run(scripts=["005_teams.sql", "006_players.sql"], force=True)
        if not result.success:
            raise RuntimeError(f"Transform failed: {result.errors}")

        tracker.complete_run(
            run_id,
            records_extracted=len(player_ids),
            records_loaded=result.total_rows_loaded,
        )
        log.info("roster_sync_done", season_year=season_year, rows_loaded=result.total_rows_loaded)

    except Exception as exc:
        tracker.fail_run(run_id, str(exc))
        log.error("roster_sync_failed", season_year=season_year, error=str(exc))
        raise
    finally:
        conn.close()


# ── Job: Standings Snapshot ────────────────────────────────────────────────────

async def standings_snapshot() -> None:
    log.info("standings_snapshot_start")

    conn = _open_conn()
    tracker = RunTracker(conn)
    run_id = tracker.start_run("standings_snapshot")

    try:
        aggregator = Aggregator(conn)
        result = aggregator.run(scripts=["008_standings_snap.sql"], force=True)
        if not result.success:
            raise RuntimeError(f"Standings aggregate failed: {result.errors}")

        tracker.complete_run(run_id, records_loaded=result.total_rows_affected)
        log.info("standings_snapshot_done", rows=result.total_rows_affected)

    except Exception as exc:
        tracker.fail_run(run_id, str(exc))
        log.error("standings_snapshot_failed", error=str(exc))
        raise
    finally:
        conn.close()


# ── Scheduler wiring ───────────────────────────────────────────────────────────

def build_scheduler() -> AsyncIOScheduler:
    tz = "America/New_York"
    scheduler = AsyncIOScheduler()

    scheduler.add_job(
        nightly_incremental,
        CronTrigger(hour=2, minute=0, month="3-11", timezone=tz),
        id="nightly_incremental",
        name="Nightly Incremental",
        misfire_grace_time=3600,
        coalesce=True,
    )
    scheduler.add_job(
        roster_sync,
        CronTrigger(hour=6, minute=0, timezone=tz),
        id="roster_sync",
        name="Roster Sync",
        misfire_grace_time=3600,
        coalesce=True,
    )
    scheduler.add_job(
        standings_snapshot,
        CronTrigger(hour=3, minute=0, month="4-10", timezone=tz),
        id="standings_snapshot",
        name="Standings Snapshot",
        misfire_grace_time=3600,
        coalesce=True,
    )

    return scheduler


# ── CLI entry point ────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MLB pipeline scheduler.")
    parser.add_argument(
        "--run",
        choices=["nightly_incremental", "roster_sync", "standings_snapshot"],
        default=None,
        metavar="JOB",
    )
    parser.add_argument("--date", type=date.fromisoformat, default=None, metavar="YYYY-MM-DD")
    parser.add_argument("--season", type=int, default=ACTIVE_SEASON)
    return parser.parse_args()


async def _run_once(args: argparse.Namespace) -> None:
    if args.run == "nightly_incremental":
        await nightly_incremental(target_date=args.date)
    elif args.run == "roster_sync":
        await roster_sync(season_year=args.season)
    elif args.run == "standings_snapshot":
        await standings_snapshot()


async def _run_daemon() -> None:
    scheduler = build_scheduler()
    scheduler.start()
    log.info("scheduler_started", jobs=[j.id for j in scheduler.get_jobs()])
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
        pass
    finally:
        scheduler.shutdown()
        log.info("scheduler_stopped")


def main() -> None:
    configure_logging()
    load_dotenv(PROJECT_ROOT / ".env", override=True)
    args = _parse_args()
    if args.run:
        asyncio.run(_run_once(args))
    else:
        asyncio.run(_run_daemon())


if __name__ == "__main__":
    main()
