"""
Backfill pitcher decision data for games missing silver.game_pitching records.

Usage:
    uv run python scripts/backfill_pitcher_decisions.py

This script:
1. Extracts game feed data from MLB API for specified game_pks
2. Writes to bronze layer in OneLake
3. Loads pitcher stats into silver.game_pitching table
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

load_dotenv()

import structlog

from src.connections import get_bronze_root, get_onelake_fs, get_warehouse_conn
from src.extractor.client import MLBClient
from src.extractor.extract import extract_game_feeds
from src.extractor.writer import BronzeWriter
from src.transformer.game_pitching import populate_from_files

log = structlog.get_logger(__name__)


async def main() -> None:
    """Extract game feeds and load pitcher stats."""
    
    # Game PKs missing pitcher decisions (March 26 - April 7, 2026)
    game_pks = [823649, 823651, 823080, 823240, 823239, 823238, 823648]
    
    log.info("backfill_start", game_pks=game_pks, count=len(game_pks))
    
    # Step 1: Extract game feeds to bronze
    fs = get_onelake_fs()
    bronze_root = get_bronze_root()
    writer = BronzeWriter(fs, bronze_root)
    
    async with MLBClient() as client:
        extracted = await extract_game_feeds(client, writer, game_pks)
        log.info("extraction_complete", extracted=len(extracted), failed=len(game_pks) - len(extracted))
    
    if not extracted:
        log.error("no_games_extracted")
        return
    
    # Step 2: Find the bronze files we just wrote (March & April 2026)
    file_paths: list[str] = []
    
    for month in ["03", "04"]:
        pattern = f"{bronze_root}/games/year=2026/month={month}/games_*.parquet"
        matches = fs.glob(pattern)
        if matches:
            file_paths.extend(matches)
            log.info("found_bronze_files", month=month, files=len(matches))
    
    if not file_paths:
        log.error("no_bronze_files_found")
        return
    
    # Step 3: Load pitcher stats from bronze to silver
    conn = get_warehouse_conn()
    try:
        rows = populate_from_files(conn, fs, file_paths)
        log.info("pitcher_stats_loaded", rows=rows, files=len(file_paths))
    finally:
        conn.close()
    
    log.info("backfill_complete", game_pks=game_pks, rows=rows)


if __name__ == "__main__":
    asyncio.run(main())
