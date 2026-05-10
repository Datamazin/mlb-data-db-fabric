"""Backfill pitcher data for all games missing game_pitching records."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

from src.connections import get_onelake_fs, get_warehouse_conn, get_bronze_root
from src.extractor.client import MLBClient
from src.extractor.extract import extract_game_feeds
from src.extractor.writer import BronzeWriter
from src.transformer.game_pitching import populate_from_files
import structlog

logger = structlog.get_logger()


async def main():
    conn = get_warehouse_conn()
    if not conn:
        logger.error("Database connection failed")
        return
    
    try:
        # Find all games missing pitcher data
        missing_games = conn.execute("""
            SELECT 
                sg.game_pk,
                sg.game_date,
                sg.season_year,
                ht.team_abbrev as home_team,
                at.team_abbrev as away_team
            FROM silver.games sg
            LEFT JOIN gold.dim_team ht ON sg.home_team_id = ht.team_id AND sg.season_year = ht.season_year
            LEFT JOIN gold.dim_team at ON sg.away_team_id = at.team_id AND sg.season_year = at.season_year
            WHERE sg.status = 'Final'
              AND sg.game_type = 'R'
              AND NOT EXISTS (SELECT 1 FROM silver.game_pitching gp WHERE gp.game_pk = sg.game_pk)
            ORDER BY sg.game_date
        """).fetchall()
        
        if not missing_games:
            logger.info("No missing pitcher data - all games have records")
            return
        
        game_pks = [row[0] for row in missing_games]
        logger.info(f"Found {len(game_pks)} games missing pitcher data")
        
        # Group by year for reporting
        by_year = {}
        for row in missing_games:
            year = row[2]
            by_year[year] = by_year.get(year, 0) + 1
        
        for year, count in sorted(by_year.items()):
            logger.info(f"  {year}: {count} games")
        
        # Extract game feeds
        logger.info("Starting extraction from MLB API...")
        fs = get_onelake_fs()
        bronze_root = get_bronze_root()
        
        async with MLBClient() as client:
            await extract_game_feeds(client, BronzeWriter(fs, bronze_root), game_pks)
        
        logger.info("Extraction complete. Loading into silver.game_pitching...")
        
        # Find bronze files (they're in year/month folders under games, not game_feed)
        bronze_files = []
        for row in missing_games:
            year = row[2]
            month = row[1].month
            pattern = f"{bronze_root}/games/year={year}/month={month:02d}/*.parquet"
            files = fs.glob(pattern)
            bronze_files.extend(files)
        
        bronze_files = sorted(set(bronze_files))  # deduplicate
        logger.info(f"Found {len(bronze_files)} bronze Parquet files")
        
        # Load into silver
        records_loaded = populate_from_files(conn, fs, bronze_files)
        
        logger.info(f"✓ Backfill complete: {records_loaded} pitcher records loaded")
        
        # Verify
        remaining = conn.execute("""
            SELECT COUNT(*)
            FROM silver.games sg
            WHERE sg.status = 'Final'
              AND sg.game_type = 'R'
              AND NOT EXISTS (SELECT 1 FROM silver.game_pitching gp WHERE gp.game_pk = sg.game_pk)
        """).fetchone()[0]
        
        logger.info(f"Remaining games without pitcher data: {remaining}")
        
    finally:
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())
