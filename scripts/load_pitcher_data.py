"""Load pitcher data from existing bronze files into silver.game_pitching."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

from src.connections import get_onelake_fs, get_warehouse_conn, get_bronze_root
from src.transformer.game_pitching import populate_from_files
import structlog

logger = structlog.get_logger()


def main():
    conn = get_warehouse_conn()
    if not conn:
        logger.error("Database connection failed")
        return
    
    try:
        # Get filesystem
        fs = get_onelake_fs()
        bronze_root = get_bronze_root()
        
        logger.info("Searching for bronze game files...")
        
        # Find all bronze game files (2025 and 2026)
        pattern = f"{bronze_root}/games/year=*/month=*/*.parquet"
        bronze_files = fs.glob(pattern)
        bronze_files = sorted(set(bronze_files))
        
        logger.info(f"Found {len(bronze_files)} bronze Parquet files")
        
        if not bronze_files:
            logger.error("No bronze files found. Check OneLake path.")
            return
        
        # Load into silver
        logger.info("Loading pitcher data into silver.game_pitching...")
        records_loaded = populate_from_files(conn, fs, bronze_files)
        
        logger.info(f"✓ Load complete: {records_loaded} pitcher records loaded")
        
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
    main()
