"""
CLI backfill — populate silver.game_batting from all bronze game Parquet files on OneLake.

Usage:
    uv run python scripts/populate_game_batting.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

load_dotenv()

from src.connections import get_bronze_root, get_onelake_fs, get_warehouse_conn
from src.transformer.game_batting import populate_from_files


def run() -> None:
    conn = get_warehouse_conn()
    fs = get_onelake_fs()
    bronze_root = get_bronze_root()

    pattern = f"{bronze_root}/games/year=*/month=*/*.parquet"
    file_paths = fs.glob(pattern)

    if not file_paths:
        print(f"No bronze game files found matching {pattern}")
        conn.close()
        return

    print(f"Processing {len(file_paths)} file(s) into silver.game_batting …")
    total = populate_from_files(conn, fs, file_paths)
    conn.close()
    print(f"\nDone — {total:,} batting rows written to silver.game_batting")


if __name__ == "__main__":
    run()
