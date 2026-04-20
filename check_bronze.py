#!/usr/bin/env python
import os
from dotenv import load_dotenv
from pathlib import Path

_PROJECT_ROOT = Path(__file__).parent
load_dotenv(_PROJECT_ROOT / ".env", override=True)

from src.connections import get_onelake_fs, get_bronze_root

fs = get_onelake_fs()
bronze_root = get_bronze_root()

print(f"Bronze root: {bronze_root}\n")
print("Bronze directory contents:\n")

try:
    paths = fs.glob(f"{bronze_root}/**")
    if paths:
        for path in sorted(paths)[:100]:  # Show first 100
            print(f"  {path}")
        print(f"\nTotal files found: {len(paths)}")
    else:
        print("  (no files found)")
except Exception as e:
    print(f"  Error listing: {e}")

SELECT 'games' AS tbl, COUNT(*) AS rows FROM silver.games   UNION ALL
SELECT 'players',      COUNT(*)           FROM silver.players UNION ALL
SELECT 'teams',        COUNT(*)           FROM silver.teams   UNION ALL
SELECT 'game_boxscore', COUNT(*)          FROM silver.game_boxscore;
