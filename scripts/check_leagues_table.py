"""Check silver.leagues table data."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from src.connections import get_warehouse_conn

conn = get_warehouse_conn()
cursor = conn.cursor()

# Check leagues table
cursor.execute("""
SELECT league_id, league_name, abbreviation
FROM silver.leagues
ORDER BY league_id
""")

print("=== SILVER.LEAGUES TABLE ===")
rows = cursor.fetchall()
if not rows:
    print("TABLE IS EMPTY!")
else:
    for row in rows:
        print(f"ID: {row[0]}, Name: {row[1]}, Abbrev: {row[2]}")

# Check teams and their league associations
cursor.execute("""
SELECT 
    t.team_id,
    t.team_abbrev,
    t.team_name,
    t.league_id,
    l.league_name,
    l.abbreviation
FROM silver.teams t
LEFT JOIN silver.leagues l ON t.league_id = l.league_id
WHERE t.season_year = 2026
ORDER BY t.team_abbrev
""")

print("\n=== SILVER.TEAMS WITH LEAGUE JOIN (2026) ===")
for row in cursor.fetchall():
    print(f"{row[1]:3s} | {row[2]:25s} | League ID: {row[3]} | League: {row[4]} | Abbrev: {row[5]}")

conn.close()
