"""Quick verification that Mets pitcher wins match team wins after backfill."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

load_dotenv()

from src.connections import get_warehouse_conn

conn = get_warehouse_conn()
cursor = conn.cursor()

# Get Mets team_id
cursor.execute("SELECT team_id FROM gold.dim_team WHERE team_abbrev = 'NYM' AND season_year = 2026")
mets_id = cursor.fetchone()[0]

# Check team wins from games table (correct count)
cursor.execute(f"""
SELECT COUNT(*) as team_wins
FROM silver.games g
WHERE (g.home_team_id = {mets_id} OR g.away_team_id = {mets_id})
  AND g.season_year = 2026
  AND g.status = 'Final'
  AND g.game_type = 'R'
  AND ((g.home_team_id = {mets_id} AND g.home_score > g.away_score) 
       OR (g.away_team_id = {mets_id} AND g.away_score > g.home_score))
""")
team_wins = cursor.fetchone()[0]

# Check pitcher wins from game_pitching table
cursor.execute(f"""
SELECT SUM(gp.wins) as pitcher_wins
FROM silver.game_pitching gp
JOIN silver.games g ON gp.game_pk = g.game_pk
WHERE gp.team_id = {mets_id}
  AND g.season_year = 2026
  AND g.status = 'Final'
  AND g.game_type = 'R'
""")

row = cursor.fetchone()
pitcher_wins = row[0] if row and row[0] is not None else 0

print("=" * 50)
print("METS 2026 REGULAR SEASON - VERIFICATION")
print("=" * 50)
print(f"Team wins (from game scores):  {team_wins}")
print(f"Pitcher wins (sum):            {pitcher_wins}")
print(f"\nMatch: {'✓ FIXED!' if team_wins == pitcher_wins else '✗ Still broken'}")
print("=" * 50)

conn.close()
