"""Check league_abbrev values in gold.dim_team."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from src.connections import get_warehouse_conn

conn = get_warehouse_conn()
cursor = conn.cursor()

# Check league_abbrev values
cursor.execute("""
SELECT DISTINCT league_abbrev, COUNT(*) as team_count
FROM gold.dim_team
WHERE season_year = 2026
GROUP BY league_abbrev
ORDER BY league_abbrev
""")

print("=== LEAGUE_ABBREV VALUES IN gold.dim_team (2026) ===")
for row in cursor.fetchall():
    print(f"League: {row[0]}, Teams: {row[1]}")

# Check specific teams
cursor.execute("""
SELECT team_abbrev, team_name, league_abbrev, division_name
FROM gold.dim_team
WHERE season_year = 2026
ORDER BY league_abbrev, team_abbrev
""")

print("\n=== ALL TEAMS WITH LEAGUE INFO ===")
for row in cursor.fetchall():
    print(f"{row[0]:3s} | {row[1]:25s} | League: {row[2]} | Division: {row[3]}")

# Test the filter that the app is using
cursor.execute("""
SELECT COUNT(*) as player_count
FROM silver.game_batting gb
JOIN silver.games sg ON gb.game_pk = sg.game_pk
JOIN gold.dim_team t ON gb.team_id = t.team_id AND sg.season_year = t.season_year
WHERE sg.season_year = 2026
  AND sg.game_type IN ('R')
  AND sg.status = 'Final'
  AND t.league_abbrev = 'AL'
GROUP BY gb.player_id
""")

row = cursor.fetchone()
print(f"\n=== TEST QUERY (AL players in 2026) ===")
print(f"Player count with AL filter: {row[0] if row else 0}")

conn.close()
