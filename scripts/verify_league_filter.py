"""Verify league filter now works in Leaders queries."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from src.connections import get_warehouse_conn

conn = get_warehouse_conn()
cursor = conn.cursor()

# Test the batting query with AL filter
cursor.execute("""
SELECT COUNT(DISTINCT p.player_id) as player_count
FROM silver.game_batting gb
JOIN silver.games sg ON gb.game_pk = sg.game_pk
JOIN silver.players p ON gb.player_id = p.player_id
JOIN gold.dim_team t ON gb.team_id = t.team_id AND sg.season_year = t.season_year
WHERE sg.season_year = 2026
  AND sg.game_type IN ('R')
  AND sg.status = 'Final'
  AND t.league_abbrev = 'AL'
""")

row = cursor.fetchone()
al_players = row[0]

# Test with NL filter
cursor.execute("""
SELECT COUNT(DISTINCT p.player_id) as player_count
FROM silver.game_batting gb
JOIN silver.games sg ON gb.game_pk = sg.game_pk
JOIN silver.players p ON gb.player_id = p.player_id
JOIN gold.dim_team t ON gb.team_id = t.team_id AND sg.season_year = t.season_year
WHERE sg.season_year = 2026
  AND sg.game_type IN ('R')
  AND sg.status = 'Final'
  AND t.league_abbrev = 'NL'
""")

row = cursor.fetchone()
nl_players = row[0]

# Test with no filter (MLB)
cursor.execute("""
SELECT COUNT(DISTINCT p.player_id) as player_count
FROM silver.game_batting gb
JOIN silver.games sg ON gb.game_pk = sg.game_pk
JOIN silver.players p ON gb.player_id = p.player_id
JOIN gold.dim_team t ON gb.team_id = t.team_id AND sg.season_year = t.season_year
WHERE sg.season_year = 2026
  AND sg.game_type IN ('R')
  AND sg.status = 'Final'
""")

row = cursor.fetchone()
mlb_players = row[0]

print("=== LEAGUE FILTER VERIFICATION (2026 BATTING) ===")
print(f"AL Players:  {al_players}")
print(f"NL Players:  {nl_players}")
print(f"MLB Players: {mlb_players}")
print(f"\nAL + NL = {al_players + nl_players} (should be close to {mlb_players})")

conn.close()
