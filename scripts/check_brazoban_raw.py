"""Check the raw JSON for Brazobán's May 4 game."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from src.connections import get_warehouse_conn

conn = get_warehouse_conn()
cursor = conn.cursor()

# Find the game_pk for May 4, 2026 Mets game where Brazobán pitched
cursor.execute("""
SELECT TOP 1 g.game_pk, g.game_date, g.home_team_id, g.away_team_id
FROM silver.game_pitching gp
JOIN silver.games g ON gp.game_pk = g.game_pk
JOIN silver.players p ON gp.player_id = p.player_id
WHERE p.full_name LIKE '%Brazobán%'
  AND g.game_date = '2026-05-04'
""")

row = cursor.fetchone()
if not row:
    print("Game not found")
    conn.close()
    exit(1)

game_pk = row[0]
print(f"Game PK: {game_pk}")
print(f"Date: {row[1]}")
print(f"Home Team ID: {row[2]}, Away Team ID: {row[3]}")

# Now check the bronze raw_json
conn.close()

# Check OneLake for this game's bronze file
from src.connections import get_onelake_fs, get_bronze_root
import pyarrow.parquet as pq

fs = get_onelake_fs()
bronze_root = get_bronze_root()

# The file should be in year=2026/month=05/games_20260504.parquet
file_path = f"{bronze_root}/games/year=2026/month=05/games_20260504.parquet"

print(f"\nChecking bronze file: {file_path}")

if not fs.exists(file_path):
    print("Bronze file not found!")
    exit(1)

# Read the parquet file
with fs.open(file_path, 'rb') as f:
    table = pq.read_table(f)
    df = table.to_pandas()

# Find the row for this game_pk
game_row = df[df['game_pk'] == game_pk]

if game_row.empty:
    print(f"Game {game_pk} not found in bronze file")
    exit(1)

import json

raw_json_str = game_row.iloc[0]['raw_json']
raw_json = json.loads(raw_json_str)

# Navigate to boxscore pitchers
boxscore = raw_json.get('liveData', {}).get('boxscore', {})
teams = boxscore.get('teams', {})

print(f"\n=== SEARCHING FOR BRAZOBÁN IN RAW JSON ===")

for side in ['away', 'home']:
    team_data = teams.get(side, {})
    players = team_data.get('players', {})
    
    for player_key, player_data in players.items():
        person = player_data.get('person', {})
        full_name = person.get('fullName', '')
        
        if 'Brazob' in full_name or 'Brazobán' in full_name:
            print(f"\nFound: {full_name} ({side.upper()} team)")
            print(f"Player ID: {person.get('id')}")
            
            stats = player_data.get('stats', {})
            pitching = stats.get('pitching', {})
            
            print(f"Games Started (API): {pitching.get('gamesStarted', 'N/A')}")
            print(f"Innings Pitched (API): {pitching.get('inningsPitched', 'N/A')}")
            print(f"Games Played (API): {pitching.get('gamesPlayed', 'N/A')}")
            
            print(f"\nAll pitching stats:")
            for k, v in pitching.items():
                print(f"  {k}: {v}")

print("\n" + "="*60)
