"""Check Huascar Brazobán's position data."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from src.connections import get_warehouse_conn

conn = get_warehouse_conn()
cursor = conn.cursor()

# Find Brazobán in players table
cursor.execute("""
SELECT p.player_id, p.full_name, p.primary_position, p.throws, p.active
FROM silver.players p
WHERE p.full_name LIKE '%Brazoban%' OR p.full_name LIKE '%Brazobán%'
""")

print("=== PLAYERS TABLE ===")
for row in cursor.fetchall():
    print(f"ID: {row[0]}, Name: {row[1]}, Position: {row[2]}, Throws: {row[3]}, Active: {row[4]}")

# Check game_pitching records - specifically games_started
cursor.execute("""
SELECT TOP 20
    gp.game_pk,
    g.game_date,
    p.full_name,
    t.team_abbrev,
    gp.games_started,
    ROUND(gp.outs / 3.0, 1) as ip,
    gp.wins,
    gp.losses,
    gp.saves,
    gp.holds
FROM silver.game_pitching gp
JOIN silver.players p ON gp.player_id = p.player_id
JOIN silver.games g ON gp.game_pk = g.game_pk
JOIN gold.dim_team t ON gp.team_id = t.team_id AND g.season_year = t.season_year
WHERE p.full_name LIKE '%Brazoban%' OR p.full_name LIKE '%Brazobán%'
ORDER BY g.game_date DESC
""")

print("\n=== GAME PITCHING RECORDS (with GS) ===")
total_gs = 0
for row in cursor.fetchall():
    gs = row[4] if row[4] else 0
    total_gs += gs
    print(f"{row[1].strftime('%Y-%m-%d')} {row[3]}: {row[2]}")
    print(f"  GS={row[4]}, IP={row[5]}, W={row[6]}, L={row[7]}, SV={row[8]}, HLD={row[9]}")

print(f"\nTOTAL GAMES_STARTED: {total_gs}")

# Get a summary
cursor.execute("""
SELECT 
    p.full_name,
    p.player_id,
    p.primary_position,
    COUNT(*) as appearances,
    SUM(gp.games_started) as total_gs,
    SUM(gp.saves) as total_sv,
    SUM(gp.holds) as total_hld,
    ROUND(SUM(gp.outs) / 3.0, 1) as total_ip
FROM silver.game_pitching gp
JOIN silver.players p ON gp.player_id = p.player_id
JOIN silver.games g ON gp.game_pk = g.game_pk
WHERE (p.full_name LIKE '%Brazoban%' OR p.full_name LIKE '%Brazobán%')
  AND g.season_year = 2026
  AND g.game_type = 'R'
  AND g.status = 'Final'
GROUP BY p.full_name, p.player_id, p.primary_position
""")

print("\n=== 2026 SEASON SUMMARY ===")
for row in cursor.fetchall():
    print(f"Name: {row[0]} (ID: {row[1]}, Position: {row[2]})")
    print(f"  Appearances: {row[3]}, GS: {row[4]}, SV: {row[5]}, HLD: {row[6]}, IP: {row[7]}")

conn.close()
