"""Fix league abbreviations in silver.leagues and re-aggregate gold.dim_team."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from src.connections import get_warehouse_conn

conn = get_warehouse_conn()
cursor = conn.cursor()

print("=== UPDATING SILVER.LEAGUES ===")

# Update league abbreviations
cursor.execute("""
UPDATE silver.leagues
SET abbreviation = 'AL'
WHERE league_id = 103
""")
print(f"✓ Updated American League (ID 103) → abbreviation = 'AL'")

cursor.execute("""
UPDATE silver.leagues
SET abbreviation = 'NL'
WHERE league_id = 104
""")
print(f"✓ Updated National League (ID 104) → abbreviation = 'NL'")

conn.commit()

# Verify
cursor.execute("""
SELECT league_id, league_name, abbreviation
FROM silver.leagues
ORDER BY league_id
""")

print("\n=== VERIFIED SILVER.LEAGUES ===")
for row in cursor.fetchall():
    print(f"ID: {row[0]}, Name: {row[1]}, Abbrev: {row[2]}")

print("\n=== RE-RUNNING GOLD.DIM_TEAM AGGREGATION ===")

# Re-run the gold.dim_team MERGE to populate league_abbrev
dim_team_sql = Path(__file__).parent.parent / "sql" / "gold" / "002_dim_team.sql"
with open(dim_team_sql, 'r', encoding='utf-8') as f:
    sql = f.read()

cursor.execute(sql)
conn.commit()

print("✓ gold.dim_team aggregation complete")

# Verify league_abbrev is now populated
cursor.execute("""
SELECT DISTINCT league_abbrev, COUNT(*) as team_count
FROM gold.dim_team
WHERE season_year = 2026
GROUP BY league_abbrev
ORDER BY league_abbrev
""")

print("\n=== VERIFIED GOLD.DIM_TEAM ===")
for row in cursor.fetchall():
    print(f"League: {row[0]}, Teams: {row[1]}")

conn.close()

print("\n✓ League filter is now fixed!")
