"""Check game_pitching data coverage."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app import get_conn

conn = get_conn()
if not conn:
    print("Database connection failed")
    sys.exit(1)

try:
    print("="*80)
    print("Checking game_pitching data coverage")
    print("="*80)
    
    # Check total games vs games with pitching data
    coverage = conn.execute("""
        SELECT 
            sg.season_year,
            COUNT(DISTINCT sg.game_pk) as total_games,
            COUNT(DISTINCT gp.game_pk) as games_with_pitching,
            COUNT(DISTINCT sg.game_pk) - COUNT(DISTINCT gp.game_pk) as missing_pitching
        FROM silver.games sg
        LEFT JOIN silver.game_pitching gp ON sg.game_pk = gp.game_pk
        WHERE sg.status = 'Final' AND sg.game_type = 'R'
        GROUP BY sg.season_year
        ORDER BY sg.season_year DESC
    """).fetchall()
    
    print("\nGame Pitching Data Coverage by Season:")
    print(f"{'Year':<6} {'Total Games':<15} {'Games w/ Pitching':<20} {'Missing':<10}")
    print("-" * 60)
    for row in coverage:
        year, total, with_pitching, missing = row
        pct = (with_pitching / total * 100) if total > 0 else 0
        print(f"{year:<6} {total:<15} {with_pitching:<20} {missing:<10} ({pct:.1f}%)")
    
    # Check which games are missing for 2025
    print("\n" + "="*80)
    print("Sample of 2025 games missing pitcher data:")
    print("="*80)
    
    missing_games = conn.execute("""
        SELECT TOP 20
            sg.game_pk,
            sg.game_date,
            ht.team_abbrev as home_team,
            at.team_abbrev as away_team
        FROM silver.games sg
        LEFT JOIN gold.dim_team ht ON sg.home_team_id = ht.team_id AND sg.season_year = ht.season_year
        LEFT JOIN gold.dim_team at ON sg.away_team_id = at.team_id AND sg.season_year = at.season_year
        WHERE sg.season_year = 2025
          AND sg.status = 'Final'
          AND sg.game_type = 'R'
          AND NOT EXISTS (SELECT 1 FROM silver.game_pitching gp WHERE gp.game_pk = sg.game_pk)
        ORDER BY sg.game_date
    """).fetchall()
    
    print(f"\nFound {len(missing_games)} games (showing first 20):")
    for row in missing_games:
        game_pk, game_date, home, away = row
        print(f"  {game_date}: {away} @ {home} (game_pk: {game_pk})")
    
    # Check if this is a date range issue
    print("\n" + "="*80)
    print("Checking date ranges with missing data:")
    print("="*80)
    
    date_ranges = conn.execute("""
        SELECT 
            MIN(sg.game_date) as first_missing,
            MAX(sg.game_date) as last_missing,
            COUNT(*) as count_missing
        FROM silver.games sg
        WHERE sg.season_year = 2025
          AND sg.status = 'Final'
          AND sg.game_type = 'R'
          AND NOT EXISTS (SELECT 1 FROM silver.game_pitching gp WHERE gp.game_pk = sg.game_pk)
    """).fetchone()
    
    if date_ranges and date_ranges[2] > 0:
        print(f"\nMissing pitcher data for games from {date_ranges[0]} to {date_ranges[1]}")
        print(f"Total missing: {date_ranges[2]} games")
    
    # Check what data we DO have
    print("\n" + "="*80)
    print("Checking what pitcher data exists:")
    print("="*80)
    
    existing = conn.execute("""
        SELECT 
            MIN(sg.game_date) as first_with_data,
            MAX(sg.game_date) as last_with_data,
            COUNT(DISTINCT sg.game_pk) as games_with_data
        FROM silver.games sg
        JOIN silver.game_pitching gp ON sg.game_pk = gp.game_pk
        WHERE sg.season_year = 2025
          AND sg.status = 'Final'
          AND sg.game_type = 'R'
    """).fetchone()
    
    if existing:
        print(f"\nPitcher data EXISTS for games from {existing[0]} to {existing[1]}")
        print(f"Total with data: {existing[2]} games")

finally:
    conn.close()

print("\n" + "="*80)
