"""Check why some games have Unknown pitcher handedness."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app import get_conn

conn = get_conn()
if not conn:
    print("Database connection failed")
    sys.exit(1)

try:
    # Aaron Judge's player_id
    judge_id = 592450
    
    print("="*80)
    print("Investigating Unknown pitcher handedness for Aaron Judge")
    print("="*80)
    
    # Find games where pitcher hand is unknown
    unknown_games = conn.execute("""
        WITH opposing_starter AS (
            SELECT
                gp.game_pk,
                gp.team_id,
                gp.player_id as pitcher_id,
                p.full_name as pitcher_name,
                p.throws,
                gp.games_started,
                gp.outs,
                ROW_NUMBER() OVER (
                    PARTITION BY gp.game_pk, gp.team_id
                    ORDER BY gp.games_started DESC, gp.outs DESC, gp.player_id
                ) AS rn
            FROM silver.game_pitching gp
            JOIN silver.players p ON gp.player_id = p.player_id
        )
        SELECT
            sg.game_pk,
            sg.game_date,
            ht.team_abbrev as home_team,
            at.team_abbrev as away_team,
            gb.team_id as judge_team_id,
            os.pitcher_id,
            os.pitcher_name,
            os.throws,
            os.games_started,
            os.outs
        FROM silver.game_batting gb
        JOIN silver.games sg ON gb.game_pk = sg.game_pk
        LEFT JOIN gold.dim_team ht ON sg.home_team_id = ht.team_id AND sg.season_year = ht.season_year
        LEFT JOIN gold.dim_team at ON sg.away_team_id = at.team_id AND sg.season_year = at.season_year
        LEFT JOIN opposing_starter os
               ON gb.game_pk = os.game_pk
              AND gb.team_id <> os.team_id
              AND os.rn = 1
        WHERE gb.player_id = ?
          AND sg.status = 'Final'
          AND sg.game_type = 'R'
          AND (os.throws IS NULL OR os.throws NOT IN ('L', 'R'))
        ORDER BY sg.game_date
    """, [judge_id]).fetchall()
    
    print(f"\nFound {len(unknown_games)} games with Unknown pitcher handedness:\n")
    
    for row in unknown_games:
        game_pk, game_date, home, away, judge_team, pitcher_id, pitcher_name, throws, gs, outs = row
        print(f"Game {game_pk} on {game_date}: {away} @ {home}")
        print(f"  Judge's team_id: {judge_team}")
        if pitcher_id:
            print(f"  Opposing starter: {pitcher_name} (ID: {pitcher_id})")
            print(f"    Throws: {throws or 'NULL'}")
            print(f"    Games Started: {gs}, Outs: {outs}")
        else:
            print(f"  No opposing starter found in game_pitching table")
        print()
    
    # Check if there are pitchers for these games
    if unknown_games:
        game_pks = [row[0] for row in unknown_games]
        placeholders = ','.join('?' * len(game_pks))
        
        print("="*80)
        print("Checking all pitchers recorded for these games:")
        print("="*80)
        
        for game_pk in game_pks:
            pitchers = conn.execute(f"""
                SELECT 
                    gp.player_id,
                    p.full_name,
                    p.throws,
                    gp.team_id,
                    gp.games_started,
                    gp.outs,
                    t.team_abbrev
                FROM silver.game_pitching gp
                JOIN silver.players p ON gp.player_id = p.player_id
                LEFT JOIN gold.dim_team t ON gp.team_id = t.team_id 
                    AND (SELECT season_year FROM silver.games WHERE game_pk = ?) = t.season_year
                WHERE gp.game_pk = ?
                ORDER BY gp.team_id, gp.games_started DESC, gp.outs DESC
            """, [game_pk, game_pk]).fetchall()
            
            print(f"\nGame {game_pk}:")
            for p_row in pitchers:
                pid, pname, throws, team_id, gs, outs, team_abbrev = p_row
                print(f"  {team_abbrev or team_id}: {pname} (throws: {throws or 'NULL'}, GS: {gs}, outs: {outs})")

finally:
    conn.close()

print("\n" + "="*80)
print("Analysis complete")
print("="*80)
