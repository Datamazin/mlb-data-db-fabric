-- =============================================================================
-- Gold 005 — head_to_head (T-SQL)
-- Season win/loss record for every team pair in Regular Season Final games.
-- Materialized by MERGE.
-- =============================================================================

WITH game_results AS (
    SELECT
        season_year,
        home_team_id                                                       AS team_id,
        away_team_id                                                       AS opponent_id,
        CASE WHEN home_score > away_score THEN 1 ELSE 0 END                AS won
    FROM silver.games
    WHERE status = 'Final' AND game_type = 'R'

    UNION ALL

    SELECT
        season_year,
        away_team_id                                                       AS team_id,
        home_team_id                                                       AS opponent_id,
        CASE WHEN away_score > home_score THEN 1 ELSE 0 END                AS won
    FROM silver.games
    WHERE status = 'Final' AND game_type = 'R'
),
aggregated AS (
    SELECT
        team_id,
        opponent_id,
        season_year,
        SUM(CASE WHEN won = 1 THEN 1 ELSE 0 END) AS wins,
        SUM(CASE WHEN won = 0 THEN 1 ELSE 0 END) AS losses,
        COUNT(*)                                  AS games_played
    FROM game_results
    GROUP BY team_id, opponent_id, season_year
)
MERGE gold.head_to_head AS tgt
USING aggregated AS src
ON tgt.team_id = src.team_id AND tgt.opponent_id = src.opponent_id AND tgt.season_year = src.season_year
WHEN MATCHED THEN UPDATE SET
    wins         = src.wins,
    losses       = src.losses,
    games_played = src.games_played,
    loaded_at    = SYSDATETIMEOFFSET()
WHEN NOT MATCHED THEN INSERT (
    team_id, opponent_id, season_year, wins, losses, games_played, loaded_at
) VALUES (
    src.team_id, src.opponent_id, src.season_year, src.wins, src.losses, src.games_played, SYSDATETIMEOFFSET()
);
