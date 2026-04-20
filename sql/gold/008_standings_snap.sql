-- =============================================================================
-- Gold 008 — standings_snap (T-SQL / Fabric Warehouse)
-- Computes current-season standings from all Final Regular Season games and
-- MERGEs one row per team keyed on (snap_date, season_year, team_id).
-- Re-running on the same calendar date is idempotent.
--
-- DuckDB → T-SQL translation notes:
--   QUALIFY ROW_NUMBER() = 1  → nested subquery with WHERE rn = 1
--   (a > b) AS won (BOOLEAN)  → CASE WHEN a > b THEN 1 ELSE 0 END AS won (BIT)
--   NOT won                   → won = 0
--   COUNT(*) FILTER (WHERE …) → SUM(CASE WHEN … THEN 1 ELSE 0 END)
--   'W' || streak_len::VARCHAR → 'W' + CAST(streak_len AS NVARCHAR(10))
--   current_date               → CAST(GETUTCDATE() AS DATE)
--   current_timestamp          → SYSDATETIMEOFFSET()
--   INSERT OR REPLACE          → MERGE
-- =============================================================================

WITH all_team_games AS (
    SELECT
        g.game_pk,
        g.season_year,
        g.game_date,
        g.home_team_id                                                              AS team_id,
        CAST(1 AS BIT)                                                              AS is_home,
        CASE WHEN g.home_score > g.away_score THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS won,
        g.home_score - g.away_score                                                 AS run_margin
    FROM silver.games g
    WHERE g.status = 'Final' AND g.game_type = 'R'

    UNION ALL

    SELECT
        g.game_pk,
        g.season_year,
        g.game_date,
        g.away_team_id                                                              AS team_id,
        CAST(0 AS BIT)                                                              AS is_home,
        CASE WHEN g.away_score > g.home_score THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS won,
        g.away_score - g.home_score                                                 AS run_margin
    FROM silver.games g
    WHERE g.status = 'Final' AND g.game_type = 'R'
),
ranked_games AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY team_id, season_year
            ORDER BY game_date DESC, game_pk DESC
        ) AS game_rank
    FROM all_team_games
),
season_records AS (
    SELECT
        team_id,
        season_year,
        SUM(CASE WHEN won = 1              THEN 1 ELSE 0 END) AS wins,
        SUM(CASE WHEN won = 0              THEN 1 ELSE 0 END) AS losses,
        SUM(CASE WHEN is_home = 1 AND won = 1 THEN 1 ELSE 0 END) AS home_wins,
        SUM(CASE WHEN is_home = 1 AND won = 0 THEN 1 ELSE 0 END) AS home_losses,
        SUM(CASE WHEN is_home = 0 AND won = 1 THEN 1 ELSE 0 END) AS away_wins,
        SUM(CASE WHEN is_home = 0 AND won = 0 THEN 1 ELSE 0 END) AS away_losses,
        SUM(run_margin)                                            AS run_diff
    FROM all_team_games
    GROUP BY team_id, season_year
),
last_10 AS (
    SELECT
        team_id,
        season_year,
        SUM(CASE WHEN won = 1 THEN 1 ELSE 0 END) AS last_10_wins,
        SUM(CASE WHEN won = 0 THEN 1 ELSE 0 END) AS last_10_losses
    FROM ranked_games
    WHERE game_rank <= 10
    GROUP BY team_id, season_year
),
streak_latest AS (
    SELECT team_id, season_year, won AS latest_result
    FROM ranked_games
    WHERE game_rank = 1
),
streak_data AS (
    SELECT
        rg.team_id,
        rg.season_year,
        rg.won,
        rg.game_rank,
        SUM(
            CASE WHEN rg.won <> sl.latest_result THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY rg.team_id, rg.season_year
            ORDER BY rg.game_rank
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS change_count
    FROM ranked_games rg
    JOIN streak_latest sl
      ON rg.team_id = sl.team_id AND rg.season_year = sl.season_year
),
current_streak AS (
    SELECT
        team_id,
        season_year,
        MAX(CASE WHEN game_rank = 1 THEN CAST(won AS INT) END) AS current_result,
        SUM(CASE WHEN change_count = 0 THEN 1 ELSE 0 END)      AS streak_len
    FROM streak_data
    GROUP BY team_id, season_year
),
streak_label AS (
    SELECT
        team_id,
        season_year,
        CASE WHEN current_result = 1 THEN 'W' ELSE 'L' END
            + CAST(streak_len AS NVARCHAR(10)) AS streak
    FROM current_streak
),
-- Best record per division; QUALIFY replaced with nested ROW_NUMBER subquery
division_leaders AS (
    SELECT division_id, season_year, wins AS leader_wins, losses AS leader_losses
    FROM (
        SELECT
            t.division_id,
            sr.season_year,
            sr.wins,
            sr.losses,
            ROW_NUMBER() OVER (
                PARTITION BY t.division_id, sr.season_year
                ORDER BY sr.wins DESC, sr.losses ASC
            ) AS rn
        FROM season_records sr
        JOIN silver.teams t
          ON sr.team_id = t.team_id AND sr.season_year = t.season_year
    ) ranked
    WHERE rn = 1
),
snap AS (
    SELECT
        CAST(GETUTCDATE() AS DATE)                                                            AS snap_date,
        sr.season_year,
        sr.team_id,
        t.division_id,
        sr.wins,
        sr.losses,
        ROUND(CAST(sr.wins AS DECIMAL(10,3)) / NULLIF(sr.wins + sr.losses, 0), 3)            AS win_pct,
        ROUND((CAST(dl.leader_wins - sr.wins + sr.losses - dl.leader_losses AS DECIMAL)) / 2.0, 1) AS games_back,
        sl.streak,
        l10.last_10_wins,
        l10.last_10_losses,
        sr.home_wins,
        sr.home_losses,
        sr.away_wins,
        sr.away_losses,
        sr.run_diff
    FROM season_records sr
    JOIN silver.teams t
        ON sr.team_id = t.team_id AND sr.season_year = t.season_year
    JOIN division_leaders dl
        ON t.division_id = dl.division_id AND sr.season_year = dl.season_year
    LEFT JOIN last_10 l10
        ON sr.team_id = l10.team_id AND sr.season_year = l10.season_year
    LEFT JOIN streak_label sl
        ON sr.team_id = sl.team_id AND sr.season_year = sl.season_year
)
MERGE gold.standings_snap AS tgt
USING snap AS src
    ON tgt.snap_date = src.snap_date
   AND tgt.season_year = src.season_year
   AND tgt.team_id = src.team_id
WHEN MATCHED THEN UPDATE SET
    tgt.division_id   = src.division_id,
    tgt.wins          = src.wins,
    tgt.losses        = src.losses,
    tgt.win_pct       = src.win_pct,
    tgt.games_back    = src.games_back,
    tgt.streak        = src.streak,
    tgt.last_10_wins  = src.last_10_wins,
    tgt.last_10_losses= src.last_10_losses,
    tgt.home_wins     = src.home_wins,
    tgt.home_losses   = src.home_losses,
    tgt.away_wins     = src.away_wins,
    tgt.away_losses   = src.away_losses,
    tgt.run_diff      = src.run_diff,
    tgt.loaded_at     = SYSDATETIMEOFFSET()
WHEN NOT MATCHED BY TARGET THEN INSERT
    (snap_date, season_year, team_id, division_id,
     wins, losses, win_pct, games_back, streak,
     last_10_wins, last_10_losses,
     home_wins, home_losses, away_wins, away_losses,
     run_diff, loaded_at)
VALUES
    (src.snap_date, src.season_year, src.team_id, src.division_id,
     src.wins, src.losses, src.win_pct, src.games_back, src.streak,
     src.last_10_wins, src.last_10_losses,
     src.home_wins, src.home_losses, src.away_wins, src.away_losses,
     src.run_diff, SYSDATETIMEOFFSET());
