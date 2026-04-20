-- =============================================================================
-- Gold 009 — league_averages (T-SQL)
-- Weighted batting averages per league/season/game_type.
-- Source: silver.fact_batting joined to silver.teams for league_id.
-- league_era is NULL until silver.fact_pitching is available.
-- =============================================================================

MERGE gold.league_averages AS tgt
USING (
    SELECT
        fb.season_year,
        t.league_id,
        fb.game_type,
        -- weighted league AVG = total H / total AB
        CASE WHEN SUM(fb.ab) > 0
            THEN CAST(CAST(SUM(fb.hits) AS DECIMAL(10,5)) / SUM(fb.ab) AS DECIMAL(5,3))
        END                                                         AS league_avg,
        -- weighted OBP = (H + BB) / (AB + BB)
        CASE WHEN SUM(fb.ab + fb.walks) > 0
            THEN CAST(
                CAST(SUM(fb.hits + fb.walks) AS DECIMAL(10,5)) /
                SUM(fb.ab + fb.walks)
            AS DECIMAL(5,3))
        END                                                         AS league_obp,
        -- weighted SLG = TB / AB
        CASE WHEN SUM(fb.ab) > 0
            THEN CAST(
                CAST(
                    SUM(fb.hits - fb.doubles - fb.triples - fb.home_runs
                        + 2*fb.doubles + 3*fb.triples + 4*fb.home_runs)
                AS DECIMAL(10,5)) / SUM(fb.ab)
            AS DECIMAL(5,3))
        END                                                         AS league_slg,
        NULL                                                        AS league_era
    FROM silver.fact_batting fb
    JOIN silver.teams t
        ON t.team_id     = fb.team_id
        AND t.season_year = fb.season_year
    WHERE t.league_id IS NOT NULL
    GROUP BY fb.season_year, t.league_id, fb.game_type
) AS src
ON  tgt.season_year = src.season_year
AND tgt.league_id   = src.league_id
AND tgt.game_type   = src.game_type
WHEN MATCHED THEN UPDATE SET
    league_avg  = src.league_avg,
    league_obp  = src.league_obp,
    league_slg  = src.league_slg,
    league_ops  = CASE WHEN src.league_obp IS NOT NULL AND src.league_slg IS NOT NULL
                       THEN CAST(src.league_obp + src.league_slg AS DECIMAL(5,3)) END,
    league_era  = src.league_era,
    loaded_at   = SYSDATETIMEOFFSET()
WHEN NOT MATCHED THEN INSERT (
    season_year, league_id, game_type,
    league_avg, league_obp, league_slg, league_ops, league_era, loaded_at
) VALUES (
    src.season_year, src.league_id, src.game_type,
    src.league_avg, src.league_obp, src.league_slg,
    CASE WHEN src.league_obp IS NOT NULL AND src.league_slg IS NOT NULL
         THEN CAST(src.league_obp + src.league_slg AS DECIMAL(5,3)) END,
    src.league_era, SYSDATETIMEOFFSET()
);
