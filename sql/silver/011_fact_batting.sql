-- =============================================================================
-- Silver 011 — fact_batting (T-SQL)
-- Aggregate silver.game_batting (per-game rows) into season-level
-- batting stats per player/team/season/game_type.
-- Joined to silver.games to get season_year and game_type.
-- =============================================================================

MERGE silver.fact_batting AS tgt
USING (
    SELECT
        gb.player_id,
        gb.team_id,
        g.season_year,
        g.game_type,
        COUNT(DISTINCT gb.game_pk)                          AS games,
        -- PA = AB + BB (walk). No SF/HBP in current bronze schema.
        SUM(gb.at_bats + gb.walks)                         AS pa,
        SUM(gb.at_bats)                                    AS ab,
        SUM(gb.hits)                                       AS hits,
        SUM(gb.doubles)                                    AS doubles,
        SUM(gb.triples)                                    AS triples,
        SUM(gb.home_runs)                                  AS home_runs,
        SUM(gb.rbi)                                        AS rbi,
        SUM(gb.runs)                                       AS runs,
        SUM(gb.walks)                                      AS walks,
        SUM(gb.strikeouts)                                 AS strikeouts,
        0                                                  AS stolen_bases,
        0                                                  AS caught_stealing,
        -- AVG = H / AB
        CASE WHEN SUM(gb.at_bats) > 0
            THEN CAST(CAST(SUM(gb.hits) AS DECIMAL(10,5)) / SUM(gb.at_bats) AS DECIMAL(5,3))
        END                                                AS avg,
        -- OBP = (H + BB) / (AB + BB)
        CASE WHEN SUM(gb.at_bats + gb.walks) > 0
            THEN CAST(
                CAST(SUM(gb.hits + gb.walks) AS DECIMAL(10,5)) /
                SUM(gb.at_bats + gb.walks)
            AS DECIMAL(5,3))
        END                                                AS obp,
        -- SLG = TB / AB  (TB = 1B + 2*2B + 3*3B + 4*HR)
        CASE WHEN SUM(gb.at_bats) > 0
            THEN CAST(
                CAST(
                    SUM(gb.hits - gb.doubles - gb.triples - gb.home_runs
                        + 2*gb.doubles + 3*gb.triples + 4*gb.home_runs)
                AS DECIMAL(10,5)) / SUM(gb.at_bats)
            AS DECIMAL(5,3))
        END                                                AS slg,
        -- OPS computed below as OBP + SLG; done as a column alias post-calc
        NULL                                               AS ops,
        -- BABIP = (H - HR) / (AB - K - HR + SF); no SF so simplified
        CASE WHEN SUM(gb.at_bats - gb.strikeouts - gb.home_runs) > 0
            THEN CAST(
                CAST(SUM(gb.hits - gb.home_runs) AS DECIMAL(10,5)) /
                SUM(gb.at_bats - gb.strikeouts - gb.home_runs)
            AS DECIMAL(5,3))
        END                                                AS babip
    FROM silver.game_batting gb
    JOIN silver.games g ON gb.game_pk = g.game_pk
    GROUP BY gb.player_id, gb.team_id, g.season_year, g.game_type
) AS src
ON  tgt.player_id   = src.player_id
AND tgt.team_id     = src.team_id
AND tgt.season_year = src.season_year
AND tgt.game_type   = src.game_type
WHEN MATCHED THEN UPDATE SET
    games           = src.games,
    pa              = src.pa,
    ab              = src.ab,
    hits            = src.hits,
    doubles         = src.doubles,
    triples         = src.triples,
    home_runs       = src.home_runs,
    rbi             = src.rbi,
    runs            = src.runs,
    walks           = src.walks,
    strikeouts      = src.strikeouts,
    stolen_bases    = src.stolen_bases,
    caught_stealing = src.caught_stealing,
    avg             = src.avg,
    obp             = src.obp,
    slg             = src.slg,
    ops             = CASE WHEN src.obp IS NOT NULL AND src.slg IS NOT NULL
                          THEN CAST(src.obp + src.slg AS DECIMAL(5,3)) END,
    babip           = src.babip,
    loaded_at       = SYSDATETIMEOFFSET()
WHEN NOT MATCHED THEN INSERT (
    player_id, team_id, season_year, game_type,
    games, pa, ab, hits, doubles, triples, home_runs, rbi, runs,
    walks, strikeouts, stolen_bases, caught_stealing,
    avg, obp, slg, ops, babip, loaded_at
) VALUES (
    src.player_id, src.team_id, src.season_year, src.game_type,
    src.games, src.pa, src.ab, src.hits, src.doubles, src.triples, src.home_runs, src.rbi, src.runs,
    src.walks, src.strikeouts, src.stolen_bases, src.caught_stealing,
    src.avg, src.obp, src.slg,
    CASE WHEN src.obp IS NOT NULL AND src.slg IS NOT NULL
         THEN CAST(src.obp + src.slg AS DECIMAL(5,3)) END,
    src.babip, SYSDATETIMEOFFSET()
);
