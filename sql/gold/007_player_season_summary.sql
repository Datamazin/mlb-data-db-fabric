-- =============================================================================
-- Gold 007 — player_season_summary (T-SQL)
-- Full batting line per player/team/season/game_type, with position info.
-- Joins silver.fact_batting with gold.dim_player for name/position.
-- =============================================================================

MERGE gold.player_season_summary AS tgt
USING (
    SELECT
        fb.player_id,
        dp.full_name,
        dp.primary_position,
        fb.team_id,
        fb.season_year,
        fb.game_type,
        fb.games,
        fb.pa,
        fb.ab,
        fb.hits,
        fb.doubles,
        fb.triples,
        fb.home_runs,
        fb.rbi,
        fb.runs,
        fb.walks,
        fb.strikeouts,
        fb.stolen_bases,
        fb.caught_stealing,
        fb.avg,
        fb.obp,
        fb.slg,
        fb.ops,
        fb.babip
    FROM silver.fact_batting fb
    LEFT JOIN gold.dim_player dp ON dp.player_id = fb.player_id
) AS src
ON  tgt.player_id   = src.player_id
AND tgt.team_id     = src.team_id
AND tgt.season_year = src.season_year
AND tgt.game_type   = src.game_type
WHEN MATCHED THEN UPDATE SET
    full_name        = src.full_name,
    primary_position = src.primary_position,
    games            = src.games,
    pa               = src.pa,
    ab               = src.ab,
    hits             = src.hits,
    doubles          = src.doubles,
    triples          = src.triples,
    home_runs        = src.home_runs,
    rbi              = src.rbi,
    runs             = src.runs,
    walks            = src.walks,
    strikeouts       = src.strikeouts,
    stolen_bases     = src.stolen_bases,
    caught_stealing  = src.caught_stealing,
    avg              = src.avg,
    obp              = src.obp,
    slg              = src.slg,
    ops              = src.ops,
    babip            = src.babip,
    loaded_at        = SYSDATETIMEOFFSET()
WHEN NOT MATCHED THEN INSERT (
    player_id, full_name, primary_position, team_id, season_year, game_type,
    games, pa, ab, hits, doubles, triples, home_runs, rbi, runs,
    walks, strikeouts, stolen_bases, caught_stealing,
    avg, obp, slg, ops, babip, loaded_at
) VALUES (
    src.player_id, src.full_name, src.primary_position, src.team_id, src.season_year, src.game_type,
    src.games, src.pa, src.ab, src.hits, src.doubles, src.triples, src.home_runs, src.rbi, src.runs,
    src.walks, src.strikeouts, src.stolen_bases, src.caught_stealing,
    src.avg, src.obp, src.slg, src.ops, src.babip, SYSDATETIMEOFFSET()
);
