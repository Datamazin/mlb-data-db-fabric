-- =============================================================================
-- Gold 012 — dim_season (T-SQL)
-- Materialized table populated by MERGE from silver.seasons.
-- =============================================================================

MERGE gold.dim_season AS tgt
USING (
    SELECT
        season_year, sport_id, regular_season_start, regular_season_end,
        postseason_start, world_series_end, games_per_team
    FROM silver.seasons
) AS src
ON tgt.season_year = src.season_year
WHEN MATCHED THEN UPDATE SET
    sport_id              = src.sport_id,
    regular_season_start  = src.regular_season_start,
    regular_season_end    = src.regular_season_end,
    postseason_start      = src.postseason_start,
    world_series_end      = src.world_series_end,
    games_per_team        = src.games_per_team,
    loaded_at             = SYSDATETIMEOFFSET()
WHEN NOT MATCHED THEN INSERT (
    season_year, sport_id, regular_season_start, regular_season_end,
    postseason_start, world_series_end, games_per_team, loaded_at
) VALUES (
    src.season_year, src.sport_id, src.regular_season_start, src.regular_season_end,
    src.postseason_start, src.world_series_end, src.games_per_team, SYSDATETIMEOFFSET()
);
