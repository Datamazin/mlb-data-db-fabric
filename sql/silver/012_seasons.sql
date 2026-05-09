-- =============================================================================
-- Silver 012 — Seasons (T-SQL)
-- Updates silver.seasons from /v1/seasons API data via staging.
-- Guarded by OBJECT_ID so the script is a safe no-op on runs where
-- extract_seasons has not yet written bronze/seasons/ data.
-- Baseline data is seeded by 001_seed_seasons.sql.
-- =============================================================================

IF OBJECT_ID('staging.seasons', 'U') IS NOT NULL
    EXEC sp_executesql N'
        MERGE INTO silver.seasons AS tgt
        USING staging.seasons AS src ON tgt.season_year = src.season_year
        WHEN MATCHED THEN UPDATE SET
            tgt.sport_id             = src.sport_id,
            tgt.regular_season_start = CAST(src.regular_season_start AS DATE),
            tgt.regular_season_end   = CAST(src.regular_season_end   AS DATE),
            tgt.postseason_start     = CAST(src.postseason_start     AS DATE),
            tgt.world_series_end     = CAST(src.world_series_end     AS DATE),
            tgt.games_per_team       = src.games_per_team,
            tgt.loaded_at            = SYSDATETIMEOFFSET()
        WHEN NOT MATCHED BY TARGET THEN INSERT
            (season_year, sport_id, regular_season_start, regular_season_end,
             postseason_start, world_series_end, games_per_team, loaded_at)
        VALUES
            (src.season_year, src.sport_id,
             CAST(src.regular_season_start AS DATE),
             CAST(src.regular_season_end   AS DATE),
             CAST(src.postseason_start     AS DATE),
             CAST(src.world_series_end     AS DATE),
             src.games_per_team, SYSDATETIMEOFFSET());
    ';
