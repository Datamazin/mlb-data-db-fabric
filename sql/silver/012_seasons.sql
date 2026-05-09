-- Load bronze seasons → silver.seasons via staging
-- Upsert pattern: MERGE ensures idempotency for re-runs

MERGE INTO silver.seasons AS target
USING staging.seasons AS source
ON target.season_year = source.season_year
WHEN MATCHED THEN UPDATE SET
    sport_id = source.sport_id,
    regular_season_start = CAST(source.regular_season_start AS DATE),
    regular_season_end = CAST(source.regular_season_end AS DATE),
    postseason_start = CAST(source.postseason_start AS DATE),
    world_series_end = CAST(source.world_series_end AS DATE),
    games_per_team = source.games_per_team,
    loaded_at = SYSDATETIMEOFFSET()
WHEN NOT MATCHED THEN INSERT 
    (season_year, sport_id, regular_season_start, regular_season_end, 
     postseason_start, world_series_end, games_per_team, loaded_at)
VALUES
    (source.season_year, source.sport_id, 
     CAST(source.regular_season_start AS DATE),
     CAST(source.regular_season_end AS DATE),
     CAST(source.postseason_start AS DATE),
     CAST(source.world_series_end AS DATE),
     source.games_per_team, SYSDATETIMEOFFSET());
