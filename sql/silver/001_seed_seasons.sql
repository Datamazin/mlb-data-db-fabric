-- =============================================================================
-- Silver 001 — Seed season reference data 2022–2026 (T-SQL)
-- Static MERGE; safe to re-run.
-- =============================================================================

MERGE silver.seasons AS tgt
USING (VALUES
    (2022, 1, '2022-04-07', '2022-10-05', '2022-10-07', '2022-11-05', 162),
    (2023, 1, '2023-03-30', '2023-10-01', '2023-10-03', '2023-11-04', 162),
    (2024, 1, '2024-03-20', '2024-09-29', '2024-10-01', '2024-10-30', 162),
    (2025, 1, '2025-03-27', '2025-09-28', '2025-10-01', '2025-10-29', 162),
    (2026, 1, '2026-03-26', '2026-09-27', NULL,          NULL,        162)
) AS src (season_year, sport_id, regular_season_start, regular_season_end,
          postseason_start, world_series_end, games_per_team)
ON tgt.season_year = src.season_year
WHEN MATCHED THEN UPDATE SET
    tgt.sport_id              = src.sport_id,
    tgt.regular_season_start  = src.regular_season_start,
    tgt.regular_season_end    = src.regular_season_end,
    tgt.postseason_start      = src.postseason_start,
    tgt.world_series_end      = src.world_series_end,
    tgt.games_per_team        = src.games_per_team,
    tgt.loaded_at             = SYSDATETIMEOFFSET()
WHEN NOT MATCHED BY TARGET THEN INSERT
    (season_year, sport_id, regular_season_start, regular_season_end,
     postseason_start, world_series_end, games_per_team, loaded_at)
VALUES
    (src.season_year, src.sport_id, src.regular_season_start, src.regular_season_end,
     src.postseason_start, src.world_series_end, src.games_per_team, SYSDATETIMEOFFSET());
