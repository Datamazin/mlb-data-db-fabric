-- =============================================================================
-- Silver 005 — Teams (T-SQL)
-- Python loads staging.teams from bronze/teams Parquet before running this.
-- Staging schema matches silver.teams columns (excluding loaded_at).
-- Python deduplicates to latest extracted_at per (team_id, season_year) and
-- filters to season_years present in silver.seasons before staging.
-- =============================================================================

MERGE silver.teams AS tgt
USING staging.teams AS src
    ON tgt.team_id = src.team_id AND tgt.season_year = src.season_year
WHEN MATCHED THEN UPDATE SET
    tgt.team_name   = src.team_name,
    tgt.team_abbrev = src.team_abbrev,
    tgt.team_code   = src.team_code,
    tgt.league_id   = src.league_id,
    tgt.division_id = src.division_id,
    tgt.venue_id    = src.venue_id,
    tgt.city        = src.city,
    tgt.first_year  = src.first_year,
    tgt.active      = src.active,
    tgt.loaded_at   = SYSDATETIMEOFFSET()
WHEN NOT MATCHED BY TARGET THEN INSERT
    (team_id, season_year, team_name, team_abbrev, team_code,
     league_id, division_id, venue_id, city, first_year, active, loaded_at)
VALUES
    (src.team_id, src.season_year, src.team_name, src.team_abbrev, src.team_code,
     src.league_id, src.division_id, src.venue_id, src.city, src.first_year,
     src.active, SYSDATETIMEOFFSET());
