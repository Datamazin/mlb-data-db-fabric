-- =============================================================================
-- Gold 010 — dim_home_team (T-SQL)
-- Role-playing materialized copy of gold.dim_team for home-team relationships.
-- Keyed by team_season_key (team_id * 10000 + season_year) so Power BI can
-- hold two independent active relationships on fact_game without USERELATIONSHIP.
-- =============================================================================

MERGE gold.dim_home_team AS tgt
USING (
    SELECT
        team_id * 10000 + season_year AS team_season_key,
        team_id,
        season_year,
        team_name,
        team_abbrev,
        city,
        league_id,
        league_name,
        league_abbrev,
        division_id,
        division_name,
        division_short_name,
        venue_id,
        venue_name,
        first_year,
        active
    FROM gold.dim_team
) AS src
ON tgt.team_season_key = src.team_season_key
WHEN MATCHED THEN UPDATE SET
    team_id             = src.team_id,
    season_year         = src.season_year,
    team_name           = src.team_name,
    team_abbrev         = src.team_abbrev,
    city                = src.city,
    league_id           = src.league_id,
    league_name         = src.league_name,
    league_abbrev       = src.league_abbrev,
    division_id         = src.division_id,
    division_name       = src.division_name,
    division_short_name = src.division_short_name,
    venue_id            = src.venue_id,
    venue_name          = src.venue_name,
    first_year          = src.first_year,
    active              = src.active,
    loaded_at           = SYSDATETIMEOFFSET()
WHEN NOT MATCHED THEN INSERT (
    team_season_key, team_id, season_year, team_name, team_abbrev, city,
    league_id, league_name, league_abbrev,
    division_id, division_name, division_short_name,
    venue_id, venue_name, first_year, active, loaded_at
) VALUES (
    src.team_season_key, src.team_id, src.season_year, src.team_name, src.team_abbrev, src.city,
    src.league_id, src.league_name, src.league_abbrev,
    src.division_id, src.division_name, src.division_short_name,
    src.venue_id, src.venue_name, src.first_year, src.active, SYSDATETIMEOFFSET()
);
