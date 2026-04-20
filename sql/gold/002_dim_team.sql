-- =============================================================================
-- Gold 002 — dim_team (T-SQL)
-- One row per (team_id, season_year). Materialized by MERGE.
-- =============================================================================

MERGE gold.dim_team AS tgt
USING (
    SELECT
        t.team_id,
        t.season_year,
        t.team_name,
        t.team_abbrev,
        t.city,
        l.league_id,
        l.league_name,
        l.abbreviation       AS league_abbrev,
        d.division_id,
        d.division_name,
        d.short_name         AS division_short_name,
        v.venue_id,
        v.venue_name,
        t.first_year,
        t.active
    FROM silver.teams t
    LEFT JOIN silver.leagues   l ON t.league_id   = l.league_id
    LEFT JOIN silver.divisions d ON t.division_id = d.division_id
    LEFT JOIN silver.venues    v ON t.venue_id    = v.venue_id
) AS src
ON tgt.team_id = src.team_id AND tgt.season_year = src.season_year
WHEN MATCHED THEN UPDATE SET
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
    team_id, season_year, team_name, team_abbrev, city,
    league_id, league_name, league_abbrev,
    division_id, division_name, division_short_name,
    venue_id, venue_name, first_year, active, loaded_at
) VALUES (
    src.team_id, src.season_year, src.team_name, src.team_abbrev, src.city,
    src.league_id, src.league_name, src.league_abbrev,
    src.division_id, src.division_name, src.division_short_name,
    src.venue_id, src.venue_name, src.first_year, src.active, SYSDATETIMEOFFSET()
);
