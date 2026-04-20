-- =============================================================================
-- Gold 004 — fact_game (T-SQL)
-- Game-level fact table enriched with team names, venue, and pitcher decisions.
-- Materialized by MERGE from silver.games + lookups.
-- =============================================================================

MERGE gold.fact_game AS tgt
USING (
    SELECT
        g.game_pk,
        g.season_year,
        g.game_date,
        g.game_datetime,
        g.game_type,
        g.status,
        g.home_team_id,
        ht.team_name        AS home_team_name,
        ht.team_abbrev      AS home_team_abbrev,
        g.away_team_id,
        at.team_name        AS away_team_name,
        at.team_abbrev      AS away_team_abbrev,
        g.home_score,
        g.away_score,
        g.innings,
        g.venue_id,
        v.venue_name,
        g.attendance,
        g.game_duration_min,
        g.double_header,
        g.series_description,
        g.series_game_num,
        g.wp_id,
        wp.last_name        AS wp_last_name,
        wp.first_name       AS wp_first_name,
        g.lp_id,
        lp.last_name        AS lp_last_name,
        lp.first_name       AS lp_first_name,
        g.sv_id,
        sv.last_name        AS sv_last_name,
        sv.first_name       AS sv_first_name
    FROM silver.games g
    LEFT JOIN silver.teams   ht ON g.home_team_id = ht.team_id AND g.season_year = ht.season_year
    LEFT JOIN silver.teams   at ON g.away_team_id = at.team_id AND g.season_year = at.season_year
    LEFT JOIN silver.venues  v  ON g.venue_id     = v.venue_id
    LEFT JOIN silver.players wp ON g.wp_id        = wp.player_id
    LEFT JOIN silver.players lp ON g.lp_id        = lp.player_id
    LEFT JOIN silver.players sv ON g.sv_id        = sv.player_id
) AS src
ON tgt.game_pk = src.game_pk
WHEN MATCHED THEN UPDATE SET
    season_year        = src.season_year,
    game_date          = src.game_date,
    game_datetime      = src.game_datetime,
    game_type          = src.game_type,
    status             = src.status,
    home_team_id       = src.home_team_id,
    home_team_name     = src.home_team_name,
    home_team_abbrev   = src.home_team_abbrev,
    away_team_id       = src.away_team_id,
    away_team_name     = src.away_team_name,
    away_team_abbrev   = src.away_team_abbrev,
    home_score         = src.home_score,
    away_score         = src.away_score,
    innings            = src.innings,
    venue_id           = src.venue_id,
    venue_name         = src.venue_name,
    attendance         = src.attendance,
    game_duration_min  = src.game_duration_min,
    double_header      = src.double_header,
    series_description = src.series_description,
    series_game_num    = src.series_game_num,
    wp_id              = src.wp_id,
    wp_last_name       = src.wp_last_name,
    wp_first_name      = src.wp_first_name,
    lp_id              = src.lp_id,
    lp_last_name       = src.lp_last_name,
    lp_first_name      = src.lp_first_name,
    sv_id              = src.sv_id,
    sv_last_name       = src.sv_last_name,
    sv_first_name      = src.sv_first_name,
    loaded_at          = SYSDATETIMEOFFSET()
WHEN NOT MATCHED THEN INSERT (
    game_pk, season_year, game_date, game_datetime, game_type, status,
    home_team_id, home_team_name, home_team_abbrev,
    away_team_id, away_team_name, away_team_abbrev,
    home_score, away_score, innings,
    venue_id, venue_name, attendance, game_duration_min,
    double_header, series_description, series_game_num,
    wp_id, wp_last_name, wp_first_name,
    lp_id, lp_last_name, lp_first_name,
    sv_id, sv_last_name, sv_first_name,
    loaded_at
) VALUES (
    src.game_pk, src.season_year, src.game_date, src.game_datetime, src.game_type, src.status,
    src.home_team_id, src.home_team_name, src.home_team_abbrev,
    src.away_team_id, src.away_team_name, src.away_team_abbrev,
    src.home_score, src.away_score, src.innings,
    src.venue_id, src.venue_name, src.attendance, src.game_duration_min,
    src.double_header, src.series_description, src.series_game_num,
    src.wp_id, src.wp_last_name, src.wp_first_name,
    src.lp_id, src.lp_last_name, src.lp_first_name,
    src.sv_id, src.sv_last_name, src.sv_first_name,
    SYSDATETIMEOFFSET()
);
