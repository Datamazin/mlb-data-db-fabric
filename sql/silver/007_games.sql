-- =============================================================================
-- Silver 007 — Games (T-SQL)
-- Python loads staging.games from bronze/games Parquet before running this.
-- Staging schema matches silver.games columns (excluding loaded_at).
-- Python applies:
--   - deduplication to latest extracted_at per game_pk
--   - TRY_CAST for game_date (DATE) and game_datetime (DATETIMEOFFSET)
--   - JSON_VALUE extraction of wp_id/lp_id/sv_id from raw_json
--   - LEFT(double_header, 1) truncation
--   - WHERE game_date IS NOT NULL AND season_year IN silver.seasons filter
-- =============================================================================

MERGE silver.games AS tgt
USING staging.games AS src ON tgt.game_pk = src.game_pk
WHEN MATCHED THEN UPDATE SET
    tgt.season_year        = src.season_year,
    tgt.game_date          = src.game_date,
    tgt.game_datetime      = src.game_datetime,
    tgt.game_type          = src.game_type,
    tgt.status             = src.status,
    tgt.home_team_id       = src.home_team_id,
    tgt.away_team_id       = src.away_team_id,
    tgt.home_score         = src.home_score,
    tgt.away_score         = src.away_score,
    tgt.innings            = src.innings,
    tgt.venue_id           = src.venue_id,
    tgt.attendance         = src.attendance,
    tgt.game_duration_min  = src.game_duration_min,
    tgt.double_header      = src.double_header,
    tgt.series_description = src.series_description,
    tgt.series_game_num    = src.series_game_num,
    tgt.wp_id              = src.wp_id,
    tgt.lp_id              = src.lp_id,
    tgt.sv_id              = src.sv_id,
    tgt.loaded_at          = SYSDATETIMEOFFSET()
WHEN NOT MATCHED BY TARGET THEN INSERT
    (game_pk, season_year, game_date, game_datetime, game_type,
     status, home_team_id, away_team_id, home_score, away_score,
     innings, venue_id, attendance, game_duration_min,
     double_header, series_description, series_game_num,
     wp_id, lp_id, sv_id, loaded_at)
VALUES
    (src.game_pk, src.season_year, src.game_date, src.game_datetime, src.game_type,
     src.status, src.home_team_id, src.away_team_id, src.home_score, src.away_score,
     src.innings, src.venue_id, src.attendance, src.game_duration_min,
     src.double_header, src.series_description, src.series_game_num,
     src.wp_id, src.lp_id, src.sv_id, SYSDATETIMEOFFSET());
