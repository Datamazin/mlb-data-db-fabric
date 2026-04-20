-- =============================================================================
-- Silver 003 — Divisions (T-SQL)
-- Python loads staging.divisions from bronze/teams Parquet before running this.
-- Staging schema: (division_id INT, division_name NVARCHAR, short_name NVARCHAR,
--                  league_id INT, extracted_at DATETIMEOFFSET)
-- Python deduplicates to latest extracted_at per division_id before staging.
-- Run after 002_leagues.sql.
-- =============================================================================

MERGE silver.divisions AS tgt
USING staging.divisions AS src ON tgt.division_id = src.division_id
WHEN MATCHED THEN UPDATE SET
    tgt.division_name = src.division_name,
    tgt.short_name    = src.short_name,
    tgt.league_id     = src.league_id,
    tgt.loaded_at     = SYSDATETIMEOFFSET()
WHEN NOT MATCHED BY TARGET THEN INSERT
    (division_id, division_name, short_name, league_id, loaded_at)
VALUES
    (src.division_id, src.division_name, src.short_name, src.league_id, SYSDATETIMEOFFSET());
