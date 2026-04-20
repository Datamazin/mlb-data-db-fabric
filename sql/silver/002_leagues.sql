-- =============================================================================
-- Silver 002 — Leagues (T-SQL)
-- Python loads staging.leagues from bronze/teams Parquet before running this.
-- Staging schema: (league_id INT, league_name NVARCHAR, short_name NVARCHAR,
--                  abbreviation NVARCHAR, extracted_at DATETIMEOFFSET)
-- Python deduplicates to latest extracted_at per league_id before staging.
-- =============================================================================

MERGE silver.leagues AS tgt
USING staging.leagues AS src ON tgt.league_id = src.league_id
WHEN MATCHED THEN UPDATE SET
    tgt.league_name  = src.league_name,
    tgt.short_name   = src.short_name,
    tgt.abbreviation = src.abbreviation,
    tgt.loaded_at    = SYSDATETIMEOFFSET()
WHEN NOT MATCHED BY TARGET THEN INSERT
    (league_id, league_name, short_name, abbreviation, loaded_at)
VALUES
    (src.league_id, src.league_name, src.short_name, src.abbreviation, SYSDATETIMEOFFSET());
