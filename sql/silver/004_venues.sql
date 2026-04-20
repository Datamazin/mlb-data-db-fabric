-- =============================================================================
-- Silver 004 — Venues (T-SQL)
-- Two-pass approach preserved: game venues first (lower priority), team venues
-- second (authoritative, overwrites via MERGE).
--
-- Python loads two staging tables before running this script:
--
-- staging.venues_games — from bronze/games Parquet (year/month partition)
--   schema: (venue_id INT, venue_name NVARCHAR, extracted_at DATETIMEOFFSET)
--   Python deduplicates to latest extracted_at per venue_id.
--
-- staging.venues_teams — from bronze/teams Parquet
--   schema: (venue_id INT, venue_name NVARCHAR, extracted_at DATETIMEOFFSET)
--   Python deduplicates to latest extracted_at per venue_id.
--
-- city/state/capacity/surface/roof_type remain NULL until a dedicated
-- /v1/venues/{venueId} extractor is added.
-- =============================================================================

-- Pass 1: game venues (lower priority)
MERGE silver.venues AS tgt
USING staging.venues_games AS src ON tgt.venue_id = src.venue_id
WHEN NOT MATCHED BY TARGET THEN INSERT
    (venue_id, venue_name, city, state, country, surface, capacity, roof_type, loaded_at)
VALUES
    (src.venue_id, src.venue_name, NULL, NULL, NULL, NULL, NULL, NULL, SYSDATETIMEOFFSET());

-- Pass 2: team venues (authoritative — updates name even if row already exists)
MERGE silver.venues AS tgt
USING staging.venues_teams AS src ON tgt.venue_id = src.venue_id
WHEN MATCHED THEN UPDATE SET
    tgt.venue_name = src.venue_name,
    tgt.loaded_at  = SYSDATETIMEOFFSET()
WHEN NOT MATCHED BY TARGET THEN INSERT
    (venue_id, venue_name, city, state, country, surface, capacity, roof_type, loaded_at)
VALUES
    (src.venue_id, src.venue_name, NULL, NULL, NULL, NULL, NULL, NULL, SYSDATETIMEOFFSET());
