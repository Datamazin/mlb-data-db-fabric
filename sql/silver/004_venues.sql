-- =============================================================================
-- Silver 004 — Venues (T-SQL)
--
-- Three-pass approach:
--   Pass 1: game venues (lower priority — inserts only)
--   Pass 2: team venues (authoritative name — upserts)
--   Pass 3: venue detail from /v1/venues extraction — fills location columns.
--           Guarded by OBJECT_ID so it is safely skipped on first run before
--           extract_venues has populated bronze/venues/venues.parquet.
--
-- staging.venues_games  — (venue_id, venue_name)
-- staging.venues_teams  — (venue_id, venue_name)
-- staging.venues_detail — (venue_id, venue_name, city, state, country,
--                          capacity, surface, roof_type)
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

-- Pass 3: venue detail — location, capacity, surface, roof_type
IF OBJECT_ID('staging.venues_detail', 'U') IS NOT NULL
    EXEC sp_executesql N'
        MERGE silver.venues AS tgt
        USING staging.venues_detail AS src ON tgt.venue_id = src.venue_id
        WHEN MATCHED THEN UPDATE SET
            tgt.venue_name = COALESCE(src.venue_name, tgt.venue_name),
            tgt.city       = COALESCE(src.city,       tgt.city),
            tgt.state      = COALESCE(src.state,      tgt.state),
            tgt.country    = COALESCE(src.country,    tgt.country),
            tgt.surface    = COALESCE(src.surface,    tgt.surface),
            tgt.capacity   = COALESCE(src.capacity,   tgt.capacity),
            tgt.roof_type  = COALESCE(src.roof_type,  tgt.roof_type),
            tgt.loaded_at  = SYSDATETIMEOFFSET()
        WHEN NOT MATCHED BY TARGET THEN INSERT
            (venue_id, venue_name, city, state, country, surface, capacity, roof_type, loaded_at)
        VALUES
            (src.venue_id, src.venue_name, src.city, src.state, src.country,
             src.surface, src.capacity, src.roof_type, SYSDATETIMEOFFSET());
    ';
