-- =============================================================================
-- Gold 003 — dim_venue (T-SQL)
-- Materialized by MERGE from silver.venues.
-- city/state/capacity etc. are NULL until the /v1/venues extractor is added.
-- =============================================================================

MERGE gold.dim_venue AS tgt
USING (
    SELECT venue_id, venue_name, city, state, country, surface, capacity, roof_type
    FROM silver.venues
) AS src
ON tgt.venue_id = src.venue_id
WHEN MATCHED THEN UPDATE SET
    venue_name = src.venue_name,
    city       = src.city,
    state      = src.state,
    country    = src.country,
    surface    = src.surface,
    capacity   = src.capacity,
    roof_type  = src.roof_type,
    loaded_at  = SYSDATETIMEOFFSET()
WHEN NOT MATCHED THEN INSERT (
    venue_id, venue_name, city, state, country, surface, capacity, roof_type, loaded_at
) VALUES (
    src.venue_id, src.venue_name, src.city, src.state, src.country,
    src.surface, src.capacity, src.roof_type, SYSDATETIMEOFFSET()
);
