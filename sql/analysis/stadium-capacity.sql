SELECT
    state,
    surface,
    roof_type,
    AVG(capacity)   AS avg_capacity,
    MIN(capacity)   AS min_capacity,
    MAX(capacity)   AS max_capacity,
    COUNT(*)        AS venue_count
FROM gold.dim_venue
WHERE [state] IS NULL
GROUP BY state, surface, roof_type
ORDER BY state,surface, roof_type;