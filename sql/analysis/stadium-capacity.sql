SELECT
    surface,
    roof_type,
    AVG(capacity)   AS avg_capacity,
    MIN(capacity)   AS min_capacity,
    MAX(capacity)   AS max_capacity,
    COUNT(*)        AS venue_count
FROM gold.dim_venue
WHERE capacity IS NOT NULL AND [state] IN ('CA', 'NY', 'TX')
GROUP BY surface, roof_type
ORDER BY avg_capacity DESC;
