-- =============================================================================
-- Migration 010 — gold.dim_season materialized table (T-SQL / Fabric Warehouse)
-- Views created via ODBC don't persist reliably; using MERGE-backed table.
-- =============================================================================

-- Drop old view if it was created by earlier aggregate runs
IF OBJECT_ID('gold.dim_season', 'V') IS NOT NULL DROP VIEW gold.dim_season;
GO

IF OBJECT_ID('gold.dim_season', 'U') IS NULL
CREATE TABLE gold.dim_season (
    season_year             INT             NOT NULL,
    sport_id                INT             NOT NULL,
    regular_season_start    DATE            NULL,
    regular_season_end      DATE            NULL,
    postseason_start        DATE            NULL,
    world_series_end        DATE            NULL,
    games_per_team          INT             NULL,
    loaded_at               DATETIMEOFFSET  NOT NULL,
    CONSTRAINT pk_dim_season PRIMARY KEY (season_year)
);
GO
