-- =============================================================================
-- Migration 003 — Add winning/losing/save pitcher IDs to silver.games (T-SQL)
-- =============================================================================

IF NOT EXISTS (
    SELECT 1 FROM sys.columns c
    JOIN sys.tables  t ON c.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'silver' AND t.name = 'games' AND c.name = 'wp_id'
)
    ALTER TABLE silver.games ADD wp_id INT NULL;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns c
    JOIN sys.tables  t ON c.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'silver' AND t.name = 'games' AND c.name = 'lp_id'
)
    ALTER TABLE silver.games ADD lp_id INT NULL;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns c
    JOIN sys.tables  t ON c.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'silver' AND t.name = 'games' AND c.name = 'sv_id'
)
    ALTER TABLE silver.games ADD sv_id INT NULL;
GO
