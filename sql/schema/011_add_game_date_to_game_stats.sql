-- =============================================================================
-- Migration 011 — Add game_date to silver.game_batting and silver.game_pitching
-- =============================================================================

IF NOT EXISTS (
    SELECT 1 FROM sys.columns c
    JOIN sys.tables t ON c.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'silver' AND t.name = 'game_batting' AND c.name = 'game_date'
)
ALTER TABLE silver.game_batting ADD game_date DATE NULL;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns c
    JOIN sys.tables t ON c.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'silver' AND t.name = 'game_pitching' AND c.name = 'game_date'
)
ALTER TABLE silver.game_pitching ADD game_date DATE NULL;
GO
