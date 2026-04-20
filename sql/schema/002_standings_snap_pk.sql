-- =============================================================================
-- Migration 002 — Fix gold.standings_snap primary key (T-SQL / Fabric Warehouse)
-- Adds season_year to the PK so standings for different seasons stored on the
-- same snap_date do not overwrite each other.
-- T-SQL supports ALTER TABLE DROP/ADD CONSTRAINT directly, so no table rebuild needed.
-- =============================================================================

-- Drop the old PK if it exists without season_year
IF EXISTS (
    SELECT 1 FROM sys.key_constraints kc
    JOIN sys.tables t  ON kc.parent_object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE kc.type = 'PK' AND s.name = 'gold' AND t.name = 'standings_snap'
      AND kc.name = 'pk_standings_snap_old'
)
    ALTER TABLE gold.standings_snap DROP CONSTRAINT pk_standings_snap_old;
GO

-- Re-create with the correct three-column PK (idempotent — only runs when constraint
-- does not already include all three columns).
IF NOT EXISTS (
    SELECT 1 FROM sys.key_constraints kc
    JOIN sys.tables t  ON kc.parent_object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE kc.type = 'PK' AND s.name = 'gold' AND t.name = 'standings_snap'
      AND kc.name = 'pk_standings_snap'
)
    ALTER TABLE gold.standings_snap
    ADD CONSTRAINT pk_standings_snap PRIMARY KEY (snap_date, season_year, team_id);
GO
