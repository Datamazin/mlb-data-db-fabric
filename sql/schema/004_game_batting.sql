-- =============================================================================
-- Migration 004 — Per-game, per-player batting stats (T-SQL / Fabric Warehouse)
-- =============================================================================

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'silver' AND t.name = 'game_batting'
)
CREATE TABLE silver.game_batting (
    game_pk         BIGINT          NOT NULL,
    player_id       INT             NOT NULL,
    team_id         INT             NOT NULL,
    is_home         BIT             NOT NULL,
    batting_order   INT             NULL,
    position_abbrev NVARCHAR(4)     NULL,
    at_bats         INT             NOT NULL DEFAULT 0,
    runs            INT             NOT NULL DEFAULT 0,
    hits            INT             NOT NULL DEFAULT 0,
    doubles         INT             NOT NULL DEFAULT 0,
    triples         INT             NOT NULL DEFAULT 0,
    home_runs       INT             NOT NULL DEFAULT 0,
    rbi             INT             NOT NULL DEFAULT 0,
    walks           INT             NOT NULL DEFAULT 0,
    strikeouts      INT             NOT NULL DEFAULT 0,
    left_on_base    INT             NOT NULL DEFAULT 0,
    loaded_at       DATETIMEOFFSET  NOT NULL,
    CONSTRAINT pk_game_batting PRIMARY KEY (game_pk, player_id)
);
GO
