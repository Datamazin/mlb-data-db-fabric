-- =============================================================================
-- Migration 005 — Per-game, per-pitcher stats (T-SQL / Fabric Warehouse)
-- outs stores total batters retired (IP = outs / 3).
-- =============================================================================

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'silver' AND t.name = 'game_pitching'
)
CREATE TABLE silver.game_pitching (
    game_pk             BIGINT          NOT NULL,
    player_id           INT             NOT NULL,
    team_id             INT             NOT NULL,
    is_home             BIT             NOT NULL,
    wins                INT             NOT NULL DEFAULT 0,
    losses              INT             NOT NULL DEFAULT 0,
    saves               INT             NOT NULL DEFAULT 0,
    holds               INT             NOT NULL DEFAULT 0,
    blown_saves         INT             NOT NULL DEFAULT 0,
    games_started       INT             NOT NULL DEFAULT 0,
    games_finished      INT             NOT NULL DEFAULT 0,
    complete_games      INT             NOT NULL DEFAULT 0,
    shutouts            INT             NOT NULL DEFAULT 0,
    outs                INT             NOT NULL DEFAULT 0,
    hits_allowed        INT             NOT NULL DEFAULT 0,
    runs_allowed        INT             NOT NULL DEFAULT 0,
    earned_runs         INT             NOT NULL DEFAULT 0,
    home_runs_allowed   INT             NOT NULL DEFAULT 0,
    walks               INT             NOT NULL DEFAULT 0,
    strikeouts          INT             NOT NULL DEFAULT 0,
    hit_by_pitch        INT             NOT NULL DEFAULT 0,
    pitches_thrown      INT             NOT NULL DEFAULT 0,
    strikes             INT             NOT NULL DEFAULT 0,
    loaded_at           DATETIMEOFFSET  NOT NULL,
    CONSTRAINT pk_game_pitching PRIMARY KEY (game_pk, player_id)
);
GO
