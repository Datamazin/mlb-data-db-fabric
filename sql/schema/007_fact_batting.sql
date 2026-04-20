-- =============================================================================
-- Migration 007 — silver.fact_batting (T-SQL / Fabric Warehouse)
-- Season-level batting stats per player/team/season/game_type, aggregated
-- from silver.game_batting by the 011_fact_batting.sql silver transform.
-- =============================================================================

IF OBJECT_ID('silver.fact_batting', 'U') IS NULL
CREATE TABLE silver.fact_batting (
    player_id       INT             NOT NULL,
    team_id         INT             NOT NULL,
    season_year     INT             NOT NULL,
    game_type       NVARCHAR(2)     NOT NULL,
    games           INT             NOT NULL DEFAULT 0,
    pa              INT             NOT NULL DEFAULT 0,
    ab              INT             NOT NULL DEFAULT 0,
    hits            INT             NOT NULL DEFAULT 0,
    doubles         INT             NOT NULL DEFAULT 0,
    triples         INT             NOT NULL DEFAULT 0,
    home_runs       INT             NOT NULL DEFAULT 0,
    rbi             INT             NOT NULL DEFAULT 0,
    runs            INT             NOT NULL DEFAULT 0,
    walks           INT             NOT NULL DEFAULT 0,
    strikeouts      INT             NOT NULL DEFAULT 0,
    stolen_bases    INT             NOT NULL DEFAULT 0,
    caught_stealing INT             NOT NULL DEFAULT 0,
    avg             DECIMAL(5,3)    NULL,
    obp             DECIMAL(5,3)    NULL,
    slg             DECIMAL(5,3)    NULL,
    ops             DECIMAL(5,3)    NULL,
    babip           DECIMAL(5,3)    NULL,
    loaded_at       DATETIMEOFFSET  NOT NULL,
    CONSTRAINT pk_fact_batting PRIMARY KEY (player_id, team_id, season_year, game_type)
);
GO
