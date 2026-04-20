-- =============================================================================
-- Migration 006 — Gold layer materialized tables (T-SQL / Fabric Warehouse)
-- Views created via ODBC don't persist reliably; replacing with MERGE-backed
-- tables. Drop any existing view objects of these names before creating tables.
-- =============================================================================

-- Drop old views if they were created by earlier aggregate runs
IF OBJECT_ID('gold.dim_player',            'V') IS NOT NULL DROP VIEW gold.dim_player;
IF OBJECT_ID('gold.dim_team',              'V') IS NOT NULL DROP VIEW gold.dim_team;
IF OBJECT_ID('gold.dim_venue',             'V') IS NOT NULL DROP VIEW gold.dim_venue;
IF OBJECT_ID('gold.fact_game',             'V') IS NOT NULL DROP VIEW gold.fact_game;
IF OBJECT_ID('gold.head_to_head',          'V') IS NOT NULL DROP VIEW gold.head_to_head;
IF OBJECT_ID('gold.leaderboards',          'V') IS NOT NULL DROP VIEW gold.leaderboards;
IF OBJECT_ID('gold.player_season_summary', 'V') IS NOT NULL DROP VIEW gold.player_season_summary;
GO

IF OBJECT_ID('gold.dim_player', 'U') IS NULL
CREATE TABLE gold.dim_player (
    player_id           INT             NOT NULL,
    full_name           NVARCHAR(100)   NULL,
    first_name          NVARCHAR(50)    NULL,
    last_name           NVARCHAR(50)    NULL,
    birth_date          DATE            NULL,
    birth_city          NVARCHAR(100)   NULL,
    birth_country       NVARCHAR(100)   NULL,
    height              NVARCHAR(20)    NULL,
    weight              INT             NULL,
    bats                NVARCHAR(5)     NULL,
    throws              NVARCHAR(5)     NULL,
    primary_position    NVARCHAR(10)    NULL,
    mlb_debut_date      DATE            NULL,
    active              BIT             NULL,
    loaded_at           DATETIMEOFFSET  NOT NULL,
    CONSTRAINT pk_dim_player PRIMARY KEY (player_id)
);
GO

IF OBJECT_ID('gold.dim_team', 'U') IS NULL
CREATE TABLE gold.dim_team (
    team_id             INT             NOT NULL,
    season_year         INT             NOT NULL,
    team_name           NVARCHAR(100)   NULL,
    team_abbrev         NVARCHAR(10)    NULL,
    city                NVARCHAR(100)   NULL,
    league_id           INT             NULL,
    league_name         NVARCHAR(100)   NULL,
    league_abbrev       NVARCHAR(10)    NULL,
    division_id         INT             NULL,
    division_name       NVARCHAR(100)   NULL,
    division_short_name NVARCHAR(50)    NULL,
    venue_id            INT             NULL,
    venue_name          NVARCHAR(200)   NULL,
    first_year          INT             NULL,
    active              BIT             NULL,
    loaded_at           DATETIMEOFFSET  NOT NULL,
    CONSTRAINT pk_dim_team PRIMARY KEY (team_id, season_year)
);
GO

IF OBJECT_ID('gold.dim_venue', 'U') IS NULL
CREATE TABLE gold.dim_venue (
    venue_id    INT             NOT NULL,
    venue_name  NVARCHAR(200)   NULL,
    city        NVARCHAR(100)   NULL,
    state       NVARCHAR(100)   NULL,
    country     NVARCHAR(100)   NULL,
    surface     NVARCHAR(50)    NULL,
    capacity    INT             NULL,
    roof_type   NVARCHAR(50)    NULL,
    loaded_at   DATETIMEOFFSET  NOT NULL,
    CONSTRAINT pk_dim_venue PRIMARY KEY (venue_id)
);
GO

IF OBJECT_ID('gold.fact_game', 'U') IS NULL
CREATE TABLE gold.fact_game (
    game_pk             INT             NOT NULL,
    season_year         INT             NULL,
    game_date           DATE            NULL,
    game_datetime       DATETIMEOFFSET  NULL,
    game_type           NVARCHAR(5)     NULL,
    status              NVARCHAR(50)    NULL,
    home_team_id        INT             NULL,
    home_team_name      NVARCHAR(100)   NULL,
    home_team_abbrev    NVARCHAR(10)    NULL,
    away_team_id        INT             NULL,
    away_team_name      NVARCHAR(100)   NULL,
    away_team_abbrev    NVARCHAR(10)    NULL,
    home_score          INT             NULL,
    away_score          INT             NULL,
    innings             INT             NULL,
    venue_id            INT             NULL,
    venue_name          NVARCHAR(200)   NULL,
    attendance          INT             NULL,
    game_duration_min   INT             NULL,
    double_header       NVARCHAR(5)     NULL,
    series_description  NVARCHAR(100)   NULL,
    series_game_num     INT             NULL,
    wp_id               INT             NULL,
    wp_last_name        NVARCHAR(50)    NULL,
    wp_first_name       NVARCHAR(50)    NULL,
    lp_id               INT             NULL,
    lp_last_name        NVARCHAR(50)    NULL,
    lp_first_name       NVARCHAR(50)    NULL,
    sv_id               INT             NULL,
    sv_last_name        NVARCHAR(50)    NULL,
    sv_first_name       NVARCHAR(50)    NULL,
    loaded_at           DATETIMEOFFSET  NOT NULL,
    CONSTRAINT pk_fact_game PRIMARY KEY (game_pk)
);
GO

IF OBJECT_ID('gold.head_to_head', 'U') IS NULL
CREATE TABLE gold.head_to_head (
    team_id         INT             NOT NULL,
    opponent_id     INT             NOT NULL,
    season_year     INT             NOT NULL,
    wins            INT             NOT NULL DEFAULT 0,
    losses          INT             NOT NULL DEFAULT 0,
    games_played    INT             NOT NULL DEFAULT 0,
    loaded_at       DATETIMEOFFSET  NOT NULL,
    CONSTRAINT pk_head_to_head PRIMARY KEY (team_id, opponent_id, season_year)
);
GO

IF OBJECT_ID('gold.leaderboards', 'U') IS NULL
CREATE TABLE gold.leaderboards (
    player_id       INT             NOT NULL,
    full_name       NVARCHAR(100)   NULL,
    team_id         INT             NOT NULL,
    season_year     INT             NOT NULL,
    game_type       NVARCHAR(2)     NOT NULL,
    games           INT             NULL,
    pa              INT             NULL,
    ab              INT             NULL,
    hits            INT             NULL,
    home_runs       INT             NULL,
    rbi             INT             NULL,
    runs            INT             NULL,
    walks           INT             NULL,
    strikeouts      INT             NULL,
    stolen_bases    INT             NULL,
    avg             DECIMAL(5,3)    NULL,
    obp             DECIMAL(5,3)    NULL,
    slg             DECIMAL(5,3)    NULL,
    ops             DECIMAL(5,3)    NULL,
    babip           DECIMAL(5,3)    NULL,
    loaded_at       DATETIMEOFFSET  NOT NULL,
    CONSTRAINT pk_leaderboards PRIMARY KEY (player_id, team_id, season_year, game_type)
);
GO

IF OBJECT_ID('gold.player_season_summary', 'U') IS NULL
CREATE TABLE gold.player_season_summary (
    player_id           INT             NOT NULL,
    full_name           NVARCHAR(100)   NULL,
    primary_position    NVARCHAR(10)    NULL,
    team_id             INT             NOT NULL,
    season_year         INT             NOT NULL,
    game_type           NVARCHAR(2)     NOT NULL,
    games               INT             NULL,
    pa                  INT             NULL,
    ab                  INT             NULL,
    hits                INT             NULL,
    doubles             INT             NULL,
    triples             INT             NULL,
    home_runs           INT             NULL,
    rbi                 INT             NULL,
    runs                INT             NULL,
    walks               INT             NULL,
    strikeouts          INT             NULL,
    stolen_bases        INT             NULL,
    caught_stealing     INT             NULL,
    avg                 DECIMAL(5,3)    NULL,
    obp                 DECIMAL(5,3)    NULL,
    slg                 DECIMAL(5,3)    NULL,
    ops                 DECIMAL(5,3)    NULL,
    babip               DECIMAL(5,3)    NULL,
    loaded_at           DATETIMEOFFSET  NOT NULL,
    CONSTRAINT pk_player_season_summary PRIMARY KEY (player_id, team_id, season_year, game_type)
);
GO
