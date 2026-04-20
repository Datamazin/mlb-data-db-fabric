-- =============================================================================
-- Migration 001 — Initial Schema (T-SQL / Fabric Warehouse)
-- =============================================================================

-- ── Schemas ──────────────────────────────────────────────────────────────────

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'meta')
    EXEC('CREATE SCHEMA meta');
GO

-- Transient staging tables are created/dropped by the Python transformer.
-- The schema must exist persistently.
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'staging')
    EXEC('CREATE SCHEMA staging');
GO


-- =============================================================================
-- META
-- =============================================================================

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'meta' AND t.name = 'pipeline_runs'
)
CREATE TABLE meta.pipeline_runs (
    run_id              NVARCHAR(36)    NOT NULL,   -- UUID generated at job start
    job_name            NVARCHAR(100)   NOT NULL,
    status              NVARCHAR(20)    NOT NULL,   -- running | success | failed
    started_at          DATETIMEOFFSET  NOT NULL,
    completed_at        DATETIMEOFFSET  NULL,
    season_year         INT             NULL,
    target_date         DATE            NULL,
    records_extracted   INT             NULL DEFAULT 0,
    records_loaded      INT             NULL DEFAULT 0,
    error_message       NVARCHAR(MAX)   NULL,
    pipeline_version    NVARCHAR(20)    NOT NULL,
    CONSTRAINT pk_pipeline_runs PRIMARY KEY (run_id)
);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'meta' AND t.name = 'entity_checksums'
)
CREATE TABLE meta.entity_checksums (
    entity_type         NVARCHAR(50)    NOT NULL,
    entity_key          NVARCHAR(100)   NOT NULL,
    response_hash       NVARCHAR(64)    NOT NULL,   -- SHA-256
    source_url          NVARCHAR(500)   NOT NULL,
    extracted_at        DATETIMEOFFSET  NOT NULL,
    transform_version   NVARCHAR(20)    NOT NULL,
    correction_source   NVARCHAR(100)   NULL,
    CONSTRAINT pk_entity_checksums PRIMARY KEY (entity_type, entity_key)
);
GO


-- =============================================================================
-- BRONZE
-- =============================================================================

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'bronze' AND t.name = 'raw_api_responses'
)
CREATE TABLE bronze.raw_api_responses (
    id              BIGINT          NOT NULL IDENTITY(1,1),
    entity_type     NVARCHAR(50)    NOT NULL,
    entity_key      NVARCHAR(100)   NOT NULL,
    source_url      NVARCHAR(500)   NOT NULL,
    response_json   NVARCHAR(MAX)   NOT NULL,   -- JSON stored as NVARCHAR(MAX)
    http_status     SMALLINT        NOT NULL,
    extracted_at    DATETIMEOFFSET  NOT NULL,
    file_path       NVARCHAR(500)   NULL,        -- OneLake path of the Parquet file
    CONSTRAINT pk_raw_api_responses PRIMARY KEY (id)
);
GO


-- =============================================================================
-- SILVER
-- =============================================================================

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'silver' AND t.name = 'leagues'
)
CREATE TABLE silver.leagues (
    league_id       INT             NOT NULL,
    league_name     NVARCHAR(100)   NOT NULL,
    short_name      NVARCHAR(50)    NULL,
    abbreviation    NVARCHAR(10)    NULL,
    loaded_at       DATETIMEOFFSET  NOT NULL,
    CONSTRAINT pk_leagues PRIMARY KEY (league_id)
);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'silver' AND t.name = 'divisions'
)
CREATE TABLE silver.divisions (
    division_id     INT             NOT NULL,
    division_name   NVARCHAR(100)   NOT NULL,
    short_name      NVARCHAR(50)    NULL,
    league_id       INT             NOT NULL,
    loaded_at       DATETIMEOFFSET  NOT NULL,
    CONSTRAINT pk_divisions PRIMARY KEY (division_id)
);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'silver' AND t.name = 'venues'
)
CREATE TABLE silver.venues (
    venue_id        INT             NOT NULL,
    venue_name      NVARCHAR(200)   NOT NULL,
    city            NVARCHAR(100)   NULL,
    state           NVARCHAR(100)   NULL,
    country         NVARCHAR(100)   NULL,
    surface         NVARCHAR(50)    NULL,
    capacity        INT             NULL,
    roof_type       NVARCHAR(50)    NULL,
    loaded_at       DATETIMEOFFSET  NOT NULL,
    CONSTRAINT pk_venues PRIMARY KEY (venue_id)
);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'silver' AND t.name = 'seasons'
)
CREATE TABLE silver.seasons (
    season_year             INT             NOT NULL,
    sport_id                INT             NOT NULL,
    regular_season_start    DATE            NOT NULL,
    regular_season_end      DATE            NOT NULL,
    postseason_start        DATE            NULL,
    world_series_end        DATE            NULL,
    games_per_team          INT             NULL,
    loaded_at               DATETIMEOFFSET  NOT NULL,
    CONSTRAINT pk_seasons PRIMARY KEY (season_year)
);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'silver' AND t.name = 'teams'
)
CREATE TABLE silver.teams (
    team_id         INT             NOT NULL,
    season_year     INT             NOT NULL,
    team_name       NVARCHAR(100)   NOT NULL,
    team_abbrev     NVARCHAR(3)     NOT NULL,
    team_code       NVARCHAR(10)    NULL,
    league_id       INT             NULL,
    division_id     INT             NULL,
    venue_id        INT             NULL,
    city            NVARCHAR(100)   NULL,
    first_year      INT             NULL,
    active          BIT             NOT NULL DEFAULT 1,
    loaded_at       DATETIMEOFFSET  NOT NULL,
    CONSTRAINT pk_teams PRIMARY KEY (team_id, season_year)
);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'silver' AND t.name = 'players'
)
CREATE TABLE silver.players (
    player_id           INT             NOT NULL,
    full_name           NVARCHAR(200)   NOT NULL,
    first_name          NVARCHAR(100)   NULL,
    last_name           NVARCHAR(100)   NULL,
    birth_date          DATE            NULL,
    birth_city          NVARCHAR(100)   NULL,
    birth_country       NVARCHAR(100)   NULL,
    height              NVARCHAR(20)    NULL,
    weight              INT             NULL,
    bats                NVARCHAR(1)     NULL,
    throws              NVARCHAR(1)     NULL,
    primary_position    NVARCHAR(2)     NULL,
    mlb_debut_date      DATE            NULL,
    active              BIT             NOT NULL DEFAULT 1,
    loaded_at           DATETIMEOFFSET  NOT NULL,
    CONSTRAINT pk_players PRIMARY KEY (player_id)
);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'silver' AND t.name = 'games'
)
CREATE TABLE silver.games (
    game_pk             BIGINT          NOT NULL,
    season_year         INT             NOT NULL,
    game_date           DATE            NOT NULL,
    game_datetime       DATETIMEOFFSET  NULL,
    game_type           NVARCHAR(2)     NOT NULL,
    status              NVARCHAR(50)    NOT NULL,
    home_team_id        INT             NOT NULL,
    away_team_id        INT             NOT NULL,
    home_score          INT             NULL,
    away_score          INT             NULL,
    innings             INT             NULL,
    venue_id            INT             NULL,
    attendance          INT             NULL,
    game_duration_min   INT             NULL,
    double_header       NVARCHAR(1)     NULL,
    series_description  NVARCHAR(100)   NULL,
    series_game_num     INT             NULL,
    wp_id               INT             NULL,
    lp_id               INT             NULL,
    sv_id               INT             NULL,
    loaded_at           DATETIMEOFFSET  NOT NULL,
    CONSTRAINT pk_games PRIMARY KEY (game_pk)
);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'silver' AND t.name = 'game_linescore'
)
CREATE TABLE silver.game_linescore (
    game_pk     BIGINT          NOT NULL,
    inning      INT             NOT NULL,
    home_runs   INT             NOT NULL DEFAULT 0,
    home_hits   INT             NOT NULL DEFAULT 0,
    home_errors INT             NOT NULL DEFAULT 0,
    away_runs   INT             NOT NULL DEFAULT 0,
    away_hits   INT             NOT NULL DEFAULT 0,
    away_errors INT             NOT NULL DEFAULT 0,
    loaded_at   DATETIMEOFFSET  NOT NULL,
    CONSTRAINT pk_game_linescore PRIMARY KEY (game_pk, inning)
);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'silver' AND t.name = 'game_boxscore'
)
CREATE TABLE silver.game_boxscore (
    game_pk         BIGINT          NOT NULL,
    team_id         INT             NOT NULL,
    is_home         BIT             NOT NULL,
    runs            INT             NULL,
    hits            INT             NULL,
    errors          INT             NULL,
    left_on_base    INT             NULL,
    batting_order   NVARCHAR(MAX)   NULL,   -- JSON array of player_ids
    pitching_order  NVARCHAR(MAX)   NULL,   -- JSON array of player_ids
    loaded_at       DATETIMEOFFSET  NOT NULL,
    CONSTRAINT pk_game_boxscore PRIMARY KEY (game_pk, team_id)
);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'silver' AND t.name = 'fact_batting'
)
CREATE TABLE silver.fact_batting (
    player_id       INT             NOT NULL,
    team_id         INT             NOT NULL,
    season_year     INT             NOT NULL,
    game_type       NVARCHAR(2)     NOT NULL,
    games           INT             NULL,
    pa              INT             NULL,
    ab              INT             NULL,
    hits            INT             NULL,
    doubles         INT             NULL,
    triples         INT             NULL,
    home_runs       INT             NULL,
    rbi             INT             NULL,
    runs            INT             NULL,
    walks           INT             NULL,
    ibb             INT             NULL,
    strikeouts      INT             NULL,
    stolen_bases    INT             NULL,
    caught_stealing INT             NULL,
    avg             DECIMAL(5,3)    NULL,
    obp             DECIMAL(5,3)    NULL,
    slg             DECIMAL(5,3)    NULL,
    ops             DECIMAL(5,3)    NULL,
    babip           DECIMAL(5,3)    NULL,
    loaded_at       DATETIMEOFFSET  NOT NULL,
    CONSTRAINT pk_fact_batting PRIMARY KEY (player_id, team_id, season_year, game_type)
);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'silver' AND t.name = 'fact_pitching'
)
CREATE TABLE silver.fact_pitching (
    player_id           INT             NOT NULL,
    team_id             INT             NOT NULL,
    season_year         INT             NOT NULL,
    game_type           NVARCHAR(2)     NOT NULL,
    games               INT             NULL,
    games_started       INT             NULL,
    wins                INT             NULL,
    losses              INT             NULL,
    saves               INT             NULL,
    holds               INT             NULL,
    ip                  DECIMAL(6,1)    NULL,
    hits_allowed        INT             NULL,
    runs_allowed        INT             NULL,
    earned_runs         INT             NULL,
    home_runs_allowed   INT             NULL,
    walks               INT             NULL,
    strikeouts          INT             NULL,
    era                 DECIMAL(5,2)    NULL,
    whip                DECIMAL(5,3)    NULL,
    k9                  DECIMAL(5,2)    NULL,
    bb9                 DECIMAL(5,2)    NULL,
    hr9                 DECIMAL(5,2)    NULL,
    k_bb_ratio          DECIMAL(5,2)    NULL,
    fip                 DECIMAL(5,2)    NULL,
    loaded_at           DATETIMEOFFSET  NOT NULL,
    CONSTRAINT pk_fact_pitching PRIMARY KEY (player_id, team_id, season_year, game_type)
);
GO


-- =============================================================================
-- GOLD — Structural tables (views defined in sql/gold/*.sql)
-- =============================================================================

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'gold' AND t.name = 'standings_snap'
)
CREATE TABLE gold.standings_snap (
    snap_date       DATE            NOT NULL,
    season_year     INT             NOT NULL,
    team_id         INT             NOT NULL,
    division_id     INT             NOT NULL,
    wins            INT             NOT NULL DEFAULT 0,
    losses          INT             NOT NULL DEFAULT 0,
    win_pct         DECIMAL(5,3)    NULL,
    games_back      DECIMAL(5,1)    NULL,
    streak          NVARCHAR(10)    NULL,
    last_10_wins    INT             NULL,
    last_10_losses  INT             NULL,
    home_wins       INT             NULL,
    home_losses     INT             NULL,
    away_wins       INT             NULL,
    away_losses     INT             NULL,
    run_diff        INT             NULL,
    loaded_at       DATETIMEOFFSET  NOT NULL,
    CONSTRAINT pk_standings_snap PRIMARY KEY (snap_date, season_year, team_id)
);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'gold' AND t.name = 'league_averages'
)
CREATE TABLE gold.league_averages (
    season_year     INT             NOT NULL,
    league_id       INT             NOT NULL,
    game_type       NVARCHAR(2)     NOT NULL,
    league_avg      DECIMAL(5,3)    NULL,
    league_obp      DECIMAL(5,3)    NULL,
    league_slg      DECIMAL(5,3)    NULL,
    league_ops      DECIMAL(5,3)    NULL,
    league_era      DECIMAL(5,2)    NULL,
    loaded_at       DATETIMEOFFSET  NOT NULL,
    CONSTRAINT pk_league_averages PRIMARY KEY (season_year, league_id, game_type)
);
GO
