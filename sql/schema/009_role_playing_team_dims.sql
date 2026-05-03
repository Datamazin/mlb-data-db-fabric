-- Drop views created by earlier attempt before creating the real tables.
IF OBJECT_ID('gold.dim_home_team', 'V') IS NOT NULL DROP VIEW gold.dim_home_team;
IF OBJECT_ID('gold.dim_away_team', 'V') IS NOT NULL DROP VIEW gold.dim_away_team;
GO

IF OBJECT_ID('gold.dim_home_team', 'U') IS NULL
CREATE TABLE gold.dim_home_team (
    team_season_key     INT             NOT NULL,
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
    CONSTRAINT pk_dim_home_team PRIMARY KEY (team_season_key)
);
GO

IF OBJECT_ID('gold.dim_away_team', 'U') IS NULL
CREATE TABLE gold.dim_away_team (
    team_season_key     INT             NOT NULL,
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
    CONSTRAINT pk_dim_away_team PRIMARY KEY (team_season_key)
);
GO
