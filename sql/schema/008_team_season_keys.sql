-- Add surrogate composite keys to gold.fact_game for Power BI role-playing
-- dim relationships (dim_home_team, dim_away_team).
-- Formula: team_id * 10000 + season_year (e.g. team 147, 2025 → 1472025)

ALTER TABLE gold.fact_game
    ADD home_team_season_key INT NULL,
        away_team_season_key INT NULL;
