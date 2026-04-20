-- =============================================================================
-- Gold 001 — dim_player (T-SQL)
-- Materialized table populated by MERGE from silver.players.
-- =============================================================================

MERGE gold.dim_player AS tgt
USING (
    SELECT
        player_id, full_name, first_name, last_name, birth_date,
        birth_city, birth_country, height, weight, bats, throws,
        primary_position, mlb_debut_date, active
    FROM silver.players
) AS src
ON tgt.player_id = src.player_id
WHEN MATCHED THEN UPDATE SET
    full_name        = src.full_name,
    first_name       = src.first_name,
    last_name        = src.last_name,
    birth_date       = src.birth_date,
    birth_city       = src.birth_city,
    birth_country    = src.birth_country,
    height           = src.height,
    weight           = src.weight,
    bats             = src.bats,
    throws           = src.throws,
    primary_position = src.primary_position,
    mlb_debut_date   = src.mlb_debut_date,
    active           = src.active,
    loaded_at        = SYSDATETIMEOFFSET()
WHEN NOT MATCHED THEN INSERT (
    player_id, full_name, first_name, last_name, birth_date,
    birth_city, birth_country, height, weight, bats, throws,
    primary_position, mlb_debut_date, active, loaded_at
) VALUES (
    src.player_id, src.full_name, src.first_name, src.last_name, src.birth_date,
    src.birth_city, src.birth_country, src.height, src.weight, src.bats, src.throws,
    src.primary_position, src.mlb_debut_date, src.active, SYSDATETIMEOFFSET()
);
