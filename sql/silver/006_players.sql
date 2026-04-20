-- =============================================================================
-- Silver 006 — Players (T-SQL)
-- SCD Type 1: latest row per player_id wins.
-- Python loads staging.players from bronze/players Parquet before running this.
-- Staging schema matches silver.players columns (excluding loaded_at).
-- Python applies:
--   - deduplication to latest extracted_at per player_id
--   - TRY_CAST for birth_date and mlb_debut_date
--   - LEFT(bats/throws, 1) to normalise full words ("Left" → "L")
--   - WHERE full_name IS NOT NULL filter
-- =============================================================================

MERGE silver.players AS tgt
USING staging.players AS src ON tgt.player_id = src.player_id
WHEN MATCHED THEN UPDATE SET
    tgt.full_name        = src.full_name,
    tgt.first_name       = src.first_name,
    tgt.last_name        = src.last_name,
    tgt.birth_date       = src.birth_date,
    tgt.birth_city       = src.birth_city,
    tgt.birth_country    = src.birth_country,
    tgt.height           = src.height,
    tgt.weight           = src.weight,
    tgt.bats             = src.bats,
    tgt.throws           = src.throws,
    tgt.primary_position = src.primary_position,
    tgt.mlb_debut_date   = src.mlb_debut_date,
    tgt.active           = src.active,
    tgt.loaded_at        = SYSDATETIMEOFFSET()
WHEN NOT MATCHED BY TARGET THEN INSERT
    (player_id, full_name, first_name, last_name, birth_date,
     birth_city, birth_country, height, weight,
     bats, throws, primary_position, mlb_debut_date, active, loaded_at)
VALUES
    (src.player_id, src.full_name, src.first_name, src.last_name, src.birth_date,
     src.birth_city, src.birth_country, src.height, src.weight,
     src.bats, src.throws, src.primary_position, src.mlb_debut_date,
     src.active, SYSDATETIMEOFFSET());
