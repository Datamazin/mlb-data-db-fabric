-- =============================================================================
-- Silver 008 — Game linescore (T-SQL)
-- Python shreds the liveData.linescore.innings JSON array from bronze/games
-- raw_json and loads one row per (game_pk, inning) into staging.game_linescore
-- before running this script. JSON array unnesting that DuckDB handled with
-- UNNEST() is performed in Python using json.loads() + iteration.
--
-- Staging schema: (game_pk BIGINT, inning INT,
--                  home_runs INT, home_hits INT, home_errors INT,
--                  away_runs INT, away_hits INT, away_errors INT)
-- Python deduplicates to the most recent feed per game_pk before staging.
-- COALESCE of NULL run/hit/error values to 0 is applied by Python.
-- =============================================================================

MERGE silver.game_linescore AS tgt
USING staging.game_linescore AS src
    ON tgt.game_pk = src.game_pk AND tgt.inning = src.inning
WHEN MATCHED THEN UPDATE SET
    tgt.home_runs   = src.home_runs,
    tgt.home_hits   = src.home_hits,
    tgt.home_errors = src.home_errors,
    tgt.away_runs   = src.away_runs,
    tgt.away_hits   = src.away_hits,
    tgt.away_errors = src.away_errors,
    tgt.loaded_at   = SYSDATETIMEOFFSET()
WHEN NOT MATCHED BY TARGET THEN INSERT
    (game_pk, inning, home_runs, home_hits, home_errors,
     away_runs, away_hits, away_errors, loaded_at)
VALUES
    (src.game_pk, src.inning, src.home_runs, src.home_hits, src.home_errors,
     src.away_runs, src.away_hits, src.away_errors, SYSDATETIMEOFFSET());
