-- =============================================================================
-- Silver 009 — Game boxscore (T-SQL)
-- Python extracts home and away team stats from liveData.boxscore.teams in
-- bronze/games raw_json and loads two rows per game_pk (is_home=1 / is_home=0)
-- into staging.game_boxscore before running this script.
--
-- Staging schema: (game_pk BIGINT, team_id INT, is_home BIT,
--                  runs INT, hits INT, errors INT, left_on_base INT,
--                  batting_order NVARCHAR(MAX), pitching_order NVARCHAR(MAX))
-- Python deduplicates to most recent feed per game_pk before staging.
-- =============================================================================

MERGE silver.game_boxscore AS tgt
USING staging.game_boxscore AS src
    ON tgt.game_pk = src.game_pk AND tgt.team_id = src.team_id
WHEN MATCHED THEN UPDATE SET
    tgt.is_home        = src.is_home,
    tgt.runs           = src.runs,
    tgt.hits           = src.hits,
    tgt.errors         = src.errors,
    tgt.left_on_base   = src.left_on_base,
    tgt.batting_order  = src.batting_order,
    tgt.pitching_order = src.pitching_order,
    tgt.loaded_at      = SYSDATETIMEOFFSET()
WHEN NOT MATCHED BY TARGET THEN INSERT
    (game_pk, team_id, is_home, runs, hits, errors,
     left_on_base, batting_order, pitching_order, loaded_at)
VALUES
    (src.game_pk, src.team_id, src.is_home, src.runs, src.hits, src.errors,
     src.left_on_base, src.batting_order, src.pitching_order, SYSDATETIMEOFFSET());
