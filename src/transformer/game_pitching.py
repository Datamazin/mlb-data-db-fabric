"""
Populate silver.game_pitching from bronze game-feed Parquet files on OneLake.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import adlfs
import pyarrow.parquet as pq
import pyodbc
import structlog

log = structlog.get_logger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_outs(innings_pitched: str | None) -> int:
    """Convert MLB innings-pitched string ('5.2') → integer outs (17)."""
    if not innings_pitched:
        return 0
    try:
        val = float(innings_pitched)
        full = int(val)
        partial = round((val - full) * 10)
        return full * 3 + partial
    except (ValueError, TypeError):
        return 0


def extract_records(game_pk: int, raw_json: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    try:
        feed = json.loads(raw_json)
        game_teams = feed.get("gameData", {}).get("teams", {})
        home_id = game_teams.get("home", {}).get("id")
        away_id = game_teams.get("away", {}).get("id")
        bs_teams = feed.get("liveData", {}).get("boxscore", {}).get("teams", {})

        for side, is_home, team_id in [("home", 1, home_id), ("away", 0, away_id)]:
            if team_id is None:
                continue
            section = bs_teams.get(side, {})
            players = section.get("players", {})
            for pid in section.get("pitchers", []):
                p = players.get(f"ID{pid}", {})
                s = p.get("stats", {}).get("pitching", {})
                records.append({
                    "game_pk":           game_pk,
                    "player_id":         pid,
                    "team_id":           team_id,
                    "is_home":           is_home,
                    "wins":              s.get("wins",           0) or 0,
                    "losses":            s.get("losses",         0) or 0,
                    "saves":             s.get("saves",          0) or 0,
                    "holds":             s.get("holds",          0) or 0,
                    "blown_saves":       s.get("blownSaves",     0) or 0,
                    "games_started":     s.get("gamesStarted",   0) or 0,
                    "games_finished":    s.get("gamesFinished",  0) or 0,
                    "complete_games":    s.get("completeGames",  0) or 0,
                    "shutouts":          s.get("shutouts",       0) or 0,
                    "outs":              _parse_outs(s.get("inningsPitched")),
                    "hits_allowed":      s.get("hits",           0) or 0,
                    "runs_allowed":      s.get("runs",           0) or 0,
                    "earned_runs":       s.get("earnedRuns",     0) or 0,
                    "home_runs_allowed": s.get("homeRuns",       0) or 0,
                    "walks":             s.get("baseOnBalls",    0) or 0,
                    "strikeouts":        s.get("strikeOuts",     0) or 0,
                    "hit_by_pitch":      s.get("hitByPitch",     0) or 0,
                    "pitches_thrown":    s.get("pitchesThrown",  0) or 0,
                    "strikes":           s.get("strikes",        0) or 0,
                })
    except Exception as exc:
        log.warning("game_pitching_parse_error", game_pk=game_pk, error=str(exc))
    return records


def populate_from_files(
    conn: pyodbc.Connection,
    fs: adlfs.AzureBlobFileSystem,
    file_paths: list[str],
) -> int:
    total = 0

    for path in file_paths:
        try:
            with fs.open(path, "rb") as handle:
                df = pq.read_table(handle).to_pandas()
        except Exception as exc:
            log.warning("game_pitching_read_error", path=path, error=str(exc))
            continue

        if "extracted_at" in df.columns:
            df = (
                df.sort_values("extracted_at", ascending=False)
                .drop_duplicates(subset=["game_pk"])
            )

        records: list[dict[str, Any]] = []
        for _, row in df.iterrows():
            records.extend(extract_records(int(row["game_pk"]), row["raw_json"]))

        if not records:
            continue

        cursor = conn.cursor()
        try:
            cursor.execute(
                "IF OBJECT_ID('staging.game_pitching_load', 'U') IS NOT NULL "
                "DROP TABLE staging.game_pitching_load"
            )
            cursor.execute(
                """
                CREATE TABLE staging.game_pitching_load (
                    game_pk             BIGINT NOT NULL,
                    player_id           INT    NOT NULL,
                    team_id             INT    NOT NULL,
                    is_home             BIT    NOT NULL,
                    wins                INT    NOT NULL DEFAULT 0,
                    losses              INT    NOT NULL DEFAULT 0,
                    saves               INT    NOT NULL DEFAULT 0,
                    holds               INT    NOT NULL DEFAULT 0,
                    blown_saves         INT    NOT NULL DEFAULT 0,
                    games_started       INT    NOT NULL DEFAULT 0,
                    games_finished      INT    NOT NULL DEFAULT 0,
                    complete_games      INT    NOT NULL DEFAULT 0,
                    shutouts            INT    NOT NULL DEFAULT 0,
                    outs                INT    NOT NULL DEFAULT 0,
                    hits_allowed        INT    NOT NULL DEFAULT 0,
                    runs_allowed        INT    NOT NULL DEFAULT 0,
                    earned_runs         INT    NOT NULL DEFAULT 0,
                    home_runs_allowed   INT    NOT NULL DEFAULT 0,
                    walks               INT    NOT NULL DEFAULT 0,
                    strikeouts          INT    NOT NULL DEFAULT 0,
                    hit_by_pitch        INT    NOT NULL DEFAULT 0,
                    pitches_thrown      INT    NOT NULL DEFAULT 0,
                    strikes             INT    NOT NULL DEFAULT 0
                )
                """
            )

            cursor.fast_executemany = True
            cursor.executemany(
                """
                INSERT INTO staging.game_pitching_load
                    (game_pk, player_id, team_id, is_home,
                     wins, losses, saves, holds, blown_saves,
                     games_started, games_finished, complete_games, shutouts,
                     outs, hits_allowed, runs_allowed, earned_runs,
                     home_runs_allowed, walks, strikeouts,
                     hit_by_pitch, pitches_thrown, strikes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    (
                        r["game_pk"], r["player_id"], r["team_id"], r["is_home"],
                        r["wins"], r["losses"], r["saves"], r["holds"], r["blown_saves"],
                        r["games_started"], r["games_finished"], r["complete_games"], r["shutouts"],
                        r["outs"], r["hits_allowed"], r["runs_allowed"], r["earned_runs"],
                        r["home_runs_allowed"], r["walks"], r["strikeouts"],
                        r["hit_by_pitch"], r["pitches_thrown"], r["strikes"],
                    )
                    for r in records
                ],
            )

            cursor.execute(
                """
                MERGE silver.game_pitching AS tgt
                USING staging.game_pitching_load AS src
                    ON tgt.game_pk = src.game_pk AND tgt.player_id = src.player_id
                WHEN MATCHED THEN UPDATE SET
                    tgt.team_id           = src.team_id,
                    tgt.is_home           = src.is_home,
                    tgt.wins              = src.wins,
                    tgt.losses            = src.losses,
                    tgt.saves             = src.saves,
                    tgt.holds             = src.holds,
                    tgt.blown_saves       = src.blown_saves,
                    tgt.games_started     = src.games_started,
                    tgt.games_finished    = src.games_finished,
                    tgt.complete_games    = src.complete_games,
                    tgt.shutouts          = src.shutouts,
                    tgt.outs              = src.outs,
                    tgt.hits_allowed      = src.hits_allowed,
                    tgt.runs_allowed      = src.runs_allowed,
                    tgt.earned_runs       = src.earned_runs,
                    tgt.home_runs_allowed = src.home_runs_allowed,
                    tgt.walks             = src.walks,
                    tgt.strikeouts        = src.strikeouts,
                    tgt.hit_by_pitch      = src.hit_by_pitch,
                    tgt.pitches_thrown    = src.pitches_thrown,
                    tgt.strikes           = src.strikes,
                    tgt.loaded_at         = SYSDATETIMEOFFSET()
                WHEN NOT MATCHED BY TARGET THEN INSERT
                    (game_pk, player_id, team_id, is_home,
                     wins, losses, saves, holds, blown_saves,
                     games_started, games_finished, complete_games, shutouts,
                     outs, hits_allowed, runs_allowed, earned_runs,
                     home_runs_allowed, walks, strikeouts,
                     hit_by_pitch, pitches_thrown, strikes, loaded_at)
                VALUES
                    (src.game_pk, src.player_id, src.team_id, src.is_home,
                     src.wins, src.losses, src.saves, src.holds, src.blown_saves,
                     src.games_started, src.games_finished, src.complete_games, src.shutouts,
                     src.outs, src.hits_allowed, src.runs_allowed, src.earned_runs,
                     src.home_runs_allowed, src.walks, src.strikeouts,
                     src.hit_by_pitch, src.pitches_thrown, src.strikes, SYSDATETIMEOFFSET());
                """
            )
            rows_written = cursor.rowcount if cursor.rowcount >= 0 else len(records)
            cursor.execute("DROP TABLE staging.game_pitching_load")
            conn.commit()
            total += rows_written
            log.info("game_pitching_loaded", path=path, rows=rows_written)

        except Exception as exc:
            conn.rollback()
            log.error("game_pitching_insert_error", path=path, error=str(exc))

    return total
