"""
Populate silver.game_batting from bronze game-feed Parquet files on OneLake.

Called by the nightly scheduler (per-date) and the backfill CLI (all dates).
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


def extract_records(game_pk: int, raw_json: str) -> list[dict[str, Any]]:
    """Parse one game feed JSON → list of batting-stat dicts."""
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
            for pid_str in section.get("battingOrder", []):
                pid = int(pid_str)
                p = players.get(f"ID{pid}", {})
                bat_ord_str = p.get("battingOrder", "")
                bat_ord = int(bat_ord_str) if bat_ord_str else None
                b = p.get("stats", {}).get("batting", {})
                records.append({
                    "game_pk":         game_pk,
                    "player_id":       pid,
                    "team_id":         team_id,
                    "is_home":         is_home,
                    "batting_order":   bat_ord,
                    "position_abbrev": p.get("position", {}).get("abbreviation") or None,
                    "at_bats":         b.get("atBats",     0) or 0,
                    "runs":            b.get("runs",        0) or 0,
                    "hits":            b.get("hits",        0) or 0,
                    "doubles":         b.get("doubles",     0) or 0,
                    "triples":         b.get("triples",     0) or 0,
                    "home_runs":       b.get("homeRuns",    0) or 0,
                    "rbi":             b.get("rbi",         0) or 0,
                    "walks":           b.get("baseOnBalls", 0) or 0,
                    "strikeouts":      b.get("strikeOuts",  0) or 0,
                    "left_on_base":    b.get("leftOnBase",  0) or 0,
                })
    except Exception as exc:
        log.warning("game_batting_parse_error", game_pk=game_pk, error=str(exc))
    return records


def populate_from_files(
    conn: pyodbc.Connection,
    fs: adlfs.AzureBlobFileSystem,
    file_paths: list[str],
) -> int:
    """
    Read game-feed Parquet files from OneLake, extract batting rows,
    and MERGE into silver.game_batting. Returns total rows written.
    """
    total = 0

    for path in file_paths:
        try:
            with fs.open(path, "rb") as handle:
                table = pq.read_table(handle)
            df = table.to_pandas()
        except Exception as exc:
            log.warning("game_batting_read_error", path=path, error=str(exc))
            continue

        # Deduplicate to latest feed per game_pk
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

        # Deduplicate by (game_pk, player_id) — keep first occurrence.
        # Duplicate keys in the MERGE source cause a PK violation on target.
        seen: set[tuple[int, int]] = set()
        deduped: list[dict[str, Any]] = []
        for r in records:
            key = (r["game_pk"], r["player_id"])
            if key not in seen:
                seen.add(key)
                deduped.append(r)
        records = deduped

        cursor = conn.cursor()
        try:
            cursor.execute(
                "IF OBJECT_ID('staging.game_batting_load', 'U') IS NOT NULL "
                "DROP TABLE staging.game_batting_load"
            )
            cursor.execute(
                """
                CREATE TABLE staging.game_batting_load (
                    game_pk         BIGINT       NOT NULL,
                    player_id       INT          NOT NULL,
                    team_id         INT          NOT NULL,
                    is_home         BIT          NOT NULL,
                    batting_order   INT,
                    position_abbrev NVARCHAR(4),
                    at_bats         INT          NOT NULL DEFAULT 0,
                    runs            INT          NOT NULL DEFAULT 0,
                    hits            INT          NOT NULL DEFAULT 0,
                    doubles         INT          NOT NULL DEFAULT 0,
                    triples         INT          NOT NULL DEFAULT 0,
                    home_runs       INT          NOT NULL DEFAULT 0,
                    rbi             INT          NOT NULL DEFAULT 0,
                    walks           INT          NOT NULL DEFAULT 0,
                    strikeouts      INT          NOT NULL DEFAULT 0,
                    left_on_base    INT          NOT NULL DEFAULT 0
                )
                """
            )

            cursor.fast_executemany = True
            cursor.executemany(
                """
                INSERT INTO staging.game_batting_load
                    (game_pk, player_id, team_id, is_home, batting_order,
                     position_abbrev, at_bats, runs, hits, doubles, triples,
                     home_runs, rbi, walks, strikeouts, left_on_base)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    (
                        r["game_pk"], r["player_id"], r["team_id"], r["is_home"],
                        r["batting_order"], r["position_abbrev"],
                        r["at_bats"], r["runs"], r["hits"], r["doubles"], r["triples"],
                        r["home_runs"], r["rbi"], r["walks"], r["strikeouts"], r["left_on_base"],
                    )
                    for r in records
                ],
            )

            cursor.execute(
                """
                MERGE silver.game_batting AS tgt
                USING staging.game_batting_load AS src
                    ON tgt.game_pk = src.game_pk AND tgt.player_id = src.player_id
                WHEN MATCHED THEN UPDATE SET
                    tgt.team_id        = src.team_id,
                    tgt.is_home        = src.is_home,
                    tgt.batting_order  = src.batting_order,
                    tgt.position_abbrev= src.position_abbrev,
                    tgt.at_bats        = src.at_bats,
                    tgt.runs           = src.runs,
                    tgt.hits           = src.hits,
                    tgt.doubles        = src.doubles,
                    tgt.triples        = src.triples,
                    tgt.home_runs      = src.home_runs,
                    tgt.rbi            = src.rbi,
                    tgt.walks          = src.walks,
                    tgt.strikeouts     = src.strikeouts,
                    tgt.left_on_base   = src.left_on_base,
                    tgt.loaded_at      = SYSDATETIMEOFFSET()
                WHEN NOT MATCHED BY TARGET THEN INSERT
                    (game_pk, player_id, team_id, is_home, batting_order,
                     position_abbrev, at_bats, runs, hits, doubles, triples,
                     home_runs, rbi, walks, strikeouts, left_on_base, loaded_at)
                VALUES
                    (src.game_pk, src.player_id, src.team_id, src.is_home, src.batting_order,
                     src.position_abbrev, src.at_bats, src.runs, src.hits, src.doubles,
                     src.triples, src.home_runs, src.rbi, src.walks, src.strikeouts,
                     src.left_on_base, SYSDATETIMEOFFSET());
                """
            )
            rows_written = cursor.rowcount if cursor.rowcount >= 0 else len(records)
            cursor.execute("DROP TABLE staging.game_batting_load")
            conn.commit()
            total += rows_written
            log.info("game_batting_loaded", path=path, rows=rows_written)

        except Exception as exc:
            conn.rollback()
            log.error("game_batting_insert_error", path=path, error=str(exc))

    return total
