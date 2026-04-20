"""
Staging loaders — bronze Parquet (OneLake) → staging.{table} in Fabric Warehouse.

Each public function corresponds to one or more silver SQL scripts. It:
  1. Reads bronze Parquet from OneLake via adlfs + pyarrow.
  2. Applies the transformations previously done in DuckDB SQL
     (JSON extraction, deduplication, type coercion, filtering).
  3. Creates staging.{table}, bulk-inserts the cleaned rows, and returns
     the list of staging table names created so the caller can drop them.

Callers are responsible for committing or rolling back the connection.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any

import adlfs
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyodbc
import structlog

log = structlog.get_logger(__name__)


# ── Shared helpers ────────────────────────────────────────────────────────────

def _read_bronze(
    fs: adlfs.AzureBlobFileSystem,
    pattern: str,
) -> pd.DataFrame:
    """Glob + read all Parquet files matching a OneLake pattern."""
    files = fs.glob(pattern)
    if not files:
        log.info("staging_no_files", pattern=pattern)
        return pd.DataFrame()

    tables = []
    for path in files:
        with fs.open(path, "rb") as handle:
            tables.append(pq.read_table(handle))
    return pa.concat_tables(tables).to_pandas()


def _create_and_load(
    cursor: pyodbc.Cursor,
    table: str,
    ddl: str,
    insert_sql: str,
    rows: list[tuple[Any, ...]],
) -> str:
    """Drop-create a staging table and bulk-insert rows. Returns table name."""
    cursor.execute(
        f"IF OBJECT_ID('{table}', 'U') IS NOT NULL DROP TABLE {table}"
    )
    cursor.execute(ddl)
    if rows:
        cursor.fast_executemany = True
        cursor.executemany(insert_sql, rows)
    log.info("staging_loaded", table=table, rows=len(rows))
    return table


def _try_date(val: Any) -> date | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return pd.to_datetime(val).date()
    except Exception:
        return None


def _try_datetimeoffset(val: Any) -> datetime | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        dt = pd.to_datetime(val, utc=True)
        return dt.to_pydatetime()
    except Exception:
        return None


def _str1(val: Any) -> str | None:
    """Left-truncate to 1 char (normalises 'Left' → 'L' etc.)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s[0] if s else None


def _nullable_str(val: Any) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    return str(val) if str(val).strip() else None


def _nullable_int(val: Any) -> int | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# ── 002_leagues ───────────────────────────────────────────────────────────────

def load_leagues(
    cursor: pyodbc.Cursor,
    fs: adlfs.AzureBlobFileSystem,
    bronze_root: str,
    **_: Any,
) -> list[str]:
    df = _read_bronze(fs, f"{bronze_root}/teams/season=*/*.parquet")
    if df.empty:
        return []

    records: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        raw = json.loads(row["raw_json"])
        league = raw.get("league", {})
        lid = _nullable_int(league.get("id"))
        if lid is None:
            continue
        records.append({
            "league_id":    lid,
            "league_name":  league.get("name", ""),
            "short_name":   _nullable_str(league.get("nameShort")),
            "abbreviation": _nullable_str(league.get("abbreviation")),
            "extracted_at": row.get("extracted_at", ""),
        })

    if not records:
        return []

    df_out = (
        pd.DataFrame(records)
        .sort_values("extracted_at", ascending=False)
        .drop_duplicates(subset=["league_id"])
    )

    return [_create_and_load(
        cursor,
        "staging.leagues",
        """
        CREATE TABLE staging.leagues (
            league_id    INT           NOT NULL,
            league_name  NVARCHAR(100) NOT NULL,
            short_name   NVARCHAR(50),
            abbreviation NVARCHAR(10)
        )
        """,
        "INSERT INTO staging.leagues VALUES (?,?,?,?)",
        [
            (r["league_id"], r["league_name"], r["short_name"], r["abbreviation"])
            for r in df_out.to_dict("records")
        ],
    )]


# ── 003_divisions ─────────────────────────────────────────────────────────────

def load_divisions(
    cursor: pyodbc.Cursor,
    fs: adlfs.AzureBlobFileSystem,
    bronze_root: str,
    **_: Any,
) -> list[str]:
    df = _read_bronze(fs, f"{bronze_root}/teams/season=*/*.parquet")
    if df.empty:
        return []

    records: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        raw = json.loads(row["raw_json"])
        div = raw.get("division", {})
        did = _nullable_int(div.get("id"))
        if did is None:
            continue
        lid = _nullable_int(raw.get("league", {}).get("id"))
        records.append({
            "division_id":   did,
            "division_name": div.get("name", ""),
            "short_name":    _nullable_str(div.get("nameShort")),
            "league_id":     lid,
            "extracted_at":  row.get("extracted_at", ""),
        })

    if not records:
        return []

    df_out = (
        pd.DataFrame(records)
        .sort_values("extracted_at", ascending=False)
        .drop_duplicates(subset=["division_id"])
    )

    return [_create_and_load(
        cursor,
        "staging.divisions",
        """
        CREATE TABLE staging.divisions (
            division_id   INT           NOT NULL,
            division_name NVARCHAR(100) NOT NULL,
            short_name    NVARCHAR(50),
            league_id     INT
        )
        """,
        "INSERT INTO staging.divisions VALUES (?,?,?,?)",
        [
            (r["division_id"], r["division_name"], r["short_name"], r["league_id"])
            for r in df_out.to_dict("records")
        ],
    )]


# ── 004_venues ────────────────────────────────────────────────────────────────

def load_venues(
    cursor: pyodbc.Cursor,
    fs: adlfs.AzureBlobFileSystem,
    bronze_root: str,
    year_glob: str = "*",
    month_glob: str = "*",
    **_: Any,
) -> list[str]:
    created: list[str] = []

    # Pass 1 — game venues (lower priority)
    df_games = _read_bronze(
        fs, f"{bronze_root}/games/year={year_glob}/month={month_glob}/*.parquet"
    )
    if not df_games.empty:
        records: list[dict[str, Any]] = []
        for _, row in df_games.iterrows():
            raw = json.loads(row["raw_json"])
            vid = _nullable_int(raw.get("gameData", {}).get("venue", {}).get("id"))
            vname = _nullable_str(raw.get("gameData", {}).get("venue", {}).get("name"))
            if vid is None or vname is None:
                continue
            records.append({"venue_id": vid, "venue_name": vname, "extracted_at": row.get("extracted_at", "")})

        if records:
            df_out = (
                pd.DataFrame(records)
                .sort_values("extracted_at", ascending=False)
                .drop_duplicates(subset=["venue_id"])
            )
            created.append(_create_and_load(
                cursor,
                "staging.venues_games",
                """
                CREATE TABLE staging.venues_games (
                    venue_id   INT           NOT NULL,
                    venue_name NVARCHAR(200) NOT NULL
                )
                """,
                "INSERT INTO staging.venues_games VALUES (?,?)",
                [(r["venue_id"], r["venue_name"]) for r in df_out.to_dict("records")],
            ))

    # Pass 2 — team venues (authoritative)
    df_teams = _read_bronze(fs, f"{bronze_root}/teams/season=*/*.parquet")
    if not df_teams.empty:
        records2: list[dict[str, Any]] = []
        for _, row in df_teams.iterrows():
            raw = json.loads(row["raw_json"])
            vid = _nullable_int(row.get("venue_id"))
            vname = _nullable_str(raw.get("venue", {}).get("name"))
            if vid is None or vname is None:
                continue
            records2.append({"venue_id": vid, "venue_name": vname, "extracted_at": row.get("extracted_at", "")})

        if records2:
            df_out2 = (
                pd.DataFrame(records2)
                .sort_values("extracted_at", ascending=False)
                .drop_duplicates(subset=["venue_id"])
            )
            created.append(_create_and_load(
                cursor,
                "staging.venues_teams",
                """
                CREATE TABLE staging.venues_teams (
                    venue_id   INT           NOT NULL,
                    venue_name NVARCHAR(200) NOT NULL
                )
                """,
                "INSERT INTO staging.venues_teams VALUES (?,?)",
                [(r["venue_id"], r["venue_name"]) for r in df_out2.to_dict("records")],
            ))

    return created


# ── 005_teams ─────────────────────────────────────────────────────────────────

def load_teams(
    cursor: pyodbc.Cursor,
    fs: adlfs.AzureBlobFileSystem,
    bronze_root: str,
    **_: Any,
) -> list[str]:
    df = _read_bronze(fs, f"{bronze_root}/teams/season=*/*.parquet")
    if df.empty:
        return []

    # Fetch valid season years from the Warehouse
    cursor.execute("SELECT season_year FROM silver.seasons")
    valid_seasons = {row[0] for row in cursor.fetchall()}

    df = df[df["season_year"].isin(valid_seasons)]
    if df.empty:
        return []

    df = (
        df.sort_values("extracted_at", ascending=False)
        .drop_duplicates(subset=["team_id", "season_year"])
    )

    return [_create_and_load(
        cursor,
        "staging.teams",
        """
        CREATE TABLE staging.teams (
            team_id     INT           NOT NULL,
            season_year INT           NOT NULL,
            team_name   NVARCHAR(100) NOT NULL,
            team_abbrev NVARCHAR(3)   NOT NULL,
            team_code   NVARCHAR(10),
            league_id   INT,
            division_id INT,
            venue_id    INT,
            city        NVARCHAR(100),
            first_year  INT,
            active      BIT           NOT NULL
        )
        """,
        "INSERT INTO staging.teams VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                r["team_id"], r["season_year"], r["team_name"], r["team_abbrev"],
                _nullable_str(r.get("team_code")), _nullable_int(r.get("league_id")),
                _nullable_int(r.get("division_id")), _nullable_int(r.get("venue_id")),
                _nullable_str(r.get("city")), _nullable_int(r.get("first_year")),
                1 if r.get("active") else 0,
            )
            for r in df.to_dict("records")
        ],
    )]


# ── 006_players ───────────────────────────────────────────────────────────────

def load_players(
    cursor: pyodbc.Cursor,
    fs: adlfs.AzureBlobFileSystem,
    bronze_root: str,
    **_: Any,
) -> list[str]:
    df = _read_bronze(fs, f"{bronze_root}/players/season=*/*.parquet")
    if df.empty:
        return []

    df = df[df["full_name"].notna() & (df["full_name"] != "")]
    df = (
        df.sort_values("extracted_at", ascending=False)
        .drop_duplicates(subset=["player_id"])
    )

    return [_create_and_load(
        cursor,
        "staging.players",
        """
        CREATE TABLE staging.players (
            player_id        INT           NOT NULL,
            full_name        NVARCHAR(200) NOT NULL,
            first_name       NVARCHAR(100),
            last_name        NVARCHAR(100),
            birth_date       DATE,
            birth_city       NVARCHAR(100),
            birth_country    NVARCHAR(100),
            height           NVARCHAR(20),
            weight           INT,
            bats             NVARCHAR(1),
            throws           NVARCHAR(1),
            primary_position NVARCHAR(2),
            mlb_debut_date   DATE,
            active           BIT NOT NULL
        )
        """,
        "INSERT INTO staging.players VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                int(r["player_id"]), str(r["full_name"]),
                _nullable_str(r.get("first_name")), _nullable_str(r.get("last_name")),
                _try_date(r.get("birth_date")), _nullable_str(r.get("birth_city")),
                _nullable_str(r.get("birth_country")), _nullable_str(r.get("height")),
                _nullable_int(r.get("weight")),
                _str1(r.get("bats")), _str1(r.get("throws")),
                _nullable_str(r.get("primary_position")),
                _try_date(r.get("mlb_debut_date")),
                1 if r.get("active") else 0,
            )
            for r in df.to_dict("records")
        ],
    )]


# ── 007_games ─────────────────────────────────────────────────────────────────

def load_games(
    cursor: pyodbc.Cursor,
    fs: adlfs.AzureBlobFileSystem,
    bronze_root: str,
    year_glob: str = "*",
    month_glob: str = "*",
    **_: Any,
) -> list[str]:
    df = _read_bronze(
        fs, f"{bronze_root}/games/year={year_glob}/month={month_glob}/*.parquet"
    )
    if df.empty:
        return []

    cursor.execute("SELECT season_year FROM silver.seasons")
    valid_seasons = {row[0] for row in cursor.fetchall()}

    df = df[df["game_date"].notna() & df["season_year"].isin(valid_seasons)]
    df = (
        df.sort_values("extracted_at", ascending=False)
        .drop_duplicates(subset=["game_pk"])
    )

    rows: list[tuple[Any, ...]] = []
    for r in df.to_dict("records"):
        raw = json.loads(r["raw_json"])
        decisions = raw.get("liveData", {}).get("decisions", {})

        def _dec_id(key: str) -> int | None:
            p = decisions.get(key, {})
            return _nullable_int(p.get("id")) if p else None

        rows.append((
            int(r["game_pk"]), int(r["season_year"]),
            _try_date(r.get("game_date")),
            _try_datetimeoffset(r.get("game_datetime")),
            _nullable_str(r.get("game_type")),
            _nullable_str(r.get("status_detailed_state")),
            _nullable_int(r.get("home_team_id")), _nullable_int(r.get("away_team_id")),
            _nullable_int(r.get("home_score")), _nullable_int(r.get("away_score")),
            _nullable_int(r.get("innings")), _nullable_int(r.get("venue_id")),
            _nullable_int(r.get("attendance")), _nullable_int(r.get("game_duration_min")),
            _str1(r.get("double_header")), _nullable_str(r.get("series_description")),
            _nullable_int(r.get("series_game_num")),
            _dec_id("winner"), _dec_id("loser"), _dec_id("save"),
        ))

    return [_create_and_load(
        cursor,
        "staging.games",
        """
        CREATE TABLE staging.games (
            game_pk            BIGINT          NOT NULL,
            season_year        INT             NOT NULL,
            game_date          DATE,
            game_datetime      DATETIMEOFFSET,
            game_type          NVARCHAR(2),
            status             NVARCHAR(50),
            home_team_id       INT,
            away_team_id       INT,
            home_score         INT,
            away_score         INT,
            innings            INT,
            venue_id           INT,
            attendance         INT,
            game_duration_min  INT,
            double_header      NVARCHAR(1),
            series_description NVARCHAR(100),
            series_game_num    INT,
            wp_id              INT,
            lp_id              INT,
            sv_id              INT
        )
        """,
        "INSERT INTO staging.games VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )]


# ── 008_game_linescore ────────────────────────────────────────────────────────

def load_game_linescore(
    cursor: pyodbc.Cursor,
    fs: adlfs.AzureBlobFileSystem,
    bronze_root: str,
    year_glob: str = "*",
    month_glob: str = "*",
    **_: Any,
) -> list[str]:
    df = _read_bronze(
        fs, f"{bronze_root}/games/year={year_glob}/month={month_glob}/*.parquet"
    )
    if df.empty:
        return []

    # Deduplicate to latest feed per game before shredding the JSON array
    df = (
        df[df["raw_json"].notna()]
        .sort_values("extracted_at", ascending=False)
        .drop_duplicates(subset=["game_pk"])
    )

    rows: list[tuple[Any, ...]] = []
    for r in df.to_dict("records"):
        raw = json.loads(r["raw_json"])
        innings = raw.get("liveData", {}).get("linescore", {}).get("innings", [])
        for inning in innings:
            inning_num = _nullable_int(inning.get("num"))
            if inning_num is None:
                continue
            home = inning.get("home", {})
            away = inning.get("away", {})
            rows.append((
                int(r["game_pk"]),
                inning_num,
                _nullable_int(home.get("runs"))   or 0,
                _nullable_int(home.get("hits"))   or 0,
                _nullable_int(home.get("errors")) or 0,
                _nullable_int(away.get("runs"))   or 0,
                _nullable_int(away.get("hits"))   or 0,
                _nullable_int(away.get("errors")) or 0,
            ))

    return [_create_and_load(
        cursor,
        "staging.game_linescore",
        """
        CREATE TABLE staging.game_linescore (
            game_pk     BIGINT NOT NULL,
            inning      INT    NOT NULL,
            home_runs   INT    NOT NULL DEFAULT 0,
            home_hits   INT    NOT NULL DEFAULT 0,
            home_errors INT    NOT NULL DEFAULT 0,
            away_runs   INT    NOT NULL DEFAULT 0,
            away_hits   INT    NOT NULL DEFAULT 0,
            away_errors INT    NOT NULL DEFAULT 0
        )
        """,
        "INSERT INTO staging.game_linescore VALUES (?,?,?,?,?,?,?,?)",
        rows,
    )]


# ── 009_game_boxscore ─────────────────────────────────────────────────────────

def load_game_boxscore(
    cursor: pyodbc.Cursor,
    fs: adlfs.AzureBlobFileSystem,
    bronze_root: str,
    year_glob: str = "*",
    month_glob: str = "*",
    **_: Any,
) -> list[str]:
    df = _read_bronze(
        fs, f"{bronze_root}/games/year={year_glob}/month={month_glob}/*.parquet"
    )
    if df.empty:
        return []

    df = (
        df[df["raw_json"].notna()]
        .sort_values("extracted_at", ascending=False)
        .drop_duplicates(subset=["game_pk"])
    )

    rows: list[tuple[Any, ...]] = []
    for r in df.to_dict("records"):
        raw = json.loads(r["raw_json"])
        bs_teams = raw.get("liveData", {}).get("boxscore", {}).get("teams", {})
        for side, is_home in (("home", 1), ("away", 0)):
            side_data = bs_teams.get(side, {})
            team_id = _nullable_int(side_data.get("team", {}).get("id"))
            if team_id is None:
                continue
            batting_stats = side_data.get("teamStats", {}).get("batting", {})
            fielding_stats = side_data.get("teamStats", {}).get("fielding", {})
            rows.append((
                int(r["game_pk"]),
                team_id,
                is_home,
                _nullable_int(batting_stats.get("runs")),
                _nullable_int(batting_stats.get("hits")),
                _nullable_int(fielding_stats.get("errors")),
                _nullable_int(batting_stats.get("leftOnBase")),
                json.dumps(side_data.get("battingOrder", [])),
                json.dumps(side_data.get("pitchers", [])),
            ))

    return [_create_and_load(
        cursor,
        "staging.game_boxscore",
        """
        CREATE TABLE staging.game_boxscore (
            game_pk        BIGINT          NOT NULL,
            team_id        INT             NOT NULL,
            is_home        BIT             NOT NULL,
            runs           INT,
            hits           INT,
            errors         INT,
            left_on_base   INT,
            batting_order  NVARCHAR(MAX),
            pitching_order NVARCHAR(MAX)
        )
        """,
        "INSERT INTO staging.game_boxscore VALUES (?,?,?,?,?,?,?,?,?)",
        rows,
    )]


# ── game_batting staging loader ───────────────────────────────────────────────

def load_game_batting(
    *,
    cursor: "pyodbc.Cursor",
    fs: adlfs.AzureBlobFileSystem,
    bronze_root: str,
    year_glob: str = "*",
    month_glob: str = "*",
) -> list[str]:
    """
    Populate silver.game_batting from OneLake bronze game-feed Parquet files,
    then return [] (staging is managed internally by game_batting module).
    """
    from transformer.game_batting import populate_from_files  # avoid circular import

    pattern = f"{bronze_root}/games/year={year_glob}/month={month_glob}/*.parquet"
    file_paths = fs.glob(pattern)
    if not file_paths:
        log.info("load_game_batting_no_files", pattern=pattern)
        return []

    conn = cursor.connection
    total = populate_from_files(conn=conn, fs=fs, file_paths=file_paths)
    log.info("load_game_batting_done", rows=total)
    return []


# ── Registry ──────────────────────────────────────────────────────────────────
# Maps silver script filename → loader function.
# Scripts not present here need no staging (e.g. static seed data or stubs).

STAGING_REGISTRY: dict[str, Any] = {
    "002_leagues.sql":        load_leagues,
    "003_divisions.sql":      load_divisions,
    "004_venues.sql":         load_venues,
    "005_teams.sql":          load_teams,
    "006_players.sql":        load_players,
    "007_games.sql":          load_games,
    "008_game_linescore.sql": load_game_linescore,
    "009_game_boxscore.sql":  load_game_boxscore,
    "011_fact_batting.sql":   load_game_batting,
}
