"""
Bronze Parquet writer — OneLake (ADLS Gen2) version.

Writes raw-but-typed records to partitioned Parquet files in the OneLake
Lakehouse Files section. Partition layout mirrors the original local layout:

    bronze/games/year=2024/month=07/games_20240704.parquet
    bronze/players/season=2024/players_2024.parquet
    bronze/teams/season=2024/teams_2024.parquet
    bronze/schedules/year=2024/month=07/schedule_20240704.parquet

The raw JSON blob is stored alongside typed columns so nothing is lost if
the Pydantic model misses a field.
"""

from __future__ import annotations

import io
import json
from datetime import date, datetime, timezone
from typing import Any

import adlfs
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

log = structlog.get_logger(__name__)

# ── Arrow schemas (unchanged from local version) ──────────────────────────────

GAME_SCHEMA = pa.schema([
    pa.field("game_pk",               pa.int64()),
    pa.field("season_year",           pa.int32()),
    pa.field("game_date",             pa.string()),
    pa.field("game_datetime",         pa.string()),
    pa.field("game_type",             pa.string()),
    pa.field("status_detailed_state", pa.string()),
    pa.field("home_team_id",          pa.int32()),
    pa.field("away_team_id",          pa.int32()),
    pa.field("home_score",            pa.int32()),
    pa.field("away_score",            pa.int32()),
    pa.field("innings",               pa.int32()),
    pa.field("venue_id",              pa.int32()),
    pa.field("attendance",            pa.int32()),
    pa.field("game_duration_min",     pa.int32()),
    pa.field("double_header",         pa.string()),
    pa.field("series_description",    pa.string()),
    pa.field("series_game_num",       pa.int32()),
    pa.field("wp_id",                 pa.int32()),
    pa.field("lp_id",                 pa.int32()),
    pa.field("sv_id",                 pa.int32()),
    pa.field("raw_json",              pa.string()),
    pa.field("extracted_at",          pa.string()),
    pa.field("source_url",            pa.string()),
])

SCHEDULE_SCHEMA = pa.schema([
    pa.field("game_pk",               pa.int64()),
    pa.field("season_year",           pa.int32()),
    pa.field("game_date",             pa.string()),
    pa.field("game_datetime",         pa.string()),
    pa.field("game_type",             pa.string()),
    pa.field("status_detailed_state", pa.string()),
    pa.field("home_team_id",          pa.int32()),
    pa.field("away_team_id",          pa.int32()),
    pa.field("home_score",            pa.int32()),
    pa.field("away_score",            pa.int32()),
    pa.field("venue_id",              pa.int32()),
    pa.field("double_header",         pa.string()),
    pa.field("series_description",    pa.string()),
    pa.field("series_game_num",       pa.int32()),
    pa.field("extracted_at",          pa.string()),
    pa.field("source_url",            pa.string()),
])

PLAYER_SCHEMA = pa.schema([
    pa.field("player_id",        pa.int32()),
    pa.field("full_name",        pa.string()),
    pa.field("first_name",       pa.string()),
    pa.field("last_name",        pa.string()),
    pa.field("birth_date",       pa.string()),
    pa.field("birth_city",       pa.string()),
    pa.field("birth_country",    pa.string()),
    pa.field("height",           pa.string()),
    pa.field("weight",           pa.int32()),
    pa.field("bats",             pa.string()),
    pa.field("throws",           pa.string()),
    pa.field("primary_position", pa.string()),
    pa.field("mlb_debut_date",   pa.string()),
    pa.field("active",           pa.bool_()),
    pa.field("raw_json",         pa.string()),
    pa.field("extracted_at",     pa.string()),
    pa.field("source_url",       pa.string()),
])

TEAM_SCHEMA = pa.schema([
    pa.field("team_id",      pa.int32()),
    pa.field("season_year",  pa.int32()),
    pa.field("team_name",    pa.string()),
    pa.field("team_abbrev",  pa.string()),
    pa.field("team_code",    pa.string()),
    pa.field("league_id",    pa.int32()),
    pa.field("division_id",  pa.int32()),
    pa.field("venue_id",     pa.int32()),
    pa.field("city",         pa.string()),
    pa.field("first_year",   pa.int32()),
    pa.field("active",       pa.bool_()),
    pa.field("raw_json",     pa.string()),
    pa.field("extracted_at", pa.string()),
    pa.field("source_url",   pa.string()),
])


# ── Writer ─────────────────────────────────────────────────────────────────────

class BronzeWriter:
    """
    Writes typed records to partitioned Parquet files in the OneLake bronze zone.

    bronze_root should be the full OneLake path prefix as returned by
    connections.get_bronze_root(), e.g.:
        {workspace_id}/{lakehouse_name}.Lakehouse/Files/bronze
    """

    def __init__(
        self,
        fs: adlfs.AzureBlobFileSystem,
        bronze_root: str,
    ) -> None:
        self._fs = fs
        self._root = bronze_root.rstrip("/")

    def _write(
        self,
        records: list[dict[str, Any]],
        schema: pa.Schema,
        onelake_path: str,
    ) -> str:
        """Serialise records to Parquet and upload to OneLake. Returns the path."""
        if not records:
            log.debug("bronze_writer_skip_empty", path=onelake_path)
            return onelake_path

        table = pa.Table.from_pylist(records, schema=schema)

        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy", write_statistics=True)
        buf.seek(0)

        with self._fs.open(onelake_path, "wb") as f:
            f.write(buf.read())

        log.info("bronze_write", path=onelake_path, rows=len(records))
        return onelake_path

    def write_games(self, records: list[dict[str, Any]], for_date: date) -> str:
        path = (
            f"{self._root}/games"
            f"/year={for_date.year}"
            f"/month={for_date.month:02d}"
            f"/games_{for_date.strftime('%Y%m%d')}.parquet"
        )
        return self._write(records, GAME_SCHEMA, path)

    def write_schedule(self, records: list[dict[str, Any]], for_date: date) -> str:
        path = (
            f"{self._root}/schedules"
            f"/year={for_date.year}"
            f"/month={for_date.month:02d}"
            f"/schedule_{for_date.strftime('%Y%m%d')}.parquet"
        )
        return self._write(records, SCHEDULE_SCHEMA, path)

    def write_players(self, records: list[dict[str, Any]], season_year: int) -> str:
        path = f"{self._root}/players/season={season_year}/players_{season_year}.parquet"
        return self._write(records, PLAYER_SCHEMA, path)

    def write_teams(self, records: list[dict[str, Any]], season_year: int) -> str:
        path = f"{self._root}/teams/season={season_year}/teams_{season_year}.parquet"
        return self._write(records, TEAM_SCHEMA, path)


# ── Record builders (unchanged logic) ────────────────────────────────────────

def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _int_or_none(v: Any) -> int | None:
    return int(v) if v is not None else None


def _decision_id(feed_model: Any, decision: str) -> int | None:
    decisions = getattr(feed_model.live_data, "decisions", None)
    if decisions is None:
        return None
    pitcher = getattr(decisions, decision, None)
    return pitcher.id if pitcher else None


def game_feed_to_record(feed_model: Any, raw: dict[str, Any], source_url: str) -> dict[str, Any]:
    gd = feed_model.game_data
    gi = gd.game_info or {}
    return {
        "game_pk":               feed_model.game_pk,
        "season_year":           _int_or_none(gd.game.season),
        "game_date":             gd.datetime.official_date,
        "game_datetime":         gd.datetime.date_time,
        "game_type":             gd.game.type,
        "status_detailed_state": gd.status.detailed_state,
        "home_team_id":          gd.teams.home.id,
        "away_team_id":          gd.teams.away.id,
        "home_score":            feed_model.home_score,
        "away_score":            feed_model.away_score,
        "innings":               feed_model.innings_played,
        "venue_id":              gd.venue.id if gd.venue else None,
        "attendance":            getattr(gi, "attendance", None),
        "game_duration_min":     getattr(gi, "game_duration_minutes", None),
        "double_header":         gd.game.double_header,
        "series_description":    gd.series_description,
        "series_game_num":       gd.series_game_number,
        "wp_id":                 _decision_id(feed_model, "winner"),
        "lp_id":                 _decision_id(feed_model, "loser"),
        "sv_id":                 _decision_id(feed_model, "save"),
        "raw_json":              json.dumps(raw),
        "extracted_at":          _now_utc(),
        "source_url":            source_url,
    }


def player_to_record(person_model: Any, raw: dict[str, Any], source_url: str) -> dict[str, Any]:
    return {
        "player_id":        person_model.id,
        "full_name":        person_model.full_name,
        "first_name":       person_model.first_name,
        "last_name":        person_model.last_name,
        "birth_date":       person_model.birth_date,
        "birth_city":       person_model.birth_city,
        "birth_country":    person_model.birth_country,
        "height":           person_model.height,
        "weight":           person_model.weight,
        "bats":             person_model.bats,
        "throws":           person_model.throws,
        "primary_position": person_model.position_code,
        "mlb_debut_date":   person_model.mlb_debut_date,
        "active":           person_model.active,
        "raw_json":         json.dumps(raw),
        "extracted_at":     _now_utc(),
        "source_url":       source_url,
    }


def team_to_record(
    team_model: Any, season_year: int, raw: dict[str, Any], source_url: str
) -> dict[str, Any]:
    return {
        "team_id":      team_model.id,
        "season_year":  season_year,
        "team_name":    team_model.name,
        "team_abbrev":  team_model.abbreviation,
        "team_code":    team_model.team_code,
        "league_id":    team_model.league.id if team_model.league else None,
        "division_id":  team_model.division.id if team_model.division else None,
        "venue_id":     team_model.venue.id if team_model.venue else None,
        "city":         team_model.location_name,
        "first_year":   _int_or_none(team_model.first_year_of_play),
        "active":       team_model.active,
        "raw_json":     json.dumps(raw),
        "extracted_at": _now_utc(),
        "source_url":   source_url,
    }
