"""
Unit tests for the bronze Parquet writer (OneLake / adlfs version).

BronzeWriter now accepts any fsspec-compatible filesystem. Tests use an
in-memory filesystem so no real OneLake credentials are needed.

Validates:
  - Files land at the correct partitioned path inside the memory FS
  - Written files are readable by pyarrow with the expected columns and types
  - Empty record lists produce no file (no zero-byte Parquet files)
  - Required fields are present; nullable fields may be None
"""

from __future__ import annotations

from datetime import date

import pyarrow.parquet as pq
import pytest
from fsspec.implementations.memory import MemoryFileSystem

from extractor.writer import (
    BronzeWriter,
    GAME_SCHEMA,
    PLAYER_SCHEMA,
    TEAM_SCHEMA,
    _now_utc,
)

BRONZE_ROOT = "test-ws/mlb_bronze.Lakehouse/Files/bronze"


def _writer(mem_fs: MemoryFileSystem) -> BronzeWriter:
    return BronzeWriter(mem_fs, BRONZE_ROOT)


def _game_record(**overrides) -> dict:
    base = {
        "game_pk":               745525,
        "season_year":           2024,
        "game_date":             "2024-07-04",
        "game_datetime":         "2024-07-04T17:10:00+00:00",
        "game_type":             "R",
        "status_detailed_state": "Final",
        "home_team_id":          111,
        "away_team_id":          147,
        "home_score":            5,
        "away_score":            3,
        "innings":               9,
        "venue_id":              3,
        "attendance":            37755,
        "game_duration_min":     185,
        "double_header":         "N",
        "series_description":    "Regular Season",
        "series_game_num":       1,
        "raw_json":              '{"gamePk": 745525}',
        "extracted_at":          _now_utc(),
        "source_url":            "/v1.1/game/745525/feed/live",
    }
    return {**base, **overrides}


def _player_record(**overrides) -> dict:
    base = {
        "player_id":        660271,
        "full_name":        "Shohei Ohtani",
        "first_name":       "Shohei",
        "last_name":        "Ohtani",
        "birth_date":       "1994-07-05",
        "birth_city":       "Oshu",
        "birth_country":    "Japan",
        "height":           "6' 4\"",
        "weight":           210,
        "bats":             "L",
        "throws":           "R",
        "primary_position": "DH",
        "mlb_debut_date":   "2018-03-29",
        "active":           True,
        "raw_json":         '{"id": 660271}',
        "extracted_at":     _now_utc(),
        "source_url":       "/v1/people/660271",
    }
    return {**base, **overrides}


def _team_record(**overrides) -> dict:
    base = {
        "team_id":      119,
        "season_year":  2024,
        "team_name":    "Los Angeles Dodgers",
        "team_abbrev":  "LAD",
        "team_code":    "lan",
        "league_id":    104,
        "division_id":  203,
        "venue_id":     22,
        "city":         "Los Angeles",
        "first_year":   1884,
        "active":       True,
        "raw_json":     '{"id": 119}',
        "extracted_at": _now_utc(),
        "source_url":   "/v1/teams?sportId=1&season=2024",
    }
    return {**base, **overrides}


class TestBronzeWriterGames:
    def test_writes_to_correct_path(self, mem_fs):
        path = _writer(mem_fs).write_games([_game_record()], for_date=date(2024, 7, 4))
        assert mem_fs.exists(path)
        assert "year=2024" in path
        assert "month=07" in path
        assert "games_20240704.parquet" in path

    def test_pyarrow_reads_columns(self, mem_fs):
        path = _writer(mem_fs).write_games([_game_record()], for_date=date(2024, 7, 4))
        table = pq.read_table(path, filesystem=mem_fs)
        assert table.num_rows == 1
        for field in GAME_SCHEMA:
            assert field.name in table.schema.names, f"Column '{field.name}' missing"

    def test_key_values_round_trip(self, mem_fs):
        path = _writer(mem_fs).write_games([_game_record()], for_date=date(2024, 7, 4))
        table = pq.read_table(path, filesystem=mem_fs)
        row = table.to_pydict()
        assert row["game_pk"][0] == 745525
        assert row["home_score"][0] == 5
        assert row["away_score"][0] == 3
        assert row["status_detailed_state"][0] == "Final"

    def test_nullable_fields_accepted(self, mem_fs):
        record = _game_record(
            home_score=None, away_score=None, innings=None,
            attendance=None, game_duration_min=None, venue_id=None,
            series_description=None, series_game_num=None, game_datetime=None,
        )
        path = _writer(mem_fs).write_games([record], for_date=date(2024, 7, 4))
        assert mem_fs.exists(path)

    def test_empty_records_no_file(self, mem_fs):
        path = _writer(mem_fs).write_games([], for_date=date(2024, 7, 4))
        assert not mem_fs.exists(path)

    def test_multiple_games_same_date(self, mem_fs):
        records = [_game_record(game_pk=745525), _game_record(game_pk=745526)]
        path = _writer(mem_fs).write_games(records, for_date=date(2024, 7, 4))
        table = pq.read_table(path, filesystem=mem_fs)
        assert table.num_rows == 2


class TestBronzeWriterPlayers:
    def test_writes_to_correct_path(self, mem_fs):
        path = _writer(mem_fs).write_players([_player_record()], season_year=2024)
        assert mem_fs.exists(path)
        assert "season=2024" in path
        assert "players_2024.parquet" in path

    def test_pyarrow_reads_player_columns(self, mem_fs):
        path = _writer(mem_fs).write_players([_player_record()], season_year=2024)
        table = pq.read_table(path, filesystem=mem_fs)
        for field in PLAYER_SCHEMA:
            assert field.name in table.schema.names

    def test_player_values_round_trip(self, mem_fs):
        path = _writer(mem_fs).write_players([_player_record()], season_year=2024)
        row = pq.read_table(path, filesystem=mem_fs).to_pydict()
        assert row["player_id"][0] == 660271
        assert row["full_name"][0] == "Shohei Ohtani"
        assert row["bats"][0] == "L"
        assert row["throws"][0] == "R"


class TestBronzeWriterTeams:
    def test_writes_to_correct_path(self, mem_fs):
        path = _writer(mem_fs).write_teams([_team_record()], season_year=2024)
        assert mem_fs.exists(path)
        assert "season=2024" in path
        assert "teams_2024.parquet" in path

    def test_team_values_round_trip(self, mem_fs):
        path = _writer(mem_fs).write_teams([_team_record()], season_year=2024)
        row = pq.read_table(path, filesystem=mem_fs).to_pydict()
        assert row["team_id"][0] == 119
        assert row["team_name"][0] == "Los Angeles Dodgers"
        assert row["team_abbrev"][0] == "LAD"
        assert row["league_id"][0] == 104
