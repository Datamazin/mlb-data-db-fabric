"""
Integration tests for the silver transformation layer (Fabric Warehouse version).

All tests use a live Fabric Warehouse connection (no mocks). Bronze fixture data is
written to an in-memory fsspec filesystem via BronzeWriter, then staging.py loaders
read it and merge into silver tables.

Requires: `make migrate` run against the target DB before this suite.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from extractor.writer import BronzeWriter, _now_utc
from transformer.transform import Transformer

SILVER_SQL_DIR = Path(__file__).parent.parent.parent / "sql" / "silver"


def _transformer(conn, mem_fs, bronze_root) -> Transformer:
    return Transformer(conn=conn, fs=mem_fs, bronze_root=bronze_root)


def _one(conn, sql: str, params=()) -> tuple | None:
    c = conn.cursor()
    c.execute(sql, params)
    return c.fetchone()


def _count(conn, sql: str, params=()) -> int:
    row = _one(conn, sql, params)
    return row[0] if row else 0


def _team_raw(
    team_id: int = 119,
    team_name: str = "Los Angeles Dodgers",
    league_id: int = 104,
    league_name: str = "National League",
    league_abbrev: str = "NL",
    division_id: int = 203,
    division_name: str = "NL West",
    venue_id: int = 22,
    venue_name: str = "Dodger Stadium",
) -> dict:
    return {
        "id": team_id,
        "name": team_name,
        "abbreviation": "LAD",
        "teamCode": "lan",
        "locationName": "Los Angeles",
        "firstYearOfPlay": "1884",
        "active": True,
        "league": {"id": league_id, "name": league_name,
                   "abbreviation": league_abbrev, "nameShort": league_name},
        "division": {"id": division_id, "name": division_name,
                     "abbreviation": "NLW", "nameShort": division_name},
        "venue": {"id": venue_id, "name": venue_name},
    }


def _team_record(
    team_id: int = 119,
    season_year: int = 2024,
    venue_id: int = 22,
    league_id: int = 104,
    division_id: int = 203,
    extracted_at: str | None = None,
) -> dict:
    raw = _team_raw(team_id=team_id, venue_id=venue_id,
                    league_id=league_id, division_id=division_id)
    return {
        "team_id":      team_id,
        "season_year":  season_year,
        "team_name":    "Los Angeles Dodgers",
        "team_abbrev":  "LAD",
        "team_code":    "lan",
        "league_id":    league_id,
        "division_id":  division_id,
        "venue_id":     venue_id,
        "city":         "Los Angeles",
        "first_year":   1884,
        "active":       True,
        "raw_json":     json.dumps(raw),
        "extracted_at": extracted_at or _now_utc(),
        "source_url":   "/v1/teams?sportId=1&season=2024",
    }


def _game_raw(
    game_pk: int = 745525,
    venue_id: int = 3,
    venue_name: str = "Fenway Park",
    innings: int = 9,
    home_runs: int = 5,
    away_runs: int = 3,
) -> dict:
    return {
        "gamePk": game_pk,
        "gameData": {
            "venue": {"id": venue_id, "name": venue_name},
        },
        "liveData": {
            "linescore": {
                "innings": [
                    {
                        "num": i,
                        "home": {"runs": home_runs if i == 1 else 0, "hits": 1, "errors": 0},
                        "away": {"runs": away_runs if i == 1 else 0, "hits": 1, "errors": 0},
                    }
                    for i in range(1, innings + 1)
                ],
            },
            "boxscore": {
                "teams": {
                    "home": {
                        "team": {"id": 111, "name": "Boston Red Sox"},
                        "teamStats": {
                            "batting": {"runs": home_runs, "hits": 8, "leftOnBase": 6},
                            "fielding": {"errors": 0},
                        },
                        "battingOrder": [100001, 100002, 100003],
                        "pitchers": [200001],
                    },
                    "away": {
                        "team": {"id": 147, "name": "New York Yankees"},
                        "teamStats": {
                            "batting": {"runs": away_runs, "hits": 7, "leftOnBase": 5},
                            "fielding": {"errors": 1},
                        },
                        "battingOrder": [300001, 300002, 300003],
                        "pitchers": [400001],
                    },
                },
            },
        },
    }


def _game_record(
    game_pk: int = 745525,
    season_year: int = 2024,
    game_date: str = "2024-07-04",
    game_type: str = "R",
    extracted_at: str | None = None,
    venue_id: int = 3,
    home_runs: int = 5,
    away_runs: int = 3,
    innings: int = 9,
) -> dict:
    raw = _game_raw(game_pk=game_pk, venue_id=venue_id,
                    home_runs=home_runs, away_runs=away_runs, innings=innings)
    return {
        "game_pk":               game_pk,
        "season_year":           season_year,
        "game_date":             game_date,
        "game_datetime":         "2024-07-04T17:10:00Z",
        "game_type":             game_type,
        "status_detailed_state": "Final",
        "home_team_id":          111,
        "away_team_id":          147,
        "home_score":            home_runs,
        "away_score":            away_runs,
        "innings":               innings,
        "venue_id":              venue_id,
        "attendance":            37755,
        "game_duration_min":     185,
        "double_header":         "N",
        "series_description":    "Regular Season",
        "series_game_num":       1,
        "raw_json":              json.dumps(raw),
        "extracted_at":          extracted_at or _now_utc(),
        "source_url":            f"/v1.1/game/{game_pk}/feed/live",
    }


def _player_record(
    player_id: int = 660271,
    bats: str = "L",
    throws: str = "R",
    birth_date: str = "1994-07-05",
    mlb_debut_date: str = "2018-03-29",
    extracted_at: str | None = None,
) -> dict:
    return {
        "player_id":        player_id,
        "full_name":        "Shohei Ohtani",
        "first_name":       "Shohei",
        "last_name":        "Ohtani",
        "birth_date":       birth_date,
        "birth_city":       "Oshu",
        "birth_country":    "Japan",
        "height":           "6' 4\"",
        "weight":           210,
        "bats":             bats,
        "throws":           throws,
        "primary_position": "DH",
        "mlb_debut_date":   mlb_debut_date,
        "active":           True,
        "raw_json":         json.dumps({"id": player_id}),
        "extracted_at":     extracted_at or _now_utc(),
        "source_url":       f"/v1/people/{player_id}",
    }


# ── Seasons ───────────────────────────────────────────────────────────────────

class TestSeedSeasons:
    def test_all_five_seasons_seeded(self, db, mem_fs, bronze_root):
        t = _transformer(db, mem_fs, bronze_root)
        t.run(scripts=["001_seed_seasons.sql"])
        assert _count(db, "SELECT COUNT(*) FROM silver.seasons") == 5

    def test_correct_season_years(self, db, mem_fs, bronze_root):
        t = _transformer(db, mem_fs, bronze_root)
        t.run(scripts=["001_seed_seasons.sql"])
        cursor = db.cursor()
        cursor.execute("SELECT season_year FROM silver.seasons")
        years = {row[0] for row in cursor.fetchall()}
        assert years == {2022, 2023, 2024, 2025, 2026}

    def test_rerun_is_idempotent(self, db, mem_fs, bronze_root):
        t = _transformer(db, mem_fs, bronze_root)
        t.run(scripts=["001_seed_seasons.sql"], force=True)
        t.run(scripts=["001_seed_seasons.sql"], force=True)
        assert _count(db, "SELECT COUNT(*) FROM silver.seasons") == 5

    def test_2026_postseason_is_null(self, db, mem_fs, bronze_root):
        t = _transformer(db, mem_fs, bronze_root)
        t.run(scripts=["001_seed_seasons.sql"])
        row = _one(
            db,
            "SELECT postseason_start, world_series_end FROM silver.seasons WHERE season_year = 2026",
        )
        assert row == (None, None)


# ── Leagues ───────────────────────────────────────────────────────────────────

class TestLeaguesTransform:
    def test_league_extracted_from_raw_json(self, db, mem_fs, bronze_root):
        BronzeWriter(mem_fs, bronze_root).write_teams([_team_record()], season_year=2024)
        t = _transformer(db, mem_fs, bronze_root)
        t.run(scripts=["002_leagues.sql"])
        row = _one(db, "SELECT league_id, league_name, abbreviation FROM silver.leagues")
        assert row == (104, "National League", "NL")

    def test_deduplication_latest_wins(self, db, mem_fs, bronze_root):
        w = BronzeWriter(mem_fs, bronze_root)
        w.write_teams([_team_record(extracted_at="2024-01-01T00:00:00Z")], season_year=2024)
        rec = _team_record(season_year=2024, extracted_at="2024-06-01T00:00:00Z")
        raw = _team_raw(league_name="National League Updated")
        rec["raw_json"] = json.dumps(raw)
        w.write_teams([rec], season_year=2024)

        t = _transformer(db, mem_fs, bronze_root)
        t.run(scripts=["002_leagues.sql"])
        row = _one(db, "SELECT league_name FROM silver.leagues WHERE league_id = 104")
        assert row[0] == "National League Updated"

    def test_multiple_teams_same_league_one_row(self, db, mem_fs, bronze_root):
        BronzeWriter(mem_fs, bronze_root).write_teams(
            [_team_record(team_id=119), _team_record(team_id=118)], season_year=2024
        )
        t = _transformer(db, mem_fs, bronze_root)
        t.run(scripts=["002_leagues.sql"])
        assert _count(db, "SELECT COUNT(*) FROM silver.leagues WHERE league_id = 104") == 1


# ── Divisions ─────────────────────────────────────────────────────────────────

class TestDivisionsTransform:
    def test_division_includes_league_id(self, db, mem_fs, bronze_root):
        BronzeWriter(mem_fs, bronze_root).write_teams([_team_record()], season_year=2024)
        t = _transformer(db, mem_fs, bronze_root)
        t.run(scripts=["002_leagues.sql", "003_divisions.sql"])
        row = _one(db, "SELECT division_id, division_name, league_id FROM silver.divisions")
        assert row == (203, "NL West", 104)


# ── Venues ────────────────────────────────────────────────────────────────────

class TestVenuesTransform:
    def test_venue_from_teams_bronze(self, db, mem_fs, bronze_root):
        BronzeWriter(mem_fs, bronze_root).write_teams([_team_record()], season_year=2024)
        _transformer(db, mem_fs, bronze_root).run(scripts=["004_venues.sql"])
        row = _one(db, "SELECT venue_id, venue_name FROM silver.venues WHERE venue_id = 22")
        assert row == (22, "Dodger Stadium")

    def test_nullable_columns_are_null(self, db, mem_fs, bronze_root):
        BronzeWriter(mem_fs, bronze_root).write_teams([_team_record()], season_year=2024)
        _transformer(db, mem_fs, bronze_root).run(scripts=["004_venues.sql"])
        row = _one(db, "SELECT city, surface, capacity FROM silver.venues WHERE venue_id = 22")
        assert row == (None, None, None)


# ── Teams ─────────────────────────────────────────────────────────────────────

class TestTeamsTransform:
    def _seed_refs(self, t: Transformer, mem_fs, bronze_root) -> None:
        BronzeWriter(mem_fs, bronze_root).write_teams([_team_record()], season_year=2024)
        t.run(scripts=["002_leagues.sql", "003_divisions.sql", "004_venues.sql"], force=True)

    def test_team_loaded_correctly(self, seeded_db, mem_fs, bronze_root):
        t = _transformer(seeded_db, mem_fs, bronze_root)
        self._seed_refs(t, mem_fs, bronze_root)
        t.run(scripts=["005_teams.sql"], force=True)
        row = _one(seeded_db, "SELECT team_id, team_name, team_abbrev, season_year FROM silver.teams")
        assert row == (119, "Los Angeles Dodgers", "LAD", 2024)

    def test_same_team_two_seasons_two_rows(self, seeded_db, mem_fs, bronze_root):
        w = BronzeWriter(mem_fs, bronze_root)
        w.write_teams([_team_record(season_year=2024)], season_year=2024)
        w.write_teams([_team_record(season_year=2025)], season_year=2025)
        t = _transformer(seeded_db, mem_fs, bronze_root)
        t.run(scripts=["002_leagues.sql", "003_divisions.sql", "004_venues.sql"])
        t.run(scripts=["005_teams.sql"])
        assert _count(seeded_db, "SELECT COUNT(*) FROM silver.teams WHERE team_id = 119") == 2

    def test_unknown_season_excluded(self, seeded_db, mem_fs, bronze_root):
        BronzeWriter(mem_fs, bronze_root).write_teams(
            [_team_record(season_year=2021)], season_year=2021
        )
        t = _transformer(seeded_db, mem_fs, bronze_root)
        t.run(scripts=["002_leagues.sql", "003_divisions.sql", "004_venues.sql", "005_teams.sql"])
        assert _count(seeded_db, "SELECT COUNT(*) FROM silver.teams") == 0


# ── Players ───────────────────────────────────────────────────────────────────

class TestPlayersTransform:
    def test_player_loaded_with_correct_fields(self, db, mem_fs, bronze_root):
        BronzeWriter(mem_fs, bronze_root).write_players([_player_record()], season_year=2024)
        _transformer(db, mem_fs, bronze_root).run(scripts=["006_players.sql"])
        row = _one(db, "SELECT player_id, full_name, bats, throws, birth_country FROM silver.players")
        assert row == (660271, "Shohei Ohtani", "L", "R", "Japan")

    def test_birth_date_cast_to_date(self, db, mem_fs, bronze_root):
        BronzeWriter(mem_fs, bronze_root).write_players([_player_record()], season_year=2024)
        _transformer(db, mem_fs, bronze_root).run(scripts=["006_players.sql"])
        row = _one(db, "SELECT birth_date FROM silver.players WHERE player_id = 660271")
        assert row[0] == date(1994, 7, 5)

    def test_invalid_birth_date_becomes_null(self, db, mem_fs, bronze_root):
        rec = _player_record(birth_date="not-a-date", mlb_debut_date="")
        BronzeWriter(mem_fs, bronze_root).write_players([rec], season_year=2024)
        _transformer(db, mem_fs, bronze_root).run(scripts=["006_players.sql"])
        row = _one(db, "SELECT birth_date, mlb_debut_date FROM silver.players WHERE player_id = 660271")
        assert row == (None, None)

    def test_scd_type1_latest_wins(self, db, mem_fs, bronze_root):
        w = BronzeWriter(mem_fs, bronze_root)
        w.write_players([_player_record(bats="L", extracted_at="2024-01-01T00:00:00Z")], season_year=2024)
        w.write_players([_player_record(bats="S", extracted_at="2024-06-01T00:00:00Z")], season_year=2025)
        _transformer(db, mem_fs, bronze_root).run(scripts=["006_players.sql"])
        row = _one(db, "SELECT bats FROM silver.players WHERE player_id = 660271")
        assert row[0] == "S"

    def test_bats_throws_truncated(self, db, mem_fs, bronze_root):
        rec = _player_record(bats="Left", throws="Right")
        BronzeWriter(mem_fs, bronze_root).write_players([rec], season_year=2024)
        _transformer(db, mem_fs, bronze_root).run(scripts=["006_players.sql"])
        row = _one(db, "SELECT bats, throws FROM silver.players WHERE player_id = 660271")
        assert row == ("L", "R")


# ── Games ─────────────────────────────────────────────────────────────────────

class TestGamesTransform:
    def _seed_venues(self, conn, mem_fs, bronze_root, game_record: dict) -> None:
        BronzeWriter(mem_fs, bronze_root).write_games([game_record], for_date=date(2024, 7, 4))
        _transformer(conn, mem_fs, bronze_root).run(scripts=["004_venues.sql"], force=True)

    def test_game_loaded_with_correct_fields(self, seeded_db, mem_fs, bronze_root):
        rec = _game_record()
        self._seed_venues(seeded_db, mem_fs, bronze_root, rec)
        _transformer(seeded_db, mem_fs, bronze_root).run(scripts=["007_games.sql"], force=True)
        row = _one(
            seeded_db,
            "SELECT game_pk, season_year, game_type, home_score, away_score, innings FROM silver.games",
        )
        assert row == (745525, 2024, "R", 5, 3, 9)

    def test_game_date_cast_to_date(self, seeded_db, mem_fs, bronze_root):
        rec = _game_record()
        self._seed_venues(seeded_db, mem_fs, bronze_root, rec)
        _transformer(seeded_db, mem_fs, bronze_root).run(scripts=["007_games.sql"], force=True)
        row = _one(seeded_db, "SELECT game_date FROM silver.games WHERE game_pk = 745525")
        assert row[0] == date(2024, 7, 4)

    def test_unknown_season_excluded(self, seeded_db, mem_fs, bronze_root):
        rec = _game_record(game_pk=800001, season_year=2021, game_date="2021-07-01")
        self._seed_venues(seeded_db, mem_fs, bronze_root, rec)
        _transformer(seeded_db, mem_fs, bronze_root).run(scripts=["007_games.sql"], force=True)
        assert _count(seeded_db, "SELECT COUNT(*) FROM silver.games WHERE game_pk = 800001") == 0


# ── Linescore ─────────────────────────────────────────────────────────────────

class TestLinescoreTransform:
    def test_inning_rows_created(self, seeded_db, mem_fs, bronze_root):
        BronzeWriter(mem_fs, bronze_root).write_games([_game_record(innings=9)], for_date=date(2024, 7, 4))
        t = _transformer(seeded_db, mem_fs, bronze_root)
        t.run(scripts=["004_venues.sql", "007_games.sql", "008_game_linescore.sql"])
        assert _count(seeded_db, "SELECT COUNT(*) FROM silver.game_linescore WHERE game_pk = 745525") == 9

    def test_runs_values_correct(self, seeded_db, mem_fs, bronze_root):
        BronzeWriter(mem_fs, bronze_root).write_games(
            [_game_record(home_runs=5, away_runs=3, innings=1)], for_date=date(2024, 7, 4)
        )
        t = _transformer(seeded_db, mem_fs, bronze_root)
        t.run(scripts=["004_venues.sql", "007_games.sql", "008_game_linescore.sql"])
        row = _one(
            seeded_db,
            "SELECT home_runs, away_runs FROM silver.game_linescore WHERE game_pk = 745525 AND inning = 1",
        )
        assert row == (5, 3)


# ── Transformer runner ─────────────────────────────────────────────────────────

class TestTransformerRunner:
    def test_run_specific_scripts(self, db, mem_fs, bronze_root):
        t = _transformer(db, mem_fs, bronze_root)
        result = t.run(scripts=["001_seed_seasons.sql"])
        assert result.scripts_run == 1
        assert _count(db, "SELECT COUNT(*) FROM silver.seasons") == 5

    def test_dry_run_does_not_write(self, db, mem_fs, bronze_root):
        t = _transformer(db, mem_fs, bronze_root)
        t.run(scripts=["001_seed_seasons.sql"], dry_run=True)
        assert _count(db, "SELECT COUNT(*) FROM silver.seasons") == 0

    def test_checksum_skips_unchanged_script(self, db, mem_fs, bronze_root):
        t = _transformer(db, mem_fs, bronze_root)
        t.run(scripts=["001_seed_seasons.sql"])
        result = t.run(scripts=["001_seed_seasons.sql"])
        assert result.scripts_run == 0

    def test_force_reruns_script(self, db, mem_fs, bronze_root):
        t = _transformer(db, mem_fs, bronze_root)
        t.run(scripts=["001_seed_seasons.sql"])
        result = t.run(scripts=["001_seed_seasons.sql"], force=True)
        assert result.scripts_run == 1

    def test_tracking_row_recorded(self, db, mem_fs, bronze_root):
        t = _transformer(db, mem_fs, bronze_root)
        t.run(scripts=["001_seed_seasons.sql"])
        row = _one(db, "SELECT script_name FROM meta._silver_transforms WHERE script_name = '001_seed_seasons.sql'")
        assert row is not None
        assert row[0] == "001_seed_seasons.sql"
