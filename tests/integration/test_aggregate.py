"""
Integration tests for the gold aggregation layer (Fabric Warehouse version).

All tests use a live Fabric Warehouse connection — no mocks. Silver data is
inserted directly so tests focus on gold SQL logic. See conftest.py for
skip conditions when no DB is configured.

Requires: `make migrate` against the target DB before this suite.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from aggregator.aggregate import Aggregator, AggregateResult

GOLD_SQL_DIR = Path(__file__).parent.parent.parent / "sql" / "gold"


def _agg(conn) -> Aggregator:
    return Aggregator(conn)


def _one(conn, sql: str, params=()) -> tuple | None:
    c = conn.cursor()
    c.execute(sql, params)
    return c.fetchone()


def _count(conn, sql: str, params=()) -> int:
    row = _one(conn, sql, params)
    return row[0] if row else 0


def _exec(conn, sql: str, params=()) -> None:
    c = conn.cursor()
    c.execute(sql, params)
    conn.commit()


# ── Silver table helpers ───────────────────────────────────────────────────────

def _ins_league(conn, league_id=104, name="National League", abbrev="NL"):
    _exec(conn,
        "INSERT INTO silver.leagues (league_id, league_name, short_name, abbreviation, loaded_at) "
        "VALUES (?,?,?,?,SYSDATETIMEOFFSET())",
        (league_id, name, name, abbrev),
    )


def _ins_division(conn, division_id=203, name="NL West", league_id=104):
    _exec(conn,
        "INSERT INTO silver.divisions (division_id, division_name, short_name, league_id, loaded_at) "
        "VALUES (?,?,?,?,SYSDATETIMEOFFSET())",
        (division_id, name, name, league_id),
    )


def _ins_venue(conn, venue_id=22, venue_name="Dodger Stadium"):
    _exec(conn,
        "INSERT INTO silver.venues (venue_id, venue_name, loaded_at) "
        "VALUES (?,?,SYSDATETIMEOFFSET())",
        (venue_id, venue_name),
    )


def _ins_team(
    conn,
    team_id=119,
    season_year=2024,
    team_name="Los Angeles Dodgers",
    team_abbrev="LAD",
    league_id=104,
    division_id=203,
    venue_id=22,
):
    _exec(conn,
        "INSERT INTO silver.teams "
        "(team_id, season_year, team_name, team_abbrev, team_code, "
        "league_id, division_id, venue_id, city, first_year, active, loaded_at) "
        "VALUES (?,?,?,?,'xxx',?,?,?,'Los Angeles',1884,1,SYSDATETIMEOFFSET())",
        (team_id, season_year, team_name, team_abbrev, league_id, division_id, venue_id),
    )


def _ins_player(conn, player_id=660271, full_name="Shohei Ohtani",
                primary_position="DH", active=1):
    _exec(conn,
        "INSERT INTO silver.players "
        "(player_id, full_name, first_name, last_name, bats, throws, "
        "primary_position, active, loaded_at) "
        "VALUES (?,?,'Shohei','Ohtani','L','R',?,?,SYSDATETIMEOFFSET())",
        (player_id, full_name, primary_position, active),
    )


def _ins_game(
    conn,
    game_pk=745525,
    season_year=2024,
    game_date="2024-07-04",
    game_type="R",
    status="Final",
    home_team_id=119,
    away_team_id=147,
    home_score=5,
    away_score=3,
    venue_id=22,
):
    _exec(conn,
        "INSERT INTO silver.games "
        "(game_pk, season_year, game_date, game_type, status, "
        "home_team_id, away_team_id, home_score, away_score, innings, venue_id, loaded_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,9,?,SYSDATETIMEOFFSET())",
        (game_pk, season_year, game_date, game_type, status,
         home_team_id, away_team_id, home_score, away_score, venue_id),
    )


def _setup_refs(conn, season_year=2024):
    _ins_league(conn)
    _ins_division(conn)
    _ins_venue(conn)
    _ins_team(conn, season_year=season_year)


# ── dim_player ────────────────────────────────────────────────────────────────

class TestDimPlayer:
    def test_view_created_and_player_appears(self, db):
        _ins_player(db)
        _agg(db).run(scripts=["001_dim_player.sql"])
        row = _one(db, "SELECT player_id, full_name FROM gold.dim_player WHERE player_id = 660271")
        assert row == (660271, "Shohei Ohtani")

    def test_empty_silver_returns_empty_view(self, db):
        _agg(db).run(scripts=["001_dim_player.sql"])
        assert _count(db, "SELECT COUNT(*) FROM gold.dim_player") == 0

    def test_view_is_idempotent(self, db):
        _ins_player(db)
        a = _agg(db)
        a.run(scripts=["001_dim_player.sql"], force=True)
        a.run(scripts=["001_dim_player.sql"], force=True)
        assert _count(db, "SELECT COUNT(*) FROM gold.dim_player") == 1


# ── dim_team ──────────────────────────────────────────────────────────────────

class TestDimTeam:
    def test_enriched_with_league_and_division(self, seeded_db):
        _setup_refs(seeded_db)
        _agg(seeded_db).run(scripts=["002_dim_team.sql"])
        row = _one(seeded_db,
            "SELECT team_name, league_name, division_name, venue_name "
            "FROM gold.dim_team WHERE team_id = 119 AND season_year = 2024")
        assert row == ("Los Angeles Dodgers", "National League", "NL West", "Dodger Stadium")

    def test_multiple_seasons_appear(self, seeded_db):
        _setup_refs(seeded_db, season_year=2024)
        _ins_team(seeded_db, team_id=119, season_year=2023)
        _agg(seeded_db).run(scripts=["002_dim_team.sql"])
        assert _count(seeded_db, "SELECT COUNT(*) FROM gold.dim_team WHERE team_id = 119") == 2


# ── dim_venue ─────────────────────────────────────────────────────────────────

class TestDimVenue:
    def test_venue_passes_through(self, db):
        _ins_venue(db, venue_id=22, venue_name="Dodger Stadium")
        _agg(db).run(scripts=["003_dim_venue.sql"])
        row = _one(db, "SELECT venue_id, venue_name FROM gold.dim_venue WHERE venue_id = 22")
        assert row == (22, "Dodger Stadium")

    def test_nullable_columns_null(self, db):
        _ins_venue(db)
        _agg(db).run(scripts=["003_dim_venue.sql"])
        row = _one(db, "SELECT city, state, surface, capacity FROM gold.dim_venue WHERE venue_id = 22")
        assert row == (None, None, None, None)


# ── fact_game ─────────────────────────────────────────────────────────────────

class TestFactGame:
    def test_enriched_with_team_names(self, seeded_db):
        _setup_refs(seeded_db)
        _ins_league(seeded_db, league_id=103, name="American League", abbrev="AL")
        _ins_division(seeded_db, division_id=201, name="AL East", league_id=103)
        _ins_venue(seeded_db, venue_id=3, venue_name="Fenway Park")
        _ins_team(seeded_db, team_id=147, team_name="New York Yankees", team_abbrev="NYY",
                  league_id=103, division_id=201, venue_id=3, season_year=2024)
        _ins_game(seeded_db, home_team_id=119, away_team_id=147, venue_id=22)

        _agg(seeded_db).run(scripts=["004_fact_game.sql"])
        row = _one(seeded_db,
            "SELECT home_team_name, away_team_name, venue_name, home_score, away_score "
            "FROM gold.fact_game WHERE game_pk = 745525")
        assert row == ("Los Angeles Dodgers", "New York Yankees", "Dodger Stadium", 5, 3)

    def test_empty_games_returns_empty(self, db):
        _agg(db).run(scripts=["004_fact_game.sql"])
        assert _count(db, "SELECT COUNT(*) FROM gold.fact_game") == 0


# ── head_to_head ──────────────────────────────────────────────────────────────

class TestHeadToHead:
    def _setup(self, conn):
        _setup_refs(conn)
        _ins_league(conn, league_id=103, name="American League", abbrev="AL")
        _ins_division(conn, division_id=201, name="AL East", league_id=103)
        _ins_venue(conn, venue_id=3, venue_name="Fenway Park")
        _ins_team(conn, team_id=147, team_name="New York Yankees", team_abbrev="NYY",
                  league_id=103, division_id=201, venue_id=3, season_year=2024)

    def test_win_count(self, seeded_db):
        self._setup(seeded_db)
        _ins_game(seeded_db, game_pk=1, home_team_id=119, away_team_id=147, home_score=5, away_score=3)
        _ins_game(seeded_db, game_pk=2, home_team_id=147, away_team_id=119, home_score=4, away_score=2)
        _ins_game(seeded_db, game_pk=3, home_team_id=119, away_team_id=147, home_score=6, away_score=1)

        _agg(seeded_db).run(scripts=["005_head_to_head.sql"])
        row = _one(seeded_db,
            "SELECT wins, losses, games_played FROM gold.head_to_head "
            "WHERE team_id = 119 AND opponent_id = 147 AND season_year = 2024")
        assert row == (2, 1, 3)

    def test_non_final_games_excluded(self, seeded_db):
        self._setup(seeded_db)
        _ins_game(seeded_db, game_pk=1, status="Postponed",
                  home_team_id=119, away_team_id=147)
        _agg(seeded_db).run(scripts=["005_head_to_head.sql"])
        assert _count(seeded_db, "SELECT COUNT(*) FROM gold.head_to_head") == 0

    def test_spring_training_excluded(self, seeded_db):
        self._setup(seeded_db)
        _ins_game(seeded_db, game_pk=1, game_type="S",
                  home_team_id=119, away_team_id=147)
        _agg(seeded_db).run(scripts=["005_head_to_head.sql"])
        assert _count(seeded_db, "SELECT COUNT(*) FROM gold.head_to_head") == 0


# ── leaderboards (stub) ───────────────────────────────────────────────────────

class TestLeaderboards:
    def test_view_created(self, db):
        _agg(db).run(scripts=["006_leaderboards.sql"])
        assert _count(db, "SELECT COUNT(*) FROM gold.leaderboards") == 0


# ── player_season_summary (stub) ─────────────────────────────────────────────

class TestPlayerSeasonSummary:
    def test_view_created(self, db):
        _agg(db).run(scripts=["007_player_season_summary.sql"])
        assert _count(db, "SELECT COUNT(*) FROM gold.player_season_summary") == 0


# ── standings_snap ────────────────────────────────────────────────────────────

class TestStandingsSnap:
    def _setup(self, conn):
        _ins_league(conn)
        _ins_division(conn)
        _ins_venue(conn)
        _ins_team(conn, team_id=119, season_year=2024, team_abbrev="LAD")
        _ins_team(conn, team_id=118, season_year=2024, team_abbrev="SF",
                  team_name="San Francisco Giants", division_id=203, venue_id=22)

    def test_wins_and_losses_counted(self, seeded_db):
        self._setup(seeded_db)
        _ins_game(seeded_db, game_pk=1, home_team_id=119, away_team_id=118, home_score=5, away_score=3)
        _ins_game(seeded_db, game_pk=2, home_team_id=118, away_team_id=119, home_score=4, away_score=2)
        _ins_game(seeded_db, game_pk=3, home_team_id=119, away_team_id=118, home_score=3, away_score=1)
        _ins_game(seeded_db, game_pk=4, home_team_id=119, away_team_id=118, home_score=7, away_score=2)

        _agg(seeded_db).run(scripts=["008_standings_snap.sql"])
        row = _one(seeded_db, "SELECT wins, losses FROM gold.standings_snap WHERE team_id = 119")
        assert row == (3, 1)

    def test_leader_has_zero_games_back(self, seeded_db):
        self._setup(seeded_db)
        _ins_game(seeded_db, game_pk=1, home_team_id=119, away_team_id=118, home_score=5, away_score=3)

        _agg(seeded_db).run(scripts=["008_standings_snap.sql"])
        row = _one(seeded_db, "SELECT games_back FROM gold.standings_snap WHERE team_id = 119")
        assert float(row[0]) == 0.0

    def test_games_back_calculated(self, seeded_db):
        self._setup(seeded_db)
        _ins_game(seeded_db, game_pk=1, home_team_id=119, away_team_id=118, home_score=5, away_score=3)
        _ins_game(seeded_db, game_pk=2, home_team_id=119, away_team_id=118, home_score=4, away_score=2)
        _ins_game(seeded_db, game_pk=3, home_team_id=119, away_team_id=118, home_score=3, away_score=1)
        _ins_game(seeded_db, game_pk=4, home_team_id=118, away_team_id=119, home_score=6, away_score=1)

        _agg(seeded_db).run(scripts=["008_standings_snap.sql"])
        row = _one(seeded_db, "SELECT games_back FROM gold.standings_snap WHERE team_id = 118")
        assert float(row[0]) == 2.0

    def test_streak_computed(self, seeded_db):
        self._setup(seeded_db)
        _ins_game(seeded_db, game_pk=1, game_date="2024-07-03",
                  home_team_id=119, away_team_id=118, home_score=5, away_score=3)
        _ins_game(seeded_db, game_pk=2, game_date="2024-07-04",
                  home_team_id=119, away_team_id=118, home_score=4, away_score=2)

        _agg(seeded_db).run(scripts=["008_standings_snap.sql"])
        row = _one(seeded_db, "SELECT streak FROM gold.standings_snap WHERE team_id = 119")
        assert row[0] == "W2"

    def test_home_away_split(self, seeded_db):
        self._setup(seeded_db)
        _ins_game(seeded_db, game_pk=1, home_team_id=119, away_team_id=118, home_score=5, away_score=3)
        _ins_game(seeded_db, game_pk=2, home_team_id=119, away_team_id=118, home_score=4, away_score=2)
        _ins_game(seeded_db, game_pk=3, home_team_id=118, away_team_id=119, home_score=2, away_score=3)

        _agg(seeded_db).run(scripts=["008_standings_snap.sql"])
        row = _one(seeded_db,
            "SELECT home_wins, home_losses, away_wins, away_losses "
            "FROM gold.standings_snap WHERE team_id = 119")
        assert row == (2, 0, 1, 0)

    def test_empty_games_no_rows_inserted(self, seeded_db):
        self._setup(seeded_db)
        _agg(seeded_db).run(scripts=["008_standings_snap.sql"])
        assert _count(seeded_db, "SELECT COUNT(*) FROM gold.standings_snap") == 0

    def test_rerun_is_idempotent(self, seeded_db):
        self._setup(seeded_db)
        _ins_game(seeded_db, game_pk=1, home_team_id=119, away_team_id=118, home_score=5, away_score=3)

        a = _agg(seeded_db)
        a.run(scripts=["008_standings_snap.sql"], force=True)
        a.run(scripts=["008_standings_snap.sql"], force=True)
        assert _count(seeded_db, "SELECT COUNT(*) FROM gold.standings_snap") == 2


# ── league_averages (stub) ────────────────────────────────────────────────────

class TestLeagueAverages:
    def test_stub_runs_without_error(self, db):
        result = _agg(db).run(scripts=["009_league_averages.sql"])
        assert result.success
        assert result.scripts_run == 1

    def test_no_rows_inserted(self, db):
        _agg(db).run(scripts=["009_league_averages.sql"])
        assert _count(db, "SELECT COUNT(*) FROM gold.league_averages") == 0


# ── Aggregator runner ─────────────────────────────────────────────────────────

class TestAggregatorRunner:
    def test_checksum_skip(self, db):
        a = _agg(db)
        r1 = a.run(scripts=["001_dim_player.sql"])
        r2 = a.run(scripts=["001_dim_player.sql"])
        assert r1.scripts_run == 1
        assert r2.scripts_run == 0

    def test_force_reruns(self, db):
        a = _agg(db)
        a.run(scripts=["001_dim_player.sql"])
        r2 = a.run(scripts=["001_dim_player.sql"], force=True)
        assert r2.scripts_run == 1

    def test_missing_script_is_error(self, db):
        result = _agg(db).run(scripts=["999_nonexistent.sql"])
        assert not result.success
        assert "Script not found" in result.errors[0]

    def test_tracking_row_recorded(self, db):
        _agg(db).run(scripts=["001_dim_player.sql"])
        row = _one(db,
            "SELECT script_name FROM meta._gold_aggregations "
            "WHERE script_name = '001_dim_player.sql'")
        assert row is not None
        assert row[0] == "001_dim_player.sql"
