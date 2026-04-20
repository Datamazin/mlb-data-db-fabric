"""
Integration tests for Fabric Warehouse schema migrations.

Validates that DDL in sql/schema/ has been applied correctly. Requires a live
Fabric Warehouse connection (see conftest.py for skip conditions).

Run `make migrate` against the target DB before this test suite.
"""

from __future__ import annotations

import pytest


def _cols(conn, schema: str, table: str) -> set[str]:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
        (schema, table),
    )
    return {row[0] for row in cursor.fetchall()}


def _schemas(conn) -> set[str]:
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sys.schemas")
    return {row[0] for row in cursor.fetchall()}


class TestSchemasExist:
    def test_all_schemas_created(self, db):
        assert {"bronze", "silver", "gold", "meta", "staging"}.issubset(_schemas(db))


class TestMetaTables:
    def test_pipeline_runs_columns(self, db):
        cols = _cols(db, "meta", "pipeline_runs")
        assert {"run_id", "job_name", "status", "started_at", "records_extracted"}.issubset(cols)

    def test_entity_checksums_columns(self, db):
        cols = _cols(db, "meta", "entity_checksums")
        assert {"entity_type", "entity_key", "response_hash", "source_url"}.issubset(cols)

    def test_pipeline_runs_insert_and_query(self, db):
        cursor = db.cursor()
        cursor.execute(
            """
            INSERT INTO meta.pipeline_runs (run_id, job_name, status, started_at, pipeline_version)
            VALUES ('test-run-1', 'test_job', 'success', SYSDATETIMEOFFSET(), '0.2.0')
            """
        )
        db.commit()
        cursor.execute("SELECT COUNT(*) FROM meta.pipeline_runs WHERE run_id = 'test-run-1'")
        assert cursor.fetchone()[0] == 1

    def test_entity_checksums_upsert(self, db):
        cursor = db.cursor()
        for hash_val in ("hash_v1", "hash_v2"):
            cursor.execute(
                """
                MERGE meta.entity_checksums AS tgt
                USING (VALUES (?, ?, ?, ?, SYSDATETIMEOFFSET(), '0.2.0'))
                    AS src(entity_type, entity_key, response_hash, source_url, extracted_at, transform_version)
                ON tgt.entity_type = src.entity_type AND tgt.entity_key = src.entity_key
                WHEN MATCHED THEN UPDATE SET tgt.response_hash = src.response_hash
                WHEN NOT MATCHED THEN INSERT
                    (entity_type, entity_key, response_hash, source_url, extracted_at, transform_version)
                VALUES (src.entity_type, src.entity_key, src.response_hash, src.source_url,
                        src.extracted_at, src.transform_version);
                """,
                ("game_feed", "745525", hash_val, "/v1/game/745525/feed/live"),
            )
        db.commit()
        cursor.execute(
            "SELECT response_hash FROM meta.entity_checksums WHERE entity_key = '745525'"
        )
        assert cursor.fetchone()[0] == "hash_v2"


class TestSilverTables:
    def test_seasons_table_exists(self, db):
        cols = _cols(db, "silver", "seasons")
        assert {"season_year", "regular_season_start", "games_per_team", "loaded_at"}.issubset(cols)

    def test_teams_table_exists(self, db):
        cols = _cols(db, "silver", "teams")
        assert {"team_id", "season_year", "team_name", "team_abbrev", "league_id"}.issubset(cols)

    def test_players_table_exists(self, db):
        cols = _cols(db, "silver", "players")
        assert {"player_id", "full_name", "bats", "throws", "primary_position"}.issubset(cols)

    def test_games_table_exists(self, db):
        cols = _cols(db, "silver", "games")
        assert {"game_pk", "season_year", "game_type", "home_team_id", "away_team_id"}.issubset(cols)

    def test_game_batting_table_exists(self, db):
        cols = _cols(db, "silver", "game_batting")
        assert {"game_pk", "player_id", "team_id", "at_bats", "home_runs", "rbi"}.issubset(cols)

    def test_game_pitching_table_exists(self, db):
        cols = _cols(db, "silver", "game_pitching")
        assert {"game_pk", "player_id", "team_id", "strikeouts", "earned_runs", "outs"}.issubset(cols)

    def test_insert_season(self, db):
        cursor = db.cursor()
        cursor.execute(
            """
            INSERT INTO silver.seasons
                (season_year, sport_id, regular_season_start, regular_season_end, loaded_at)
            VALUES (2024, 1, '2024-03-20', '2024-09-29', SYSDATETIMEOFFSET())
            """
        )
        db.commit()
        cursor.execute("SELECT season_year FROM silver.seasons WHERE season_year = 2024")
        assert cursor.fetchone()[0] == 2024


class TestGoldTables:
    def test_standings_snap_table_exists(self, db):
        cols = _cols(db, "gold", "standings_snap")
        assert {"snap_date", "team_id", "wins", "losses", "win_pct", "games_back"}.issubset(cols)

    def test_league_averages_table_exists(self, db):
        cols = _cols(db, "gold", "league_averages")
        assert {"season_year", "league_id", "league_obp", "league_slg", "league_era"}.issubset(cols)
