"""
Shared pytest fixtures for unit and integration tests.

Integration tests require a live Fabric Warehouse connection:
  - Set FABRIC_CONNECTION_STRING (or FABRIC_SERVER + FABRIC_DATABASE) in your environment.
  - Run `make migrate` against the target database before running the test suite.

Unit tests (test_models, test_writer) run fully offline with no DB required.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from fsspec.implementations.memory import MemoryFileSystem

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent))

SILVER_SQL_DIR = Path(__file__).parent.parent / "sql" / "silver"

# Tables to wipe between tests (delete in FK-safe order)
_CLEANUP_TABLES = [
    "silver.game_batting",
    "silver.game_pitching",
    "silver.game_linescore",
    "silver.game_boxscore",
    "silver.fact_batting",
    "silver.fact_pitching",
    "silver.games",
    "silver.teams",
    "silver.players",
    "silver.venues",
    "silver.divisions",
    "silver.leagues",
    "silver.seasons",
    "gold.standings_snap",
    "gold.league_averages",
    "gold.head_to_head",
    "meta.pipeline_runs",
    "meta.entity_checksums",
]

_TRACKING_TABLES = [
    "meta._silver_transforms",
    "meta._gold_aggregations",
]


def _has_db_config() -> bool:
    return bool(os.getenv("FABRIC_CONNECTION_STRING") or os.getenv("FABRIC_SERVER"))


def _cleanup(conn) -> None:
    cursor = conn.cursor()
    for table in _CLEANUP_TABLES:
        try:
            cursor.execute(f"DELETE FROM {table}")
        except Exception:
            pass
    for table in _TRACKING_TABLES:
        try:
            cursor.execute(f"DELETE FROM {table}")
        except Exception:
            pass
    try:
        conn.commit()
    except Exception:
        pass


@pytest.fixture(scope="session")
def warehouse_conn():
    """Session-scoped Fabric Warehouse connection. Skips if not configured."""
    if not _has_db_config():
        pytest.skip(
            "Fabric DB not configured. "
            "Set FABRIC_CONNECTION_STRING (or FABRIC_SERVER + FABRIC_DATABASE) to run integration tests."
        )
    from connections import get_warehouse_conn
    conn = get_warehouse_conn()
    yield conn
    conn.close()


@pytest.fixture
def db(warehouse_conn):
    """Per-test Fabric connection. Cleans up all test rows after each test."""
    yield warehouse_conn
    _cleanup(warehouse_conn)


@pytest.fixture
def seeded_db(db, mem_fs, bronze_root):
    """Fabric connection with silver.seasons pre-populated via 001_seed_seasons.sql."""
    from transformer.transform import Transformer
    t = Transformer(conn=db, fs=mem_fs, bronze_root=bronze_root)
    t.run(scripts=["001_seed_seasons.sql"], force=True)
    yield db


@pytest.fixture
def mem_fs():
    """In-memory fsspec filesystem — drop-in for adlfs in tests."""
    return MemoryFileSystem()


@pytest.fixture
def bronze_root():
    """Fake OneLake bronze root path for memory-FS tests."""
    return "test-workspace/mlb_bronze.Lakehouse/Files/bronze"
