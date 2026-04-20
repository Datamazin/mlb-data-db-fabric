"""
Migration runner — Fabric Warehouse (pyodbc / T-SQL) version.

Applies SQL migration files from sql/schema/ in filename order, tracking
applied migrations in meta._schema_migrations so each script runs exactly once.

T-SQL migration files use GO as a batch separator. This runner splits each file
on GO before executing so pyodbc receives one batch per cursor.execute() call.

Usage:
    python migrations/migrate.py
    python migrations/migrate.py --dry-run
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyodbc
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from connections import get_warehouse_conn

MIGRATIONS_DIR = Path(__file__).parent.parent / "sql" / "schema"
PROJECT_ROOT = Path(__file__).parent.parent

# Executed once to bootstrap the tracking table; split into individual
# batches here so no GO parsing is needed for the bootstrap itself.
_TRACKING_STMTS = [
    "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'meta') EXEC('CREATE SCHEMA meta')",
    """
    IF NOT EXISTS (
        SELECT 1 FROM sys.tables  t
        JOIN   sys.schemas s ON t.schema_id = s.schema_id
        WHERE  s.name = 'meta' AND t.name = '_schema_migrations'
    )
    CREATE TABLE meta._schema_migrations (
        migration_id    NVARCHAR(100)   NOT NULL,
        checksum        NVARCHAR(64)    NOT NULL,
        applied_at      DATETIMEOFFSET  NOT NULL,
        applied_by      NVARCHAR(200)   NOT NULL,
        CONSTRAINT pk_schema_migrations PRIMARY KEY (migration_id)
    )
    """,
]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _split_batches(sql: str) -> list[str]:
    """Split a T-SQL script on GO batch separators (case-insensitive, own line)."""
    batches = re.split(r"^\s*GO\s*$", sql, flags=re.IGNORECASE | re.MULTILINE)
    return [b.strip() for b in batches if b.strip()]


def _applied_migrations(conn: pyodbc.Connection) -> set[str]:
    cursor = conn.cursor()
    cursor.execute("SELECT migration_id FROM meta._schema_migrations ORDER BY migration_id")
    return {row[0] for row in cursor.fetchall()}


def _pending_migrations(applied: set[str]) -> list[Path]:
    files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    return [f for f in files if f.name not in applied]


def run(conn: pyodbc.Connection, dry_run: bool = False) -> None:
    # Bootstrap tracking table — each statement runs in its own batch
    if not dry_run:
        cursor = conn.cursor()
        conn.autocommit = True
        for stmt in _TRACKING_STMTS:
            cursor.execute(stmt)
        conn.autocommit = False

    applied = _applied_migrations(conn) if not dry_run else set()
    pending = _pending_migrations(applied)

    if not pending:
        print("No pending migrations.")
        return

    for migration_file in pending:
        sql = migration_file.read_text(encoding="utf-8")
        checksum = _sha256(migration_file)
        batches = _split_batches(sql)

        print(f"  {'[DRY RUN] ' if dry_run else ''}Applying {migration_file.name} "
              f"({len(batches)} batch{'es' if len(batches) != 1 else ''}) ...")

        if dry_run:
            for i, batch in enumerate(batches, 1):
                print(f"    -- batch {i} --")
                print(batch[:300])
            continue

        cursor = conn.cursor()
        try:
            # Fabric Warehouse requires autocommit=True for DDL (CREATE/DROP TABLE/VIEW).
            conn.autocommit = True
            for batch in batches:
                cursor.execute(batch)

            # Record the migration in tracking table (still under autocommit).
            cursor.execute(
                """
                INSERT INTO meta._schema_migrations (migration_id, checksum, applied_at, applied_by)
                VALUES (?, ?, ?, ?)
                """,
                (migration_file.name, checksum, datetime.now(timezone.utc), sys.argv[0]),
            )
            print(f"    OK — {migration_file.name}")

        except Exception as exc:
            print(f"    FAILED — {migration_file.name}: {exc}", file=sys.stderr)
            raise

        finally:
            conn.autocommit = False


def main() -> None:
    # Local dev convenience: load settings from project .env when present.
    # override=True ensures .env values take precedence over any stale env vars
    # that may have been set in the shell from a previous run of this file.
    load_dotenv(PROJECT_ROOT / ".env", override=True)

    parser = argparse.ArgumentParser(description="Apply Fabric Warehouse schema migrations.")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL without executing")
    args = parser.parse_args()

    conn = get_warehouse_conn()
    try:
        run(conn, dry_run=args.dry_run)
    finally:
        conn.close()
    print("Migrations complete.")


if __name__ == "__main__":
    main()
