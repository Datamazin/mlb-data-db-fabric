"""
Gold aggregation runner — Fabric Warehouse (pyodbc / T-SQL) version.

Executes sql/gold/*.sql scripts in alphabetical order, building gold views
and materialized tables from the clean silver tables.

Usage:
    uv run python -m src.aggregator.aggregate
    uv run python -m src.aggregator.aggregate --scripts 008_standings_snap.sql
    uv run python -m src.aggregator.aggregate --force
    uv run python -m src.aggregator.aggregate --dry-run
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pyodbc
import structlog
from dotenv import load_dotenv
from src.logging_config import configure_logging

_PROJECT_ROOT = Path(__file__).parent.parent.parent
load_dotenv(_PROJECT_ROOT / ".env", override=True)

sys.path.insert(0, str(_PROJECT_ROOT))
from connections import get_warehouse_conn
from run_tracker.tracker import RunTracker

log = structlog.get_logger(__name__)

SQL_DIR = Path(__file__).parent.parent.parent / "sql" / "gold"

_TRACKING_DDL = """
IF NOT EXISTS (
    SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'meta' AND t.name = '_gold_aggregations'
)
CREATE TABLE meta._gold_aggregations (
    script_name   NVARCHAR(100)  NOT NULL,
    checksum      NVARCHAR(64)   NOT NULL,
    last_run_at   DATETIMEOFFSET NOT NULL,
    rows_affected INT            NOT NULL DEFAULT 0,
    CONSTRAINT pk_gold_aggregations PRIMARY KEY (script_name)
)
"""


@dataclass
class AggregateResult:
    scripts_run: int = 0
    total_rows_affected: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _split_statements(sql: str) -> list[str]:
    sql_no_comments = re.sub(r"--[^\n]*", "", sql)
    # Split on GO batch separator (must be on its own line) first, then on ;
    batches = re.split(r"^\s*GO\s*$", sql_no_comments, flags=re.IGNORECASE | re.MULTILINE)
    statements: list[str] = []
    for batch in batches:
        for stmt in batch.split(";"):
            stmt = stmt.strip()
            if stmt:
                statements.append(stmt)
    return statements


class Aggregator:
    def __init__(self, conn: pyodbc.Connection) -> None:
        self._conn = conn
        self._bootstrap()

    def _bootstrap(self) -> None:
        cursor = self._conn.cursor()
        cursor.execute(_TRACKING_DDL)
        self._conn.commit()

    def _is_up_to_date(self, script_name: str, checksum: str) -> bool:
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT checksum FROM meta._gold_aggregations WHERE script_name = ?",
            (script_name,),
        )
        row = cursor.fetchone()
        return row is not None and row[0] == checksum

    def _record(self, script_name: str, checksum: str, rows: int) -> None:
        cursor = self._conn.cursor()
        cursor.execute(
            """
            MERGE meta._gold_aggregations AS tgt
            USING (VALUES (?, ?, ?, ?)) AS src(script_name, checksum, last_run_at, rows_affected)
            ON tgt.script_name = src.script_name
            WHEN MATCHED THEN UPDATE SET
                tgt.checksum     = src.checksum,
                tgt.last_run_at  = src.last_run_at,
                tgt.rows_affected= src.rows_affected
            WHEN NOT MATCHED BY TARGET THEN INSERT
                (script_name, checksum, last_run_at, rows_affected)
            VALUES (src.script_name, src.checksum, src.last_run_at, src.rows_affected);
            """,
            (script_name, checksum, datetime.now(timezone.utc), rows),
        )
        self._conn.commit()

    def run_script(self, script_path: Path, dry_run: bool = False) -> int:
        sql = script_path.read_text(encoding="utf-8")
        statements = _split_statements(sql)

        if dry_run:
            log.info("aggregate_dry_run", script=script_path.name, sql_preview=sql[:120])
            return 0

        # Fabric Warehouse requires autocommit=True for DDL (CREATE/DROP VIEW).
        # Switch to autocommit for the duration of this script.
        self._conn.autocommit = True
        total_rows = 0
        cursor = self._conn.cursor()
        try:
            for stmt in statements:
                cursor.execute(stmt + ";")
                rows = cursor.rowcount if (cursor.rowcount is not None and cursor.rowcount >= 0) else 0
                total_rows += rows
        except Exception:
            raise
        finally:
            self._conn.autocommit = False
        return total_rows

    def run(
        self,
        scripts: list[str] | None = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> AggregateResult:
        if scripts:
            paths = sorted(SQL_DIR / s for s in scripts)
        else:
            paths = sorted(SQL_DIR.glob("*.sql"))

        if not paths:
            log.warning("aggregate_no_scripts_found", sql_dir=str(SQL_DIR))
            return AggregateResult()

        result = AggregateResult()

        for path in paths:
            if not path.exists():
                msg = f"Script not found: {path}"
                log.error("aggregate_script_missing", path=str(path))
                result.errors.append(msg)
                continue

            checksum = _sha256(path.read_text(encoding="utf-8"))

            if not force and not dry_run and self._is_up_to_date(path.name, checksum):
                log.info("aggregate_skip_unchanged", script=path.name)
                continue

            log.info("aggregate_run_script", script=path.name, force=force)
            try:
                rows = self.run_script(path, dry_run=dry_run)
                if not dry_run:
                    self._record(path.name, checksum, rows)
                result.scripts_run += 1
                result.total_rows_affected += rows
                log.info("aggregate_script_done", script=path.name, rows=rows)
            except Exception as exc:
                msg = f"{path.name}: {exc}"
                log.error("aggregate_script_error", script=path.name, error=str(exc))
                result.errors.append(msg)

        return result


# ── CLI entry point ────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run gold aggregation scripts (silver → gold).")
    parser.add_argument("--scripts", nargs="+", default=None, metavar="SCRIPT")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = _parse_args()

    conn = get_warehouse_conn()
    tracker = RunTracker(conn)
    run_id = tracker.start_run("gold_aggregate")

    try:
        aggregator = Aggregator(conn)
        result = aggregator.run(scripts=args.scripts, force=args.force, dry_run=args.dry_run)

        if result.success:
            tracker.complete_run(run_id, records_loaded=result.total_rows_affected)
            log.info(
                "aggregate_complete",
                scripts_run=result.scripts_run,
                rows_affected=result.total_rows_affected,
            )
        else:
            tracker.fail_run(run_id, "; ".join(result.errors))
            log.error("aggregate_finished_with_errors", errors=result.errors)
            sys.exit(1)

    except Exception as exc:
        tracker.fail_run(run_id, str(exc))
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
