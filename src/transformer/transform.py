"""
Silver transformation runner — Fabric Warehouse (pyodbc / T-SQL) version.

For each sql/silver/*.sql script the runner:
  1. Calls the matching loader from staging.py to read bronze Parquet from
     OneLake and populate one or more staging.{table} tables.
  2. Executes the SQL script (a MERGE from staging into silver).
  3. Drops the staging tables in a finally block.

Features:
  - Checksum-based skip: unchanged scripts are not re-executed.
  - --force flag to re-run all scripts regardless of checksum.
  - --dry-run to print SQL without executing.
  - RunTracker integration for pipeline observability.

Usage:
    uv run python -m src.transformer.transform
    uv run python -m src.transformer.transform --scripts 007_games.sql
    uv run python -m src.transformer.transform --force
    uv run python -m src.transformer.transform --dry-run
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import adlfs
import pyodbc
import structlog
from dotenv import load_dotenv
from src.logging_config import configure_logging

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from connections import get_warehouse_conn, get_onelake_fs, get_bronze_root
from run_tracker.tracker import RunTracker
from transformer.staging import STAGING_REGISTRY

log = structlog.get_logger(__name__)

SQL_DIR = Path(__file__).parent.parent.parent / "sql" / "silver"
PROJECT_ROOT = Path(__file__).parent.parent.parent

_TRACKING_DDL = """
IF NOT EXISTS (
    SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'meta' AND t.name = '_silver_transforms'
)
CREATE TABLE meta._silver_transforms (
    script_name   NVARCHAR(100)  NOT NULL,
    checksum      NVARCHAR(64)   NOT NULL,
    last_run_at   DATETIMEOFFSET NOT NULL,
    rows_loaded   INT            NOT NULL DEFAULT 0,
    CONSTRAINT pk_silver_transforms PRIMARY KEY (script_name)
)
"""


@dataclass
class TransformResult:
    scripts_run: int = 0
    total_rows_loaded: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


class Transformer:
    def __init__(
        self,
        conn: pyodbc.Connection,
        fs: adlfs.AzureBlobFileSystem,
        bronze_root: str,
        year_glob: str = "*",
        month_glob: str = "*",
    ) -> None:
        self._conn = conn
        self._fs = fs
        self._bronze_root = bronze_root
        self._year_glob = year_glob
        self._month_glob = month_glob
        self._bootstrap()

    def _bootstrap(self) -> None:
        cursor = self._conn.cursor()
        cursor.execute(_TRACKING_DDL)
        self._conn.commit()

    def _is_up_to_date(self, script_name: str, checksum: str) -> bool:
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT checksum FROM meta._silver_transforms WHERE script_name = ?",
            (script_name,),
        )
        row = cursor.fetchone()
        return row is not None and row[0] == checksum

    def _record(self, script_name: str, checksum: str, rows: int) -> None:
        cursor = self._conn.cursor()
        cursor.execute(
            """
            MERGE meta._silver_transforms AS tgt
            USING (VALUES (?, ?, ?, ?)) AS src(script_name, checksum, last_run_at, rows_loaded)
            ON tgt.script_name = src.script_name
            WHEN MATCHED THEN UPDATE SET
                tgt.checksum    = src.checksum,
                tgt.last_run_at = src.last_run_at,
                tgt.rows_loaded = src.rows_loaded
            WHEN NOT MATCHED BY TARGET THEN INSERT
                (script_name, checksum, last_run_at, rows_loaded)
            VALUES (src.script_name, src.checksum, src.last_run_at, src.rows_loaded);
            """,
            (script_name, checksum, datetime.now(timezone.utc), rows),
        )
        self._conn.commit()

    def run_script(self, script_path: Path, dry_run: bool = False) -> int:
        """
        Load staging tables for this script, execute the MERGE, drop staging.
        Returns rows affected. Raises on error so callers can decide handling.
        """
        sql = script_path.read_text(encoding="utf-8")

        if dry_run:
            log.info("transform_dry_run", script=script_path.name, sql_preview=sql[:120])
            return 0

        # 1 — Populate staging tables for this script (if a loader is registered)
        staging_tables: list[str] = []
        loader = STAGING_REGISTRY.get(script_path.name)
        if loader is not None:
            cursor = self._conn.cursor()
            staging_tables = loader(
                cursor=cursor,
                fs=self._fs,
                bronze_root=self._bronze_root,
                year_glob=self._year_glob,
                month_glob=self._month_glob,
            )
            self._conn.commit()

        # 2 — Execute the MERGE SQL
        rows = 0
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql)
            rows = cursor.rowcount if cursor.rowcount >= 0 else 0
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        finally:
            # 3 — Drop staging tables regardless of outcome
            for tbl in staging_tables:
                try:
                    cursor.execute(f"IF OBJECT_ID('{tbl}', 'U') IS NOT NULL DROP TABLE {tbl}")
                    self._conn.commit()
                except Exception as drop_exc:
                    log.warning("staging_drop_failed", table=tbl, error=str(drop_exc))

        return rows

    def run(
        self,
        scripts: list[str] | None = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> TransformResult:
        if scripts:
            paths = sorted(SQL_DIR / s for s in scripts)
        else:
            paths = sorted(SQL_DIR.glob("*.sql"))

        if not paths:
            log.warning("transform_no_scripts_found", sql_dir=str(SQL_DIR))
            return TransformResult()

        result = TransformResult()

        for path in paths:
            if not path.exists():
                msg = f"Script not found: {path}"
                log.error("transform_script_missing", path=str(path))
                result.errors.append(msg)
                continue

            checksum = _sha256(path.read_text(encoding="utf-8"))

            if not force and not dry_run and self._is_up_to_date(path.name, checksum):
                log.info("transform_skip_unchanged", script=path.name)
                continue

            log.info("transform_run_script", script=path.name, force=force)
            try:
                rows = self.run_script(path, dry_run=dry_run)
                if not dry_run:
                    self._record(path.name, checksum, rows)
                result.scripts_run += 1
                result.total_rows_loaded += rows
                log.info("transform_script_done", script=path.name, rows=rows)
            except Exception as exc:
                msg = f"{path.name}: {exc}"
                log.error("transform_script_error", script=path.name, error=str(exc))
                result.errors.append(msg)

        return result


# ── CLI entry point ────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run silver transformation scripts (bronze → silver)."
    )
    parser.add_argument("--scripts", nargs="+", default=None, metavar="SCRIPT")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--year-glob", default="*", metavar="YYYY")
    parser.add_argument("--month-glob", default="*", metavar="MM")
    return parser.parse_args()


def main() -> None:
    configure_logging()
    load_dotenv(PROJECT_ROOT / ".env", override=True)
    args = _parse_args()

    conn = get_warehouse_conn()
    tracker = RunTracker(conn)
    run_id = tracker.start_run("silver_transform")

    try:
        transformer = Transformer(
            conn=conn,
            fs=get_onelake_fs(),
            bronze_root=get_bronze_root(),
            year_glob=args.year_glob,
            month_glob=args.month_glob,
        )
        result = transformer.run(
            scripts=args.scripts,
            force=args.force,
            dry_run=args.dry_run,
        )

        if result.success:
            tracker.complete_run(run_id, records_loaded=result.total_rows_loaded)
            log.info(
                "transform_complete",
                scripts_run=result.scripts_run,
                rows_loaded=result.total_rows_loaded,
            )
        else:
            tracker.fail_run(run_id, "; ".join(result.errors))
            log.error("transform_finished_with_errors", errors=result.errors)
            sys.exit(1)

    except Exception as exc:
        tracker.fail_run(run_id, str(exc))
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
