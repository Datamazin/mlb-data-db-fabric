"""
Pipeline run tracker — Fabric Warehouse (pyodbc) version.

Provides two guarantees:
  1. Idempotency — before extracting an entity, check meta.entity_checksums.
     If the entity_key is present, skip re-extraction.
  2. Observability — every pipeline run is recorded in meta.pipeline_runs
     with status, timing, and record counts.
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import date, datetime, timezone
from typing import Any

import pyodbc
import structlog

log = structlog.get_logger(__name__)

PIPELINE_VERSION = "0.2.0"


class RunTracker:
    """
    Wraps a pyodbc connection and exposes run-state operations.

    Usage:
        tracker = RunTracker(conn)
        run_id = tracker.start_run("backfill", season_year=2024)
        try:
            ...
            tracker.complete_run(run_id, records_extracted=n, records_loaded=n)
        except Exception as exc:
            tracker.fail_run(run_id, str(exc))
            raise
    """

    def __init__(self, conn: pyodbc.Connection) -> None:
        self._conn = conn

    # ── Run lifecycle ─────────────────────────────────────────────────────────

    def start_run(
        self,
        job_name: str,
        season_year: int | None = None,
        target_date: date | None = None,
    ) -> str:
        run_id = str(uuid.uuid4())
        cursor = self._conn.cursor()
        cursor.execute(
            """
            INSERT INTO meta.pipeline_runs
                (run_id, job_name, status, started_at, season_year, target_date, pipeline_version)
            VALUES (?, ?, 'running', ?, ?, ?, ?)
            """,
            (run_id, job_name, _utc_now(), season_year, target_date, PIPELINE_VERSION),
        )
        self._conn.commit()
        log.info("run_started", run_id=run_id, job=job_name, season=season_year)
        return run_id

    def complete_run(
        self,
        run_id: str,
        records_extracted: int = 0,
        records_loaded: int = 0,
    ) -> None:
        cursor = self._conn.cursor()
        cursor.execute(
            """
            UPDATE meta.pipeline_runs
               SET status            = 'success',
                   completed_at      = ?,
                   records_extracted = ?,
                   records_loaded    = ?
             WHERE run_id = ?
            """,
            (_utc_now(), records_extracted, records_loaded, run_id),
        )
        self._conn.commit()
        log.info("run_completed", run_id=run_id, extracted=records_extracted, loaded=records_loaded)

    def fail_run(self, run_id: str, error_message: str) -> None:
        cursor = self._conn.cursor()
        cursor.execute(
            """
            UPDATE meta.pipeline_runs
               SET status        = 'failed',
                   completed_at  = ?,
                   error_message = ?
             WHERE run_id = ?
            """,
            (_utc_now(), error_message[:2000], run_id),
        )
        self._conn.commit()
        log.error("run_failed", run_id=run_id, error=error_message[:200])

    # ── Idempotency ───────────────────────────────────────────────────────────

    def is_extracted(self, entity_type: str, entity_key: str) -> bool:
        cursor = self._conn.cursor()
        cursor.execute(
            """
            SELECT 1 FROM meta.entity_checksums
             WHERE entity_type = ? AND entity_key = ?
            """,
            (entity_type, entity_key),
        )
        return cursor.fetchone() is not None

    def filter_unextracted(self, entity_type: str, entity_keys: list[str]) -> list[str]:
        """Return only the entity_keys not yet in meta.entity_checksums."""
        if not entity_keys:
            return []
        placeholders = ", ".join("?" for _ in entity_keys)
        cursor = self._conn.cursor()
        cursor.execute(
            f"""
            SELECT entity_key FROM meta.entity_checksums
             WHERE entity_type = ?
               AND entity_key IN ({placeholders})
            """,
            (entity_type, *entity_keys),
        )
        already_done = {row[0] for row in cursor.fetchall()}
        return [k for k in entity_keys if k not in already_done]

    def record_checksum(
        self,
        entity_type: str,
        entity_key: str,
        raw_json: str,
        source_url: str,
        correction_source: str | None = None,
    ) -> None:
        response_hash = hashlib.sha256(raw_json.encode()).hexdigest()
        cursor = self._conn.cursor()
        cursor.execute(
            """
            MERGE meta.entity_checksums AS tgt
            USING (VALUES (?, ?, ?, ?, ?, ?, ?)) AS src
                (entity_type, entity_key, response_hash, source_url,
                 extracted_at, transform_version, correction_source)
            ON tgt.entity_type = src.entity_type AND tgt.entity_key = src.entity_key
            WHEN MATCHED THEN UPDATE SET
                tgt.response_hash      = src.response_hash,
                tgt.source_url         = src.source_url,
                tgt.extracted_at       = src.extracted_at,
                tgt.transform_version  = src.transform_version,
                tgt.correction_source  = src.correction_source
            WHEN NOT MATCHED BY TARGET THEN INSERT
                (entity_type, entity_key, response_hash, source_url,
                 extracted_at, transform_version, correction_source)
            VALUES
                (src.entity_type, src.entity_key, src.response_hash, src.source_url,
                 src.extracted_at, src.transform_version, src.correction_source);
            """,
            (
                entity_type, entity_key, response_hash, source_url,
                _utc_now(), PIPELINE_VERSION, correction_source,
            ),
        )
        self._conn.commit()

    def record_checksums_bulk(
        self,
        entity_type: str,
        entries: list[dict[str, Any]],
    ) -> None:
        """Bulk upsert checksums via a staging table + MERGE."""
        if not entries:
            return

        cursor = self._conn.cursor()

        cursor.execute(
            "IF OBJECT_ID('staging.checksum_bulk', 'U') IS NOT NULL "
            "DROP TABLE staging.checksum_bulk"
        )
        cursor.execute(
            """
            CREATE TABLE staging.checksum_bulk (
                entity_type       NVARCHAR(50)    NOT NULL,
                entity_key        NVARCHAR(100)   NOT NULL,
                response_hash     NVARCHAR(64)    NOT NULL,
                source_url        NVARCHAR(500)   NOT NULL,
                extracted_at      DATETIMEOFFSET  NOT NULL,
                transform_version NVARCHAR(20)    NOT NULL,
                correction_source NVARCHAR(100)   NULL
            )
            """
        )

        rows = [
            (
                entity_type,
                e["entity_key"],
                hashlib.sha256(e["raw_json"].encode()).hexdigest(),
                e["source_url"],
                _utc_now(),
                PIPELINE_VERSION,
                e.get("correction_source"),
            )
            for e in entries
        ]
        cursor.fast_executemany = True
        cursor.executemany(
            """
            INSERT INTO staging.checksum_bulk
                (entity_type, entity_key, response_hash, source_url,
                 extracted_at, transform_version, correction_source)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

        cursor.execute(
            """
            MERGE meta.entity_checksums AS tgt
            USING staging.checksum_bulk AS src
                ON tgt.entity_type = src.entity_type AND tgt.entity_key = src.entity_key
            WHEN MATCHED THEN UPDATE SET
                tgt.response_hash     = src.response_hash,
                tgt.source_url        = src.source_url,
                tgt.extracted_at      = src.extracted_at,
                tgt.transform_version = src.transform_version,
                tgt.correction_source = src.correction_source
            WHEN NOT MATCHED BY TARGET THEN INSERT
                (entity_type, entity_key, response_hash, source_url,
                 extracted_at, transform_version, correction_source)
            VALUES
                (src.entity_type, src.entity_key, src.response_hash, src.source_url,
                 src.extracted_at, src.transform_version, src.correction_source);
            """
        )
        cursor.execute("DROP TABLE staging.checksum_bulk")
        self._conn.commit()
        log.debug("checksums_recorded", entity_type=entity_type, count=len(rows))

    # ── Inspection ────────────────────────────────────────────────────────────

    def extraction_count(self, entity_type: str) -> int:
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM meta.entity_checksums WHERE entity_type = ?",
            (entity_type,),
        )
        row = cursor.fetchone()
        return row[0] if row else 0

    def last_successful_run(self, job_name: str) -> dict[str, Any] | None:
        cursor = self._conn.cursor()
        cursor.execute(
            """
            SELECT TOP 1
                run_id, started_at, completed_at, records_extracted, records_loaded
              FROM meta.pipeline_runs
             WHERE job_name = ? AND status = 'success'
             ORDER BY completed_at DESC
            """,
            (job_name,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return {
            "run_id": row[0],
            "started_at": row[1],
            "completed_at": row[2],
            "records_extracted": row[3],
            "records_loaded": row[4],
        }


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
