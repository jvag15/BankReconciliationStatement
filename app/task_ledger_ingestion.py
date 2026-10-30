"""Celery task for internal ledger export ingestion.

Mirrors the bank-statement ingestion flow in ``app/task_ingestion.py``:
decrypt the uploaded chunk in memory, parse it with Polars, batch-insert,
then hand off to the next stage.  Here the hand-off is the reconciliation
matching engine, since freshly loaded ledger rows are what rules 1A-4A
need in order to find counterparties for already-scored bank rows.
"""

from __future__ import annotations

import traceback
import uuid
from pathlib import Path
from typing import Any

from sqlalchemy import text

from app.celery_app import celery_app as app
from app.db_dlq import _write_to_dlq
from app.polars_ledger_ingestion import (
    LedgerSchemaError,
    ingest_ledger_export,
    validate_ledger_frame,
)
from app.session import get_db_connection
from app.task_base import DLQBackedTask

_INSERT_BATCH_SIZE = 2_000


@app.task(  # type: ignore[untyped-decorator]
    base=DLQBackedTask,
    bind=True,
    name="brs_engine.tasks.process_ledger_chunk",
    queue="ingestion",
)
def process_ledger_chunk(
    self: Any,
    chunk_id: str,
    file_origin: str,
    file_path_encrypted: str,
    job_id: str | None = None,
    company_name: str = "Internal Ledger",
    **kwargs: Any,
) -> dict[str, Any]:
    """Decrypt, parse and persist one ledger export chunk.

    On success dispatches ``run_matching_engine`` for any bank rows that
    are sitting in ``SCORED`` — the state the matching engine consumes.
    """
    from cryptography.fernet import InvalidToken

    from app.storage_encryption import decrypt_file_to_memory

    if not job_id:
        job_id = str(uuid.uuid4())

    file_path_obj = Path(file_path_encrypted)

    # ── Decrypt ────────────────────────────────────────────────────────
    try:
        decrypted_bytes = decrypt_file_to_memory(file_path_encrypted)
    except InvalidToken as exc:
        _write_to_dlq(
            raw_payload=file_path_encrypted,
            file_origin=file_origin,
            worker_id=self.request.id or "unknown",
            exception_class=type(exc).__name__,
            stack_trace=traceback.format_exc(),
        )
        if file_path_obj.exists():
            file_path_obj.unlink()
        raise

    # ── Parse ──────────────────────────────────────────────────────────
    try:
        df = ingest_ledger_export(decrypted_bytes)
        validate_ledger_frame(df)
    except LedgerSchemaError as exc:
        # A malformed export is an operator-fixable problem: capture the
        # reason in the DLQ so it can be corrected and re-submitted.
        _write_to_dlq(
            raw_payload=f"chunk_id={chunk_id}; origin={file_origin}",
            file_origin=file_origin,
            worker_id=self.request.id or "unknown",
            exception_class=type(exc).__name__,
            stack_trace=traceback.format_exc(),
        )
        if file_path_obj.exists():
            file_path_obj.unlink()
        raise

    if file_path_obj.exists():
        file_path_obj.unlink()

    if df.is_empty():
        return {
            "status": "SUCCESS",
            "rows_written": 0,
            "job_id": job_id,
            "chunk_id": chunk_id,
            "note": "Ledger export contained no rows.",
        }

    # ── Persist ────────────────────────────────────────────────────────
    rows_written = 0
    with get_db_connection() as conn:
        with conn.begin():
            conn.execute(
                text(
                    """
                    INSERT INTO audit_jobs (job_id, company_name, status, file_origin)
                    VALUES (:job_id, :company_name, 'INGESTING', :origin)
                    ON CONFLICT (job_id) DO NOTHING
                    """
                ),
                {
                    "job_id": job_id,
                    "company_name": company_name[:255],
                    "origin": file_origin[:255],
                },
            )

            insert_stmt = text(
                """
                INSERT INTO ledger_transactions (
                    job_id, source_chunk_id, file_origin, ledger_date,
                    amount, description, account_code, cost_center,
                    voucher_number, utr, neft_ref, rtgs_ref, imps_ref,
                    upi_ref, ifsc
                ) VALUES (
                    :job_id, :source_chunk_id, :file_origin, :ledger_date,
                    :amount, :description, :account_code, :cost_center,
                    :voucher_number, :utr, :neft_ref, :rtgs_ref, :imps_ref,
                    :upi_ref, :ifsc
                )
                """
            )

            batch: list[dict[str, Any]] = []
            for row in df.iter_rows(named=True):
                batch.append(
                    {
                        "job_id": job_id,
                        "source_chunk_id": chunk_id[:36],
                        "file_origin": file_origin[:255],
                        "ledger_date": row["ledger_date"],
                        # Persist rupees: bank_transactions.amount is rupees and
                        # matching_rules compares the two with a rupee-denominated
                        # tolerance. amount_paisa stays a derived analysis column.
                        "amount": row["amount"],
                        "description": row["description"] or "",
                        "account_code": (row["account_code"] or "UNCLASSIFIED")[:50],
                        "cost_center": _truncate(row["cost_center"], 50),
                        "voucher_number": _truncate(row["voucher_number"], 50),
                        "utr": _truncate(row["utr"], 22),
                        "neft_ref": _truncate(row["neft_ref"], 30),
                        "rtgs_ref": _truncate(row["rtgs_ref"], 30),
                        "imps_ref": _truncate(row["imps_ref"], 12),
                        "upi_ref": _truncate(row["upi_ref"], 18),
                        "ifsc": _truncate(row["ifsc"], 11),
                    }
                )
                if len(batch) >= _INSERT_BATCH_SIZE:
                    conn.execute(insert_stmt, batch)
                    rows_written += len(batch)
                    batch = []

            if batch:
                conn.execute(insert_stmt, batch)
                rows_written += len(batch)

            conn.execute(
                text(
                    """
                    UPDATE audit_jobs
                    SET rows_processed = rows_processed + :n, updated_at = NOW()
                    WHERE job_id = :job_id
                    """
                ),
                {"n": rows_written, "job_id": job_id},
            )

    # ── Hand off to the matching engine ────────────────────────────────
    matching_task_id = _dispatch_matching(job_id)

    return {
        "status": "SUCCESS",
        "rows_written": rows_written,
        "job_id": job_id,
        "chunk_id": chunk_id,
        "matching_task_id": matching_task_id,
    }


def _truncate(value: Any, limit: int) -> str | None:
    """Coerce an optional cell to a length-bounded string, or ``None``."""
    if value is None:
        return None
    text_value = str(value).strip()
    if not text_value:
        return None
    return text_value[:limit]


def _dispatch_matching(job_id: str) -> str | None:
    """Queue the matching engine for bank rows awaiting reconciliation.

    ``run_matching_engine`` consumes bank transactions in ``SCORED`` — the
    state written by the risk-scoring stage.  If nothing has reached that
    state there is nothing to reconcile against, so we skip the dispatch
    rather than queue a no-op task.

    Dispatch is deduplicated across chunks: concurrent ledger uploads all
    see the same SCORED window, so one pending matching pass covers them
    all.  A chunk must claim the Redis dispatch flag first; the matching
    task releases it on completion, letting the next chunk queue a fresh
    pass over whatever is still (or newly) SCORED.
    """
    from app.matching_dispatch_guard import (
        acquire_dispatch_flag,
        release_dispatch_flag,
    )
    from app.task_matching_engine import run_matching_engine

    if not acquire_dispatch_flag(owner=job_id):
        # A matching pass is already pending or running over this window;
        # queuing another would cost minutes on the solo-pool worker for
        # zero additional matches.
        return None

    with get_db_connection() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id FROM bank_transactions
                WHERE status = 'SCORED'
                ORDER BY ingested_at ASC
                LIMIT 5000
                """
            )
        ).fetchall()

    batch_ids = [int(r[0]) for r in rows]
    if not batch_ids:
        release_dispatch_flag()
        return None

    try:
        result = run_matching_engine.delay(batch_ids=batch_ids)
    except Exception:
        release_dispatch_flag()
        raise
    return str(result.id)
