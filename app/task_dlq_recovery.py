"""Automated Dead Letter Queue recovery.

A beat-scheduled pass over ``dlq_transactions`` that decides, per failed
entry, whether the failure was transient (re-enqueue the original chunk for
another attempt at ``app/task_ingestion.py``) or terminal (mark
``FAILED_UNRECOVERABLE`` so it is never retried again).

Loop safety
-----------
Three independent guards prevent the "retry forever" failure mode that the
directive calls out:

1. Only exception classes on :data:`_TRANSIENT_EXCEPTIONS` are ever retried.
   A missing file (``FileNotFoundError``) or a bad key
   (``InvalidToken``) is terminal by definition.
2. Retries are capped at :data:`MAX_RECOVERY_ATTEMPTS`; beyond that the entry
   is marked terminal regardless of its exception class.
3. Re-enqueue requires the referenced ciphertext to still exist on disk.
   Because ``process_statement_chunk`` deletes the file as it runs, a
   re-enqueued chunk that fails again lands in the DLQ with its file gone,
   so the *next* pass classifies it as a missing-file terminal failure.

The decision logic (:func:`decide_action`) is pure and filesystem-free so it
can be reasoned about and tested in isolation; the task performs the actual
I/O around it.
"""

from __future__ import annotations

import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text

from app.celery_app import celery_app as app
from app.session import get_db_connection
from app.storage_paths import get_storage_dir

# Verdicts returned by decide_action.
ACTION_REQUEUE = "REQUEUE"
ACTION_FAIL = "FAIL"

# Terminal states written back to dlq_transactions.recovery_status.
STATUS_REQUEUED = "REQUEUED"
STATUS_FAILED = "FAILED_UNRECOVERABLE"

MAX_RECOVERY_ATTEMPTS = 3
_SCAN_BATCH_LIMIT = 200

# Exception classes considered transient — infrastructure hiccups that a
# later attempt may clear. Matched by the class *name* recorded in the DLQ.
_TRANSIENT_EXCEPTIONS: frozenset[str] = frozenset(
    {
        "OperationalError",        # SQLAlchemy: lock timeout, connection drop
        "DBAPIError",
        "InterfaceError",
        "TimeoutError",
        "PoolTimeout",
        "LockNotAvailable",
        "DeadlockDetected",
        "ConnectionError",         # redis-py
        "RedisConnectionError",    # raised by app/task_base.py on_retry
    }
)

# Extensions that mark a raw_payload as a spool file path we can replay.
_CHUNK_SUFFIXES: tuple[str, ...] = (".enc", ".csv", ".txt")


def is_transient(exception_class: str) -> bool:
    """True when *exception_class* names a retryable infrastructure fault."""
    return exception_class in _TRANSIENT_EXCEPTIONS


def looks_like_chunk_path(raw_payload: str) -> bool:
    """True when *raw_payload* looks like a spool file path (not a marker).

    DLQ payloads are heterogeneous: a file path for decrypt failures, a bare
    origin string (``"api_gateway_uploader"``) for generic task failures, or
    a ``str(batch_ids)`` list for the scoring/matching tasks. Only the first
    kind can be replayed through the ingestion entrypoint.
    """
    if not raw_payload:
        return False
    candidate = raw_payload.strip()
    if not candidate.lower().endswith(_CHUNK_SUFFIXES):
        return False
    return "/" in candidate or "\\" in candidate


def decide_action(
    exception_class: str,
    raw_payload: str,
    file_exists: bool,
    attempts: int,
) -> str:
    """Pure verdict for one DLQ entry: :data:`ACTION_REQUEUE` or :data:`ACTION_FAIL`.

    ``file_exists`` is supplied by the caller so this function stays free of
    filesystem access and is trivially testable.
    """
    if attempts >= MAX_RECOVERY_ATTEMPTS:
        return ACTION_FAIL
    if not is_transient(exception_class):
        return ACTION_FAIL
    if not looks_like_chunk_path(raw_payload):
        # Transient, but not an ingestion chunk we know how to replay.
        return ACTION_FAIL
    if not file_exists:
        # Missing-file failure — terminal, per the recovery spec.
        return ACTION_FAIL
    return ACTION_REQUEUE


@app.task(  # type: ignore[untyped-decorator]
    bind=True,
    name="brs_engine.tasks.recover_dlq_entries",
    queue="ingestion",
)
def recover_dlq_entries(self: Any, batch_limit: int = _SCAN_BATCH_LIMIT) -> dict[str, int]:
    """Scan PENDING DLQ rows and either re-enqueue or terminally fail each.

    Returns a summary of the pass. Safe to run on an empty queue.
    """
    from app.task_ingestion import process_statement_chunk

    with get_db_connection() as conn:
        # Read the pending set inside its own transaction and let it close,
        # so no autobegun transaction lingers to collide with the per-row
        # conn.begin() blocks below (SQLAlchemy 2.0 forbids nested begin()).
        with conn.begin():
            pending = conn.execute(
                text(
                    """
                    SELECT id, raw_payload, file_origin, exception_class,
                           recovery_attempts
                    FROM dlq_transactions
                    WHERE recovery_status = 'PENDING'
                    ORDER BY created_at ASC
                    LIMIT :limit
                    """
                ),
                {"limit": batch_limit},
            ).fetchall()

        scanned = len(pending)
        requeued = 0
        failed = 0

        for row in pending:
            dlq_id = int(row[0])
            raw_payload = row[1] or ""
            file_origin = row[2] or "dlq_recovery"
            exception_class = row[3] or "UnknownException"
            attempts = int(row[4] or 0)

            file_exists = (
                looks_like_chunk_path(raw_payload)
                and Path(raw_payload).exists()
            )
            action = decide_action(
                exception_class, raw_payload, file_exists, attempts,
            )

            now = datetime.now(timezone.utc)

            if action == ACTION_REQUEUE:
                # Enqueue before recording REQUEUED so a broker failure leaves
                # the row PENDING for the next pass rather than silently lost.
                process_statement_chunk.apply_async(
                    kwargs={
                        "chunk_id": f"dlq_retry_{secrets.token_hex(6)}",
                        "file_origin": file_origin,
                        "file_path_encrypted": raw_payload,
                    }
                )
                with conn.begin():
                    conn.execute(
                        text(
                            """
                            UPDATE dlq_transactions
                            SET recovery_status = :status,
                                recovery_attempts = recovery_attempts + 1,
                                last_recovery_at = :now
                            WHERE id = :id
                            """
                        ),
                        {"status": STATUS_REQUEUED, "now": now, "id": dlq_id},
                    )
                requeued += 1
            else:
                with conn.begin():
                    conn.execute(
                        text(
                            """
                            UPDATE dlq_transactions
                            SET recovery_status = :status,
                                recovery_attempts = recovery_attempts + 1,
                                last_recovery_at = :now
                            WHERE id = :id
                            """
                        ),
                        {"status": STATUS_FAILED, "now": now, "id": dlq_id},
                    )
                failed += 1

    return {
        "scanned": scanned,
        "requeued": requeued,
        "failed_unrecoverable": failed,
    }
