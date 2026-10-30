"""Dead Letter Queue write operations."""

from __future__ import annotations

import sys
from datetime import datetime, timezone

from sqlalchemy import text


_MAX_PAYLOAD_LEN = 65_535
_MAX_TRACE_LEN = 65_535


def _write_to_dlq(
    raw_payload: str,
    file_origin: str,
    worker_id: str,
    exception_class: str,
    stack_trace: str,
) -> None:
    """
    Write a failed transaction record to dlq_transactions.

    **CRITICAL CONTRACT (Security Auditor Check 5):**

    1. This function is called inside an ``except`` block.
    2. If the DLQ write itself fails, the original exception is **not**
       suppressed.
    3. The caller re-raises the original exception after calling this
       function.
    """
    try:
        from app.session import get_db_connection

        with get_db_connection() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO dlq_transactions (
                        raw_payload, file_origin, worker_id,
                        exception_class, stack_trace, created_at
                    ) VALUES (
                        :raw_payload, :file_origin, :worker_id,
                        :exception_class, :stack_trace, :created_at
                    )
                    """
                ),
                {
                    "raw_payload": (raw_payload or "")[:_MAX_PAYLOAD_LEN],
                    "file_origin": (file_origin or "unknown")[:255],
                    "worker_id": (worker_id or "unknown")[:255],
                    "exception_class": (exception_class or "UnknownException")[:255],
                    "stack_trace": (stack_trace or "")[:_MAX_TRACE_LEN],
                    "created_at": datetime.now(timezone.utc),
                },
            )
            conn.commit()
    except Exception as dlq_write_exc:
        print(
            f"CRITICAL: DLQ write failed for worker={worker_id}, "
            f"file_origin={file_origin}. "
            f"DLQ exception: {type(dlq_write_exc).__name__}: {dlq_write_exc}",
            file=sys.stderr,
        )
        # Fall through — caller re-raises the original exception.
