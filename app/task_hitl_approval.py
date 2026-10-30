"""Celery task for Human-in-the-Loop reconciliation approval."""

from __future__ import annotations

import traceback
from typing import Any

from celery import shared_task
from sqlalchemy import text

from app.db_dlq import _write_to_dlq
from app.session import get_db_connection


@shared_task(  # type: ignore[untyped-decorator]
    bind=True,
    max_retries=0,
    acks_late=True,
    reject_on_worker_lost=True,
    name="brs_engine.tasks.reconcile_hitl_approval",
)
def reconcile_hitl_approval(
    self: Any,
    bank_transaction_id: int,
    operator_uuid: str,
) -> dict[str, str]:
    """Transition MATCHED -> RECONCILED with operator UUID audit trail.

    No retries — prevents duplicate reconciliation attempts.
    """
    try:
        with get_db_connection() as conn:
            with conn.begin():
                conn.execute(
                    text(
                        """
                        UPDATE bank_transactions
                        SET status = 'RECONCILED', updated_at = NOW()
                        WHERE id = :tx_id AND status = 'MATCHED'
                        """
                    ),
                    {"tx_id": bank_transaction_id},
                )

                conn.execute(
                    text(
                        """
                        INSERT INTO audit_log (
                            transaction_id, from_status, to_status,
                            worker_id, rule_id, confidence_score,
                            created_at
                        ) VALUES (
                            :tx_id, 'MATCHED', 'RECONCILED',
                            :worker_id, 'HITL_APPROVAL', 1.0, NOW()
                        )
                        """
                    ),
                    {
                        "tx_id": bank_transaction_id,
                        "worker_id": operator_uuid,
                    },
                )

        return {
            "status": "RECONCILED",
            "transaction_id": str(bank_transaction_id),
        }
    except Exception as exc:
        _write_to_dlq(
            raw_payload=str(bank_transaction_id),
            file_origin="hitl_approval",
            worker_id=operator_uuid,
            exception_class=type(exc).__name__,
            stack_trace=traceback.format_exc(),
        )
        raise
