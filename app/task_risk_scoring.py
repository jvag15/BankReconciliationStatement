"""Celery task for LightGBM risk scoring."""

from __future__ import annotations

import traceback
from typing import Any

from celery import shared_task
from sqlalchemy import text

from app.db_batch_claim import claim_parsed_batch, mark_batch_as_scored
from app.db_dlq import _write_to_dlq
from app.session import get_db_connection
from app.ml_risk_scoring import run_risk_scoring_stream


@shared_task(  # type: ignore[untyped-decorator]
    bind=True,
    max_retries=3,
    default_retry_delay=60,
    acks_late=True,
    reject_on_worker_lost=True,
    name="brs_engine.tasks.run_risk_scoring",
)
def run_risk_scoring(
    self: Any,
    batch_ids: list[int],
) -> dict[str, str]:
    """Read PARSED rows, run LightGBM in 1 000-row slices, and update scores."""
    try:
        with get_db_connection() as conn:
            with conn.begin():
                claimed_ids: list[int] = claim_parsed_batch(
                    conn, len(batch_ids),
                )
                if not claimed_ids:
                    return {"status": "NO_ROWS", "scored": "0"}

                result = conn.execute(
                    text(
                        """
                        SELECT id, amount, value_date, description_raw
                        FROM bank_transactions
                        WHERE id = ANY(:ids) AND status = 'PARSED'
                        """
                    ),
                    {"ids": claimed_ids},
                )
                rows = list(result)
                tx_ids: list[int] = [int(r[0]) for r in rows]

                import numpy as np

                feature_matrix: np.ndarray = np.zeros(
                    (len(tx_ids), 10), dtype=np.float32,
                )

                for batch in run_risk_scoring_stream(tx_ids, feature_matrix):
                    for record in batch:
                        conn.execute(
                            text(
                                """
                                UPDATE bank_transactions
                                SET risk_score = :risk_score,
                                    status = 'SCORED',
                                    updated_at = NOW()
                                WHERE id = :transaction_id
                                """
                            ),
                            record,
                        )
                        conn.execute(
                            text(
                                """
                                INSERT INTO audit_log (
                                    transaction_id, from_status,
                                    to_status, worker_id,
                                    rule_id, confidence_score,
                                    created_at
                                ) VALUES (
                                    :transaction_id, 'PARSED',
                                    'SCORED', :worker_id,
                                    'ML_RISK_LGBM', :risk_score,
                                    NOW()
                                )
                                """
                            ),
                            {
                                "transaction_id": record["transaction_id"],
                                "worker_id": self.request.id or "unknown",
                                "risk_score": record["risk_score"],
                            },
                        )
                    del batch

                mark_batch_as_scored(
                    conn, claimed_ids, self.request.id or "unknown",
                )

        del feature_matrix
        return {"status": "SUCCESS", "scored": str(len(tx_ids))}
    except Exception as exc:
        _write_to_dlq(
            raw_payload=str(batch_ids),
            file_origin="risk_scoring",
            worker_id=self.request.id or "unknown",
            exception_class=type(exc).__name__,
            stack_trace=traceback.format_exc(),
        )
        raise self.retry(exc=exc, countdown=2 ** self.request.retries * 30)
