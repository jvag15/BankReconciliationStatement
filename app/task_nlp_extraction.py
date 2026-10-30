"""Celery task for NLP entity extraction."""

from __future__ import annotations

import traceback
from typing import Any

from celery import shared_task
from sqlalchemy import text

from app.db_batch_claim import claim_unprocessed_batch, mark_batch_as_parsed
from app.db_dlq import _write_to_dlq
from app.session import get_db_connection
from app.ml_nlp_inference import run_nlp_extraction_stream


@shared_task(  # type: ignore[untyped-decorator]
    bind=True,
    max_retries=3,
    default_retry_delay=60,
    acks_late=True,
    reject_on_worker_lost=True,
    name="brs_engine.tasks.run_nlp_extraction",
)
def run_nlp_extraction(
    self: Any,
    batch_ids: list[int],
) -> dict[str, str]:
    """Claim UNPROCESSED rows with SKIP LOCKED and run spaCy EntityRuler."""
    try:
        with get_db_connection() as conn:
            with conn.begin():
                claimed_ids: list[int] = claim_unprocessed_batch(
                    conn, len(batch_ids),
                )
                if not claimed_ids:
                    return {"status": "NO_ROWS", "claimed": "0"}

                result = conn.execute(
                    text(
                        """
                        SELECT id, description_raw
                        FROM bank_transactions
                        WHERE id = ANY(:ids)
                        """
                    ),
                    {"ids": claimed_ids},
                )
                rows = list(result)
                tx_ids: list[int] = [int(r[0]) for r in rows]
                descriptions: list[str] = [str(r[1]) for r in rows]

                for batch in run_nlp_extraction_stream(tx_ids, descriptions):
                    for record in batch:
                        conn.execute(
                            text(
                                """
                                UPDATE bank_transactions
                                SET utr = :utr,
                                    neft_ref = :neft_ref,
                                    rtgs_ref = :rtgs_ref,
                                    imps_ref = :imps_ref,
                                    upi_ref = :upi_ref,
                                    ifsc = :ifsc,
                                    cin = :cin,
                                    pan = :pan,
                                    updated_at = NOW()
                                WHERE id = :transaction_id
                                """
                            ),
                            record,
                        )
                    del batch

                mark_batch_as_parsed(
                    conn, claimed_ids, self.request.id or "unknown",
                )

        return {"status": "SUCCESS", "claimed": str(len(claimed_ids))}
    except Exception as exc:
        _write_to_dlq(
            raw_payload=str(batch_ids),
            file_origin="nlp_extraction",
            worker_id=self.request.id or "unknown",
            exception_class=type(exc).__name__,
            stack_trace=traceback.format_exc(),
        )
        raise self.retry(exc=exc, countdown=2 ** self.request.retries * 30)
