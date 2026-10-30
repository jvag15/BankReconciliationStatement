"""Celery task for BRS matching engine."""

from __future__ import annotations

import traceback
from typing import Any

from celery import shared_task
from loguru import logger
from sqlalchemy import text

from app.db_dlq import _write_to_dlq
from app.matching_dispatch_guard import release_dispatch_flag
from app.session import get_db_connection
from app.matching_rules import (
    MatchResult,
    match_rule_1a,
    match_rule_1b,
    match_rule_2a,
    match_rule_2b,
    match_rule_3a,
    match_rule_3b,
    match_rule_4a,
    match_rule_4b,
)


def _apply_match(conn: Any, m: MatchResult, worker_id: str) -> None:
    """Persist a single MatchResult to transactions + audit_log."""
    if m.ledger_id:
        conn.execute(
            text(
                """
                UPDATE bank_transactions
                SET status = :status,
                    matched_ledger_id = :ledger_id,
                    confidence_score = :confidence,
                    match_rule_id = :rule_id,
                    updated_at = NOW()
                WHERE id = :bank_id
                """
            ),
            {
                "status": m.status,
                "ledger_id": m.ledger_id,
                "confidence": float(m.confidence),
                "rule_id": m.rule_id,
                "bank_id": m.bank_id,
            },
        )
    else:
        conn.execute(
            text(
                """
                UPDATE bank_transactions
                SET status = :status,
                    confidence_score = :confidence,
                    match_rule_id = :rule_id,
                    updated_at = NOW()
                WHERE id = :bank_id
                """
            ),
            {
                "status": m.status,
                "confidence": float(m.confidence),
                "rule_id": m.rule_id,
                "bank_id": m.bank_id,
            },
        )

    from_status: str = "SCORED"
    conn.execute(
        text(
            """
            INSERT INTO audit_log (
                transaction_id, from_status, to_status,
                worker_id, rule_id, confidence_score, created_at
            ) VALUES (
                :tx_id, :from_status, :to_status,
                :worker_id, :rule_id, :confidence, NOW()
            )
            """
        ),
        {
            "tx_id": m.bank_id,
            "from_status": from_status,
            "to_status": m.status,
            "worker_id": worker_id,
            "rule_id": m.rule_id,
            "confidence": float(m.confidence),
        },
    )


@shared_task(  # type: ignore[untyped-decorator]
    bind=True,
    max_retries=3,
    default_retry_delay=60,
    acks_late=True,
    reject_on_worker_lost=True,
    name="brs_engine.tasks.run_matching_engine",
)
def run_matching_engine(
    self: Any,
    batch_ids: list[int],
) -> dict[str, str]:
    """Apply ADR-014 reconciliation rules to SCORED bank transactions."""
    try:
        with get_db_connection() as conn:
            with conn.begin():
                bank_result = conn.execute(
                    text(
                        """
                        SELECT id, amount, value_date, utr, neft_ref,
                               rtgs_ref, imps_ref, upi_ref, ifsc,
                               risk_score
                        FROM bank_transactions
                        WHERE id = ANY(:ids) AND status = 'SCORED'
                        """
                    ),
                    {"ids": batch_ids},
                )
                bank_rows: list[Any] = list(bank_result)

                if not bank_rows:
                    release_dispatch_flag()
                    return {"status": "NO_ROWS", "matched": "0"}

                ledger_result = conn.execute(
                    text(
                        """
                        SELECT id, amount, ledger_date, utr, neft_ref,
                               rtgs_ref, imps_ref, upi_ref, ifsc,
                               description
                        FROM ledger_transactions
                        WHERE ledger_date >= CURRENT_DATE
                            - INTERVAL '30 days'
                        """
                    ),
                )
                ledger_rows: list[Any] = list(ledger_result)

                matched_bank_ids: set[int] = set()

                # Rule 1A — Exact UTR + exact amount + same date
                for m in match_rule_1a(bank_rows, ledger_rows):
                    _apply_match(conn, m, self.request.id or "unknown")
                    matched_bank_ids.add(m.bank_id)

                remaining_1b = [
                    b for b in bank_rows
                    if b.id not in matched_bank_ids
                ]
                for m in match_rule_1b(remaining_1b, ledger_rows):
                    _apply_match(conn, m, self.request.id or "unknown")
                    matched_bank_ids.add(m.bank_id)

                remaining_2a = [
                    b for b in bank_rows
                    if b.id not in matched_bank_ids
                ]
                for m in match_rule_2a(remaining_2a, ledger_rows):
                    _apply_match(conn, m, self.request.id or "unknown")
                    matched_bank_ids.add(m.bank_id)

                remaining_2b = [
                    b for b in bank_rows
                    if b.id not in matched_bank_ids
                ]
                for m in match_rule_2b(remaining_2b, ledger_rows):
                    _apply_match(conn, m, self.request.id or "unknown")
                    matched_bank_ids.add(m.bank_id)

                # Rules 3A/3B require the fastText model. A worker without a
                # loaded model (fresh environment, retrain has not produced
                # one yet) must not fail the whole batch over the optional
                # fuzzy-description rules — the deterministic rules above and
                # the anomaly rules below still apply. Degrade by skipping.
                try:
                    remaining_3a = [
                        b for b in bank_rows
                        if b.id not in matched_bank_ids
                    ]
                    for m in match_rule_3a(remaining_3a, ledger_rows):
                        _apply_match(conn, m, self.request.id or "unknown")
                        matched_bank_ids.add(m.bank_id)

                    remaining_3b = [
                        b for b in bank_rows
                        if b.id not in matched_bank_ids
                    ]
                    for m in match_rule_3b(remaining_3b, ledger_rows):
                        _apply_match(conn, m, self.request.id or "unknown")
                        matched_bank_ids.add(m.bank_id)
                except RuntimeError as model_exc:
                    logger.warning(
                        "Matching rules 3A/3B skipped (model unavailable): {}",
                        model_exc,
                    )

                remaining_4a = [
                    b for b in bank_rows
                    if b.id not in matched_bank_ids
                ]
                for m in match_rule_4a(remaining_4a, ledger_rows):
                    _apply_match(conn, m, self.request.id or "unknown")
                    matched_bank_ids.add(m.bank_id)

                # Rule 4B — anomaly flag (no ledger consumed)
                for m in match_rule_4b(bank_rows):
                    _apply_match(conn, m, self.request.id or "unknown")

        # Open the gate for the next ledger chunk: a fresh dispatch will
        # re-select SCORED rows and re-read the ledger window, sweeping up
        # anything this pass missed. On the retry path below the flag stays
        # held so retries do not overlap a newly queued pass; if retries
        # exhaust, the flag TTL clears it.
        release_dispatch_flag()
        return {"status": "SUCCESS", "matched": str(len(matched_bank_ids))}
    except Exception as exc:
        _write_to_dlq(
            raw_payload=str(batch_ids),
            file_origin="matching_engine",
            worker_id=self.request.id or "unknown",
            exception_class=type(exc).__name__,
            stack_trace=traceback.format_exc(),
        )
        raise self.retry(exc=exc, countdown=2 ** self.request.retries * 30)
