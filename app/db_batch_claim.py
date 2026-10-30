"""Batch claim operations using SELECT FOR UPDATE SKIP LOCKED."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection


def claim_unprocessed_batch(
    conn: Connection,
    batch_size: int = 1_000,
) -> list[int]:
    """
    Atomically claim a non-overlapping batch of UNPROCESSED
    ``bank_transaction`` rows.

    Uses ``FOR UPDATE SKIP LOCKED`` so other workers skip
    already-locked rows without blocking.
    """
    result = conn.execute(
        text(
            """
            SELECT id
            FROM bank_transactions
            WHERE status = 'UNPROCESSED'
            ORDER BY ingested_at ASC
            LIMIT :batch_size
            FOR UPDATE SKIP LOCKED
            """
        ),
        {"batch_size": batch_size},
    )
    return [row[0] for row in result.all()]


def claim_parsed_batch(
    conn: Connection,
    batch_size: int = 1_000,
) -> list[int]:
    """Claim PARSED rows for NLP scoring.  Same SKIP LOCKED pattern."""
    result = conn.execute(
        text(
            """
            SELECT id
            FROM bank_transactions
            WHERE status = 'PARSED'
            ORDER BY ingested_at ASC
            LIMIT :batch_size
            FOR UPDATE SKIP LOCKED
            """
        ),
        {"batch_size": batch_size},
    )
    return [row[0] for row in result.all()]


def claim_scored_batch(
    conn: Connection,
    batch_size: int = 1_000,
) -> list[int]:
    """Claim SCORED rows for the matching engine.  SKIP LOCKED."""
    result = conn.execute(
        text(
            """
            SELECT id
            FROM bank_transactions
            WHERE status = 'SCORED'
            ORDER BY ingested_at ASC
            LIMIT :batch_size
            FOR UPDATE SKIP LOCKED
            """
        ),
        {"batch_size": batch_size},
    )
    return [row[0] for row in result.all()]


def mark_batch_as_parsed(
    conn: Connection,
    transaction_ids: list[int],
    worker_id: str,
) -> None:
    """
    Update claimed rows from ``UNPROCESSED`` to ``PARSED`` and append
    ``audit_log`` entries.  Both operations must be inside the same
    transaction as the batch claim.
    """
    if not transaction_ids:
        return

    conn.execute(
        text(
            """
            UPDATE bank_transactions
            SET status = 'PARSED', updated_at = NOW()
            WHERE id = ANY(:ids)
            """
        ),
        {"ids": transaction_ids},
    )

    conn.execute(
        text(
            """
            INSERT INTO audit_log (
                transaction_id, from_status, to_status,
                worker_id, rule_id, confidence_score, created_at
            )
            SELECT
                unnest(:transaction_ids::int[]),
                'UNPROCESSED', 'PARSED',
                :worker_id, NULL, NULL, NOW()
            """
        ),
        {"transaction_ids": transaction_ids, "worker_id": worker_id},
    )


def mark_batch_as_scored(
    conn: Connection,
    transaction_ids: list[int],
    worker_id: str,
) -> None:
    """
    Update claimed rows from ``PARSED`` to ``SCORED`` and append
    ``audit_log`` entries.
    """
    if not transaction_ids:
        return

    conn.execute(
        text(
            """
            UPDATE bank_transactions
            SET status = 'SCORED', updated_at = NOW()
            WHERE id = ANY(:ids)
            """
        ),
        {"ids": transaction_ids},
    )

    conn.execute(
        text(
            """
            INSERT INTO audit_log (
                transaction_id, from_status, to_status,
                worker_id, rule_id, confidence_score, created_at
            )
            SELECT
                unnest(:transaction_ids::int[]),
                'PARSED', 'SCORED',
                :worker_id, 'ML_RISK_LGBM', NULL, NOW()
            """
        ),
        {"transaction_ids": transaction_ids, "worker_id": worker_id},
    )
