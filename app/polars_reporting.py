"""Polars-backed reconciliation summary reporting.

Produces an on-demand summary extract over ``bank_transactions`` and
``ledger_transactions``.  Monetary figures are reported in **rupees** —
``amount`` is persisted as ``NUMERIC(20,8)`` rupees on both tables, and
the matching rules compare them with a rupee tolerance, so summing the
column directly yields rupees (the derived integer ``amount_paisa`` is
never persisted).
"""

from __future__ import annotations

from typing import Any

import polars as pl

# A bank row is "matched" once it reaches a terminal reconciled state. The
# in-between states (UNPROCESSED/PARSED/SCORED/PARTIAL_MATCH) count as
# unmatched for a reconciliation snapshot.
_MATCHED_STATUSES: tuple[str, ...] = ("MATCHED", "RECONCILED")


def _ensure_amount_float(frame: pl.DataFrame) -> pl.DataFrame:
    """Normalise the ``amount`` column to Float64 rupees (NUMERIC -> Decimal)."""
    if "amount" in frame.columns:
        return frame.with_columns(pl.col("amount").cast(pl.Float64, strict=False))
    return frame


def _count_and_sum(frame: pl.DataFrame) -> tuple[int, float]:
    """Return (row count, summed amount) with an empty frame summing to 0.0."""
    count = frame.height
    if count == 0:
        return 0, 0.0
    total = frame.get_column("amount").sum()
    return count, round(float(total or 0.0), 2)


def generate_reconciliation_summary(connection: Any) -> pl.DataFrame:
    """Compute the reconciliation summary as a Polars DataFrame.

    Parameters
    ----------
    connection:
        A SQLAlchemy Connection (sync). Injected by the caller so the
        function does not own engine lifecycle.

    Returns
    -------
    A DataFrame with one row per metric:
        metric, transaction_count, total_amount_rupees
    covering matched bank transactions, unmatched bank transactions, and
    unmatched ledger transactions.
    """
    bank = _ensure_amount_float(
        pl.read_database(
            "SELECT id, status, matched_ledger_id, amount FROM bank_transactions",
            connection=connection,
        )
    )
    ledger = _ensure_amount_float(
        pl.read_database(
            "SELECT id, amount FROM ledger_transactions",
            connection=connection,
        )
    )

    matched_bank = bank.filter(pl.col("status").is_in(_MATCHED_STATUSES))
    unmatched_bank = bank.filter(~pl.col("status").is_in(_MATCHED_STATUSES))

    # A ledger row is matched if some bank row points at it via matched_ledger_id.
    matched_ledger_ids = (
        bank.filter(pl.col("matched_ledger_id").is_not_null())
        .get_column("matched_ledger_id")
        .to_list()
    )
    unmatched_ledger = ledger.filter(~pl.col("id").is_in(matched_ledger_ids))

    matched_count, matched_sum = _count_and_sum(matched_bank)
    unmatched_bank_count, unmatched_bank_sum = _count_and_sum(unmatched_bank)
    unmatched_ledger_count, unmatched_ledger_sum = _count_and_sum(unmatched_ledger)

    return pl.DataFrame(
        {
            "metric": [
                "Matched Transactions",
                "Unmatched Bank Transactions",
                "Unmatched Ledger Transactions",
            ],
            "transaction_count": [
                matched_count,
                unmatched_bank_count,
                unmatched_ledger_count,
            ],
            "total_amount_rupees": [
                matched_sum,
                unmatched_bank_sum,
                unmatched_ledger_sum,
            ],
        }
    )


def summary_to_csv(summary: pl.DataFrame) -> str:
    """Serialise a summary DataFrame to CSV text."""
    return summary.write_csv()
