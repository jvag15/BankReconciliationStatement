"""BRS reconciliation matching rules.

Each rule corresponds to an ADR-014 accounting logic rule.  Rules are
applied in priority order (1A → 1B → 2A → 2B → 3A → 3B → 4A → 4B).
When a match is found by a rule, lower-priority rules are skipped for
that bank transaction.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
import polars as pl


# Upper bound on the candidate set handed to the brute-force subset-sum search
# (rules 2A/2B). Without it, the search is combinations(len(candidates), r) for
# r up to max_items, i.e. C(N, 5) — an unbounded compute cliff once a single
# date window holds thousands of ledger entries (a high-volume finding).
# The per-call worst case is C(cap, 5) and it is paid once per *unmatched*
# row, so the cap must stay small: C(24, 5) is ~42k (a few seconds across a
# realistic unmatched set) whereas C(48, 5) is ~1.7M (minutes) — the latter
# alone breached the matching task timeout under load. 24 closest-by-amount
# candidates is still ample headroom for genuine split-payment subsets, and
# real matches operate on far smaller sets, so this never engages in tests.
# Overridable via env for tuning.
_MAX_SUBSET_CANDIDATES = int(os.getenv("MAX_SUBSET_CANDIDATES", "24"))


@dataclass
class MatchResult:
    """A single bank→ledger match produced by one reconciliation rule."""

    bank_id: int
    ledger_id: int  # 0 when no specific ledger (partial / anomaly)
    status: str  # 'MATCHED' | 'PARTIAL_MATCH' | 'UNMATCHED'
    confidence: Decimal
    rule_id: str


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _row_decimal(row: Any, col: str) -> Decimal:
    """Extract a NUMERIC column as a Decimal, safely.

    Handles ``None`` values by treating them as ``Decimal('0')``.
    """
    val = getattr(row, col, None)
    if val is None:
        return Decimal("0")
    return val if isinstance(val, Decimal) else Decimal(str(val))


def _same_date(d1: Any, d2: Any) -> bool:
    """True when two date/datetime values share the same calendar day."""
    return str(d1) == str(d2)


def _amount_match(a: Decimal, b: Decimal, tolerance: Decimal) -> bool:
    """True when |a − b| ≤ tolerance."""
    return abs(a - b) <= tolerance


# ---------------------------------------------------------------------------
# Rule 1A — Exact UTR + Exact amount + Same date
# ---------------------------------------------------------------------------

def match_rule_1a(
    bank_rows: list[Any],
    ledger_rows: list[Any],
    *,
    max_tolerance: Decimal = Decimal("0.00"),
) -> list[MatchResult]:
    """Rule 1A: Exact UTR match + exact amount + same calendar date → MATCHED."""
    ledger_by_utr: dict[str, Any] = {}
    for row in ledger_rows:
        utr = getattr(row, "utr", None)
        if utr:
            ledger_by_utr[utr] = row

    results: list[MatchResult] = []
    for bank in bank_rows:
        utr = getattr(bank, "utr", None)
        if not utr:
            continue
        ledger = ledger_by_utr.get(utr)
        if ledger is None:
            continue
        if _amount_match(
            _row_decimal(bank, "amount"),
            _row_decimal(ledger, "amount"),
            max_tolerance,
        ) and _same_date(
            getattr(bank, "value_date", None),
            getattr(ledger, "ledger_date", None),
        ):
            results.append(
                MatchResult(
                    bank_id=bank.id,
                    ledger_id=ledger.id,
                    status="MATCHED",
                    confidence=Decimal("1.0"),
                    rule_id="RULE_1A",
                )
            )
    return results


# ---------------------------------------------------------------------------
# Rule 1B — Exact reference match + Exact amount + Same date
# ---------------------------------------------------------------------------

_REF_COLUMNS: tuple[str, ...] = (
    "neft_ref", "rtgs_ref", "imps_ref", "upi_ref",
)


def match_rule_1b(
    bank_rows: list[Any],
    ledger_rows: list[Any],
    *,
    max_tolerance: Decimal = Decimal("0.00"),
) -> list[MatchResult]:
    """Rule 1B: Exact NEFT/RTGS/IMPS/UPI ref + exact amount + same date → MATCHED."""
    # Index ledger rows by each reference column
    ledger_by_ref: dict[str, dict[str, Any]] = {
        ref_col: {} for ref_col in _REF_COLUMNS
    }
    for row in ledger_rows:
        for ref_col in _REF_COLUMNS:
            ref_val = getattr(row, ref_col, None)
            if ref_val:
                ledger_by_ref[ref_col][ref_val] = row

    results: list[MatchResult] = []
    for bank in bank_rows:
        for ref_col in _REF_COLUMNS:
            ref_val = getattr(bank, ref_col, None)
            if not ref_val:
                continue
            ledger = ledger_by_ref[ref_col].get(ref_val)
            if ledger is None:
                continue
            if _amount_match(
                _row_decimal(bank, "amount"),
                _row_decimal(ledger, "amount"),
                max_tolerance,
            ) and _same_date(
                getattr(bank, "value_date", None),
                getattr(ledger, "ledger_date", None),
            ):
                results.append(
                    MatchResult(
                        bank_id=bank.id,
                        ledger_id=ledger.id,
                        status="MATCHED",
                        confidence=Decimal("1.0"),
                        rule_id="RULE_1B",
                    )
                )
                break  # One ref match is sufficient
    return results


# ---------------------------------------------------------------------------
# Rule 2A — 1:N Settlement (one bank → many ledger)
# ---------------------------------------------------------------------------

def match_rule_2a(
    bank_rows: list[Any],
    ledger_rows: list[Any],
    *,
    max_tolerance: Decimal = Decimal("0.01"),
    date_window_days: int = 3,
) -> list[MatchResult]:
    """Rule 2A: Sum of N ledger amounts = one bank amount ± tolerance.

    All N ledger dates must fall within the bank value-date window.
    State transition: SCORED → PARTIAL_MATCH (holding).
    """
    results: list[MatchResult] = []

    for bank in bank_rows:
        bank_amount: Decimal = _row_decimal(bank, "amount")
        bank_date = getattr(bank, "value_date", None)
        if bank_date is None:
            continue

        # Collect candidate ledger entries within the date window
        candidates: list[tuple[int, Decimal]] = []
        running_sum: Decimal = Decimal("0")
        for ledger in ledger_rows:
            ledger_date = getattr(ledger, "ledger_date", None)
            if ledger_date is None:
                continue
            diff = bank_date - ledger_date
            if abs(diff.days) <= date_window_days:
                candidates.append(
                    (ledger.id, _row_decimal(ledger, "amount"))
                )
                running_sum += _row_decimal(ledger, "amount")

        # Try combinations of up to 5 ledger entries summing to bank amount
        subset_ids = _find_subset_sum(
            bank_amount, candidates, max_tolerance, max_items=5,
        )
        if subset_ids:
            results.append(
                MatchResult(
                    bank_id=bank.id,
                    ledger_id=0,  # 1:N — no single ledger
                    status="PARTIAL_MATCH",
                    confidence=Decimal("1.0"),
                    rule_id="RULE_2A",
                )
            )
    return results


# ---------------------------------------------------------------------------
# Rule 2B — N:1 Settlement (many bank → one ledger)
# ---------------------------------------------------------------------------

def match_rule_2b(
    bank_rows: list[Any],
    ledger_rows: list[Any],
    *,
    max_tolerance: Decimal = Decimal("0.01"),
    date_window_days: int = 3,
) -> list[MatchResult]:
    """Rule 2B: One ledger amount = sum of N bank amounts ± tolerance.

    State transition: SCORED → PARTIAL_MATCH (holding).
    """
    results: list[MatchResult] = []

    for ledger in ledger_rows:
        ledger_amount: Decimal = _row_decimal(ledger, "amount")
        ledger_date = getattr(ledger, "ledger_date", None)
        if ledger_date is None:
            continue

        candidates: list[tuple[int, Decimal]] = []
        for bank in bank_rows:
            bank_date = getattr(bank, "value_date", None)
            if bank_date is None:
                continue
            diff = ledger_date - bank_date
            if abs(diff.days) <= date_window_days:
                candidates.append(
                    (bank.id, _row_decimal(bank, "amount"))
                )

        subset_ids = _find_subset_sum(
            ledger_amount, candidates, max_tolerance, max_items=5,
        )
        if subset_ids:
            results.append(
                MatchResult(
                    bank_id=0,  # N:1 — multiple banks
                    ledger_id=ledger.id,
                    status="PARTIAL_MATCH",
                    confidence=Decimal("1.0"),
                    rule_id="RULE_2B",
                )
            )
    return results


# ---------------------------------------------------------------------------
# Rule 3A — Value date ±3 days + amount ±₹0.01 + IFSC exact + fuzzy description
# ---------------------------------------------------------------------------

def match_rule_3a(
    bank_rows: list[Any],
    ledger_rows: list[Any],
    *,
    max_tolerance: Decimal = Decimal("0.01"),
    date_window_days: int = 3,
    similarity_threshold: float = 0.90,
) -> list[MatchResult]:
    """Rule 3A: IFSC + date window + amount ±₹0.01 + fastText cos-sim ≥ 0.90.

    Confidence = 0.92.  HITL if < 0.99.
    """
    # Load fastText once for sentence embeddings
    from app.ml_worker_init import get_categorization_model

    ft_model: Any = get_categorization_model()

    results: list[MatchResult] = []
    for bank in bank_rows:
        ifsc = getattr(bank, "ifsc", None)
        if not ifsc:
            continue
        bank_amount: Decimal = _row_decimal(bank, "amount")
        bank_date = getattr(bank, "value_date", None)
        bank_desc: str = getattr(bank, "description_raw", "") or ""

        for ledger in ledger_rows:
            if getattr(ledger, "ifsc", None) != ifsc:
                continue
            if not _amount_match(
                bank_amount, _row_decimal(ledger, "amount"), max_tolerance,
            ):
                continue
            ledger_date = getattr(ledger, "ledger_date", None)
            if ledger_date is None:
                continue
            diff = bank_date - ledger_date
            if abs(diff.days) > date_window_days:
                continue

            # Cosine similarity via fastText sentence vectors
            ledger_desc: str = getattr(ledger, "description", "") or ""
            sim = _fasttext_cosine(ft_model, bank_desc, ledger_desc)
            if sim >= similarity_threshold:
                results.append(
                    MatchResult(
                        bank_id=bank.id,
                        ledger_id=ledger.id,
                        status="MATCHED",
                        confidence=Decimal("0.92"),
                        rule_id="RULE_3A",
                    )
                )
                break  # One match per bank
    return results


# ---------------------------------------------------------------------------
# Rule 3B — Value date ±3 days + amount ±₹0.01 + no IFSC + fastText category
# ---------------------------------------------------------------------------

def match_rule_3b(
    bank_rows: list[Any],
    ledger_rows: list[Any],
    *,
    max_tolerance: Decimal = Decimal("0.01"),
    date_window_days: int = 3,
) -> list[MatchResult]:
    """Rule 3B: No IFSC present; match on date window + amount + category.

    Confidence = 0.88.  HITL if < 0.99.
    """
    from app.ml_worker_init import get_categorization_model

    ft_model: Any = get_categorization_model()

    results: list[MatchResult] = []
    for bank in bank_rows:
        if getattr(bank, "ifsc", None):
            continue  # Only applies when IFSC is absent
        bank_amount: Decimal = _row_decimal(bank, "amount")
        bank_date = getattr(bank, "value_date", None)
        bank_desc: str = getattr(bank, "description_raw", "") or ""
        bank_cat = _classify_single(ft_model, bank_desc)

        for ledger in ledger_rows:
            if not _amount_match(
                bank_amount, _row_decimal(ledger, "amount"), max_tolerance,
            ):
                continue
            ledger_date = getattr(ledger, "ledger_date", None)
            if ledger_date is None:
                continue
            diff = bank_date - ledger_date
            if abs(diff.days) > date_window_days:
                continue

            ledger_desc: str = getattr(ledger, "description", "") or ""
            ledger_cat = _classify_single(ft_model, ledger_desc)

            if bank_cat == ledger_cat and bank_cat != "UNKNOWN":
                results.append(
                    MatchResult(
                        bank_id=bank.id,
                        ledger_id=ledger.id,
                        status="MATCHED",
                        confidence=Decimal("0.88"),
                        rule_id="RULE_3B",
                    )
                )
                break
    return results


# ---------------------------------------------------------------------------
# Rule 4A — Value date ±3 days + amount ±₹0.01 + nothing else
# ---------------------------------------------------------------------------

def match_rule_4a(
    bank_rows: list[Any],
    ledger_rows: list[Any],
    *,
    max_tolerance: Decimal = Decimal("0.01"),
    date_window_days: int = 3,
) -> list[MatchResult]:
    """Rule 4A: Only date window + amount match; no refs, no IFSC, no category.

    Confidence = 0.75.  Mandatory HITL review.
    """
    # Pre-filter: only banks with no references and no IFSC
    eligible_banks: list[Any] = [
        b for b in bank_rows
        if not any(
            getattr(b, col, None)
            for col in ("utr", "neft_ref", "rtgs_ref", "imps_ref", "upi_ref", "ifsc")
        )
    ]

    results: list[MatchResult] = []
    matched_bank_ids: set[int] = set()

    for bank in eligible_banks:
        bank_amount: Decimal = _row_decimal(bank, "amount")
        bank_date = getattr(bank, "value_date", None)

        for ledger in ledger_rows:
            if not _amount_match(
                bank_amount, _row_decimal(ledger, "amount"), max_tolerance,
            ):
                continue
            ledger_date = getattr(ledger, "ledger_date", None)
            if ledger_date is None:
                continue
            diff = bank_date - ledger_date
            if abs(diff.days) <= date_window_days:
                results.append(
                    MatchResult(
                        bank_id=bank.id,
                        ledger_id=ledger.id,
                        status="PARTIAL_MATCH",
                        confidence=Decimal("0.75"),
                        rule_id="RULE_4A",
                    )
                )
                matched_bank_ids.add(bank.id)
                break
    return results


# ---------------------------------------------------------------------------
# Rule 4B — LightGBM risk_score ≥ 0.80 → HITL
# ---------------------------------------------------------------------------

def match_rule_4b(
    bank_rows: list[Any],
) -> list[MatchResult]:
    """Rule 4B: High anomaly risk → PARTIAL_MATCH, mandatory HITL.

    Confidence = 0.70.  Does NOT consume a ledger match.
    """
    results: list[MatchResult] = []
    for bank in bank_rows:
        risk: float = float(getattr(bank, "risk_score", 0) or 0)
        if risk >= 0.80:
            results.append(
                MatchResult(
                    bank_id=bank.id,
                    ledger_id=0,
                    status="PARTIAL_MATCH",
                    confidence=Decimal("0.70"),
                    rule_id="RULE_4B",
                )
            )
    return results


# ---------------------------------------------------------------------------
# Subset-sum solver (limited to small N for performance)
# ---------------------------------------------------------------------------

def _find_subset_sum(
    target: Decimal,
    items: list[tuple[int, Decimal]],
    tolerance: Decimal,
    max_items: int = 5,
) -> list[int]:
    """Return IDs of up to *max_items* items summing to *target*.

    Brute-force over combinations.  For large datasets restrict
    *items* to a pre-filtered candidate set upstream.

    When the candidate set exceeds ``_MAX_SUBSET_CANDIDATES`` the search is
    bounded instead of exploding as C(N, max_items): candidates are first
    restricted to those that could actually participate in a non-cancelling
    subset (same sign as *target*, magnitude not exceeding it), then the
    largest by magnitude are kept. This preserves exact single-entry matches
    unconditionally (an amount equal to *target* is always the largest
    survivor) and biases toward the large components that form low-
    cardinality splits. Mixed-sign cancellation subsets are out of scope
    once the guard engages — an accepted trade-off, since without the bound
    a single dense date window stalls the whole matching task.
    """
    from itertools import combinations

    if items is not None and len(items) > _MAX_SUBSET_CANDIDATES:
        negative_target = target < 0
        bound = abs(target) + tolerance
        viable = [
            it for it in items
            if (it[1] < 0) == negative_target and abs(it[1]) <= bound
        ]
        viable.sort(key=lambda it: abs(it[1]), reverse=True)
        items = viable[:_MAX_SUBSET_CANDIDATES]

    n = min(len(items), max_items)
    for r in range(1, n + 1):
        for combo in combinations(items, r):
            total = sum(item[1] for item in combo)
            if abs(total - target) <= tolerance:
                return [item[0] for item in combo]
    return []


# ---------------------------------------------------------------------------
# fastText helpers
# ---------------------------------------------------------------------------

def _classify_single(ft_model: Any, text: str) -> str:
    """Return the fastText category label for a single text string."""
    clean: str = (text or "").replace("\n", " ")
    labels: list[str]
    labels, _ = ft_model.predict(clean, k=1)
    return labels[0].replace("__label__", "") if labels else "UNKNOWN"


def _fasttext_cosine(
    ft_model: Any, text_a: str, text_b: str,
) -> float:
    """Cosine similarity between two strings via fastText sentence vectors."""
    import numpy as np

    a: str = (text_a or "").replace("\n", " ")
    b: str = (text_b or "").replace("\n", " ")
    vec_a: np.ndarray = ft_model.get_sentence_vector(a)
    vec_b: np.ndarray = ft_model.get_sentence_vector(b)
    dot = float(np.dot(vec_a, vec_b))
    norm_a: float = float(np.linalg.norm(vec_a))
    norm_b: float = float(np.linalg.norm(vec_b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)

def create_value_date_window(
    lf: pl.LazyFrame,
    date_column: str,
    window_days: int = 3,
) -> pl.LazyFrame:
    return lf.with_columns(
        [
            (pl.col(date_column) - pl.duration(days=window_days)).alias(
                "window_start"
            ),
            (pl.col(date_column) + pl.duration(days=window_days)).alias(
                "window_end"
            ),
        ]
    )

def match_within_value_date_window(
    bank_lf: pl.LazyFrame,
    ledger_lf: pl.LazyFrame,
    amount_col: str = "amount_paisa",
    window_days: int = 3,
) -> pl.LazyFrame:
    bank_with_window: pl.LazyFrame = create_value_date_window(
        bank_lf, "value_date", window_days
    )

    return (
        bank_with_window.join(
            ledger_lf.select([amount_col, "ledger_date", "ledger_id"]),
            how="inner",
            left_on=[amount_col],
            right_on=[amount_col],
        )
        .filter(
            (pl.col("ledger_date") >= pl.col("window_start"))
            & (pl.col("ledger_date") <= pl.col("window_end"))
        )
        .select(
            [
                "bank_id",
                "ledger_id",
                amount_col,
                "value_date",
                "ledger_date",
                "window_start",
                "window_end",
            ]
        )
    )
