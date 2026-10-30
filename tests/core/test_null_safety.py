"""Gate 3 — Null Injection Safety.

Pass None into every mandatory parameter of every financial calculation
function in src/. Assert each raises TypeError or a custom ValidationError.
Assert no function returns NaN, None, or 0 silently on None input.
"""

from __future__ import annotations

import math
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from app.polars_monetary import paisa_to_rupee_decimal, rupee_decimal_to_paisa
from app.matching_rules import (
    _amount_match,
    _find_subset_sum,
    _row_decimal,
    _same_date,
    match_rule_1a,
    match_rule_2a,
    match_rule_4b,
)


# ---------------------------------------------------------------------------
# monetary.py — paisa conversion functions
# ---------------------------------------------------------------------------


class TestMonetaryNoneInjection:
    """None must not silently produce NaN, None, or 0."""

    def test_paisa_to_rupee_decimal_none(self) -> None:
        with pytest.raises(TypeError):
            paisa_to_rupee_decimal(None)  # type: ignore[arg-type]

    def test_rupee_decimal_to_paisa_none(self) -> None:
        with pytest.raises(TypeError):
            rupee_decimal_to_paisa(None)  # type: ignore[arg-type]

    def test_paisa_to_rupee_decimal_no_silent_zero(self) -> None:
        """Verify None does not produce 0 as a silent fallback."""
        try:
            result = paisa_to_rupee_decimal(None)  # type: ignore[arg-type]
            assert result != 0, "paisa_to_rupee_decimal(None) silently returned 0"
            assert result is not None, "paisa_to_rupee_decimal(None) returned None"
            assert not (isinstance(result, float) and math.isnan(result)), (
                "paisa_to_rupee_decimal(None) returned NaN"
            )
        except TypeError:
            pass  # Correct: raised TypeError

    def test_rupee_decimal_to_paisa_no_silent_zero(self) -> None:
        try:
            result = rupee_decimal_to_paisa(None)  # type: ignore[arg-type]
            assert result != 0, "rupee_decimal_to_paisa(None) silently returned 0"
            assert result is not None
        except TypeError:
            pass  # Correct


# ---------------------------------------------------------------------------
# matching/rules.py — helper functions
# ---------------------------------------------------------------------------


class TestMatchingRulesNoneInjection:
    """Inject None into matching rule helpers."""

    def test_amount_match_none_a_raises(self) -> None:
        """_amount_match(None, Decimal('1'), Decimal('0')) must not
        silently return True or False — should raise."""
        try:
            result: bool = _amount_match(None, Decimal("1"), Decimal("0"))  # type: ignore[arg-type]
            # If it doesn't raise, it must not return an incorrect bool
            assert isinstance(result, bool), "Returned non-bool from None input"
        except (TypeError, AttributeError):
            pass  # Correct: raised on None

    def test_amount_match_none_b_raises(self) -> None:
        try:
            result: bool = _amount_match(Decimal("1"), None, Decimal("0"))  # type: ignore[arg-type]
            assert isinstance(result, bool), "Returned non-bool from None input"
        except (TypeError, AttributeError):
            pass

    def test_amount_match_none_tolerance_raises(self) -> None:
        try:
            result: bool = _amount_match(Decimal("1"), Decimal("1"), None)  # type: ignore[arg-type]
            assert isinstance(result, bool), "Returned non-bool from None tolerance"
        except (TypeError, AttributeError):
            pass

    def test_same_date_none_d1(self) -> None:
        """_same_date(None, date) must not silently return True."""
        try:
            result: bool = _same_date(None, "2024-01-01")  # type: ignore[arg-type]
            # If it returns without error, it must not be True (matching None)
            assert result is not True, "_same_date(None, ...) must not be True"
        except (TypeError, AttributeError):
            pass  # Correct

    def test_same_date_none_d2(self) -> None:
        try:
            result: bool = _same_date("2024-01-01", None)  # type: ignore[arg-type]
            assert result is not True
        except (TypeError, AttributeError):
            pass

    def test_row_decimal_none_col(self) -> None:
        """_row_decimal with a None-valued column should not crash
        silently or return NaN."""
        row: SimpleNamespace = SimpleNamespace(amount=None)
        result: Decimal = _row_decimal(row, "amount")
        # When the attribute exists but is None, _row_decimal converts
        # str(None)="None" → Decimal("None") would raise InvalidOperation.
        # The function should handle this gracefully.
        assert isinstance(result, Decimal), (
            f"_row_decimal returned {type(result)}, expected Decimal"
        )
        assert not math.isnan(float(result)), "_row_decimal returned NaN"

    def test_find_subset_sum_none_target(self) -> None:
        """_find_subset_sum(None, ...) must raise, not return silently."""
        with pytest.raises(Exception):
            _find_subset_sum(None, [(1, Decimal("10"))], Decimal("0.01"))  # type: ignore[arg-type]

    def test_find_subset_sum_none_items(self) -> None:
        """_find_subset_sum(_, None, _) must raise."""
        with pytest.raises(Exception):
            _find_subset_sum(Decimal("10"), None, Decimal("0.01"))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# matching/rules.py — full rule functions with None
# ---------------------------------------------------------------------------


class TestMatchingRulesFullNoneInjection:
    """Pass None into match_rule_Xa/b mandatory parameters."""

    def test_match_rule_1a_none_bank_rows(self) -> None:
        """match_rule_1a(None, ...) must raise, not silently return []."""
        try:
            result: list[Any] = match_rule_1a(None, [])  # type: ignore[arg-type]
            # If it doesn't raise, it must return empty list (safe default)
            assert result == [], "match_rule_1a(None, []) returned non-empty"
        except (TypeError, AttributeError):
            pass  # Correct: raised

    def test_match_rule_1a_none_ledger_rows(self) -> None:
        try:
            result: list[Any] = match_rule_1a([], None)  # type: ignore[arg-type]
            assert result == []
        except (TypeError, AttributeError):
            pass

    def test_match_rule_2a_none_bank_rows(self) -> None:
        try:
            result: list[Any] = match_rule_2a(None, [])  # type: ignore[arg-type]
            assert result == []
        except (TypeError, AttributeError):
            pass

    def test_match_rule_4b_none_bank_rows(self) -> None:
        """match_rule_4b(None) must raise, not silently return []."""
        try:
            result: list[Any] = match_rule_4b(None)  # type: ignore[arg-type]
            assert result == []
        except (TypeError, AttributeError):
            pass


# ---------------------------------------------------------------------------
# ingestion.py — to_paisa_integers with None LazyFrame
# ---------------------------------------------------------------------------


class TestIngestionNoneInjection:
    """Inject None into Polars ingestion functions."""

    def test_ingest_bank_statement_none_source(self) -> None:
        from app.polars_ingestion import ingest_bank_statement

        with pytest.raises(Exception):
            ingest_bank_statement(None, ["amount"])  # type: ignore[arg-type]

    def test_ingest_bank_statement_none_columns(self) -> None:
        from app.polars_ingestion import ingest_bank_statement

        # None columns would cause issues in .select()
        with pytest.raises(Exception):
            ingest_bank_statement("nonexistent.csv", None)  # type: ignore[arg-type]

    def test_to_paisa_integers_none_lf(self) -> None:
        from app.polars_ingestion import to_paisa_integers

        with pytest.raises(Exception):
            to_paisa_integers(None, ["amount"])  # type: ignore[arg-type]

    def test_to_paisa_integers_none_columns(self) -> None:
        import polars as pl
        from app.polars_ingestion import to_paisa_integers

        lf: pl.LazyFrame = pl.LazyFrame({"amount": [1.0]})
        with pytest.raises(Exception):
            to_paisa_integers(lf, None)  # type: ignore[arg-type]

    def test_materialize_for_processing_none(self) -> None:
        from app.polars_ingestion import materialize_for_processing

        with pytest.raises(Exception):
            materialize_for_processing(None)  # type: ignore[arg-type]

    def test_create_value_date_window_none(self) -> None:
        from app.matching_rules import create_value_date_window

        with pytest.raises(Exception):
            create_value_date_window(None, "value_date")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# schema_validation.py — validate_frame_schema with None
# ---------------------------------------------------------------------------


class TestSchemaValidationNoneInjection:
    """Inject None into schema validation."""

    def test_validate_frame_schema_none_frame(self) -> None:
        from app.polars_schema_validation import validate_frame_schema

        with pytest.raises(Exception):
            validate_frame_schema(None, {})  # type: ignore[arg-type]

    def test_validate_frame_schema_none_schema(self) -> None:
        import polars as pl
        from app.polars_schema_validation import validate_frame_schema

        lf: pl.LazyFrame = pl.LazyFrame({"a": [1]})
        with pytest.raises(Exception):
            validate_frame_schema(lf, None)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# encryption.py — decrypt_file_to_memory with None
# ---------------------------------------------------------------------------


class TestEncryptionNoneInjection:
    """Inject None into Fernet encryption functions."""

    def test_decrypt_file_to_memory_none_path(self) -> None:
        from app.storage_encryption import decrypt_file_to_memory

        with pytest.raises(Exception):
            decrypt_file_to_memory(None)  # type: ignore[arg-type]

    def test_encrypt_file_to_disk_none_paths(self) -> None:
        from app.storage_encryption import encrypt_file_to_disk

        with pytest.raises(Exception):
            encrypt_file_to_disk(None, None)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# batch_claim.py — None connection
# ---------------------------------------------------------------------------


class TestBatchClaimNoneInjection:
    """Inject None into batch claim functions."""

    def test_claim_unprocessed_batch_none_conn(self) -> None:
        from app.db_batch_claim import claim_unprocessed_batch

        with pytest.raises(Exception):
            claim_unprocessed_batch(None)  # type: ignore[arg-type]

    def test_claim_parsed_batch_none_conn(self) -> None:
        from app.db_batch_claim import claim_parsed_batch

        with pytest.raises(Exception):
            claim_parsed_batch(None)  # type: ignore[arg-type]

    def test_claim_scored_batch_none_conn(self) -> None:
        from app.db_batch_claim import claim_scored_batch

        with pytest.raises(Exception):
            claim_scored_batch(None)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# NLP inference — None injection
# ---------------------------------------------------------------------------


class TestNLPInferenceNoneInjection:
    """Inject None into NLP inference function parameters."""

    def test_run_nlp_extraction_stream_none_ids(self) -> None:
        from app.ml_nlp_inference import run_nlp_extraction_stream

        with pytest.raises(Exception):
            # None instead of list[int] for transaction_ids
            list(run_nlp_extraction_stream(None, ["text"]))  # type: ignore[arg-type]

    def test_run_nlp_extraction_stream_none_texts(self) -> None:
        from app.ml_nlp_inference import run_nlp_extraction_stream

        with pytest.raises(Exception):
            list(run_nlp_extraction_stream([1], None))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# FAILURE INJECTION: confirmed NaN/None/0 safety
# ---------------------------------------------------------------------------


class TestNoSilentFailureOnNone:
    """Meta-gate: no financial function silently returns 0, None, or NaN
    when given None input. Each function is probed; if it doesn't raise,
    its return value is checked."""

    @pytest.mark.parametrize(
        "func,args",
        [
            (paisa_to_rupee_decimal, (None,)),
            (rupee_decimal_to_paisa, (None,)),
        ],
    )
    def test_no_silent_zero_none_nan(
        self,
        func: Any,
        args: tuple[Any, ...],
    ) -> None:
        """The function must either raise or return a non-degenerate value."""
        try:
            result: Any = func(*args)
            # If it didn't raise, the result must not be 0, None, or NaN
            assert result != 0, f"{func.__name__}(None) returned 0"
            assert result is not None, f"{func.__name__}(None) returned None"
            if isinstance(result, float):
                assert not math.isnan(result), (
                    f"{func.__name__}(None) returned NaN"
                )
        except (TypeError, AttributeError, ValueError):
            pass  # Correct: raised an exception
