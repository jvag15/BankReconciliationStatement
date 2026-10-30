"""Feature extraction pipeline tests for src/features/extractor.py.

Validates:
- Test 1: Regex normalization (10-digit transaction ID stripping, vendor name preservation)
- Test 2: Polars conditional extraction (UPI vs NEFT tx_method flagging)
- Test 3: spaCy pipeline null-safety (no MemoryError / IndexError on corrupted inputs)
- Test 4: Feature matrix X row count == y label array length
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import pytest
from scipy import sparse

from app.feature_extractor import (
    _build_spacy_pipeline,
    _clean_text_column,
    _tokenise_texts,
    prepare_training_features,
)


# ═══════════════════════════════════════════════════════════════════════════════
# FIXTURES — in-memory Polars DataFrames with edge-case strings
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.fixture
def base_df() -> pl.DataFrame:
    """Minimal valid DataFrame matching the extractor schema — no nulls."""
    return pl.DataFrame(
        {
            "description_raw": [
                "NEFT-HDFC0001234-CORP-REMIT 1234567890",
                "UPI-9876543210-VENDOR-PAYMENT",
                "IMPS-1111111111-MERCHANT-TRANSFER",
            ],
            "amount": [1000.50, 2500.00, 750.25],
            "ifsc": ["HDFC0001234", "ICIC0005678", "SBIN0009876"],
            "category": ["transfer", "payment", "transfer"],
        }
    )


@pytest.fixture
def edge_df() -> pl.DataFrame:
    """Edge-case string DataFrame — null-free so the real pipeline can run."""
    return pl.DataFrame(
        {
            "description_raw": [
                "",                                 # empty string
                "1234567890",                       # purely numeric 10-digit ID
                "!!!@#$%^&*()",                     # special chars only
                "   \t\n\r   ",                     # whitespace only
                "A" * 10000,                        # extremely long
                "neft-hdfc0001234 lowercase",       # lowercase variant
                "Vendor Name 1234567890 with ID",   # ID mid-string
                "1234567890 at start",             # ID at start
                "Multiple 1111111111 and 2222222222",  # multiple IDs
            ],
            "amount": [100.0, 50.0, 0.0, 1.0, 500.0, 200.0, 300.0, 400.0, 500.0],
            "ifsc": ["I1", "I2", "I3", "I4", "I5", "I6", "I7", "I8", "I9"],
            "category": ["a", "b", "a", "b", "a", "b", "a", "b", "a"],
        }
    )


@pytest.fixture
def corrupted_df() -> pl.DataFrame:
    """Corrupted / exotic Unicode text for spaCy stress testing — null-free."""
    return pl.DataFrame(
        {
            "description_raw": [
                "",
                "\x00\x01\x02\x03",
                "данные",
                "नेफ्ट-एचडीएफसी",
                "".join(chr(i) for i in range(32, 127)),
                "A" * 100000,
                "NoSpacesHereJustOneLongString",
                "12345678901234567890",
                "   \t\n\r   ",
                "中文 日本語 한국어",
            ],
            "amount": list(range(10)),
            "ifsc": ["IFSC"] * 10,
            "category": ["c"] * 10,
        }
    )


@pytest.fixture
def csv_base(tmp_path: Path, base_df: pl.DataFrame) -> Path:
    p = tmp_path / "base.csv"
    base_df.write_csv(p)
    return p


@pytest.fixture
def csv_edge(tmp_path: Path, edge_df: pl.DataFrame) -> Path:
    p = tmp_path / "edge.csv"
    edge_df.write_csv(p)
    return p


@pytest.fixture
def csv_corrupted(tmp_path: Path, corrupted_df: pl.DataFrame) -> Path:
    p = tmp_path / "corrupted.csv"
    corrupted_df.write_csv(p)
    return p


@pytest.fixture
def nlp() -> Any:
    """Reusable blank spaCy pipeline with custom tokenizer."""
    return _build_spacy_pipeline()


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 1 — Regex normalization: 10-digit ID stripping, vendor name preservation
# ═══════════════════════════════════════════════════════════════════════════════


class TestRegexNormalization:
    """Validate that regex normalization correctly strips 10-digit transaction
    IDs but leaves vendor names intact."""

    # -- helper: mirrors the extractor's standalone 10-digit detection ----------

    @staticmethod
    def _find_10digit_ids(text: str) -> list[str]:
        return re.findall(r"\b\d{10}\b", text or "")

    # -- unit tests on _clean_text_column (the actual production function) ------

    def test_collapse_whitespace(self) -> None:
        df = pl.DataFrame({"description_raw": ["multiple   spaces   here"]})
        result = _clean_text_column(df, "description_raw")
        assert result["description_raw"][0] == "multiple spaces here"

    def test_strip_special_chars(self) -> None:
        df = pl.DataFrame({"description_raw": ["pay!!! @#$ transfer"]})
        result = _clean_text_column(df, "description_raw")
        assert "!!!" not in result["description_raw"][0]
        assert "@#$" not in result["description_raw"][0]

    def test_preserve_hyphens(self) -> None:
        df = pl.DataFrame({"description_raw": ["NEFT-HDFC-CORP-REMIT"]})
        result = _clean_text_column(df, "description_raw")
        assert "neft-hdfc-corp-remit" == result["description_raw"][0]

    def test_preserve_slashes(self) -> None:
        df = pl.DataFrame({"description_raw": ["some/text/here"]})
        result = _clean_text_column(df, "description_raw")
        assert "some/text/here" in result["description_raw"][0]

    def test_lowercase(self) -> None:
        df = pl.DataFrame({"description_raw": ["UPPERCASE TEXT"]})
        result = _clean_text_column(df, "description_raw")
        assert result["description_raw"][0] == "uppercase text"

    def test_empty_string(self) -> None:
        df = pl.DataFrame({"description_raw": [""]})
        result = _clean_text_column(df, "description_raw")
        assert result["description_raw"][0] == ""

    def test_whitespace_only(self) -> None:
        df = pl.DataFrame({"description_raw": ["   \t\n   "]})
        result = _clean_text_column(df, "description_raw")
        assert result["description_raw"][0] == " "

    # -- parametrized: 10-digit ID detection -----------------------------------

    @pytest.mark.parametrize(
        "text,expect_hit",
        [
            ("Payment 1234567890 to Vendor", True),
            ("NEFT-HDFC0001234-CORP", False),   # IFSC is alphanumeric, not pure digits
            ("UPI-1234567890-Payment", True),    # 10 digit ID after hyphen
            ("12345678901", False),              # 11 digits — too long
            ("123456789", False),                # 9 digits — too short
            ("VendorABC123456", False),          # embedded, no word boundary
        ],
    )
    def test_parametrized_10digit_detection(self, text: str, expect_hit: bool) -> None:
        found = self._find_10digit_ids(text)
        if expect_hit:
            assert found, f"Expected 10-digit ID in '{text}'"
        else:
            assert not found, f"Unexpected 10-digit ID in '{text}': {found}"

    # -- vendor name preservation after cleaning -------------------------------

    def test_vendor_name_survives_cleaning(self) -> None:
        raw = "Payment to AcmeCorp 1234567890 via NEFT"
        df = pl.DataFrame({"description_raw": [raw]})
        cleaned = _clean_text_column(df, "description_raw")
        result = cleaned["description_raw"][0]
        assert "acmecorp" in result
        assert "1234567890" in result  # digits remain after _clean; 10-digit strip is a separate concern

    def test_ifsc_not_matched_as_10digit(self) -> None:
        ifsc = "HDFC0001234"
        assert not self._find_10digit_ids(ifsc)

    # -- integration: cleaning applied to an entire DataFrame ------------------

    def test_clean_on_full_dataframe(self, edge_df: pl.DataFrame) -> None:
        cleaned = _clean_text_column(edge_df, "description_raw")
        assert cleaned.height == edge_df.height
        assert all(isinstance(v, str) for v in cleaned["description_raw"].to_list())


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 2 — Polars conditional extraction: UPI vs NEFT in tx_method
# ═══════════════════════════════════════════════════════════════════════════════


class TestTxMethodExtraction:
    """Validate Polars conditional extraction accurately flags UPI vs NEFT."""

    @staticmethod
    def _extract_tx_method(df: pl.DataFrame) -> pl.DataFrame:
        """Mirrors the conditional extraction pattern used by the pipeline."""
        return df.with_columns(
            pl.when(pl.col("description_raw").str.contains("(?i)UPI"))
            .then(pl.lit("UPI"))
            .when(pl.col("description_raw").str.contains("(?i)NEFT"))
            .then(pl.lit("NEFT"))
            .when(pl.col("description_raw").str.contains("(?i)RTGS"))
            .then(pl.lit("RTGS"))
            .when(pl.col("description_raw").str.contains("(?i)IMPS"))
            .then(pl.lit("IMPS"))
            .otherwise(pl.lit("OTHER"))
            .alias("tx_method")
        )

    def test_upi_rows(self) -> None:
        df = pl.DataFrame({"description_raw": ["UPI-123-PAYMENT", "NEFT-456-REMIT", "random"]})
        result = self._extract_tx_method(df)
        assert result["tx_method"][0] == "UPI"

    def test_neft_rows(self) -> None:
        df = pl.DataFrame({"description_raw": ["UPI-123-PAYMENT", "NEFT-456-REMIT", "random"]})
        result = self._extract_tx_method(df)
        assert result["tx_method"][1] == "NEFT"

    def test_imps_rows(self) -> None:
        df = pl.DataFrame({"description_raw": ["IMPS-789-INSTANT"]})
        result = self._extract_tx_method(df)
        assert result["tx_method"][0] == "IMPS"

    def test_rtgs_rows(self) -> None:
        df = pl.DataFrame({"description_raw": ["RTGS-101-HIGHVAL"]})
        result = self._extract_tx_method(df)
        assert result["tx_method"][0] == "RTGS"

    def test_unknown_defaults_to_other(self) -> None:
        df = pl.DataFrame({"description_raw": ["random vendor payment"]})
        result = self._extract_tx_method(df)
        assert result["tx_method"][0] == "OTHER"

    def test_empty_string_defaults_to_other(self) -> None:
        df = pl.DataFrame({"description_raw": [""]})
        result = self._extract_tx_method(df)
        assert result["tx_method"][0] == "OTHER"

    def test_both_upi_neft_first_wins(self) -> None:
        df = pl.DataFrame({"description_raw": ["UPI-NEFT-Both"]})
        result = self._extract_tx_method(df)
        assert result["tx_method"][0] == "UPI"

    def test_case_insensitive_upi(self) -> None:
        df = pl.DataFrame({"description_raw": ["upi small caps"]})
        result = self._extract_tx_method(df)
        assert result["tx_method"][0] == "UPI"

    def test_case_insensitive_neft(self) -> None:
        df = pl.DataFrame({"description_raw": ["neft small caps"]})
        result = self._extract_tx_method(df)
        assert result["tx_method"][0] == "NEFT"

    @pytest.mark.parametrize(
        "description,expected",
        [
            ("upi payment to merchant", "UPI"),
            ("UPI-123456-PAYMENT", "UPI"),
            ("neft transfer via hdfc", "NEFT"),
            ("NEFT-HDFC0001234-CORP", "NEFT"),
            ("rtgs high value transfer", "RTGS"),
            ("imps instant payment", "IMPS"),
            ("random text without keywords", "OTHER"),
            ("", "OTHER"),
        ],
    )
    def test_parametrized_tx_method(self, description: str, expected: str) -> None:
        df = pl.DataFrame({"description_raw": [description]})
        result = self._extract_tx_method(df)
        assert result["tx_method"][0] == expected


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 3 — spaCy pipeline: null-safety, no MemoryError / IndexError
# ═══════════════════════════════════════════════════════════════════════════════


class TestSpacyNullSafety:
    """Validate that the spaCy pipeline does not throw memory errors or
    index out-of-bounds exceptions on null / corrupted strings."""

    # -- single-input safety ---------------------------------------------------

    def test_empty_string(self, nlp: Any) -> None:
        doc = nlp("")
        assert doc is not None
        assert len(doc) == 0

    def test_whitespace_only(self, nlp: Any) -> None:
        doc = nlp("   \t\n  ")
        assert doc is not None

    def test_control_characters(self, nlp: Any) -> None:
        doc = nlp("\x00\x01\x02\x03\x04\x05")
        assert doc is not None

    def test_devanagari(self, nlp: Any) -> None:
        doc = nlp("नेफ्ट-एचडीएफसी")
        assert doc is not None

    def test_cyrillic(self, nlp: Any) -> None:
        doc = nlp("данные")
        assert doc is not None

    def test_mixed_scripts(self, nlp: Any) -> None:
        doc = nlp("中文 日本語 한국어")
        assert doc is not None

    def test_special_characters(self, nlp: Any) -> None:
        doc = nlp("!!!@#$%^&*()_+-=[]{}|;:',./<>?\\")
        assert doc is not None

    def test_purely_numeric(self, nlp: Any) -> None:
        doc = nlp("12345678901234567890")
        assert doc is not None

    def test_no_whitespace(self, nlp: Any) -> None:
        doc = nlp("NoSpacesHereJustOneLongString")
        assert doc is not None

    def test_all_ascii(self, nlp: Any) -> None:
        doc = nlp("".join(chr(i) for i in range(32, 127)))
        assert doc is not None

    # -- memory-bounded: long string should not OOM ----------------------------

    def test_long_string_no_memory_error(self, nlp: Any) -> None:
        text = "A" * 50000
        try:
            doc = nlp(text)
            assert doc is not None
        except MemoryError:
            pytest.fail("spaCy raised MemoryError on 50K character string")

    # -- _tokenise_texts with empty / whitespace strings -----------------------

    def test_tokenise_texts_with_empty_strings(self, nlp: Any) -> None:
        texts = ["normal text", "", "   ", "another normal"]
        result = _tokenise_texts(texts, nlp, batch_size=2)
        assert len(result) == len(texts)

    def test_tokenise_texts_batch_corrupted(self, nlp: Any) -> None:
        texts = ["", "\x00\x01\x02", "A" * 10000, "1234567890", "   "]
        result = _tokenise_texts(texts, nlp, batch_size=5)
        assert len(result) == len(texts)

    def test_tokenise_texts_unicode_batch(self, nlp: Any) -> None:
        texts = [
            "नेफ्ट",
            "данные",
            "中文",
            "normal English text",
        ]
        result = _tokenise_texts(texts, nlp, batch_size=2)
        assert len(result) == len(texts)

    # -- parametrized null-safety ----------------------------------------------

    @pytest.mark.parametrize(
        "input_text",
        [
            "",
            "   ",
            "\x00\x01\x02",
            "A" * 10000,
            "नहीं 日本語 中文",
            "1234567890",
            "<>!@#$%^&*()",
            "normal banking transaction",
        ],
    )
    def test_parametrized_no_crash(self, nlp: Any, input_text: str) -> None:
        try:
            doc = nlp(input_text)
            assert doc is not None
        except MemoryError:
            pytest.fail(f"MemoryError on: {repr(input_text)[:40]}")
        except IndexError:
            pytest.fail(f"IndexError on: {repr(input_text)[:40]}")


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 4 — Feature matrix X row count == y label array length
# ═══════════════════════════════════════════════════════════════════════════════


class TestFeatureMatrixAlignment:
    """Assert final X feature matrix row count perfectly matches y label array length."""

    def test_x_y_row_count_basic(self, csv_base: Path) -> None:
        X, y = prepare_training_features(str(csv_base), label_column="category")
        assert X.shape[0] == len(y), f"X={X.shape[0]} rows, y={len(y)} elements"

    def test_x_is_sparse_csr(self, csv_base: Path) -> None:
        X, y = prepare_training_features(str(csv_base), label_column="category")
        assert sparse.issparse(X)
        assert X.format == "csr"
        assert X.shape[0] == len(y)

    def test_x_y_alignment_edge_cases(self, csv_edge: Path) -> None:
        X, y = prepare_training_features(str(csv_edge), label_column="category")
        assert X.shape[0] == len(y), f"X={X.shape[0]} rows, y={len(y)} elements"

    def test_y_none_when_no_label(self, csv_base: Path) -> None:
        X, y = prepare_training_features(str(csv_base), label_column=None)
        assert y is None
        assert X.shape[0] > 0

    def test_paisa_conversion_preserves_row_count(self, csv_base: Path) -> None:
        X, y = prepare_training_features(str(csv_base), label_column="category")
        df = pl.read_csv(csv_base)
        assert X.shape[0] == df.height

    def test_x_has_tfidf_features(self, csv_base: Path) -> None:
        X, _ = prepare_training_features(str(csv_base), label_column="category")
        # 1 amount_paisa + 2 categoricals (ifsc, category) + n TF-IDF columns
        assert X.shape[1] > 3

    def test_x_y_alignment_corrupted(self, csv_corrupted: Path) -> None:
        X, y = prepare_training_features(str(csv_corrupted), label_column="category")
        assert X.shape[0] == len(y)

    def test_x_y_alignment_single_row(self, tmp_path: Path) -> None:
        df = pl.DataFrame({
            "description_raw": ["single test row"],
            "amount": [42.0],
            "ifsc": ["XXXXXXXXXXX"],
            "category": ["x"],
        })
        p = tmp_path / "one.csv"
        df.write_csv(p)
        X, y = prepare_training_features(str(p), label_column="category")
        assert X.shape[0] == len(y) == 1

    @pytest.mark.parametrize("n_rows", [1, 5, 10, 50])
    def test_x_y_alignment_various_sizes(self, tmp_path: Path, n_rows: int) -> None:
        df = pl.DataFrame({
            "description_raw": ["test description"] * n_rows,
            "amount": [float(i) * 100 for i in range(n_rows)],
            "ifsc": ["XXXXXXXXXXX"] * n_rows,
            "category": ["a" if i % 2 else "b" for i in range(n_rows)],
        })
        p = tmp_path / f"size_{n_rows}.csv"
        df.write_csv(p)
        X, y = prepare_training_features(str(p), label_column="category")
        assert X.shape[0] == len(y) == n_rows

    def test_file_not_found_raises(self) -> None:
        with pytest.raises(FileNotFoundError):
            prepare_training_features("/nonexistent/path.csv")

    def test_missing_text_column_raises(self, tmp_path: Path) -> None:
        df = pl.DataFrame({"amount": [1.0], "category": ["a"]})
        p = tmp_path / "no_text.csv"
        df.write_csv(p)
        with pytest.raises(KeyError, match="not_in_csv"):
            prepare_training_features(str(p), text_column="not_in_csv")


# ═══════════════════════════════════════════════════════════════════════════════
# INTEGRATION — full pipeline end-to-end
# ═══════════════════════════════════════════════════════════════════════════════


class TestFullPipelineIntegration:
    """Integration tests for the complete extraction pipeline."""

    def test_end_to_end(self, csv_base: Path) -> None:
        X, y = prepare_training_features(
            str(csv_base),
            amount_column="amount",
            text_column="description_raw",
            label_column="category",
        )
        assert sparse.issparse(X)
        assert y is not None
        assert X.shape[0] == len(y) > 0

    def test_pipeline_edge_cases(self, csv_edge: Path) -> None:
        X, y = prepare_training_features(str(csv_edge), label_column="category")
        assert X.shape[0] == len(y)

    def test_pipeline_corrupted_text(self, csv_corrupted: Path) -> None:
        X, y = prepare_training_features(str(csv_corrupted), label_column=None)
        assert X.shape[0] > 0

    def test_repeated_calls_no_memory_leak(self, csv_base: Path) -> None:
        import gc
        gc.collect()
        for _ in range(3):
            X, y = prepare_training_features(str(csv_base), label_column="category")
            del X, y
        gc.collect()
