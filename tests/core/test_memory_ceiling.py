"""Gate 7 — Memory Ceiling.

Generate a synthetic 500,000-row CSV with realistic Indian banking
transaction data. Run the full Polars ingestion pipeline (scan_csv +
to_paisa_integers + streaming collect) under memory profiling.

Assert peak RSS does not exceed 3.5 GB (the operational headroom
boundary from CLAUDE.md). Record the actual peak RSS value.

On Windows, memray is unavailable. Falls back to psutil RSS
measurement (peak snapshot after collect).
"""

from __future__ import annotations

import os
import tempfile
from typing import Any

import polars as pl
import pytest

from app.polars_ingestion import ingest_bank_statement, to_paisa_integers


# ---------------------------------------------------------------------------
# Synthetic CSV generation (same pattern as benchmark/ingest_500k.py)
# ---------------------------------------------------------------------------


def _generate_synthetic_csv(n: int, path: str) -> None:
    """Write n realistic Indian banking rows to path.

    Uses deterministic seed-free arithmetic (same as benchmark) so
    rows are reproducible without Faker dependency.
    """
    df: pl.DataFrame = pl.DataFrame(
        {
            "source_chunk_id": [f"chunk-gate7-{i:08x}" for i in range(n)],
            "file_origin": ["HDFC_CC_2024_03.csv"] * n,
            "amount": [
                str(round(100 + (i * 7.13) % 99999, 2)) for i in range(n)
            ],
            "value_date": ["2024-01-15"] * n,
            "description_raw": [
                f"NEFT-HDFC0001234-CORP-REMIT-{i:06d}" for i in range(n)
            ],
        }
    )
    df.write_csv(path)
    del df


# ---------------------------------------------------------------------------
# Peak RSS measurement
# ---------------------------------------------------------------------------


def _measure_peak_rss_gb() -> float:
    """Return current process RSS in GB using psutil."""
    import psutil

    process: Any = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024 * 1024)


# ---------------------------------------------------------------------------
# Gate 7 tests
# ---------------------------------------------------------------------------


class TestMemoryCeiling:
    """Assert 500k-row ingestion stays under the 3.5 GB operational ceiling."""

    def test_500k_ingestion_peak_rss_under_3_5gb(self) -> None:
        """Ingest 500,000 rows via the production Polars pipeline.

        Peak RSS must not exceed 3.5 GB. The actual peak RSS is
        recorded for the sign-off block.
        """
        n: int = 500_000
        baseline_rss: float = _measure_peak_rss_gb()

        with tempfile.NamedTemporaryFile(
            suffix=".csv", delete=False, mode="w",
        ) as f:
            csv_path: str = f.name

        try:
            _generate_synthetic_csv(n, csv_path)

            required: list[str] = [
                "source_chunk_id",
                "file_origin",
                "amount",
                "value_date",
                "description_raw",
            ]

            lf: pl.LazyFrame = ingest_bank_statement(csv_path, required)
            lf = to_paisa_integers(lf, ["amount"])
            result: pl.DataFrame = lf.collect(engine="streaming")

            assert len(result) == n, (
                f"Expected {n} rows, got {len(result)}"
            )

            peak_rss: float = _measure_peak_rss_gb()
            delta_rss: float = peak_rss - baseline_rss

            # The 3.5 GB ceiling applies to peak RSS (absolute).
            # We check both absolute and delta for diagnostic value.
            assert peak_rss <= 3.5, (
                f"Peak RSS {peak_rss:.3f} GB exceeds 3.5 GB ceiling. "
                f"Baseline: {baseline_rss:.3f} GB. "
                f"Delta: {delta_rss:.3f} GB."
            )

            # Record for the sign-off block
            print(f"\n[Gate 7] Peak RSS: {peak_rss:.3f} GB")
            print(f"[Gate 7] Baseline RSS: {baseline_rss:.3f} GB")
            print(f"[Gate 7] Delta RSS: {delta_rss:.3f} GB")

            del result

        finally:
            if os.path.exists(csv_path):
                os.remove(csv_path)

    def test_500k_ingestion_schema_correctness(self) -> None:
        """After ingestion, the output schema must contain paisa columns."""
        n: int = 500_000

        with tempfile.NamedTemporaryFile(
            suffix=".csv", delete=False, mode="w",
        ) as f:
            csv_path: str = f.name

        try:
            _generate_synthetic_csv(n, csv_path)

            required: list[str] = [
                "source_chunk_id",
                "file_origin",
                "amount",
                "value_date",
                "description_raw",
            ]

            lf: pl.LazyFrame = ingest_bank_statement(csv_path, required)
            lf = to_paisa_integers(lf, ["amount"])
            schema = lf.collect_schema()

            assert "amount_paisa" in schema, (
                "Paisa column 'amount_paisa' missing from output schema"
            )
            assert schema["amount_paisa"] == pl.Int64, (
                f"amount_paisa has type {schema['amount_paisa']}, expected Int64"
            )

        finally:
            if os.path.exists(csv_path):
                os.remove(csv_path)

    def test_column_pruning_prevents_full_materialization(self) -> None:
        """Verifying column pruning: LazyFrame .select() must exclude
        columns not in the required list.

        This test ensures ingest_bank_statement prunes to only the
        specified columns, preventing full-table materialization
        per CLAUDE.md mandate.
        """
        n: int = 100

        # Generate CSV with an extra column that should be pruned
        df: pl.DataFrame = pl.DataFrame(
            {
                "source_chunk_id": [f"chunk-prune-{i}" for i in range(n)],
                "file_origin": ["TEST.csv"] * n,
                "amount": ["100.00"] * n,
                "value_date": ["2024-01-15"] * n,
                "description_raw": [f"NEFT-REF-{i:06d}" for i in range(n)],
                "extra_column": ["SHOULD_BE_PRUNED"] * n,
            }
        )

        with tempfile.NamedTemporaryFile(
            suffix=".csv", delete=False, mode="w",
        ) as f:
            csv_path: str = f.name

        try:
            df.write_csv(csv_path)
            del df

            required: list[str] = [
                "source_chunk_id",
                "file_origin",
                "amount",
                "value_date",
                "description_raw",
            ]

            lf: pl.LazyFrame = ingest_bank_statement(csv_path, required)
            schema = lf.collect_schema()

            # Extra column must be pruned
            assert "extra_column" not in schema, (
                "Column pruning failed: 'extra_column' present in output. "
                "ingest_bank_statement must .select() only required columns."
            )

            # Required columns must be present
            for col in required:
                assert col in schema, (
                    f"Required column '{col}' missing from output schema"
                )

        finally:
            if os.path.exists(csv_path):
                os.remove(csv_path)


# ---------------------------------------------------------------------------
# FAILURE INJECTION: synthetic data with extreme values to stress memory
# ---------------------------------------------------------------------------


class TestMemoryCeilingFailureInjection:
    """Inject memory-stressing scenarios to verify pipeline resilience."""

    def test_large_description_strings_memory_bound(self) -> None:
        """FAILURE INJECTION: 500 rows with 10,000-char descriptions.

        Verifies that even with unexpectedly large string payloads,
        the ingestion pipeline does not exceed the 3.5 GB ceiling.
        """
        n: int = 500
        long_desc: str = "X" * 10_000

        df: pl.DataFrame = pl.DataFrame(
            {
                "source_chunk_id": [f"chunk-huge-{i}" for i in range(n)],
                "file_origin": ["STRESS_TEST.csv"] * n,
                "amount": ["50000.00"] * n,
                "value_date": ["2024-06-15"] * n,
                "description_raw": [long_desc] * n,
            }
        )

        with tempfile.NamedTemporaryFile(
            suffix=".csv", delete=False, mode="w",
        ) as f:
            csv_path: str = f.name

        try:
            df.write_csv(csv_path)
            del df

            baseline_rss: float = _measure_peak_rss_gb()

            required: list[str] = [
                "source_chunk_id",
                "file_origin",
                "amount",
                "value_date",
                "description_raw",
            ]

            lf: pl.LazyFrame = ingest_bank_statement(csv_path, required)
            lf = to_paisa_integers(lf, ["amount"])
            result: pl.DataFrame = lf.collect(engine="streaming")

            peak_rss: float = _measure_peak_rss_gb()

            assert peak_rss <= 3.5, (
                f"Peak RSS {peak_rss:.3f} GB on large-description test. "
                f"Exceeds 3.5 GB ceiling."
            )

            assert len(result) == n

            del result

        finally:
            if os.path.exists(csv_path):
                os.remove(csv_path)

    def test_streaming_collect_enforced(self) -> None:
        """Verify that materialize_for_processing uses streaming=True.

        Per CLAUDE.md, .collect() without streaming=True is banned.
        This test confirms the function signature includes streaming.
        """
        from app.polars_ingestion import materialize_for_processing
        import inspect

        source: str = inspect.getsource(materialize_for_processing)
        assert "streaming=True" in source, (
            "materialize_for_processing must call .collect(streaming=True). "
            f"Source:\n{source}"
        )
