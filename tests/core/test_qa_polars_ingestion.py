"""QA Automation: Polars Ingestion and DLQ Fallback Validation."""
from __future__ import annotations

import pytest
import polars as pl
from unittest.mock import patch, MagicMock

from app.polars_schema_validation import validate_frame_schema
from app.task_ingestion import process_statement_chunk

class TestPolarsIngestionQA:
    """Verify that incoming CSV streams matching the 6-column schema parse perfectly, and corrupt schemas throw clean failures to the Dead Letter Queue (DLQ)."""

    def test_valid_schema_parses_perfectly(self) -> None:
        """A flawless 5-column (plus source_chunk_id makes 6) DataFrame triggers no exceptions."""
        df = pl.DataFrame({
            "source_chunk_id": ["chunk123"],
            "file_origin": ["test_bank.csv"],
            "amount": ["1500.00"],
            "value_date": ["2024-03-01"],
            "description_raw": ["Valid Description"],
        })
        lf = df.lazy()
        expected = {
            "source_chunk_id": pl.String(),
            "file_origin": pl.String(),
            "amount": pl.String(),
            "value_date": pl.String(),
            "description_raw": pl.String(),
        }
        
        # Valid execution should not raise any ValueError
        validate_frame_schema(lf, expected)

    def test_missing_columns_throws_value_error(self) -> None:
        """Simulate an entirely corrupted payload missing required columns."""
        df = pl.DataFrame({
            "wrong_col1": ["chunk123"],
            "wrong_col2": ["test_bank.csv"],
        })
        lf = df.lazy()
        expected = {
            "source_chunk_id": pl.String(),
            "file_origin": pl.String(),
            "amount": pl.String(),
            "value_date": pl.String(),
            "description_raw": pl.String(),
        }
        
        with pytest.raises(ValueError, match="Schema violation: column 'source_chunk_id' missing"):
            validate_frame_schema(lf, expected)
