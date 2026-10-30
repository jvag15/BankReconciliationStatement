"""Schema validation for ingested Polars DataFrames."""

from __future__ import annotations

from typing import Any, Mapping

import polars as pl

# Local alias: ``polars.type_aliases`` is deprecated as an import surface;
# a schema mapping is simply column name -> dtype.
SchemaDict = Mapping[str, Any]


def validate_frame_schema(
    lf: pl.LazyFrame,
    expected_schema: SchemaDict,
) -> None:
    """
    Collect only the frame's schema (no data materialization).

    Raises ``ValueError`` if any expected column is missing or has the
    wrong type.  Call immediately after ``ingest_bank_statement()``,
    before any transformations.
    """
    # collect_schema() is the non-deprecated, resolution-safe accessor.
    actual_schema = lf.collect_schema()

    for col_name, expected_dtype in expected_schema.items():
        if col_name not in actual_schema:
            msg = (
                f"Schema violation: column '{col_name}' missing from "
                f"ingested frame. "
                f"Expected columns: {list(expected_schema.keys())}. "
                f"Actual columns: {list(actual_schema.keys())}."
            )
            raise ValueError(msg)

        actual_dtype = actual_schema[col_name]
        if actual_dtype != expected_dtype:
            msg = (
                f"Schema violation: column '{col_name}' has type "
                f"{actual_dtype}, expected {expected_dtype}."
            )
            raise ValueError(msg)
