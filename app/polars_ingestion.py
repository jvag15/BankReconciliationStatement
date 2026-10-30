"""Polars LazyFrame ingestion and monetary conversion utilities."""

from __future__ import annotations
from typing import Sequence
import polars as pl

def ingest_bank_statement(
    source: str | bytes,
    required_columns: Sequence[str] | None,
) -> pl.LazyFrame:
    """Production ingestion entry point. Never materializes the full file."""
    if not source:
        raise TypeError("ingest_bank_statement requires a non-empty source path or bytes")
    if not required_columns:
        raise TypeError("required_columns must be a non-empty list of column names")
        
    import sys
    import traceback

    try:
        lf: pl.LazyFrame = (
            pl.scan_csv(
                source,
                infer_schema_length=10_000,
                null_values=["", "NULL", "N/A", "-", "NA", "NIL", " "],
                try_parse_dates=False,
                encoding="utf8-lossy",
                low_memory=True,
            )
        )
        columns = lf.collect_schema().names()
    except Exception as e:
        sys.stderr.write(f"FATAL ERROR: Failed to parse CSV schema in Polars. Traceback follows:\n")
        traceback.print_exc(file=sys.stderr)
        raise

    # Dynamic whitespace-insensitive column mapping
    txn_date_col = next((c for c in columns if c.strip() == "Txn Date"), None)
    value_date_col = next((c for c in columns if c.strip() == "Value Date"), None)
    desc_col = next((c for c in columns if c.strip() == "Description"), None)
    debit_col = next((c for c in columns if c.strip() == "Debit"), None)
    credit_col = next((c for c in columns if c.strip() == "Credit"), None)

    if txn_date_col and desc_col and debit_col and credit_col:
        lf = lf.with_columns([
            pl.col(txn_date_col).alias("date"),
            pl.col(value_date_col).alias("value_date") if value_date_col else pl.col(txn_date_col).alias("value_date"),
            pl.col(desc_col).alias("description_raw"),
            # Strip whitespace, remove formatting commas, cast leniently, and balance ledger polarity
            (
                pl.col(credit_col).str.strip_chars().str.replace_all(",", "").cast(pl.Float64, strict=False).fill_null(0.0) - 
                pl.col(debit_col).str.strip_chars().str.replace_all(",", "").cast(pl.Float64, strict=False).fill_null(0.0)
            ).alias("amount"),
            pl.lit(None).alias("category"),
            pl.lit(None).alias("confidence_score")
        ])
    else:
        if "value_date" not in columns:
            lf = lf.with_columns(pl.lit(None).alias("value_date"))
        
    lf = lf.select(required_columns)
    return lf

def to_paisa_integers(
    lf: pl.LazyFrame,
    amount_columns: Sequence[str],
) -> pl.LazyFrame:
    conversions = [
        (pl.col(col) * 100).round(0).cast(pl.Int64).alias(f"{col}_paisa")
        for col in amount_columns
    ]
    return lf.with_columns(conversions)

def materialize_for_processing(lf: pl.LazyFrame) -> pl.DataFrame:
    return lf.collect(streaming=True)

if __name__ == "__main__":
    import sys
    import uuid
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        try:
            from app.task_ingestion import spool_audit_job
            job_id = str(uuid.uuid4())
            print(f"Triggering ingestion for {file_path} with job_id {job_id}")
            result = spool_audit_job(job_id, file_path)
            print(f"Ingestion spooled: {result}")
        except Exception as e:
            sys.stderr.write(f"FATAL ERROR: Ingestion script failed: {e}\n")
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.exit(1)
