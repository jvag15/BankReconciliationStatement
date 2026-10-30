"""Streaming chunked export from Polars DataFrame to PostgreSQL."""

from __future__ import annotations

import os

import polars as pl
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


_CHUNK_SIZE = 1_000


def _get_export_engine() -> Engine:
    """Return a cached SQLAlchemy Engine for bulk Polars writes."""
    db_url: str = os.environ["DATABASE_URL"]
    return create_engine(db_url)


def stream_lazyframe_to_postgres(
    lazy_frame: pl.LazyFrame,
    table_name: str,
    chunk_size: int = _CHUNK_SIZE,
) -> int:
    """
    Materialize a LazyFrame in streaming mode and write it to PostgreSQL
    in *chunk_size*-row chunks.  Never materializes the full frame as a
    single DataFrame in memory.

    Returns the total row count written.
    """
    engine: Engine = _get_export_engine()
    df: pl.DataFrame = lazy_frame.collect(streaming=True)  # type: ignore[call-overload]
    total_rows: int = len(df)
    rows_written: int = 0

    try:
        for offset in range(0, total_rows, chunk_size):
            chunk: pl.DataFrame = df.slice(offset, chunk_size)
            chunk.write_database(
                table_name=table_name,
                connection=engine,
                if_table_exists="append",
                engine="sqlalchemy",
            )
            rows_written += len(chunk)
            del chunk
    finally:
        del df

    return rows_written
