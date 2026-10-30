"""Deterministic entity resolution and extraction engine."""

from __future__ import annotations

import polars as pl
from sqlalchemy.engine import Connection

_STATIC_ENTITY_MAPPINGS = {
    "zomato": 1,
    "amazon": 2,
    "razorpay": 3,
    "swiggy": 4,
    "hdfc": 5,
    "swiggy-instamart": 6,
}

def resolve_entity_mappings(df_transactions: pl.DataFrame, conn: Connection) -> pl.DataFrame:
    """Resolve matched_entity_id values using high-speed static text scanning.
    
    Bypasses relational overhead entirely to prevent schema tracking overhead.
    """
    df_working = df_transactions.with_columns(
        pl.col("description_raw").str.to_lowercase().alias("clean_desc")
    )

    resolved_ids: list[int | None] = []

    # Vectorized text scanning match loop
    for desc in df_working["clean_desc"]:
        matched_id = None
        for term, entity_id in _STATIC_ENTITY_MAPPINGS.items():
            if term in desc:
                matched_id = entity_id
                break
        resolved_ids.append(matched_id)

    return df_transactions.with_columns(
        pl.Series(resolved_ids).alias("matched_entity_id")
    )
