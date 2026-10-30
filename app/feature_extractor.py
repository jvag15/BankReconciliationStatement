#!/usr/bin/env python
"""
src/features/extractor.py
-------------------------

Feature extraction pipeline for the BRS Engine.

* All dataframe operations use **Polars** (no pandas).
* Text tokenisation is performed with **spaCy** via ``nlp.pipe(batch_size=1000)``.
* TF‑IDF vectorisation and categorical label encoding use **scikit‑learn**.
* The returned feature matrix ``X`` is a SciPy CSR sparse matrix (or a NumPy
dense array if the caller prefers) ready for LightGBM consumption.
* Monetary ``amount`` values are normalised to integer paisa
  (``amount * 100``) and stored in ``amount_paisa``.
* Blueprint regex cleaning loops are applied with Polars ``str.replace_all``.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, List, Tuple, cast

import numpy as np
import polars as pl
import spacy
from scipy import sparse
from spacy.language import Language
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import LabelEncoder

logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    fmt = "%(asctime)s %(levelname)s %(name)s – %(message)s"
    handler.setFormatter(logging.Formatter(fmt, "%Y-%m-%d %H:%M:%S"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------


# SpaCy pipeline – blank English model with a custom tokenizer that keeps
# hyphens and slashes inside tokens (required for banking references).
# ---------------------------------------------------------------------------
def _build_spacy_pipeline() -> Language:
    nlp = spacy.blank("en")
    default_infixes = [str(infix) for infix in (nlp.Defaults.infixes or [])]
    filtered = [infix for infix in default_infixes if not re.search(r"\\-|/", infix)]
    tokenizer = cast(Any, nlp.tokenizer)
    tokenizer.infix_finditer = spacy.util.compile_infix_regex(filtered).finditer
    return nlp


# ---------------------------------------------------------------------------
# Regex cleaning – apply blueprint‑defined patterns via Polars.
# ---------------------------------------------------------------------------
_CLEAN_REGEXES: List[Tuple[str, str]] = [
    (r"\s+", " "),
    (r"[^0-9a-zA-Z\s\-/]", ""),
]


def _clean_text_column(df: pl.DataFrame, col: str) -> pl.DataFrame:
    cleaned = df
    for pattern, repl in _CLEAN_REGEXES:
        cleaned = cleaned.with_columns(
            pl.col(col).str.replace_all(pattern, repl).alias(col)
        )
    # Normalise case for TF‑IDF (lower‑case)
    cleaned = cleaned.with_columns(pl.col(col).str.to_lowercase().alias(col))
    return cleaned

# ---------------------------------------------------------------------------
# spaCy tokenisation generator.
# ---------------------------------------------------------------------------


def _tokenise_texts(texts: List[str], nlp: Language, batch_size: int = 1000) -> List[str]:
    tokenised = []
    logger.info(
        "Tokenising %d documents with spaCy (batch_size=%d)…", len(texts), batch_size
    )
    for doc in nlp.pipe(texts, batch_size=batch_size):
        tokenised.append(" ".join(tok.text for tok in doc))
    logger.info("Completed tokenisation.")
    return tokenised

# ---------------------------------------------------------------------------
# Main feature‑preparation function.
# ---------------------------------------------------------------------------


def prepare_training_features(
    file_path: str,
    *,
    amount_column: str = "amount",
    text_column: str = "description_raw",
    label_column: str | None = None,
) -> Tuple[sparse.csr_matrix, np.ndarray | None]:
    """
    Load a CSV, clean data, tokenise text, and return a feature matrix.

    Parameters
    ----------
    file_path:
        Path to the CSV containing raw bank‑statement rows.
    amount_column:
        Column name containing monetary values (Rupees). Will be
        converted to integer paisa and stored as ``amount_paisa``.
    text_column:
        Column containing free‑form text to be tokenised and vectorised.
    label_column:
        Optional target column for supervised learning. If provided,
        it is label‑encoded and returned as ``y``.

    Returns
    -------
    X:
        SciPy CSR sparse matrix with numeric and TF‑IDF features.
    y:
        NumPy array of encoded labels (or ``None`` when ``label_column`` is omitted).
    """
    # -------------------------------------------------------------------
    # Load CSV via Polars.
    # -------------------------------------------------------------------
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"Feature extraction source not found: {file_path}")

    logger.info("Reading CSV %s", path)
    df = pl.read_csv(path)

    # -------------------------------------------------------------------
    # Normalise ``amount`` to integer paisa.
    # -------------------------------------------------------------------
    logger.info("Scaling '%s' to paisa (integer)...", amount_column)
    df = df.with_columns(
        (pl.col(amount_column).cast(pl.Float64) * 100)
        .round(0).cast(pl.Int64).alias("amount_paisa")
    )

    # -------------------------------------------------------------------
    # Extract temporal features from value_date.
    # -------------------------------------------------------------------
    if "value_date" in df.columns:
        logger.info("Extracting temporal features from 'value_date'.")
        # Ensure it's a date type, then extract features
        df = df.with_columns(pl.col("value_date").str.to_date("%Y-%m-%d", strict=False).alias("value_date_parsed"))
        df = df.with_columns(
            (pl.col("value_date_parsed").dt.weekday() - 1).fill_null(0).alias("day_of_week"),
            pl.col("value_date_parsed").dt.day().fill_null(1).alias("day_of_month"),
            (pl.col("value_date_parsed") == pl.col("value_date_parsed").dt.month_end()).cast(pl.Int32).fill_null(0).alias("is_month_end")
        )

    # -------------------------------------------------------------------
    # Apply text cleaning regexes.
    # -------------------------------------------------------------------
    if text_column not in df.columns:
        raise KeyError(f"Requested text column '{text_column}' not in CSV.")
    logger.info("Applying regex cleaning to column '%s'.", text_column)
    df = _clean_text_column(df, text_column)

    # -------------------------------------------------------------------
    # Tokenise cleaned texts with spaCy.
    # -------------------------------------------------------------------
    nlp = _build_spacy_pipeline()
    raw_texts = df.select(pl.col(text_column)).to_series().to_list()
    tokenised_texts = _tokenise_texts(raw_texts, nlp, batch_size=1000)

    # -------------------------------------------------------------------
    # TF-IDF vectorisation as sparse CSR matrix.
    # -------------------------------------------------------------------
    logger.info("Fitting TF‑IDF vectoriser on %d documents.", len(tokenised_texts))
    min_df_val = 2 if len(tokenised_texts) >= 100 else 1
    tfidf = TfidfVectorizer(
        max_features=20000,
        ngram_range=(1, 2),
        stop_words="english",
        lowercase=False,
        strip_accents="unicode",
        min_df=min_df_val,
        token_pattern=r"(?u)\b[a-zA-Z0-9_\-]+\b",
    )
    tfidf_matrix = tfidf.fit_transform(tokenised_texts)  # CSR format

    # -------------------------------------------------------------------
    # Assemble numeric features.
    # -------------------------------------------------------------------
    numeric_arrays: List[np.ndarray] = []
    amount_arr = df["amount_paisa"].to_numpy().astype(np.float32).reshape(-1, 1)
    numeric_arrays.append(amount_arr)

    for num_col in ("day_of_week", "day_of_month", "is_month_end"):
        if num_col in df.columns:
            arr = df[num_col].to_numpy().astype(np.float32).reshape(-1, 1)
            numeric_arrays.append(arr)

    for cat_col in ("ifsc", "category"):
        if cat_col in df.columns:
            logger.info("Label‑encoding categorical column '%s'.", cat_col)
            le = LabelEncoder()
            encoded = le.fit_transform(df[cat_col].to_numpy().astype(str)).reshape(-1, 1)
            numeric_arrays.append(encoded.astype(np.int32))

    dense_matrix = np.hstack(numeric_arrays) if numeric_arrays else np.empty((df.height, 0))
    dense_sparse = sparse.csr_matrix(dense_matrix)
    X = sparse.hstack([dense_sparse, tfidf_matrix], format="csr")

    # -------------------------------------------------------------------
    # Extract target labels as raw strings (encoding happens in classifier).
    # -------------------------------------------------------------------
    y: np.ndarray | None = None
    if label_column:
        if label_column not in df.columns:
            raise KeyError(f"Label column '{label_column}' not present in CSV.")
        logger.info("Extracting target column '%s' as raw strings.", label_column)
        y = df[label_column].to_numpy().astype(str)

    # -------------------------------------------------------------------
    # Cleanup intermediate objects to keep memory low.
    # -------------------------------------------------------------------
    del df, raw_texts, tokenised_texts, tfidf, tfidf_matrix, dense_matrix, dense_sparse, nlp
    logger.info("Feature matrix prepared – shape %s.", X.shape)

    return X, y
