"""Polars parsing and schema mapping for internal company ledger exports.

Ledger exports arrive from the firm's accounting system in a variety of
column spellings (Tally, ERP extracts, hand-maintained sheets).  This
module normalises them into the exact column set that
``ledger_transactions`` expects, so ``app/task_ledger_ingestion.py`` can
batch-insert without any further reshaping.

Monetary policy
---------------
``amount`` is emitted in **rupees** because that is what
``bank_transactions.amount`` already stores, and the reconciliation rules
in ``app/matching_rules.py`` compare the two columns directly with a
``Decimal("0.01")`` tolerance — i.e. the tolerance is denominated in
rupees.  Emitting paisa here would make every bank-to-ledger comparison
off by 100x and reduce the match rate to zero.

``amount_paisa`` is emitted *alongside* it as an exact integer for
drift-free arithmetic (subset-sum in rules 2A/2B especially).  It is a
derived analysis column, not the persisted value.
"""

from __future__ import annotations

from typing import Sequence

import polars as pl

# Columns this module guarantees downstream, in insert order.
LEDGER_OUTPUT_COLUMNS: tuple[str, ...] = (
    "ledger_date",
    "amount",
    "amount_paisa",
    "description",
    "account_code",
    "cost_center",
    "voucher_number",
    "utr",
    "neft_ref",
    "rtgs_ref",
    "imps_ref",
    "upi_ref",
    "ifsc",
)

# Accepted header spellings per logical field, matched case- and
# whitespace-insensitively.  Order matters: earlier entries win.
_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "ledger_date": (
        "ledger date", "voucher date", "posting date", "txn date",
        "transaction date", "date", "value date",
    ),
    "description": (
        "description", "particulars", "narration", "details", "remarks",
    ),
    "account_code": (
        "account code", "ledger code", "gl code", "account", "ledger",
        "account head",
    ),
    "cost_center": ("cost center", "cost centre", "costcenter", "department"),
    "voucher_number": (
        "voucher number", "voucher no", "vch no", "vch number", "voucher",
        "document no",
    ),
    "amount": ("amount", "net amount", "value"),
    "debit": ("debit", "dr", "debit amount"),
    "credit": ("credit", "cr", "credit amount"),
    "utr": ("utr", "utr no", "utr number", "rrn"),
    "neft_ref": ("neft ref", "neft reference", "neft"),
    "rtgs_ref": ("rtgs ref", "rtgs reference", "rtgs"),
    "imps_ref": ("imps ref", "imps reference", "imps"),
    "upi_ref": ("upi ref", "upi reference", "upi", "upi txn id"),
    "ifsc": ("ifsc", "ifsc code"),
}

# Date formats seen in Indian accounting exports, tried in order.
_DATE_FORMATS: tuple[str, ...] = (
    "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d",
    "%d-%b-%Y", "%d %b %Y", "%d-%B-%Y", "%m/%d/%Y",
)

_NULL_TOKENS: list[str] = ["", "NULL", "N/A", "-", "NA", "NIL", " "]


class LedgerSchemaError(ValueError):
    """Raised when a ledger export cannot be mapped to the target schema."""


def _resolve(columns: Sequence[str], field: str) -> str | None:
    """Return the source column matching *field*, or ``None``.

    Comparison is case-insensitive and ignores surrounding whitespace so
    that ``" Voucher No "`` and ``"voucher_no"`` both resolve.
    """
    normalised = {
        col.strip().lower().replace("_", " "): col for col in columns
    }
    for alias in _COLUMN_ALIASES.get(field, ()):
        if alias in normalised:
            return normalised[alias]
    return None


def _clean_currency(col: pl.Expr) -> pl.Expr:
    """Strip formatting from a currency string and cast to Float64.

    Handles thousands separators, currency symbols, stray whitespace, and
    the accounting convention of parenthesising negatives — ``(1,234.50)``
    becomes ``-1234.50``.
    """
    text = col.cast(pl.Utf8).str.strip_chars()
    is_negative = text.str.starts_with("(") & text.str.ends_with(")")
    magnitude = (
        text.str.replace_all(r"[(),₹Rs\s]", "")
        .str.replace_all(",", "")
        .cast(pl.Float64, strict=False)
        .fill_null(0.0)
    )
    return pl.when(is_negative).then(-magnitude).otherwise(magnitude)


def _parse_date(col: pl.Expr) -> pl.Expr:
    """Parse a date column, trying each known format in turn."""
    attempts = [
        col.cast(pl.Utf8).str.strip_chars().str.to_date(fmt, strict=False)
        for fmt in _DATE_FORMATS
    ]
    return pl.coalesce(attempts)


def ingest_ledger_export(
    source: str | bytes,
    *,
    strict_dates: bool = True,
) -> pl.DataFrame:
    """Parse a ledger export into the normalised ``ledger_transactions`` shape.

    Parameters
    ----------
    source:
        Path to a CSV, or the raw CSV bytes (as produced by decrypting an
        uploaded chunk).
    strict_dates:
        When ``True`` (default) a row whose date matches none of the known
        formats raises :class:`LedgerSchemaError`.  Financial rows must not
        be silently re-dated — a wrong ``ledger_date`` would quietly shift
        a row outside the ±3-day reconciliation window and cause a false
        non-match.

    Returns
    -------
    A ``DataFrame`` containing exactly :data:`LEDGER_OUTPUT_COLUMNS`.

    Raises
    ------
    LedgerSchemaError
        If a required field is absent or dates cannot be parsed.
    """
    if not source:
        raise LedgerSchemaError("ingest_ledger_export requires a non-empty source.")

    lf: pl.LazyFrame = pl.scan_csv(
        source,
        infer_schema_length=10_000,
        null_values=_NULL_TOKENS,
        try_parse_dates=False,
        encoding="utf8-lossy",
        low_memory=True,
        truncate_ragged_lines=True,
    )
    columns: list[str] = lf.collect_schema().names()

    date_col = _resolve(columns, "ledger_date")
    desc_col = _resolve(columns, "description")
    if date_col is None:
        raise LedgerSchemaError(
            "Ledger export is missing a date column. Accepted spellings: "
            f"{list(_COLUMN_ALIASES['ledger_date'])}. Found: {columns}"
        )
    if desc_col is None:
        raise LedgerSchemaError(
            "Ledger export is missing a description/particulars column. "
            f"Accepted spellings: {list(_COLUMN_ALIASES['description'])}. "
            f"Found: {columns}"
        )

    # Amount: either a single signed column, or a Debit/Credit pair which we
    # fold using the same polarity as bank ingestion (credit positive).
    amount_col = _resolve(columns, "amount")
    debit_col = _resolve(columns, "debit")
    credit_col = _resolve(columns, "credit")

    if amount_col is not None:
        amount_expr = _clean_currency(pl.col(amount_col))
    elif debit_col is not None or credit_col is not None:
        credit_expr = (
            _clean_currency(pl.col(credit_col))
            if credit_col is not None
            else pl.lit(0.0)
        )
        debit_expr = (
            _clean_currency(pl.col(debit_col))
            if debit_col is not None
            else pl.lit(0.0)
        )
        amount_expr = credit_expr - debit_expr
    else:
        raise LedgerSchemaError(
            "Ledger export needs either an amount column or a debit/credit "
            f"pair. Found: {columns}"
        )

    def _optional(field: str, dtype: pl.DataType = pl.Utf8) -> pl.Expr:
        src = _resolve(columns, field)
        if src is None:
            return pl.lit(None, dtype=dtype).alias(field)
        return pl.col(src).cast(dtype).alias(field)

    lf = lf.with_columns(
        [
            _parse_date(pl.col(date_col)).alias("ledger_date"),
            amount_expr.alias("amount"),
            pl.col(desc_col).cast(pl.Utf8).fill_null("").alias("description"),
            # account_code is NOT NULL in the target table; fall back to a
            # sentinel rather than dropping the row, so the entry still
            # reconciles on amount/date/reference.
            (
                pl.col(_resolve(columns, "account_code")).cast(pl.Utf8)
                if _resolve(columns, "account_code")
                else pl.lit("UNCLASSIFIED")
            ).fill_null("UNCLASSIFIED").alias("account_code"),
            _optional("cost_center"),
            _optional("voucher_number"),
            _optional("utr"),
            _optional("neft_ref"),
            _optional("rtgs_ref"),
            _optional("imps_ref"),
            _optional("upi_ref"),
            _optional("ifsc"),
        ]
    ).with_columns(
        # Exact integer paisa for drift-free arithmetic downstream. Derived,
        # not persisted — see the module docstring.
        (pl.col("amount") * 100).round(0).cast(pl.Int64).alias("amount_paisa"),
    )

    df: pl.DataFrame = lf.select(LEDGER_OUTPUT_COLUMNS).collect()

    if strict_dates:
        unparsed = int(df["ledger_date"].null_count())
        if unparsed:
            raise LedgerSchemaError(
                f"{unparsed} ledger row(s) have a date that matches none of "
                f"the supported formats {list(_DATE_FORMATS)}. Source column: "
                f"'{date_col}'. Refusing to import rather than re-date "
                f"financial records."
            )
    else:
        df = df.filter(pl.col("ledger_date").is_not_null())

    return df


def validate_ledger_frame(df: pl.DataFrame) -> None:
    """Assert a parsed frame satisfies the target table's NOT NULL contract.

    Call immediately before insert.  Raises :class:`LedgerSchemaError`
    describing the first violation found.
    """
    for column in ("ledger_date", "amount", "description", "account_code"):
        if column not in df.columns:
            raise LedgerSchemaError(f"Parsed ledger frame is missing '{column}'.")
        if df[column].null_count():
            raise LedgerSchemaError(
                f"Column '{column}' is NOT NULL in ledger_transactions but the "
                f"parsed frame contains {df[column].null_count()} null(s)."
            )
