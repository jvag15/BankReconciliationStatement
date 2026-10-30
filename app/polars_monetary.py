"""Decimal Rupee ↔ integer paisa conversion utilities."""

from __future__ import annotations

from decimal import Decimal


def paisa_to_rupee_decimal(paisa: int) -> Decimal:
    """Convert integer paisa back to Decimal Rupees at the output boundary.

    Example: ``99999 → Decimal("999.99")``
    """
    return Decimal(paisa) / Decimal(100)


def rupee_decimal_to_paisa(amount: Decimal) -> int:
    """Convert a Decimal Rupee amount to integer paisa.

    Example: ``Decimal("999.99") → 99999``
    """
    return int((amount * Decimal(100)).quantize(Decimal("1")))
