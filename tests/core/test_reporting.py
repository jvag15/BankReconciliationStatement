"""Tests for the reconciliation reporting engine and endpoint."""

from __future__ import annotations

import os

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from app.main import app

client = TestClient(app)


def _auth_headers() -> dict[str, str]:
    token = os.environ.get("API_AUTH_TOKEN", "dev-token")
    return {"X-API-Token": token}


def test_reconciliation_report_returns_csv() -> None:
    """The endpoint returns 200 with a text/csv attachment."""
    resp = client.get("/brs/reports/reconciliation", headers=_auth_headers())

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")
    assert "attachment" in resp.headers.get("content-disposition", "")

    body = resp.text
    # Header plus the three summary metric rows.
    assert "metric,transaction_count,total_amount_rupees" in body
    assert "Matched Transactions" in body
    assert "Unmatched Bank Transactions" in body
    assert "Unmatched Ledger Transactions" in body


def test_reconciliation_report_requires_auth() -> None:
    """Without a token the endpoint rejects with 401."""
    resp = client.get("/brs/reports/reconciliation")
    assert resp.status_code == 401


def test_generate_reconciliation_summary_math() -> None:
    """Metric counts and rupee sums are computed correctly over sample rows."""
    from app.polars_reporting import generate_reconciliation_summary

    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE bank_transactions "
                "(id INTEGER, status TEXT, matched_ledger_id INTEGER, amount REAL)"
            )
        )
        conn.execute(text("CREATE TABLE ledger_transactions (id INTEGER, amount REAL)"))
        conn.execute(
            text(
                "INSERT INTO bank_transactions VALUES "
                "(1,'MATCHED',10,100.0),"
                "(2,'RECONCILED',11,200.0),"
                "(3,'UNPROCESSED',NULL,50.0),"
                "(4,'SCORED',NULL,25.0)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO ledger_transactions VALUES "
                "(10,100.0),(11,200.0),(12,999.0)"
            )
        )

    with engine.connect() as conn:
        summary = generate_reconciliation_summary(conn)

    rows = {
        r["metric"]: (r["transaction_count"], r["total_amount_rupees"])
        for r in summary.to_dicts()
    }
    assert rows["Matched Transactions"] == (2, 300.0)
    assert rows["Unmatched Bank Transactions"] == (2, 75.0)
    # Ledger 12 is referenced by no bank row, so it is unmatched.
    assert rows["Unmatched Ledger Transactions"] == (1, 999.0)

    engine.dispose()
