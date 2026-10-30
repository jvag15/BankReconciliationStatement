"""Reconciliation reporting routes.

Exposes GET /brs/reports/reconciliation, a downloadable CSV summary of
the current reconciliation state. Authenticated like the other /brs
endpoints, since it exposes aggregate financial figures.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from app.deps import get_current_user

router = APIRouter(prefix="/brs/reports", tags=["reports"])


@router.get(
    "/reconciliation",
    operation_id="download_reconciliation_report",
    summary="Download the reconciliation summary as CSV.",
    responses={
        200: {
            "description": "CSV attachment (reconciliation_summary.csv).",
            "content": {"text/csv": {"schema": {"type": "string"}}},
        },
        401: {"description": "Not authenticated."},
    },
)
def reconciliation_report(user: str = Depends(get_current_user)) -> StreamingResponse:
    """Return the reconciliation summary as a downloadable CSV file.

    Defined as a sync endpoint so FastAPI runs the blocking Polars +
    SQLAlchemy work in its threadpool rather than on the event loop.
    """
    from app.polars_reporting import generate_reconciliation_summary, summary_to_csv
    from app.session import get_db_connection

    with get_db_connection() as conn:
        summary = generate_reconciliation_summary(conn)

    csv_text = summary_to_csv(summary)
    return StreamingResponse(
        iter([csv_text]),
        media_type="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=reconciliation_summary.csv"
        },
    )
