"""Webhook dispatch for reconciliation-complete callbacks.

Because the engine is headless, clients register HTTP endpoints in
``webhook_subscriptions`` and are notified when a statement batch finishes
reconciliation.  Each delivery POSTs a structured JSON envelope and, when
the subscription carries a ``secret``, an HMAC-SHA256 signature over the
exact request body so the receiver can verify authenticity.

Delivery is best-effort with a bounded in-task retry (transient network
faults only).  A subscription that keeps failing does not block the
others: each endpoint is attempted independently and the per-endpoint
outcome is returned in the task summary.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from app.celery_app import celery_app as app
from app.session import get_db_connection

_EVENT_BATCH_RECONCILED = "batch.reconciled"
_REQUEST_TIMEOUT_SECONDS = 10
_MAX_DELIVERY_ATTEMPTS = 3


def build_envelope(job_id: str, event: str, extra: dict[str, Any]) -> dict[str, Any]:
    """Construct the canonical webhook payload for *job_id*."""
    envelope: dict[str, Any] = {
        "event": event,
        "job_id": job_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    envelope.update(extra)
    return envelope


def sign_payload(secret: str, body: bytes) -> str:
    """Return the hex HMAC-SHA256 of *body* under *secret*."""
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


def _collect_job_summary(conn: Any, job_id: str) -> dict[str, Any]:
    """Gather company name and per-status transaction counts for the job."""
    job_row = conn.execute(
        text("SELECT company_name, status FROM audit_jobs WHERE job_id = :job_id"),
        {"job_id": job_id},
    ).fetchone()

    status_rows = conn.execute(
        text(
            """
            SELECT status, COUNT(*)
            FROM bank_transactions
            WHERE job_id = :job_id
            GROUP BY status
            """
        ),
        {"job_id": job_id},
    ).fetchall()

    return {
        "company_name": job_row[0] if job_row else None,
        "job_status": job_row[1] if job_row else None,
        "counts": {row[0]: int(row[1]) for row in status_rows},
    }


@app.task(  # type: ignore[untyped-decorator]
    bind=True,
    name="brs_engine.tasks.dispatch_reconciliation_webhooks",
    queue="ingestion",
)
def dispatch_reconciliation_webhooks(
    self: Any,
    job_id: str,
    event: str = _EVENT_BATCH_RECONCILED,
) -> dict[str, Any]:
    """Deliver the reconciliation-complete callback to all active subscribers.

    Returns a summary listing each subscription's delivery outcome.  Never
    raises for an individual endpoint failure — a bad subscriber must not
    take down the notification pass.
    """
    import requests

    with get_db_connection() as conn:
        with conn.begin():
            subs = conn.execute(
                text(
                    """
                    SELECT id, target_url, secret
                    FROM webhook_subscriptions
                    WHERE active = true AND event = :event
                    """
                ),
                {"event": event},
            ).fetchall()
            summary = _collect_job_summary(conn, job_id)

    if not subs:
        return {"job_id": job_id, "event": event, "delivered": 0, "results": []}

    envelope = build_envelope(job_id, event, summary)
    body = json.dumps(envelope, separators=(",", ":"), sort_keys=True).encode("utf-8")

    results: list[dict[str, Any]] = []
    delivered = 0

    for sub in subs:
        sub_id = str(sub[0])
        target_url = sub[1]
        secret = sub[2]

        headers = {
            "Content-Type": "application/json",
            "X-BRS-Event": event,
            "X-BRS-Delivery": sub_id,
        }
        if secret:
            headers["X-BRS-Signature"] = "sha256=" + sign_payload(secret, body)

        outcome = _deliver(requests, target_url, body, headers)
        outcome["subscription_id"] = sub_id
        if outcome["ok"]:
            delivered += 1
        results.append(outcome)

    return {
        "job_id": job_id,
        "event": event,
        "delivered": delivered,
        "results": results,
    }


def _deliver(
    requests_mod: Any,
    target_url: str,
    body: bytes,
    headers: dict[str, str],
) -> dict[str, Any]:
    """POST *body* to *target_url* with bounded retries on transient faults."""
    last_error = ""
    for attempt in range(1, _MAX_DELIVERY_ATTEMPTS + 1):
        try:
            resp = requests_mod.post(
                target_url,
                data=body,
                headers=headers,
                timeout=_REQUEST_TIMEOUT_SECONDS,
            )
            if 200 <= resp.status_code < 300:
                return {"ok": True, "status_code": resp.status_code, "attempts": attempt}
            # 4xx is a client contract error — do not keep retrying.
            if 400 <= resp.status_code < 500:
                return {
                    "ok": False,
                    "status_code": resp.status_code,
                    "attempts": attempt,
                    "error": "client_error",
                }
            last_error = f"status_{resp.status_code}"
        except Exception as exc:  # network/timeout — transient, retry
            last_error = type(exc).__name__

    return {"ok": False, "status_code": None, "attempts": _MAX_DELIVERY_ATTEMPTS, "error": last_error}
