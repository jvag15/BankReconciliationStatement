"""Deep diagnostics routing for the BRS Engine.

Exposes GET /health/deep, an unauthenticated probe (like /metrics) that
orchestrators and monitoring can poll without credentials.  It aggregates
per-subsystem checks and returns 503 when a core connectivity dependency
is down so a load balancer can pull the instance out of rotation.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app import health_inspector

router = APIRouter(tags=["health"])


class HealthDeepResponse(BaseModel):
    """Aggregated subsystem diagnostics report.

    ``checks`` maps subsystem name (database, redis, storage,
    model_singleton) to its metrics dict; the metric keys vary per
    subsystem, so they are documented as free-form objects.
    """

    status: str
    timestamp: str
    checks: dict[str, dict[str, object]]


@router.get(
    "/health/deep",
    operation_id="get_deep_health",
    summary="Aggregate subsystem diagnostics (unauthenticated probe).",
    responses={
        200: {"model": HealthDeepResponse, "description": "All core subsystems reachable."},
        503: {"model": HealthDeepResponse, "description": "A core dependency is down; instance should leave rotation."},
    },
)
async def health_deep() -> JSONResponse:
    """Aggregate subsystem health into a single structured report."""
    checks: dict[str, Any] = {
        "database": await health_inspector.check_database(),
        "redis": await health_inspector.check_redis(),
        "storage": health_inspector.check_storage(),
        "model_singleton": health_inspector.check_model(),
    }
    overall_status, http_code = health_inspector.evaluate(checks)

    body = {
        "status": overall_status,
        "timestamp": datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "checks": checks,
    }
    return JSONResponse(status_code=http_code, content=body)
