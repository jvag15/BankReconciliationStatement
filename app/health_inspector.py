"""Subsystem probes for the deep diagnostics endpoint.

Each probe returns a small JSON-serialisable dict and never raises: a
failing subsystem is reported as ``DOWN`` with its exception class, so a
single dead dependency degrades the report instead of 500-ing the whole
endpoint.  ``evaluate`` maps the per-subsystem results to an overall
status and HTTP code.

Core components for liveness are the database, Redis and storage.  The
model artifact is reported for observability (staleness/version) but,
being non-essential to ingestion and matching, does not affect the HTTP
status code.
"""

from __future__ import annotations

import os
import shutil
import time
from datetime import datetime, timezone
from typing import Any

# Celery queues declared in app/celery_app.py; their Redis list keys hold
# the pending (unacknowledged-by-a-worker) task backlog.
_CELERY_QUEUES: tuple[str, ...] = ("ingestion", "retrain", "ml_predict")

# Active model artifact whose mtime represents "last retrain".
_MODEL_ARTIFACT = "lgbm_matcher.bin"


async def check_database() -> dict[str, Any]:
    """Ping PostgreSQL and report asyncpg pool occupancy."""
    from app.db import get_db_pool

    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute("SELECT 1")
        size = pool.get_size()
        idle = pool.get_idle_size()
        return {
            "status": "UP",
            "active_connections": size - idle,
            "idle_connections": idle,
            "pool_size": pool.get_max_size(),
        }
    except Exception as exc:  # noqa: BLE001 - report, never raise
        return {"status": "DOWN", "error": type(exc).__name__}


async def check_redis() -> dict[str, Any]:
    """Ping Redis and sum pending messages across the Celery queues."""
    import redis.asyncio as aioredis

    broker = (
        os.environ.get("CELERY_BROKER_URL")
        or os.environ.get("REDIS_BROKER_URL")
        or "redis://brs_redis:6379/0"
    )
    client = None
    try:
        client = aioredis.from_url(
            broker, socket_connect_timeout=2, socket_timeout=2,
        )
        await client.ping()
        depth = 0
        for queue in _CELERY_QUEUES:
            try:
                depth += int(await client.llen(queue))
            except Exception:  # noqa: BLE001 - a missing key is depth 0
                pass
        return {"status": "UP", "queue_depth": depth, "queues": list(_CELERY_QUEUES)}
    except Exception as exc:  # noqa: BLE001
        return {"status": "DOWN", "error": type(exc).__name__}
    finally:
        if client is not None:
            try:
                await client.aclose()
            except Exception:  # noqa: BLE001
                pass


def check_storage() -> dict[str, Any]:
    """Report free capacity on the STORAGE_DIR filesystem."""
    from app.storage_paths import get_storage_dir

    try:
        path = get_storage_dir()
        usage = shutil.disk_usage(str(path))
        return {
            "status": "UP",
            "free_gb": round(usage.free / (1024 ** 3), 2),
            "total_gb": round(usage.total / (1024 ** 3), 2),
            "path": str(path),
        }
    except Exception as exc:  # noqa: BLE001
        return {"status": "DOWN", "error": type(exc).__name__}


def check_model() -> dict[str, Any]:
    """Report the age and mtime-derived version of the active model artifact."""
    from app.storage_paths import get_model_dir

    try:
        model_path = get_model_dir() / _MODEL_ARTIFACT
        if not model_path.exists():
            return {"status": "MISSING", "age_hours": None, "version": None}
        mtime = model_path.stat().st_mtime
        age_hours = round((time.time() - mtime) / 3600, 2)
        version = datetime.fromtimestamp(mtime, timezone.utc).strftime("v%Y%m%d_%H%M")
        return {"status": "UP", "age_hours": age_hours, "version": version}
    except Exception as exc:  # noqa: BLE001
        return {"status": "DOWN", "error": type(exc).__name__}


def evaluate(checks: dict[str, dict[str, Any]]) -> tuple[str, int]:
    """Map subsystem results to (overall_status, http_code).

    503 when a core connectivity dependency (DB or Redis) is down; 200
    DEGRADED when only storage is down; 200 HEALTHY otherwise.  The model
    check is informational and never changes the code.
    """
    db_ok = checks["database"]["status"] == "UP"
    redis_ok = checks["redis"]["status"] == "UP"
    storage_ok = checks["storage"]["status"] == "UP"

    if not db_ok or not redis_ok:
        return "UNHEALTHY", 503
    if not storage_ok:
        return "DEGRADED", 200
    return "HEALTHY", 200
