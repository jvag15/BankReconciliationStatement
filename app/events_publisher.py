"""Redis pub/sub publishing for real-time processing telemetry.

Celery tasks call :func:`publish_event` to broadcast lifecycle events
(batch started / completed / failed) onto a single Redis channel. The SSE
endpoint (``app/routers/events.py``) subscribes to the same channel and
relays them to connected dashboards.

Publishing is strictly best-effort: a telemetry failure must never break
or slow the task emitting it, so every error is swallowed and reported via
the boolean return.
"""

from __future__ import annotations

import json
import os
from typing import Any

# Single channel shared by publisher and SSE subscriber.
EVENTS_CHANNEL = "brs_processing_events"


def _broker_url() -> str:
    """Resolve the Redis URL from the same config Celery uses."""
    return (
        os.environ.get("CELERY_BROKER_URL")
        or os.environ.get("REDIS_BROKER_URL")
        or "redis://brs_redis:6379/0"
    )


def publish_event(event_type: str, payload: dict[str, Any]) -> bool:
    """Publish ``{"event_type", "payload"}`` as JSON to the events channel.

    Returns ``True`` when the message was published, ``False`` on any
    failure. Never raises — a broker hiccup must not disrupt ingestion.
    """
    client = None
    try:
        import redis

        client = redis.Redis.from_url(
            _broker_url(), socket_connect_timeout=2, socket_timeout=2,
        )
        body = json.dumps(
            {"event_type": event_type, "payload": payload}, default=str,
        )
        client.publish(EVENTS_CHANNEL, body)
        return True
    except Exception:  # noqa: BLE001 - telemetry is best-effort
        return False
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:  # noqa: BLE001
                pass
