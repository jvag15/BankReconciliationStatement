"""Server-Sent Events (SSE) streaming of processing telemetry.

Exposes GET /brs/events/stream: an authenticated ``text/event-stream``
that relays events published to the ``brs_processing_events`` Redis
channel (see ``app/events_publisher.py``) to connected dashboards, so
they observe ingestion/reconciliation progress without long-polling.
"""

from __future__ import annotations

import asyncio
from typing import AsyncIterator

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from app.deps import get_stream_identity
from app.events_publisher import EVENTS_CHANNEL, _broker_url

router = APIRouter(prefix="/brs/events", tags=["events"])


async def _subscribe_events() -> AsyncIterator[str]:
    """Yield SSE-formatted frames from the Redis events channel.

    On client disconnect Starlette cancels this generator; the
    ``CancelledError`` path plus the ``finally`` block unsubscribe and
    close the Redis connection so nothing leaks.
    """
    import redis.asyncio as aioredis

    client = aioredis.from_url(_broker_url())
    pubsub = client.pubsub()
    try:
        await pubsub.subscribe(EVENTS_CHANNEL)
        async for message in pubsub.listen():
            if not message or message.get("type") != "message":
                # Skip subscribe/unsubscribe confirmations and empty ticks.
                continue
            data = message["data"]
            if isinstance(data, (bytes, bytearray)):
                data = bytes(data).decode("utf-8", errors="replace")
            yield f"data: {data}\n\n"
    except asyncio.CancelledError:
        # Client went away — let it propagate after cleanup runs.
        raise
    finally:
        try:
            await pubsub.unsubscribe(EVENTS_CHANNEL)
            await pubsub.aclose()
        except Exception:  # noqa: BLE001
            pass
        try:
            await client.aclose()
        except Exception:  # noqa: BLE001
            pass


@router.get(
    "/stream",
    operation_id="stream_processing_events",
    summary="Stream processing telemetry as server-sent events.",
    responses={
        200: {
            "description": "Continuous SSE frames relayed from the events channel.",
            "content": {"text/event-stream": {"schema": {"type": "string"}}},
        },
        401: {"description": "Not authenticated (or stream token expired/consumed)."},
    },
)
async def events_stream(user: str = Depends(get_stream_identity)) -> StreamingResponse:
    """Stream processing events to the client as SSE.

    Accepts standard credentials (headers) or a single-use ephemeral
    stream token via ``?token=`` — mint one at POST /brs/auth/stream-token.
    """
    return StreamingResponse(
        _subscribe_events(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            # Disable proxy buffering so events flush immediately.
            "X-Accel-Buffering": "no",
        },
    )
