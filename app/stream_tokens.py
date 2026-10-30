"""Ephemeral single-use SSE stream tokens, backed by Redis.

The long-lived API token must never ride in a query string (it leaks into
proxy/access logs), but native ``EventSource`` clients cannot set headers.
The bridge: an authenticated client POSTs /brs/auth/stream-token to mint a
short-lived token, then opens ``/brs/events/stream?token=<st_...>``. The
token is stored in Redis under ``stream_token:<token>`` with a 60-second
TTL and is consumed atomically (GETDEL) on first use, so a leaked or
replayed token is worthless: it either expired or was already spent.

Only ``get_stream_identity`` (app/deps.py) consults these tokens, and only
for the SSE stream; ``get_current_user`` never reads Redis, so a stream
token presented to any standard endpoint fails auth by construction.
"""

from __future__ import annotations

import os
import secrets

# Key namespace in Redis; the full key is _KEY_PREFIX + token.
_KEY_PREFIX = "stream_token:"

# Token lifetime. Short by design: a token is minted immediately before the
# EventSource connect, and reconnects mint a fresh one.
STREAM_TOKEN_TTL_SECONDS = 60


def _redis_url() -> str:
    """Resolve the Redis URL from the same config the rest of the app uses."""
    return (
        os.environ.get("CELERY_BROKER_URL")
        or os.environ.get("REDIS_BROKER_URL")
        or "redis://brs_redis:6379/0"
    )


async def mint_stream_token(identity: str) -> str:
    """Create and persist a fresh single-use stream token for *identity*.

    Raises on Redis failure: minting must not hand out a token that was
    never stored (the client would get an instant 401 on connect with no
    hint why).
    """
    import redis.asyncio as aioredis

    token = f"st_{secrets.token_urlsafe(32)}"
    client = aioredis.from_url(_redis_url())
    try:
        await client.set(
            _KEY_PREFIX + token, identity, ex=STREAM_TOKEN_TTL_SECONDS,
        )
    finally:
        try:
            await client.aclose()
        except Exception:  # noqa: BLE001
            pass
    return token


async def consume_stream_token(candidate: str) -> str | None:
    """Atomically redeem *candidate*; return its identity or ``None``.

    GETDEL makes redemption single-use: two racing connects with the same
    token cannot both win. Fails closed — any Redis error reads as "no such
    token" (401), never as an authenticated identity.
    """
    if not candidate or not candidate.startswith("st_"):
        return None
    try:
        import redis.asyncio as aioredis

        client = aioredis.from_url(_redis_url())
        try:
            value = await client.getdel(_KEY_PREFIX + candidate)
        finally:
            try:
                await client.aclose()
            except Exception:  # noqa: BLE001
                pass
    except Exception:  # noqa: BLE001 - fail closed
        return None
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace")
    return str(value)
