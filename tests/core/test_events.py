"""Tests for the SSE processing-events stream."""

from __future__ import annotations

from typing import AsyncIterator
from unittest.mock import patch

from fastapi.testclient import TestClient

import app.routers.events as events_mod
from app.deps import get_stream_identity
from app.main import app


async def _fake_stream() -> AsyncIterator[str]:
    """A finite stand-in for the Redis-backed generator so tests don't hang."""
    yield 'data: {"event_type": "BATCH_STARTED", "payload": {"chunk_id": "abc"}}\n\n'
    yield 'data: {"event_type": "BATCH_COMPLETED", "payload": {"rows_written": 5}}\n\n'


def test_events_stream_returns_event_stream_headers() -> None:
    app.dependency_overrides[get_stream_identity] = lambda: "tester@example.com"
    try:
        # Swap the real Redis subscription for a finite fake so the stream ends.
        with patch.object(events_mod, "_subscribe_events", _fake_stream):
            client = TestClient(app)
            resp = client.get("/brs/events/stream")

        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/event-stream")
        assert resp.headers.get("cache-control") == "no-cache"

        body = resp.text
        assert body.count("data: ") == 2
        assert "BATCH_STARTED" in body
        assert body.endswith("\n\n")
    finally:
        app.dependency_overrides.pop(get_stream_identity, None)


def test_events_stream_requires_auth() -> None:
    client = TestClient(app)
    resp = client.get("/brs/events/stream")
    assert resp.status_code == 401


def test_events_stream_auth_via_query_param() -> None:
    """Native EventSource clients authenticate via ?token= (no headers)."""
    from app.config import settings

    with patch.object(events_mod, "_subscribe_events", _fake_stream):
        client = TestClient(app)
        resp = client.get(f"/brs/events/stream?token={settings.api_auth_token}")

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/event-stream")
    assert "BATCH_STARTED" in resp.text


def test_events_stream_auth_via_bearer_header() -> None:
    """Bearer-header clients can also reach the SSE stream."""
    from app.config import settings

    with patch.object(events_mod, "_subscribe_events", _fake_stream):
        client = TestClient(app)
        resp = client.get(
            "/brs/events/stream",
            headers={"Authorization": f"Bearer {settings.api_auth_token}"},
        )

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/event-stream")


def test_publish_event_serializes_and_publishes() -> None:
    """publish_event pushes an event_type/payload JSON string to the channel."""
    from app import events_publisher

    captured: dict[str, object] = {}

    class _FakeRedis:
        def publish(self, channel: str, body: str) -> int:
            captured["channel"] = channel
            captured["body"] = body
            return 1

        def close(self) -> None:
            captured["closed"] = True

    with patch("redis.Redis.from_url", return_value=_FakeRedis()):
        ok = events_publisher.publish_event("BATCH_STARTED", {"chunk_id": "z1"})

    assert ok is True
    assert captured["channel"] == events_publisher.EVENTS_CHANNEL
    import json

    parsed = json.loads(captured["body"])  # type: ignore[arg-type]
    assert parsed == {"event_type": "BATCH_STARTED", "payload": {"chunk_id": "z1"}}
    assert captured.get("closed") is True


def test_publish_event_never_raises_on_failure() -> None:
    """A broker error is swallowed and reported as False, not raised."""
    from app import events_publisher

    with patch("redis.Redis.from_url", side_effect=OSError("redis down")):
        assert events_publisher.publish_event("BATCH_FAILED", {"x": 1}) is False


# ---------------------------------------------------------------------------
# Ephemeral stream tokens (POST /brs/auth/stream-token + ?token= redemption)
# ---------------------------------------------------------------------------


class _FakeAsyncRedis:
    """Minimal async Redis stand-in backing mint (SET EX) and consume (GETDEL)."""

    def __init__(self, store: dict) -> None:
        self._store = store

    async def set(self, key: str, value: str, ex: int | None = None) -> bool:
        self._store[key] = (value, ex)
        return True

    async def getdel(self, key: str):
        entry = self._store.pop(key, None)
        return None if entry is None else entry[0]

    async def aclose(self) -> None:
        pass


def _patched_redis(store: dict):
    return patch("redis.asyncio.from_url", lambda *a, **k: _FakeAsyncRedis(store))


def test_stream_token_mint_returns_token_with_ttl() -> None:
    """Minting requires standard auth and stores the token with a 60s TTL."""
    from app.config import settings

    store: dict = {}
    with _patched_redis(store):
        client = TestClient(app)
        resp = client.post(
            "/brs/auth/stream-token",
            headers={"X-API-Token": settings.api_auth_token},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["stream_token"].startswith("st_")
    assert body["expires_in"] == 60

    key, (identity, ttl) = next(iter(store.items()))
    assert key == f"stream_token:{body['stream_token']}"
    assert identity == "api"
    assert ttl == 60


def test_stream_token_mint_requires_auth() -> None:
    client = TestClient(app)
    resp = client.post("/brs/auth/stream-token")
    assert resp.status_code == 401


def test_stream_token_single_use_flow() -> None:
    """A minted token opens the stream once; the second attempt gets 401."""
    from app.config import settings

    store: dict = {}
    with _patched_redis(store):
        client = TestClient(app)
        tok = client.post(
            "/brs/auth/stream-token",
            headers={"X-API-Token": settings.api_auth_token},
        ).json()["stream_token"]

        with patch.object(events_mod, "_subscribe_events", _fake_stream):
            first = client.get(f"/brs/events/stream?token={tok}")
            second = client.get(f"/brs/events/stream?token={tok}")

    assert first.status_code == 200
    assert first.headers["content-type"].startswith("text/event-stream")
    assert "BATCH_STARTED" in first.text
    # GETDEL consumed the key on the first connect.
    assert second.status_code == 401
    assert store == {}


def test_stream_token_rejected_on_standard_route() -> None:
    """Scope isolation: a stream token authenticates nothing but the stream."""
    from app.config import settings

    store: dict = {}
    with _patched_redis(store):
        client = TestClient(app)
        tok = client.post(
            "/brs/auth/stream-token",
            headers={"X-API-Token": settings.api_auth_token},
        ).json()["stream_token"]

        resp = client.get(f"/api/v1/admin/flagged_transactions?token={tok}")

    assert resp.status_code == 401
    # get_current_user never touches Redis: the token was not consumed.
    assert f"stream_token:{tok}" in store


def test_stream_token_expired_or_unknown_401() -> None:
    """A token absent from Redis (expired or never minted) is rejected."""
    store: dict = {}  # empty: simulates TTL expiry having removed the key
    with _patched_redis(store):
        client = TestClient(app)
        resp = client.get("/brs/events/stream?token=st_expired_or_bogus")
    assert resp.status_code == 401
