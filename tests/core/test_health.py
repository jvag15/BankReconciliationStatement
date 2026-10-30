"""Tests for the /health/deep deep-diagnostics endpoint."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)

_DB_UP = {"status": "UP", "active_connections": 2, "idle_connections": 8, "pool_size": 10}
_DB_DOWN = {"status": "DOWN", "error": "ConnectionDoesNotExistError"}
_REDIS_UP = {"status": "UP", "queue_depth": 0, "queues": ["ingestion", "retrain", "ml_predict"]}
_REDIS_DOWN = {"status": "DOWN", "error": "ConnectionError"}
_STORAGE_UP = {"status": "UP", "free_gb": 42.5, "total_gb": 100.0, "path": "/app/var/uploads"}
_STORAGE_DOWN = {"status": "DOWN", "error": "FileNotFoundError"}
_MODEL_UP = {"status": "UP", "age_hours": 1.2, "version": "v20260722_0700"}


def test_health_deep_all_up_returns_200_healthy() -> None:
    with patch("app.health_inspector.check_database", new=AsyncMock(return_value=_DB_UP)), \
         patch("app.health_inspector.check_redis", new=AsyncMock(return_value=_REDIS_UP)), \
         patch("app.health_inspector.check_storage", return_value=_STORAGE_UP), \
         patch("app.health_inspector.check_model", return_value=_MODEL_UP):
        resp = client.get("/health/deep")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "HEALTHY"
    assert body["checks"]["database"]["status"] == "UP"
    assert body["checks"]["database"]["pool_size"] == 10
    assert body["checks"]["redis"]["queue_depth"] == 0
    assert body["checks"]["storage"]["free_gb"] == 42.5
    assert body["checks"]["model_singleton"]["version"] == "v20260722_0700"
    assert body["timestamp"].endswith("Z")


def test_health_deep_database_down_returns_503() -> None:
    with patch("app.health_inspector.check_database", new=AsyncMock(return_value=_DB_DOWN)), \
         patch("app.health_inspector.check_redis", new=AsyncMock(return_value=_REDIS_UP)), \
         patch("app.health_inspector.check_storage", return_value=_STORAGE_UP), \
         patch("app.health_inspector.check_model", return_value=_MODEL_UP):
        resp = client.get("/health/deep")

    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "UNHEALTHY"
    assert body["checks"]["database"]["status"] == "DOWN"


def test_health_deep_redis_down_returns_503() -> None:
    with patch("app.health_inspector.check_database", new=AsyncMock(return_value=_DB_UP)), \
         patch("app.health_inspector.check_redis", new=AsyncMock(return_value=_REDIS_DOWN)), \
         patch("app.health_inspector.check_storage", return_value=_STORAGE_UP), \
         patch("app.health_inspector.check_model", return_value=_MODEL_UP):
        resp = client.get("/health/deep")

    assert resp.status_code == 503
    assert resp.json()["status"] == "UNHEALTHY"


def test_health_deep_storage_down_returns_200_degraded() -> None:
    # Storage loss is degraded, not fatal: DB + Redis still serve traffic.
    with patch("app.health_inspector.check_database", new=AsyncMock(return_value=_DB_UP)), \
         patch("app.health_inspector.check_redis", new=AsyncMock(return_value=_REDIS_UP)), \
         patch("app.health_inspector.check_storage", return_value=_STORAGE_DOWN), \
         patch("app.health_inspector.check_model", return_value=_MODEL_UP):
        resp = client.get("/health/deep")

    assert resp.status_code == 200
    assert resp.json()["status"] == "DEGRADED"


def test_evaluate_model_missing_does_not_downgrade() -> None:
    from app.health_inspector import evaluate

    checks = {
        "database": _DB_UP,
        "redis": _REDIS_UP,
        "storage": _STORAGE_UP,
        "model_singleton": {"status": "MISSING", "age_hours": None, "version": None},
    }
    status, code = evaluate(checks)
    assert status == "HEALTHY"
    assert code == 200
