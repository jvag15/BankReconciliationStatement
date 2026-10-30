"""OpenAPI schema completeness and standardized error-envelope tests."""

from __future__ import annotations

from fastapi.testclient import TestClient

from app.config import settings
from app.main import app

_ENVELOPE_KEYS = {"detail", "error_code", "timestamp"}


def _assert_envelope(body: dict, expected_code: str) -> None:
    assert set(body.keys()) == _ENVELOPE_KEYS, f"unexpected envelope shape: {body}"
    assert body["error_code"] == expected_code
    assert isinstance(body["detail"], str) and body["detail"]
    assert body["timestamp"].endswith("Z")


# ---------------------------------------------------------------------------
# Schema completeness
# ---------------------------------------------------------------------------


def test_openapi_schema_loads() -> None:
    resp = TestClient(app).get("/openapi.json")
    assert resp.status_code == 200
    schema = resp.json()
    assert schema["info"]["title"] == "Automated Financial Reconciliation Engine"
    assert schema["paths"], "no paths documented"


def test_every_operation_has_id_summary_and_lowercase_tags() -> None:
    schema = TestClient(app).get("/openapi.json").json()
    seen_ids: set[str] = set()
    for path, methods in schema["paths"].items():
        for method, op in methods.items():
            if method not in ("get", "post", "put", "delete", "patch"):
                continue
            op_id = op.get("operationId")
            assert op_id, f"{method.upper()} {path} missing operationId"
            assert op_id not in seen_ids, f"duplicate operationId {op_id}"
            seen_ids.add(op_id)
            assert op.get("summary"), f"{method.upper()} {path} missing summary"
            for tag in op.get("tags", []):
                assert tag == tag.lower(), f"{method.upper()} {path} has non-lowercase tag {tag}"


def test_openapi_tags_metadata_registered() -> None:
    schema = TestClient(app).get("/openapi.json").json()
    declared = {t["name"] for t in schema.get("tags", [])}
    # Every tag used by an operation must be declared with a description.
    used: set[str] = set()
    for methods in schema["paths"].values():
        for method, op in methods.items():
            if isinstance(op, dict):
                used.update(op.get("tags", []))
    assert used <= declared, f"tags used but undeclared: {used - declared}"


def test_spa_catch_all_not_in_schema() -> None:
    schema = TestClient(app).get("/openapi.json").json()
    assert "/{full_path}" not in schema["paths"]


# ---------------------------------------------------------------------------
# Standardized error envelope
# ---------------------------------------------------------------------------


def test_unknown_api_path_returns_json_404_envelope() -> None:
    resp = TestClient(app).get("/api/v1/does-not-exist")
    assert resp.status_code == 404
    assert resp.headers["content-type"].startswith("application/json")
    _assert_envelope(resp.json(), "NOT_FOUND")


def test_unknown_brs_path_returns_json_404_envelope() -> None:
    resp = TestClient(app).get("/brs/nope")
    assert resp.status_code == 404
    _assert_envelope(resp.json(), "NOT_FOUND")


def test_spa_client_routes_still_serve_html() -> None:
    """Non-API unknown paths keep serving the SPA shell for client routing."""
    resp = TestClient(app).get("/dashboard/some/client/route")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/html")


def test_unauthenticated_call_returns_401_envelope() -> None:
    resp = TestClient(app).get("/brs/webhooks")
    assert resp.status_code == 401
    _assert_envelope(resp.json(), "UNAUTHORIZED")


def test_validation_error_returns_422_envelope() -> None:
    # Authenticated PUT with a non-integer path param trips validation
    # without touching the database.
    resp = TestClient(app).put(
        "/api/v1/admin/transactions/not-an-int/category",
        json={"category": "Utilities"},
        headers={"X-API-Token": settings.api_auth_token},
    )
    assert resp.status_code == 422
    body = resp.json()
    _assert_envelope(body, "VALIDATION_ERROR")
    assert "transaction_id" in body["detail"]


def test_unhandled_exception_returns_generic_500_envelope() -> None:
    """Server faults return the envelope with no stack trace leakage."""
    from app.db import get_db_pool
    from app.deps import get_current_user

    def _boom() -> None:
        raise RuntimeError("secret internal failure detail")

    app.dependency_overrides[get_current_user] = lambda: "tester"
    app.dependency_overrides[get_db_pool] = _boom
    try:
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/brs/triage")
    finally:
        app.dependency_overrides.pop(get_current_user, None)
        app.dependency_overrides.pop(get_db_pool, None)

    assert resp.status_code == 500
    body = resp.json()
    _assert_envelope(body, "INTERNAL_SERVER_ERROR")
    # The internal exception message must not leak to the client.
    assert "secret internal failure detail" not in resp.text
