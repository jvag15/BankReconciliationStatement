"""Unit tests for the get_current_user token-extraction precedence."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import patch

import pytest
from fastapi import HTTPException

from app.config import settings
from app.deps import get_current_user, get_stream_identity

_VALID = settings.api_auth_token


def _call(**overrides: Any) -> str:
    """Invoke get_current_user directly with all auth inputs defaulted to None."""
    params: dict[str, Any] = {
        "x_authentik_email": None,
        "x_authentik_username": None,
        "x_api_token": None,
        "authorization": None,
        "token": None,
    }
    params.update(overrides)
    return asyncio.run(get_current_user(**params))


def test_auth_via_x_api_token() -> None:
    assert _call(x_api_token=_VALID) == "api"


def test_auth_via_authorization_bearer() -> None:
    assert _call(authorization=f"Bearer {_VALID}") == "api"


def test_auth_bearer_scheme_is_case_insensitive() -> None:
    assert _call(authorization=f"bearer {_VALID}") == "api"


def test_auth_via_query_parameter() -> None:
    assert _call(token=_VALID) == "api"


def test_auth_authentik_email_takes_precedence() -> None:
    # A trusted forward-auth identity wins even over a (wrong) token.
    assert _call(x_authentik_email="User@Example.COM", x_api_token="wrong") == "user@example.com"


def test_auth_all_invalid_raises_401() -> None:
    with pytest.raises(HTTPException) as exc:
        _call(x_api_token="bad", authorization="Bearer bad", token="bad")
    assert exc.value.status_code == 401


def test_auth_no_credentials_raises_401() -> None:
    with pytest.raises(HTTPException) as exc:
        _call()
    assert exc.value.status_code == 401


def test_auth_malformed_authorization_ignored() -> None:
    # A non-Bearer Authorization value falls through to 401 (no query/header token).
    with pytest.raises(HTTPException) as exc:
        _call(authorization=_VALID)  # missing "Bearer " prefix
    assert exc.value.status_code == 401


# ---------------------------------------------------------------------------
# get_stream_identity — the SSE-only dependency with ephemeral-token support
# ---------------------------------------------------------------------------


def _call_stream(**overrides: Any) -> str:
    """Invoke get_stream_identity directly with all auth inputs defaulted."""
    params: dict[str, Any] = {
        "x_authentik_email": None,
        "x_authentik_username": None,
        "x_api_token": None,
        "authorization": None,
        "token": None,
    }
    params.update(overrides)
    return asyncio.run(get_stream_identity(**params))


def _consume_returning(identity: str | None):
    async def _fake(candidate: str) -> str | None:
        return identity

    return _fake


def test_stream_identity_standard_header_passthrough() -> None:
    assert _call_stream(x_api_token=_VALID) == "api"


def test_stream_identity_api_token_via_query_still_works() -> None:
    assert _call_stream(token=_VALID) == "api"


def test_stream_identity_redeems_ephemeral_query_token() -> None:
    with patch("app.stream_tokens.consume_stream_token", _consume_returning("tester@example.com")):
        assert _call_stream(token="st_fresh") == "tester@example.com"


def test_stream_identity_consumed_or_expired_token_401() -> None:
    with patch("app.stream_tokens.consume_stream_token", _consume_returning(None)):
        with pytest.raises(HTTPException) as exc:
            _call_stream(token="st_spent")
    assert exc.value.status_code == 401


def test_stream_identity_wrong_header_not_overridden_by_query_token() -> None:
    # Precedence contract: a present-but-wrong header credential is rejected;
    # a lower-precedence query token must not rescue it.
    with patch("app.stream_tokens.consume_stream_token", _consume_returning("x")):
        with pytest.raises(HTTPException) as exc:
            _call_stream(x_api_token="wrong", token="st_fresh")
    assert exc.value.status_code == 401


def test_stream_identity_header_carried_stream_token_rejected() -> None:
    # Ephemeral tokens are redeemable only from the query parameter.
    with patch("app.stream_tokens.consume_stream_token", _consume_returning("x")):
        with pytest.raises(HTTPException) as exc:
            _call_stream(x_api_token="st_fresh")
    assert exc.value.status_code == 401
