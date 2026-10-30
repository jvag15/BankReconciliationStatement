"""Shared FastAPI deps — auth + DB pool."""
from fastapi import Depends, Header, HTTPException, Query, status
from fastapi.security import APIKeyHeader
from app.config import settings

api_key_header = APIKeyHeader(name="X-API-Token", auto_error=False)


def _extract_bearer(authorization: str | None) -> str | None:
    """Return the token from an ``Authorization: Bearer <token>`` header, or None."""
    if not authorization:
        return None
    value = authorization.strip()
    if value.lower().startswith("bearer "):
        return value[7:].strip() or None
    return None


async def get_current_user(
    x_authentik_email:    str | None = Header(default=None, alias="X-authentik-email"),
    x_authentik_username: str | None = Header(default=None, alias="X-authentik-username"),
    x_api_token:          str | None = Depends(api_key_header),
    authorization:        str | None = Header(default=None, alias="Authorization"),
    token:                str | None = Query(default=None),
) -> str:
    """
    Identity resolution:
    1. Authentik forwardAuth header (trusted proxy-injected identity).
    2. Valid API token, delivered by (in precedence order):
       a. ``X-API-Token`` header               (standard programmatic clients)
       b. ``Authorization: Bearer <token>``    (generic bearer clients)
       c. ``?token=<token>`` query parameter   (native EventSource SSE clients,
          which cannot set request headers)
    3. 401 Unauthorized.

    The first credential present in precedence order is the one validated;
    a present-but-wrong higher-precedence credential is not silently
    overridden by a lower one.
    """
    if x_authentik_email:
        return x_authentik_email.lower().strip()

    candidate: str | None = None
    if x_api_token and x_api_token.strip():
        candidate = x_api_token.strip()
    elif (bearer := _extract_bearer(authorization)) is not None:
        candidate = bearer
    elif token and token.strip():
        candidate = token.strip()

    if candidate is not None and candidate == settings.api_auth_token:
        return "api"

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Not authenticated",
    )

api_key_auth = get_current_user


async def get_stream_identity(
    x_authentik_email:    str | None = Header(default=None, alias="X-authentik-email"),
    x_authentik_username: str | None = Header(default=None, alias="X-authentik-username"),
    x_api_token:          str | None = Depends(api_key_header),
    authorization:        str | None = Header(default=None, alias="Authorization"),
    token:                str | None = Query(default=None),
) -> str:
    """Auth dependency for the SSE stream ONLY.

    Same resolution as :func:`get_current_user`, with one extension: when
    the selected credential is the ``?token=`` query parameter and it is
    not the long-lived API token, it is redeemed as a single-use ephemeral
    stream token (minted via POST /brs/auth/stream-token, stored in Redis
    with a 60s TTL, consumed atomically on first use).

    Scope isolation: standard endpoints depend on ``get_current_user``,
    which never consults Redis — a stream token is worthless anywhere but
    the stream. The precedence contract is preserved: a present-but-wrong
    header credential is rejected outright, never overridden by a
    lower-precedence query token.
    """
    if x_authentik_email:
        return x_authentik_email.lower().strip()

    candidate: str | None = None
    from_query = False
    if x_api_token and x_api_token.strip():
        candidate = x_api_token.strip()
    elif (bearer := _extract_bearer(authorization)) is not None:
        candidate = bearer
    elif token and token.strip():
        candidate = token.strip()
        from_query = True

    if candidate is not None:
        if candidate == settings.api_auth_token:
            return "api"
        if from_query:
            from app.stream_tokens import consume_stream_token

            identity = await consume_stream_token(candidate)
            if identity is not None:
                return identity

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Not authenticated",
    )
