"""Authentication utility endpoints.

POST /brs/auth/stream-token mints a short-lived, single-use token for the
SSE stream (see app/stream_tokens.py for the security rationale). Minting
itself requires standard authentication, so the flow is: authenticate with
the long-lived credential over a header, receive an ephemeral token, and
hand only that ephemeral token to the query string that EventSource forces
on browser clients.

Contract for browser clients: tokens are single-use and expire in 60
seconds, and EventSource auto-reconnects after a drop — so the client must
mint a fresh token in its ``onerror`` handler and reopen the stream with
it, never retry the old URL.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.deps import get_current_user
from app.stream_tokens import STREAM_TOKEN_TTL_SECONDS, mint_stream_token

router = APIRouter(prefix="/brs/auth", tags=["auth"])


class StreamTokenResponse(BaseModel):
    stream_token: str
    expires_in: int = STREAM_TOKEN_TTL_SECONDS


@router.post(
    "/stream-token",
    response_model=StreamTokenResponse,
    operation_id="mint_stream_token",
    summary="Mint a single-use 60s token for the SSE stream.",
    responses={401: {"description": "Standard authentication required to mint."}},
)
async def issue_stream_token(
    user: str = Depends(get_current_user),
) -> StreamTokenResponse:
    """Mint a single-use SSE stream token bound to the caller's identity."""
    token = await mint_stream_token(user)
    return StreamTokenResponse(
        stream_token=token, expires_in=STREAM_TOKEN_TTL_SECONDS,
    )
