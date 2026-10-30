"""Redis-backed dedup guard for matching-engine dispatch.

Every successful ledger chunk used to queue its own ``run_matching_engine``
pass over the same 5,000-id window of SCORED bank rows. With N concurrent
chunk uploads (the stress harness sends 10) that produced N overlapping
passes, each costing minutes on the solo-pool worker even when it matched
zero rows. This module serialises the hand-off: a chunk may dispatch only
after claiming a TTL-bounded Redis flag (SET NX), and the matching task
clears the flag on completion so the next ledger chunk can queue a fresh
pass — which re-selects SCORED rows and re-reads the ledger window, so
nothing skipped here is lost.

The guard is a dedup flag, not a mutex: acquire fails open when Redis is
unreachable, because a redundant matching pass is merely slow while a
silently skipped one leaves rows unreconciled until the next upload.
"""

from __future__ import annotations

import os

# Single flag shared by the ledger-chunk dispatcher and the matching task.
DISPATCH_FLAG_KEY = "brs:matching_dispatch_pending"

# Safety-net expiry if the matching task dies before releasing the flag.
# Sized to comfortably cover an observed worst-case pass (~230s under
# stress load) plus queue wait on the solo pool.
_DEFAULT_TTL_SECONDS = 600


def _flag_ttl_seconds() -> int:
    """Resolve the flag TTL, overridable via MATCHING_DISPATCH_LOCK_TTL."""
    raw = os.environ.get("MATCHING_DISPATCH_LOCK_TTL", "")
    try:
        ttl = int(raw)
    except ValueError:
        return _DEFAULT_TTL_SECONDS
    return ttl if ttl > 0 else _DEFAULT_TTL_SECONDS


def _broker_url() -> str:
    """Resolve the Redis URL from the same config Celery uses."""
    return (
        os.environ.get("CELERY_BROKER_URL")
        or os.environ.get("REDIS_BROKER_URL")
        or "redis://brs_redis:6379/0"
    )


def acquire_dispatch_flag(owner: str) -> bool:
    """Claim the matching-dispatch slot; ``True`` means dispatch may proceed.

    ``owner`` is stored as the flag value purely for observability (which
    job claimed the slot). Returns ``True`` on any Redis failure — fail
    open, never block reconciliation on a telemetry-grade dependency.
    """
    client = None
    try:
        import redis

        client = redis.Redis.from_url(
            _broker_url(), socket_connect_timeout=2, socket_timeout=2,
        )
        return bool(
            client.set(
                DISPATCH_FLAG_KEY, owner, nx=True, ex=_flag_ttl_seconds(),
            )
        )
    except Exception:  # noqa: BLE001 - fail open, see docstring
        return True
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:  # noqa: BLE001
                pass


def release_dispatch_flag() -> bool:
    """Clear the slot so the next ledger chunk can dispatch a fresh pass.

    Best-effort and idempotent: deleting an absent (or expired) flag is
    harmless, and on failure the TTL clears it eventually.
    """
    client = None
    try:
        import redis

        client = redis.Redis.from_url(
            _broker_url(), socket_connect_timeout=2, socket_timeout=2,
        )
        client.delete(DISPATCH_FLAG_KEY)
        return True
    except Exception:  # noqa: BLE001 - TTL is the fallback
        return False
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:  # noqa: BLE001
                pass
