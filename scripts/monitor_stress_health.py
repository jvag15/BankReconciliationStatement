#!/usr/bin/env python
"""System resource and health monitor for BRS stress runs.

Polls GET /health/deep and prints a compact, real-time readout of memory
pressure, PostgreSQL pool occupancy and Redis queue depth. Usable two
ways:

- Standalone CLI:  python scripts/monitor_stress_health.py --duration 60
- Imported by the load harness: ``await sample_health(client, base_url)``
  for a single snapshot, or ``await monitor_loop(...)`` as a background task.

The endpoint is unauthenticated (like /metrics), so no token is required.
"""

from __future__ import annotations

import argparse
import asyncio
import time
from typing import Any

import httpx


async def sample_health(client: httpx.AsyncClient, base_url: str) -> dict[str, Any]:
    """Return one /health/deep snapshot, or an error marker on failure."""
    try:
        resp = await client.get(f"{base_url}/health/deep", timeout=5.0)
        return resp.json()
    except Exception as exc:  # noqa: BLE001 - monitoring must not crash the run
        return {"status": "UNREACHABLE", "error": type(exc).__name__}


def format_snapshot(snap: dict[str, Any]) -> str:
    """Render a snapshot as a single compact line."""
    checks = snap.get("checks", {})
    db = checks.get("database", {})
    redis = checks.get("redis", {})
    storage = checks.get("storage", {})
    model = checks.get("model_singleton", {})
    return (
        f"status={snap.get('status', '?'):9s} "
        f"db={db.get('status', '?')}(act={db.get('active_connections', '?')}/"
        f"pool={db.get('pool_size', '?')}) "
        f"redis={redis.get('status', '?')}(qdepth={redis.get('queue_depth', '?')}) "
        f"storage_free_gb={storage.get('free_gb', '?')} "
        f"model={model.get('status', '?')}"
    )


async def monitor_loop(
    base_url: str,
    interval: float,
    stop_event: asyncio.Event,
    samples: list[dict[str, Any]] | None = None,
    quiet: bool = False,
) -> list[dict[str, Any]]:
    """Poll /health/deep every *interval* seconds until *stop_event* is set."""
    collected = samples if samples is not None else []
    async with httpx.AsyncClient() as client:
        while not stop_event.is_set():
            snap = await sample_health(client, base_url)
            snap["_ts"] = time.time()
            collected.append(snap)
            if not quiet:
                print(f"[health] {format_snapshot(snap)}", flush=True)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
    return collected


async def _run_for(base_url: str, interval: float, duration: float) -> None:
    stop = asyncio.Event()

    async def _stopper() -> None:
        await asyncio.sleep(duration)
        stop.set()

    await asyncio.gather(monitor_loop(base_url, interval, stop), _stopper())


def main() -> None:
    parser = argparse.ArgumentParser(description="Poll /health/deep during a stress run.")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--duration", type=float, default=60.0)
    args = parser.parse_args()
    asyncio.run(_run_for(args.base_url, args.interval, args.duration))


if __name__ == "__main__":
    main()
