#!/usr/bin/env python
"""Asynchronous load/stress harness for the BRS Engine.

Drives the payloads produced by scripts/generate_stress_data.py against a
live stack and reports throughput, latency distribution and task
completion timing, while a background monitor samples /health/deep.

Dispatch modes
--------------
- ``--mode http`` (default): concurrent httpx uploads to the real ingest
  API (/api/v1/statements/ingest for statements, /brs/ledger/ingest for
  ledgers) using X-API-Token auth. Requires INGEST_RATE_LIMIT raised
  (docker-compose sets it high for the local stack).
- ``--mode celery``: dispatch process_statement_chunk / process_ledger_chunk
  directly via Celery, bypassing the HTTP layer and its rate limiter, to
  isolate worker/DB throughput.

After ingestion the harness optionally exercises the matching engine
(app/task_matching_engine.py): it transitions the freshly ingested bank
rows for this run to SCORED and dispatches run_matching_engine over a
bounded id batch, timing completion. Matching is scoped to this run's
job_ids so it never touches unrelated data.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import sys
import time
import uuid
from typing import Any

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import httpx  # noqa: E402

from scripts.monitor_stress_health import monitor_loop  # noqa: E402

_STMT_ENDPOINT = "/api/v1/statements/ingest"
_LEDGER_ENDPOINT = "/brs/ledger/ingest"

# Completion is read from the Celery *result backend* directly, not from the
# HTTP GET /api/v1/statements/tasks/{id} endpoint. That endpoint reads the
# Postgres ``celery_taskmeta`` table, but the worker (app/celery_app.py) is
# configured with a Redis result backend, so the Postgres table is never
# written and the endpoint always reports PENDING. Reading the worker's own
# backend via AsyncResult is the faithful signal for task completion + result.
_CELERY_APP: Any = None


def _token() -> str:
    tok = os.environ.get("API_AUTH_TOKEN")
    if tok:
        return tok
    try:
        from app.config import settings

        return settings.api_auth_token
    except Exception:  # noqa: BLE001
        return "dev-token"


def _percentile(data: list[float], pct: float) -> float:
    if not data:
        return 0.0
    ordered = sorted(data)
    k = (len(ordered) - 1) * pct / 100.0
    lo = int(k)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo)


# ---------------------------------------------------------------------------
# HTTP dispatch
# ---------------------------------------------------------------------------

async def _upload(
    client: httpx.AsyncClient,
    base_url: str,
    endpoint: str,
    path: pathlib.Path,
    headers: dict[str, str],
    params: dict[str, str] | None,
    sem: asyncio.Semaphore,
) -> dict[str, Any]:
    async with sem:
        data = path.read_bytes()
        t0 = time.perf_counter()
        try:
            resp = await client.post(
                f"{base_url}{endpoint}",
                headers=headers,
                files={"file": (path.name, data, "application/octet-stream")},
                params=params or {},
                timeout=60.0,
            )
            latency = time.perf_counter() - t0
            ok = resp.status_code in (200, 202)
            task_id = None
            if ok:
                try:
                    task_id = resp.json().get("task_id")
                except Exception:  # noqa: BLE001
                    pass
            return {
                "file": path.name, "status": resp.status_code,
                "latency": latency, "ok": ok, "task_id": task_id,
                "bytes": len(data),
            }
        except Exception as exc:  # noqa: BLE001
            return {
                "file": path.name, "status": None,
                "latency": time.perf_counter() - t0, "ok": False,
                "error": type(exc).__name__, "task_id": None, "bytes": len(data),
            }


def _worker_app() -> Any:
    """Return (cached) a Celery app pointing at the worker's result backend."""
    global _CELERY_APP
    if _CELERY_APP is None:
        from app.celery_app import create_celery_app

        _CELERY_APP = create_celery_app()
    return _CELERY_APP


def _poll_backend_blocking(task_id: str, timeout: float) -> tuple[str, float, dict | None]:
    from celery.result import AsyncResult

    app = _worker_app()
    start = time.perf_counter()
    while time.perf_counter() - start < timeout:
        res = AsyncResult(task_id, app=app)
        st = res.status
        if st in ("SUCCESS", "FAILURE"):
            result = res.result if st == "SUCCESS" else None
            if not isinstance(result, dict):
                result = None
            return st, time.perf_counter() - start, result
        time.sleep(0.5)
    return "TIMEOUT", time.perf_counter() - start, None


async def _poll_backend(task_id: str, timeout: float) -> tuple[str, float, dict | None]:
    """Poll the Celery result backend for a task, off the event loop."""
    return await asyncio.to_thread(_poll_backend_blocking, task_id, timeout)


# ---------------------------------------------------------------------------
# Celery dispatch
# ---------------------------------------------------------------------------

def _celery_dispatch(manifest: dict, stress_dir: pathlib.Path) -> list[str]:
    """Dispatch statement + ledger chunks straight to Celery. Returns task ids."""
    from app.task_ingestion import process_statement_chunk
    from app.task_ledger_ingestion import process_ledger_chunk

    task_ids: list[str] = []
    for entry in manifest["statements"]:
        r = process_statement_chunk.apply_async(kwargs={
            "chunk_id": f"stress_{uuid.uuid4().hex[:12]}",
            "file_origin": f"stress::{entry['company']}",
            "file_path_encrypted": str(stress_dir / entry["file"]),
        })
        task_ids.append(r.id)
    # Ledger task ids are polled too: their results carry the ledger-side
    # job_ids that --cleanup needs (ledger rows do not share statement jobs).
    for entry in manifest["ledgers"]:
        r = process_ledger_chunk.apply_async(kwargs={
            "chunk_id": f"stress_led_{uuid.uuid4().hex[:12]}",
            "file_origin": f"stress::{entry['company']}",
            "file_path_encrypted": str(stress_dir / entry["file"]),
            "company_name": entry["company"],
        })
        task_ids.append(r.id)
    return task_ids


# ---------------------------------------------------------------------------
# Matching-engine exercise (scoped to this run's job_ids)
# ---------------------------------------------------------------------------

def _score_and_collect(job_ids: list[str], match_limit: int) -> list[int]:
    """Transition this run's bank rows to SCORED and return a bounded id set."""
    from sqlalchemy import text

    from app.session import get_db_connection

    if not job_ids:
        return []
    with get_db_connection() as conn:
        with conn.begin():
            conn.execute(
                text(
                    "UPDATE bank_transactions SET status='SCORED', updated_at=NOW() "
                    "WHERE job_id::text = ANY(:ids) AND status='UNPROCESSED'"
                ),
                {"ids": job_ids},
            )
            rows = conn.execute(
                text(
                    "SELECT id FROM bank_transactions WHERE job_id::text = ANY(:ids) "
                    "AND status='SCORED' ORDER BY id LIMIT :lim"
                ),
                {"ids": job_ids, "lim": match_limit},
            ).fetchall()
    return [int(r[0]) for r in rows]


def _dispatch_matching(batch_ids: list[int]) -> str | None:
    from app.task_matching_engine import run_matching_engine

    if not batch_ids:
        return None
    return run_matching_engine.apply_async(kwargs={"batch_ids": batch_ids}).id


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def _cleanup(job_ids: list[str]) -> dict[str, int]:
    from sqlalchemy import text

    from app.session import get_db_connection

    deleted = {"bank": 0, "ledger": 0, "jobs": 0}
    if not job_ids:
        return deleted
    with get_db_connection() as conn:
        with conn.begin():
            deleted["bank"] = conn.execute(
                text("DELETE FROM bank_transactions WHERE job_id::text = ANY(:ids)"), {"ids": job_ids}
            ).rowcount or 0
            deleted["ledger"] = conn.execute(
                text("DELETE FROM ledger_transactions WHERE job_id::text = ANY(:ids)"), {"ids": job_ids}
            ).rowcount or 0
            deleted["jobs"] = conn.execute(
                text("DELETE FROM audit_jobs WHERE job_id::text = ANY(:ids)"), {"ids": job_ids}
            ).rowcount or 0
    return deleted


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

async def run(args: argparse.Namespace) -> dict[str, Any]:
    stress_dir = pathlib.Path(args.dir) if args.dir else pathlib.Path(
        os.environ.get("STORAGE_DIR", "/tmp/brs_storage")
    ) / "stress"
    manifest = json.loads((stress_dir / "manifest.json").read_text())
    headers = {"X-API-Token": _token()}

    stop = asyncio.Event()
    health_samples: list[dict] = []
    monitor = asyncio.create_task(
        monitor_loop(args.base_url, args.interval, stop, health_samples)
    )

    report: dict[str, Any] = {"run_id": manifest["run_id"], "mode": args.mode}
    wall_start = time.perf_counter()
    job_ids: list[str] = []

    try:
        if args.mode == "celery":
            task_ids = _celery_dispatch(manifest, stress_dir)
            report["dispatched_tasks"] = len(task_ids)
            # Poll statement tasks for completion + job_ids via the result backend.
            polled = await asyncio.gather(*[
                _poll_backend(tid, args.task_timeout) for tid in task_ids
            ])
            for st, _dt, result in polled:
                if st == "SUCCESS" and isinstance(result, dict) and result.get("job_id"):
                    job_ids.append(result["job_id"])
        else:
            sem = asyncio.Semaphore(args.concurrency)
            async with httpx.AsyncClient() as client:
                stmt_uploads = await asyncio.gather(*[
                    _upload(client, args.base_url, _STMT_ENDPOINT,
                            stress_dir / e["file"], headers,
                            None, sem)
                    for e in manifest["statements"]
                ])
                corrupt_uploads = await asyncio.gather(*[
                    _upload(client, args.base_url, _STMT_ENDPOINT,
                            stress_dir / e["file"], headers, None, sem)
                    for e in manifest.get("corrupt", [])
                ])
                ledger_uploads = await asyncio.gather(*[
                    _upload(client, args.base_url, _LEDGER_ENDPOINT,
                            stress_dir / e["file"], headers,
                            {"company_name": e["company"]}, sem)
                    for e in manifest["ledgers"]
                ])

                latencies = [u["latency"] for u in stmt_uploads if u["ok"]]
                accepted = [u for u in stmt_uploads if u["ok"]]
                report["http"] = {
                    "statements_sent": len(stmt_uploads),
                    "statements_accepted": len(accepted),
                    "rate_limited_429": sum(1 for u in stmt_uploads if u["status"] == 429),
                    "errors": sum(1 for u in stmt_uploads if not u["ok"]),
                    "corrupt_sent": len(corrupt_uploads),
                    "ledgers_accepted": sum(1 for u in ledger_uploads if u["ok"]),
                    "latency_p50": round(_percentile(latencies, 50), 4),
                    "latency_p95": round(_percentile(latencies, 95), 4),
                    "latency_p99": round(_percentile(latencies, 99), 4),
                }

            # Poll statement tasks for completion + job_ids via the result backend.
            polled = await asyncio.gather(*[
                _poll_backend(u["task_id"], args.task_timeout)
                for u in accepted if u["task_id"]
            ])
            completions = [dt for st, dt, _r in polled if st == "SUCCESS"]
            for st, _dt, result in polled:
                if st == "SUCCESS" and isinstance(result, dict) and result.get("job_id"):
                    job_ids.append(result["job_id"])

            # Ledger chunks run under their own job_ids; harvest those too so
            # --cleanup can remove this run's ledger rows (they are invisible
            # to the statement job_ids and would otherwise accumulate).
            ledger_polled = await asyncio.gather(*[
                _poll_backend(u["task_id"], args.task_timeout)
                for u in ledger_uploads if u.get("task_id")
            ])
            for st, _dt, result in ledger_polled:
                if st == "SUCCESS" and isinstance(result, dict) and result.get("job_id"):
                    job_ids.append(result["job_id"])
            report["ingest_completion"] = {
                "tasks_polled": len(polled),
                "succeeded": sum(1 for st, _d, _r in polled if st == "SUCCESS"),
                "failed": sum(1 for st, _d, _r in polled if st == "FAILURE"),
                "timeout": sum(1 for st, _d, _r in polled if st == "TIMEOUT"),
                "completion_p50": round(_percentile(completions, 50), 3),
                "completion_p95": round(_percentile(completions, 95), 3),
            }

        report["unique_jobs"] = len(set(job_ids))

        # Matching-engine exercise.
        if not args.no_matching and job_ids:
            batch_ids = await asyncio.to_thread(_score_and_collect, list(set(job_ids)), args.match_limit)
            match_task = await asyncio.to_thread(_dispatch_matching, batch_ids)
            match_result = {"scored_ids": len(batch_ids), "task_id": match_task}
            if match_task:
                st, dt, result = await _poll_backend(match_task, args.task_timeout)
                match_result.update({"status": st, "seconds": round(dt, 3), "result": result})
            report["matching"] = match_result

    finally:
        stop.set()
        await monitor

    report["wall_seconds"] = round(time.perf_counter() - wall_start, 3)
    total_rows = manifest.get("rows_written", 0)
    if report["wall_seconds"] > 0:
        report["rows_per_sec"] = round(total_rows / report["wall_seconds"], 1)
    report["health_samples"] = len(health_samples)
    report["health_final"] = health_samples[-1].get("status") if health_samples else None
    report["job_ids"] = sorted(set(job_ids))

    if args.cleanup:
        report["cleanup"] = await asyncio.to_thread(_cleanup, list(set(job_ids)))

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="BRS Engine load/stress harness.")
    parser.add_argument("--dir", default=None, help="Stress payload dir (default: STORAGE_DIR/stress).")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--mode", choices=["http", "celery"], default="http")
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--interval", type=float, default=2.0, help="Health-poll interval (s).")
    parser.add_argument("--task-timeout", type=float, default=180.0)
    parser.add_argument("--match-limit", type=int, default=100, help="Max bank ids fed to the matching engine as a bounded sample. The engine's 2A/2B rules are combinatorial in the per-window candidate set, so full-population reconciliation is a heavier batch job; this samples it to demonstrate the match path completes under load. Raise with care.")
    parser.add_argument("--no-matching", action="store_true", help="Skip the matching-engine exercise.")
    parser.add_argument("--cleanup", action="store_true", help="Delete this run's rows afterwards.")
    parser.add_argument("--report", default=None, help="Write the JSON report to this path.")
    args = parser.parse_args()

    report = asyncio.run(run(args))
    text = json.dumps(report, indent=2)
    print(text)
    if args.report:
        try:
            pathlib.Path(args.report).write_text(text)
        except OSError as exc:  # noqa: BLE001 - report is a convenience, never fatal
            print(f"[warn] could not write report to {args.report}: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
