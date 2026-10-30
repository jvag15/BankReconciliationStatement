# TESTING.md — BRS Engine Test & Forensics Guide

How to run the regression suite, the high-volume stress harness, and post-run
forensic verification. All commands run from the repo root on the host; the
tests themselves execute inside the containers (the host needs no Python
dependencies).

## 1. Prerequisites

- Docker Desktop running (Linux/WSL2 engine).
- A `.env` file at the repo root (copy `.env.example`); `FERNET_KEY` must be a
  valid Fernet key or encrypted uploads cannot be decrypted by the worker.
- Stack up and migrated:

```bash
docker compose up -d --build
```

```bash
docker compose exec brs_web alembic upgrade head
```

Migrations are NOT applied automatically. A fresh `pgdata` volume has no
schema until `alembic upgrade head` runs (current head: `007_webhook_subscriptions`).

## 2. Standard regression suite

WARNING: the suite's fixtures WIPE shared tables (`bank_transactions` and
others) for isolation and leave ~2,000 `chunk-concurrency-*` rows behind.
Never run pytest while stress data you care about is in the DB or while a
harness run is in flight — it will delete the evidence mid-run.

```bash
docker compose exec brs_web python -m pytest tests/
```

Expected baseline: **228 passed, 1 skipped, 2 deselected**.

- The 1 skip is a seed-dependent float-attrition case in
  `tests/core/test_monetary_precision.py` — normal.
- The 2 deselected are the stress tests (marker `stress`, deselected by
  `addopts = -m 'not stress'` in `pyproject.toml`).
- Any other number is a regression. Investigate before proceeding.

Single file or test:

```bash
docker compose exec brs_web python -m pytest tests/core/test_ingestion_pipeline.py -v
```

```bash
docker compose exec brs_web python -m pytest tests/core/test_null_safety.py -k subset_sum
```

## 3. Gated stress suite (pytest)

Runs the generator at volume plus a 5,000-row eager-mode ingestion through the
real Celery task body (no broker round-trip). Requires the live DB.

```bash
docker compose exec -e RUN_STRESS=1 brs_web python -m pytest -m stress tests/stress/ -v
```

Without `RUN_STRESS=1` the suite skips cleanly; without `-m stress` it is
deselected entirely.

## 4. High-volume load harness (live stack)

### 4.1 Generate payloads

```bash
docker compose exec brs_web python scripts/generate_stress_data.py --rows 10000
```

Writes encrypted statement/ledger batches plus `manifest.json` to
`STORAGE_DIR/stress` (`/app/var/uploads/stress`, i.e. `./var/uploads/stress`
on the host). The manifest records `rows_written` and `ledger_rows_written` —
keep it; forensics reconciles DB counts against it. Useful flags:
`--batches`, `--anomaly-rate` (default 0.05), `--match-fraction` (default
0.6), `--dlq-batches` (corrupt batches for DLQ routing), `--seed`.

### 4.2 Run the harness

```bash
docker compose exec brs_web python scripts/run_stress_test.py
```

Defaults: HTTP mode, concurrency 8, 2s health sampling, 180s task timeout,
matching sampled at `--match-limit 100`. Prints a JSON report: upload latency
p50/p95/p99, per-task ingest completion times, matching outcome, rows/sec,
final health status, and this run's `job_ids` (needed for forensics).

Options:

- `--mode celery` — dispatch straight to Celery, bypassing HTTP and the rate
  limiter, to isolate worker/DB throughput.
- `--cleanup` — delete this run's `bank_transactions` / `ledger_transactions`
  / `audit_jobs` rows afterwards (by both statement and ledger `job_id`s).
  Without it, rows accumulate.
- `--task-timeout N` — completion-poll deadline per task (default 180s). If
  ledger-triggered matching over a large SCORED backlog is grinding ahead of
  the harness's own sample on the solo-pool worker, raise this (e.g. 900).
- `--no-matching` — skip the matching-engine exercise.
- `--match-limit N` — matching sample size. The 2A/2B subset-sum rules are
  combinatorial (`C(MAX_SUBSET_CANDIDATES, 5)` per unmatched row, cap default
  24 via env `MAX_SUBSET_CANDIDATES`); raise with care.
- `--report path` — also write the JSON report to a file (use a path under
  `/app/var/...` inside the container; from Git Bash prefix the command with
  `MSYS_NO_PATHCONV=1` or the `/app` path gets rewritten).

Live health readout during (or independent of) a run:

```bash
docker compose exec brs_web python scripts/monitor_stress_health.py --duration 60
```

### 4.3 What "good" looks like

- `statements_accepted` == statements sent, `rate_limited_429: 0`
  (compose sets `INGEST_RATE_LIMIT=100000/minute` for local runs).
- `ingest_completion.succeeded` == tasks polled, `timeout: 0`.
- `matching.status: "SUCCESS"` within the task timeout.
- `health_final: "HEALTHY"`.

## 5. Forensics (post-run verification)

Substitute your DB credentials from `docker-compose.yml` / `.env`.

Row counts for this run (use the `job_ids` from the harness report):

```bash
docker compose exec brs_postgres psql -U brs_user -d brs_db -c "SELECT job_id, status, COUNT(*) FROM bank_transactions GROUP BY job_id, status ORDER BY job_id;"
```

Note: `job_id` columns are Postgres `uuid`. Scalar comparisons against text
implicitly cast, but array comparisons need `job_id::text = ANY(...)`.

Reconcile totals against the generator manifest (`rows_written`,
`ledger_rows_written`):

```bash
docker compose exec brs_postgres psql -U brs_user -d brs_db -c "SELECT (SELECT COUNT(*) FROM bank_transactions) bank, (SELECT COUNT(*) FROM ledger_transactions) ledger, (SELECT COUNT(*) FROM dlq_transactions) dlq;"
```

DLQ routing check — the corrupt batch must land here, not crash the worker
(the DLQ table is `dlq_transactions`):

```bash
docker compose exec brs_postgres psql -U brs_user -d brs_db -c "SELECT file_origin, exception_class, recovery_status, created_at FROM dlq_transactions ORDER BY created_at DESC LIMIT 10;"
```

Matching outcomes:

```bash
docker compose exec brs_postgres psql -U brs_user -d brs_db -c "SELECT status, COUNT(*) FROM bank_transactions GROUP BY status;"
```

Worker behaviour during the run (ingestion, chained ML analysis, matching,
retries, DLQ writes):

```bash
docker compose logs brs_worker --since 30m
```

Deep health after the run (DB pool, Redis queue depth, storage, model
singleton):

```bash
curl -s http://localhost:8000/health/deep
```

### Task-status caveat

`GET /api/v1/statements/tasks/{id}` reads the Postgres `celery_taskmeta`
table, but the worker stores results in Redis — so that endpoint reports
`PENDING` forever. Real completion status comes from the Redis result
backend; the harness does this internally (`_poll_backend` in
`scripts/run_stress_test.py`) via `AsyncResult` against the worker's Celery
app. For manual checks:

```bash
docker compose exec brs_web python -c "from app.celery_app import create_celery_app; from celery.result import AsyncResult; print(AsyncResult('TASK_ID', app=create_celery_app()).status)"
```

## 6. Resetting to a clean slate

The harness `--cleanup` removes only its own run. For a full reset (destroys
all DB and Redis data — local/dev only):

```bash
docker compose down -v
```

```bash
docker compose up -d
```

```bash
docker compose exec brs_web alembic upgrade head
```

## 7. Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| Uploads return 429 | `INGEST_RATE_LIMIT` not raised; set it in the web service env (compose already does for local). |
| Uploads return 400 "missing required columns" | Statement CSV header must contain `amount` and `description_raw`. |
| Ingest tasks never complete in harness | Worker down, or `FERNET_KEY` mismatch between generator and worker (payloads then route to DLQ instead). Check `docker compose logs brs_worker`. |
| Matching task times out | Sample too large or subset cap raised: lower `--match-limit`, keep `MAX_SUBSET_CANDIDATES` at 24. Worker must be restarted after changing the env/cap (it is read at import). |
| No matches found | Ledger dates outside the engine's 30-day window (`ledger_date >= CURRENT_DATE - 30 days`). The generator emits recent dates by design. |
| `relation ... does not exist` | Fresh volume without migrations: run `alembic upgrade head`. |
| pytest count differs in-container | Image predates `pyproject.toml` COPY (needed for marker deselection): rebuild with `docker compose up -d --build`. |
