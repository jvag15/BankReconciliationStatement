# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Operating rules (operator-mandated)

- **Never run git commands** (`git add/commit/push/status`). The human operator handles all version control on the host.
- **Flat topology in `app/`**: every module lives directly under `app/` — the only permitted subdirectory is `app/routers/`. New helpers go in `app/`, new routers in `app/routers/` (response models are defined inline in their router file, not in a central schemas module).
- **Module prefix taxonomy** in `app/`: `task_*` (Celery tasks), `polars_*` (dataframe/ingestion), `ml_*` (models/inference), `db_*` (database helpers), `storage_*` (spool/encryption). Follow it for new modules.
- **No emojis** in code, logs, or comments.
- **Monetary baseline**: rupees are persisted as `NUMERIC(20,8)`; `amount_paisa` is a derived integer (`rupees * 100`) only — never the source of truth.

## Commands

Everything runs inside Docker; the host has no Python deps installed.

```bash
# Full stack (Postgres 15, Redis 7, brs_web:8000, brs_worker)
docker compose up -d --build --force-recreate

# Migrations are NOT applied automatically. On a fresh DB volume:
docker compose exec brs_web alembic upgrade head          # head: 007_webhook_subscriptions

# Test suite (expected baseline: 228 passed, 1 skipped, 2 deselected)
docker compose exec brs_web python -m pytest tests/

# Single test
docker compose exec brs_web python -m pytest tests/core/test_ingestion_pipeline.py -k <name>

# Stress suite (deselected by default via pyproject addopts; double-gated)
docker compose exec -e RUN_STRESS=1 brs_web python -m pytest -m stress tests/stress/

# Load/stress harness against the live stack
docker compose exec brs_web python scripts/generate_stress_data.py --rows 10000
docker compose exec brs_web python scripts/run_stress_test.py            # http mode
docker compose exec brs_web python scripts/monitor_stress_health.py      # standalone health poller
```

Test-count discipline: any change must keep the suite green at the baseline above. Stress tests carry a module-level `pytestmark = pytest.mark.stress` (a conftest `pytestmark` does not apply to modules) so `addopts = -m 'not stress'` in `pyproject.toml` deselects them.

## Architecture

Headless bank-statement reconciliation engine: FastAPI + Celery + Postgres + Redis, with a React SPA served by the same container.

**Containers** (`docker-compose.yml`): `brs_web` (uvicorn `app.main:app`, serves API + SPA), `brs_worker` (Celery with embedded beat `-B`, queues `ingestion,retrain,ml_predict`), `brs_postgres`, `brs_redis`. `./app`, `./alembic`, `./scripts`, `./var` are bind-mounted into both web and worker (`./tests` into web only) — code edits are live on container restart, no rebuild. `pyproject.toml`, `alembic.ini`, `requirements.txt`, and the SPA build are baked into the image — changes to those require a rebuild. A blanket `.:/app` mount must never be added: it would shadow the baked `/app/static`.

**Statement ingestion flow**: `POST /api/v1/statements/ingest` (`app/main.py`) normalises every upload to the encrypted-at-rest contract before it touches `STORAGE_DIR` (`/app/var/uploads`, shared web↔worker): a payload that decrypts with `FERNET_KEY` gets the fast-path header check (400 unless it contains `amount` and `description_raw`) and is stored as received; a raw `.csv`/`.txt` is encrypted server-side (`storage_encryption.encrypt_bytes`) with header validation left to the worker's schema check; a `.enc` that does not decrypt is stored byte-for-byte so the worker DLQs it (`InvalidToken`) — the deliberate corrupt-ciphertext contract. It then dispatches `process_statement_chunk` (`app/task_ingestion.py`), which publishes `BATCH_STARTED`/`BATCH_COMPLETED`/`BATCH_FAILED` events keyed by `chunk_id` (= the receipt's `filename_assigned`) that the SPA's upload screen tracks over SSE. The worker decrypts, parses via Polars with **adaptive chunk sizing** (`app/polars_memory.py` reads cgroup memory pressure and downscales 5000→2500→1250→500), writes `bank_transactions`, then chains unsupervised ML analysis. Decrypt/parse failures route to `dlq_transactions` (note: table is `dlq_transactions`, not `dead_letter_queue`); a beat task sweeps the DLQ for recovery.

**Ledger + matching**: `POST /brs/ledger/ingest` → `process_ledger_chunk` → `ledger_transactions`, then dispatches `run_matching_engine` (`app/task_matching_engine.py`) for bank rows in `SCORED` state. Dispatch is deduplicated across concurrent chunks by a Redis SET NX flag (`app/matching_dispatch_guard.py`, TTL via `MATCHING_DISPATCH_LOCK_TTL`, default 600s): only one matching pass can be pending at a time; the matching task releases the flag on completion so the next ledger chunk queues a fresh pass. Matching applies ADR-014 rules in priority order 1A→4B (`app/matching_rules.py`): exact UTR/amount/date first, then subset-sum split-payment rules 2A/2B, then fuzzy/temporal rules. The ledger query window is `CURRENT_DATE - 30 days` — synthetic/test data must use recent dates to match. Subset-sum candidates are capped by `MAX_SUBSET_CANDIDATES` (env, default 24); the brute force is `C(cap, 5)` per *unmatched* row, so raising the cap has combinatorial cost (48 breached the task timeout under load).

**Known quirk — split result backend**: the Celery worker (`app/celery_app.py`) stores results in **Redis**, but `GET /api/v1/statements/tasks/{id}` (`app/main.py`) reads the Postgres `celery_taskmeta` table, which is never written — that endpoint always returns `PENDING`. To observe real task completion, poll `AsyncResult` against the worker's Celery app (see `scripts/run_stress_test.py:_poll_backend`).

**Auth** (`app/deps.py`): resolution order is `X-authentik-email` header → `X-API-Token` header → `Authorization: Bearer` → `?token=` query param, all compared to `settings.api_auth_token`. `/health/*`, `/metrics`, and the task-status endpoint are unauthenticated. The SSE stream uses a dedicated dependency, `get_stream_identity`: same resolution, plus a `?token=st_...` value is redeemed as an **ephemeral stream token** — minted at `POST /brs/auth/stream-token` (requires standard auth), stored in Redis under `stream_token:<token>` with a 60s TTL, consumed atomically (GETDEL) on first use (`app/stream_tokens.py`). `get_current_user` never reads Redis, so stream tokens authenticate nothing but the stream; browser clients must mint a fresh token per EventSource (re)connect.

**Events**: workers publish to Redis pub/sub channel `brs_processing_events` (`app/events_publisher.py`); `GET /brs/events/stream` (`app/routers/events.py`) relays it as SSE.

**Health**: `GET /health/deep` (`app/routers/health.py`) reports DB pool occupancy, Redis queue depth, storage free space, and ML model singleton state; returns 200/HEALTHY or 503/DEGRADED.

**Rate limiting**: the ingest route limit is `INGEST_RATE_LIMIT` (env, default `10/minute`); compose sets `100000/minute` for local load testing — production must leave it unset.

**Error envelope + OpenAPI**: every API error is shaped as `ErrorResponse {detail, error_code, timestamp}` via global handlers in `app/main.py` (`StarletteHTTPException`, `RequestValidationError`, catch-all `Exception` — the 500 handler logs server-side and never leaks stack traces; note `TestClient` needs `raise_server_exceptions=False` to observe it). The SPA catch-all returns the JSON 404 envelope for unknown paths under `api/`, `brs/`, `health`, `metrics` and still serves `index.html` for client routes; it is `include_in_schema=False`. Every route declares `operation_id`, `summary`, a lowercase tag (declared in `openapi_tags`), and a `response_model` (`/brs/analysis` returns `AnalysisDataPayload`, mirrored key-for-key from `src/lib/types.ts` — response validation will reject drift, so change both together). `/health/deep` keeps its dynamic 200/503 `JSONResponse` with both documented via `responses=`.

**DB details**: SQLAlchemy via `app/session.py:get_db_connection`; `job_id` columns are Postgres `uuid` — compare against string lists with `job_id::text = ANY(:ids)` (scalar `= :id` implicitly casts, arrays do not). Config comes from `.env` (untracked; see `.env.example`) — `FERNET_KEY` is required for the encrypted upload spool.

**Stress framework**: `scripts/generate_stress_data.py` builds encrypted statement/ledger batches (recent dates, controlled anomaly rate, optional corrupt batch for DLQ routing) plus `manifest.json`; `scripts/run_stress_test.py` drives them via HTTP (`--mode http`) or direct Celery dispatch (`--mode celery`), measures latency/throughput/completion, exercises matching on a bounded sample (`--match-limit`), and can `--cleanup` its own rows by `job_id`.
