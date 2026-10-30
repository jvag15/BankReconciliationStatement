# BRS Engine — Deployment & Server Integration Guide

> **Audience:** the Claude Code agent (or engineer) integrating this app into
> the firm's central hosting stack. Read this file in full before changing
> `docker-compose.yml`, `Dockerfile`, or any `app/` module.

BRS Engine (Bank Reconciliation Subsystem) ingests bank statements and internal
ledger exports, reconciles them against each other with deterministic accounting
rules, applies ML anomaly scoring, and exposes a review UI for human triage.

---

## 1. What this app is, architecturally

| Component | Role |
|---|---|
| `brs_web` | FastAPI + Uvicorn on **:8000**. Serves the REST API **and** the compiled React SPA from `/app/static`. |
| `brs_worker` | Celery worker. Consumes queues `ingestion`, `retrain`, `ml_predict`. |
| `brs_postgres` | PostgreSQL 15. Relational store + Celery result backend. |
| `brs_redis` | Redis 7. Celery broker and rate-limit storage. |

**Single-image design.** The `Dockerfile` is multi-stage: stage 1 (`node:20-alpine`)
builds the SPA with Vite; stage 2 (`python:3.11-slim`) installs Python deps and copies
the built `dist/` to `/app/static`. `brs_web` and `brs_worker` run the *same image*
with different commands. There is no separate frontend container.

### ⚠️ Deviations from `corporate-app-template` — read before "fixing" them

This app deliberately differs from the standard template. These are decisions, not drift:

| Area | Template | BRS Engine | Why |
|---|---|---|---|
| Async work | none | **Celery + Redis + worker** | Statement files run 200k–500k rows. Synchronous HTTP would time out. The template has no async story; this is an additive extension, not a violation. |
| Migrations | `migrations/*.sql` applied via `psql` | **Alembic** (`alembic/versions/`) | BRS has 5 versioned revisions and ongoing schema evolution. Alembic is strictly more capable here. Do **not** squash into a single `001_init.sql`. |
| Database | schema inside shared `data_analytics-postgres` | **own database** (`brs_db`) | Not yet migrated to the core-consumer pattern. See §6. |
| Python | 3.12-slim | **3.11-slim** | `hdbscan` / `lightgbm` wheel availability. Verify before bumping. |

**Architectural law (do not break):** `app/` stays **flat**. The only permitted
subdirectory is `app/routers/`. Modules use defensive prefixes — `db_*`, `task_*`,
`polars_*`, `ml_*`, `model_*` — to prevent namespace shadowing. Never create a
directory whose name collides with an existing module (e.g. an `app/db/` package
alongside `app/db.py`) — that breaks `importlib` resolution.

All imports resolve from the flat root: `from app.db_models import metadata`.
Note `app/db_models.py` is SQLAlchemy **Core** (`Table` objects), not ORM classes —
there is no `Transaction` model class to import.

---

## 2. Required environment variables

Set these in the host `.env` that Compose reads. **Never commit real values.**

| Variable | Required | Purpose |
|---|---|---|
| `DATABASE_URL` | ✅ | `postgresql://user:pass@host:5432/brs_db`. Used by API, worker, and Alembic. |
| `CELERY_BROKER_URL` | ✅ | `redis://<redis-host>:6379/0`. |
| `POSTGRES_RESULT_BACKEND_URL` | ✅ | `db+postgresql://user:pass@host:5432/brs_db`. Celery task-state store. |
| `FERNET_KEY` | ✅ | Fernet key for at-rest encryption of uploads. **Generate per environment**; if unset, the ledger endpoint returns `503` rather than writing plaintext. |
| `API_AUTH_TOKEN` | ✅ | Bearer token for the `X-API-Token` header. |
| `API_URL` | ✅ (worker) | Internal URL the worker calls back on, e.g. `http://brs_web:8000`. |
| `BRS_UPLOAD_DIR` | optional | Overrides the encrypted upload spool. Defaults to `<app-root>/var/uploads`. |
| `LIGHTGBM_MODEL_PATH` | optional | LightGBM booster artifact. If unset the worker still boots; ML tasks fail on access. |
| `FASTTEXT_MODEL_PATH` | optional | fastText model. Same fail-soft behaviour. |
| `HDBSCAN_CACHE_DIR` | optional | HDBSCAN memoisation cache. |
| `HYPERPARAMETER_TUNING_ITERATIONS` | optional | `RandomizedSearchCV` iterations (default 5). Bounds retrain time. |

Generate secrets:

```bash
# FERNET_KEY
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# API_AUTH_TOKEN
openssl rand -hex 32
```

**Token binding gotcha:** the Pydantic field is `Settings.api_auth_token`, which binds
the `API_AUTH_TOKEN` env var. Do not rename it to `auth_token` — that silently unbinds
the variable and every request falls back to the `dev-token` default.

**SPA token:** the frontend reads `VITE_API_AUTH_TOKEN`, injected as a **build arg**
and baked into the JS bundle at build time. It is therefore *not* a secret from the
browser's perspective — treat it as a low-privilege gate and rely on Authentik
forwardAuth (§6) for real authentication. Changing it requires an image rebuild.

---

## 3. Resource limits

Current limits, tuned for a ~4 GB on-prem target:

| Service | Memory | Notes |
|---|---|---|
| `brs_web` | 2 GB | FastAPI is light; headroom is for upload buffering. |
| `brs_worker` | 4 GB | Highest ceiling — Polars + scikit-learn + LightGBM all materialise here. |
| `brs_postgres` | 2 GB | Raise with data volume; watch `shared_buffers`. |
| `brs_redis` | 1 GB | Queue depth only. |

**Sizing guidance**
- Worker memory is the binding constraint. Ingestion is chunked and Polars scans
  lazily, but ML inference materialises feature matrices. Below 4 GB, expect OOM
  kills on large statements.
- Celery concurrency auto-sizes to `cpu_count() - 2` (min 1), reserving cores for
  Postgres and the web tier — see `create_celery_app()` in `app/celery_app.py`.
- `worker_prefetch_multiplier=1` and `task_acks_late=True` are deliberate: they
  trade throughput for crash-safety so an OOM-killed worker doesn't lose a chunk.
- **Connection pool pressure is a known scar.** `app/session.py` opens a pooled
  engine (`pool_size=20, max_overflow=20`) *per process*. Total Postgres connections
  ≈ `(web + worker processes) × 40`. Raise `max_connections` on Postgres or lower the
  pool before scaling worker replicas, or you will exhaust the server.

---

## 4. Storage volumes

| Path | Purpose | Requirement |
|---|---|---|
| `/app/var/uploads` | Encrypted upload spool | **Must be shared between `brs_web` and `brs_worker`.** The API writes ciphertext here; the worker reads it back *by path*. If it isn't shared, every ingest fails with `FileNotFoundError`. |
| `/app/static` | Compiled SPA | Baked into the image. **Never** shadow it with a blanket `.:/app` bind mount — that hides the build output and the UI 404s. |
| pgdata / redis_data | Named volumes | Standard persistence. |

Create host directories **before** adding a bind mount. Docker auto-creates missing
paths as empty directories, which produces confusing `not a directory` mount errors.

---

## 5. Deploy & verify

```bash
docker compose build brs_web            # rebuilds SPA + Python image
docker compose up -d                    # all four services
docker compose exec brs_web alembic upgrade head
```

Verification checklist:

```bash
docker compose exec brs_web alembic current          # expect: 005_... (head)
docker compose exec brs_web python -m pytest tests/  # expect: 185 passed, 1 skipped
curl -so /dev/null -w '%{http_code}\n' http://localhost:8000/            # 200 (SPA)
curl -so /dev/null -w '%{http_code}\n' http://localhost:8000/brs/analysis # 401 without token
curl -so /dev/null -w '%{http_code}\n' -H "X-API-Token: $API_AUTH_TOKEN" \
     http://localhost:8000/brs/analysis                                   # 200
docker compose logs brs_worker | grep ready                              # worker online
```

**Migrations are not automatic.** No service runs `alembic upgrade` on startup;
run it explicitly after deploy. Revision `005` restores append-only `RULE`s on
`audit_log` that `metadata.create_all()` cannot express — if a database was ever
bootstrapped by `create_all()` instead of Alembic, run `alembic stamp head`
followed by `alembic upgrade head` to reconcile without dropping data.

**Never** provision production schema via `create_all()`. It silently omits the
`CREATE RULE` statements and leaves the audit ledger mutable.

---

## 6. Integrating with the central hosting stack

To run BRS behind the firm's Traefik + Authentik front door as a `corporate-core`
consumer, the following work is required. **None of it is done yet** — the app
currently ships as a standalone Compose stack.

1. **Auth is already wired.** `app/deps.py` resolves identity from the Authentik
   forwardAuth headers `X-authentik-email` / `X-authentik-username`, falling back
   to the `X-API-Token` header. No code change needed — just route traffic through
   the Authentik middleware.
2. **Add a Compose service block** to the central `docker-compose.yml` modelled on
   `docker-compose.snippet.yml`: Traefik labels
   (`entrypoints=websecure`, `certresolver=letsencrypt`,
   `middlewares=authentik@file`), `loadbalancer.server.port=8000`, and the
   `proxy` network.
3. **Redis and the worker have no template equivalent.** The snippet assumes a
   single stateless container. You must additionally provision a Redis instance and
   a second service running the worker command, both on the app network:
   ```
   celery -A app.celery_app worker -Q ingestion,retrain,ml_predict --loglevel=info
   ```
4. **Database.** BRS currently owns `brs_db`. Migrating to a schema inside the
   shared `data_analytics-postgres` means porting all Alembic revisions to a
   schema-qualified search path and creating a `brs_engine_app` role with
   `core_reader` inherited. Treat as a distinct project; do not attempt inline.
5. **Register as a consumer** in `core.consumers` (slug, display name, base URL,
   db role) so the app appears in the suite.
6. **Engagement scoping.** Suite apps carry an `EngagementPicker` bound to
   `core.engagements`. BRS has no engagement concept today — its tenancy unit is
   `audit_jobs.job_id`. Mapping one to the other is a design decision, not a
   mechanical port.

---

## 7. Known gaps — do not mistake these for regressions

Verified against a live stack; carrying these forward is intentional, fixing them is welcome.

1. **The processing chain is not fully wired.** `process_statement_chunk` dispatches
   only `execute_unsupervised_analysis`. The tasks `run_nlp_extraction`,
   `run_risk_scoring`, and `run_matching_engine` are implemented and registered but
   never dispatched by the ingestion flow, so bank rows remain `UNPROCESSED` and
   never reach `SCORED`. Consequence: **the matching engine never fires
   automatically**, because it consumes `SCORED` rows only.
2. **Ledger ingestion works and is the intended trigger.** `POST /brs/ledger/ingest`
   → `app/task_ledger_ingestion.py` → `ledger_transactions`, then dispatches
   `run_matching_engine` for any `SCORED` bank rows. It correctly skips the dispatch
   when none exist, which is the current state until (1) is resolved.
3. **Risk scores are not meaningful.** `app/task_risk_scoring.py` builds an
   all-zeros feature matrix and passes it to LightGBM. Wire real feature extraction
   before trusting `risk_score`, and note rule 4B keys off `risk_score >= 0.80`.
4. **Monetary units.** `bank_transactions.amount` and `ledger_transactions.amount`
   both store **rupees** in `NUMERIC(20,8)`; `matching_rules` compares them with a
   rupee-denominated `Decimal("0.01")` tolerance. The ledger parser also emits a
   derived `amount_paisa` integer for exact arithmetic — it is deliberately *not*
   persisted. Storing paisa in `amount` would make every comparison off by 100x.
   Separately, `run_ml_audit` and the flagged-transactions endpoint divide `amount`
   by 100 as though it were paisa — that is a **live bug**, amounts there read 100x low.
5. **Hardcoded upload path.** `app/main.py` sets the bank upload dir to an absolute
   Windows path. On Linux this resolves *relative*, creating a literal `./E:/...`
   directory inside the container, and the worker then can't find the file. The
   ledger endpoint in `app/routers/brs.py` uses the correct app-root-relative
   pattern — port `main.py` to match.
6. **Subset-sum performance.** `_find_subset_sum` caps subset size at 5 but iterates
   combinations over the *entire* candidate list — `C(N,5)`. With a large ±3-day
   window this will hang a worker. Pre-filter candidates before scaling up.
7. **Test tooling ships in the runtime image.** `pytest` and `freezegun` are in
   `requirements.txt` so the mandated in-container test command works. Split into a
   `requirements-dev.txt` if you want a leaner production image.

---

## 8. Test protocol

Run the suite **inside the running container** — do not write wrapper scripts:

```bash
docker compose exec brs_web python -m pytest tests/
```

Baseline: **185 passed, 1 skipped**. `tests/` and `scripts/` are copied into the
image explicitly; if you trim the `Dockerfile`'s `COPY` list, collection breaks
(`tests/qa_vectors/test_qa_cross_platform_chunking.py` imports from `scripts/`).
Do not weaken the temporal-window assertions in `tests/core/test_temporal_matching.py`.
