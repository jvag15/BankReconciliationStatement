# BRS — Bank Reconciliation System

## What it is

Headless bank-statement reconciliation engine. FastAPI + Celery + Postgres + Redis backend, React SPA frontend served by same container. Docker-composed: `brs_web` (API+SPA), `brs_worker` (Celery, embedded beat), `brs_postgres`, `brs_redis`.

## What it's for

Ingest bank statements (CSV, encrypted) and a ledger, match transactions against each other via rule-based + ML matching engine (ADR-014 rules, priority 1A-4B: exact UTR/amount/date, subset-sum split payments, fuzzy/temporal), flag anomalies via unsupervised ML (clustering, isolation forest, risk scoring, NLP categorization), and surface it all in a reconciliation dashboard (risk grid, DBSCAN scatter, temporal heatmap, vendor risk concentration, etc). Built for high-volume statement processing with adaptive memory-aware chunking and stress-test tooling.

## File structure

```
app/                    Python backend, flat topology (only app/routers/ subdir allowed)
  main.py               FastAPI app, ingest endpoint, error envelope, OpenAPI setup
  deps.py                auth resolution (header/token/bearer/query)
  session.py, db.py, db_models.py, db_dlq.py, db_batch_claim.py    DB layer
  celery_app.py          Celery app (Redis result backend)
  config.py              settings
  events_publisher.py    Redis pub/sub for processing events
  stream_tokens.py        ephemeral SSE stream tokens
  matching_rules.py, matching_dispatch_guard.py    matching engine + dedup lock
  storage_encryption.py, storage_paths.py           encrypted upload spool
  polars_*.py            Polars ingestion/parsing/memory-adaptive chunking/reporting
  ml_*.py, model_classifier.py, feature_*.py         unsupervised ML (clustering, risk scoring, NLP, categorization)
  task_*.py              Celery tasks (ingestion, matching, retrain, DLQ recovery, webhook, etc)
  routers/               auth.py, brs.py, events.py, health.py, reports.py

alembic/                DB migrations (head: 007_webhook_subscriptions)
scripts/                stress/load test harness (generate_stress_data.py, run_stress_test.py, monitor_stress_health.py)
tests/                  pytest suite (baseline: 228 passed, 1 skipped, 2 deselected)
src/                    React/TS frontend
  App.tsx, main.tsx, index.css
  components/            IngestScreen, ReconciliationDashboard, AnalysisDashboard, TransactionRiskGrid, Navbar, charts/*
  lib/types.ts            mirrors AnalysisDataPayload response model key-for-key
  lib/features/, lib/utils/   Python helper scripts (feature extraction, report generation) bundled under src/
var/                    runtime storage (uploads spool), bind-mounted web+worker
docs/                   project docs
demo/                   demo assets
models/                 trained ML model artifacts
uploads/                sample/working uploads
dist/                   frontend build output
CLAUDE.md               operator rules + architecture notes for Claude Code
DEPLOYMENT.md, INTEGRATION.md, TESTING.md   ops/integration/test docs
docker-compose.yml, docker-compose.prod.yml, Dockerfile, Dockerfile.prod
pyproject.toml, requirements.txt, alembic.ini    Python config
package.json, vite.config.ts, tsconfig*.json, tailwind.config.js, postcss.config.js   frontend config
```

## Key architectural notes

- Statement ingest normalizes uploads to encrypted-at-rest contract before touching `STORAGE_DIR`; corrupt ciphertext deliberately routes to `dlq_transactions`.
- Matching dispatch deduped via Redis SET NX flag; ledger query window is rolling 30 days.
- Known quirk: task-status endpoint reads Postgres `celery_taskmeta` (never written) — always returns PENDING; real status must poll Celery's Redis backend directly.
- Monetary baseline: `NUMERIC(20,8)` rupees; `amount_paisa` is derived, never source of truth.
- Auth resolution order: `X-authentik-email` header, `X-API-Token`, `Authorization: Bearer`, `?token=`. SSE stream uses separate ephemeral stream-token flow.
