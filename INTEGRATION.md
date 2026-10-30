# BRS Engine: Local Testing & Enterprise Server Integration Guide

Operational instructions for local stakeholder testing, production deployment
onto main firm infrastructure, and a diagnostic questionnaire for network/AI
system administrators.

Companion documents: `DEPLOYMENT.md` (server integration details),
`TESTING.md` (test & forensics runbook), `CLAUDE.md` (architecture notes for
AI coding agents).

---

## Part 1: Local Testing Guide (For Stakeholders & Internal Testers)

### Prerequisites

- **Docker Desktop:** installed and running (Linux containers / WSL2 mode).
- **RAM:** minimum 8 GB available host RAM allocated to Docker.
- **Storage:** 5 GB free disk space.

### Quick Start Instructions

1. **Clone & navigate:**

```bash
git clone <repository_url>
```

```bash
cd bank-statement
```

2. **Create the environment file** (the repo intentionally does not track
   `.env`). Copy the template and fill in the two secrets:

```bash
cp .env.example .env
```

   Generate a Fernet key for `FERNET_KEY` (required — uploads are encrypted
   at rest and the stack refuses to persist plaintext without it):

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

   Set `API_AUTH_TOKEN` to the shared local testing token or your own value.
   The application default used by local tooling is
   `your-api-auth-token-here` — a development-only value
   that must never reach production.

3. **Launch the system:**

```bash
docker compose up -d --build
```

4. **Apply database migrations** (NOT automatic — a fresh database has no
   schema until this runs; current head: `007_webhook_subscriptions`):

```bash
docker compose exec brs_web alembic upgrade head
```

5. **Access the application:** open `http://localhost:8000` in a browser.
   API requests authenticate with the `X-API-Token` header carrying the
   token from your `.env`.

### Testing Workflow Walkthrough

1. **Upload & processing** (`Upload Statement` view):
   - Drag and drop a bank statement (`.csv`, `.txt`, or `.enc`).
   - Observe the real-time processing bar and chunk events pushed over
     Server-Sent Events (SSE).
   - Raw CSV payloads are automatically encrypted server-side (Fernet)
     before touching disk; `.enc` files are stored as received.
2. **Automated matching** (`Reconciliation Engine` view):
   - Click **Reconcile Statement** on upload completion.
   - Review matched vs. unmatched transaction metrics. Monetary values are
     persisted as `NUMERIC(20,8)` rupees; paisa integers are derived only.
3. **Multi-algorithm risk triage** (`Triage & Analysis` view):
   - Review anomaly scoring across nine analytical views backed by
     Isolation Forest, DBSCAN spatial clustering, Z-score deviation and
     temporal profiling.
   - Charts are scoped exclusively to the active `job_id` (the most recent
     upload by default; `GET /brs/analysis?job_id=<uuid>` for a specific one).
4. **PDF report export:**
   - Click **Export PDF Report** in the sidebar and inspect the multi-page
     print preview (summary metrics, charts, high-risk queue).
5. **Clean session reset:**
   - Click **Clear Session / New Job** in the top navigation to truncate
     transaction tables, clear Redis task caches, and return to a clean
     dropzone. The append-only `audit_log` is never cleared.

---

## Part 2: Main Firm Server Integration & Production Architecture

### System Architecture Overview

- **Web runtime (`brs_web`):** FastAPI application serving the REST/SSE API
  and the static bundled SPA.
- **Async task worker (`brs_worker`):** Celery worker executing Polars
  ingestion chunking, ADR-014 matching rules, and unsupervised ML scoring.
- **Database (`brs_postgres`):** PostgreSQL storing `bank_transactions`,
  `ledger_transactions`, `audit_jobs`, and the append-only `audit_log`.
- **Broker & cache (`brs_redis`):** Redis for Celery queues, pub/sub event
  distribution, and short-lived single-use SSE stream tokens.

### 1. Container Orchestration & Production Override

Use a `docker-compose.prod.yml` override. Note two corrections relative to
naive templates: the engine reads `CELERY_BROKER_URL` (there is no
`REDIS_URL` variable), and the base compose file raises `INGEST_RATE_LIMIT`
for local load testing — production must override it back down.

```yaml
services:
  brs_web:
    restart: always
    environment:
      - API_AUTH_TOKEN=${PROD_API_TOKEN}
      - FERNET_KEY=${PROD_FERNET_KEY}
      - DATABASE_URL=postgresql://${DB_USER}:${DB_PASS}@prod-db-host:5432/brs_production
      - CELERY_BROKER_URL=redis://prod-redis-host:6379/0
      - POSTGRES_RESULT_BACKEND_URL=db+postgresql://${DB_USER}:${DB_PASS}@prod-db-host:5432/brs_production
      # The base compose sets 100000/minute for local stress testing.
      # Production restores the strict default.
      - INGEST_RATE_LIMIT=10/minute
      # Uncomment under strict retention/audit obligations (disables the
      # destructive POST /brs/session/reset endpoint with HTTP 403):
      # - DISABLE_RESET_ENDPOINT=true

  brs_worker:
    restart: always
    environment:
      - CELERY_BROKER_URL=redis://prod-redis-host:6379/0
    deploy:
      resources:
        limits:
          cpus: '4.0'
          memory: 8G
```

### 2. Reverse Proxy & SSE Buffering (Nginx)

Buffer disabling is mandatory for `/brs/events/stream`; a buffering proxy
holds SSE frames and freezes real-time progress. The application already
sends `X-Accel-Buffering: no` on the stream response; the proxy must simply
honour it and keep the connection un-buffered:

```nginx
server {
    listen 443 ssl http2;
    server_name brs.firm-domain.internal;

    ssl_certificate /etc/ssl/certs/firm_cert.crt;
    ssl_certificate_key /etc/ssl/private/firm_key.key;

    # Standard API and UI proxying
    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # CRITICAL: no response buffering for the real-time SSE stream
    location /brs/events/stream {
        proxy_pass http://localhost:8000;
        proxy_set_header Host $host;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_buffering off;
        proxy_cache off;
        proxy_read_timeout 3600s;
    }
}
```

Access-log note: browser SSE clients authenticate with single-use 60-second
stream tokens in the query string (`?token=st_...`), minted per connection
at `POST /brs/auth/stream-token`. A logged token is already spent or
expired; still, keep access logs for this path access-controlled.

### 3. Enterprise Database & Security Hardening

- Stock PostgreSQL 15 suffices — no extensions are required. Apply the
  schema with `alembic upgrade head` (never assume auto-migration).
- Generate a production-grade Fernet key:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

- Set strict network policies isolating `brs_postgres` and `brs_redis` from
  external interfaces; only `brs_web` and `brs_worker` need to reach them.
- Rotate `API_AUTH_TOKEN` away from any value that ever appeared in a
  development environment.

---

## Part 3: Infrastructure & AI Agent Connection Questionnaire

Before connecting the BRS Engine to the main firm server or exposing its
endpoints to an automated AI agent, the system administrator / network
security team should answer:

### A. Network Posture & API Access

1. **Network boundary:** Will the BRS Engine run on an isolated internal
   VLAN, behind an API gateway (e.g., Kong, Apigee), or inside a VPN
   perimeter?
2. **Reverse proxy & domain:** What internal domain or IP is designated for
   the API (e.g., `https://brs-engine.internal.firm/`)?
3. **SSE proxying:** Does the enterprise load balancer / proxy allow
   unbuffered, long-lived streaming connections for `/brs/events/stream`?

### B. Authentication & Security Policy

1. **Token provisioning:** Static bearer header (`X-API-Token`), or
   integration with the firm's OAuth2 / OIDC / mTLS provider? (The engine
   also honours a trusted `X-authentik-email` forward-auth header.)
2. **Encryption key management:** Will `FERNET_KEY` be injected via
   environment variables or fetched from an enterprise secrets manager
   (HashiCorp Vault, AWS Secrets Manager, Azure Key Vault)?
3. **Data retention & reset restrictions:** Is `POST /brs/session/reset`
   permitted in production, or should it be disabled with
   `DISABLE_RESET_ENDPOINT=true` (returns HTTP 403; evaluated per request)
   to enforce regulatory audit compliance?

### C. Database & Persistence Strategy

1. **Database provisioning:** Managed external PostgreSQL (AWS RDS, Azure
   Database for PostgreSQL) or the containerized `brs_postgres` with
   host-mounted persistent volumes?
2. **Ledger data pipelines:** Batch CSV uploads via UI/API
   (`POST /brs/ledger/ingest`), or direct database sync from the firm's
   core ERP/accounting software?

### D. AI Agent Interoperability & Tool Calling

1. **OpenAPI schema exposition:** Should the AI agent consume the live
   specification at `http://<host>:8000/openapi.json`? Every operation
   carries a stable `operationId`, typed request/response models, and a
   uniform `ErrorResponse {detail, error_code, timestamp}` error envelope.
2. **Agent scoping:** Restrict the agent to read-only endpoints
   (`GET /brs/analysis`, `GET /brs/reports/reconciliation`), or grant
   execution permissions (`POST /api/v1/statements/ingest`,
   `POST /brs/session/reset`)?
3. **Stream handling:** Will the agent hold real-time SSE listeners
   (minting a fresh single-use token per connection — tokens are consumed
   atomically on first use and expire in 60 seconds), or poll job state via
   `GET /brs/analysis?job_id=...`?
