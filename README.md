# Automated Bank Reconciliation Statement (BRS) Engine

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/Python-3.11+-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111+-009688.svg?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![React 18](https://img.shields.io/badge/React-18-61DAFB.svg?logo=react&logoColor=black)](https://react.dev/)
[![Docker Compose](https://img.shields.io/badge/Docker-Compose-2496ED.svg?logo=docker&logoColor=white)](https://www.docker.com/)

> **Architected & Developed by Joel Anthony ([@jvag15](https://github.com/jvag15))**  
> **Contact:** `mail@uncoalesced.com`

---

> [!IMPORTANT]
> ### Project Status: Closed & Unmaintained
> This repository is a personal portfolio showcase of an automated, enterprise-scale bank reconciliation engine developed by Joel Anthony. **This repository is closed and will NOT be maintained further.**
> 
> You are fully welcome and encouraged to fork, adapt, and build upon this codebase under the terms of the [MIT License](LICENSE).

---

## Overview

The **BRS Engine** is a high-throughput, enterprise-grade financial data processing and reconciliation platform. Built to eliminate the friction, manual error, and operational latency of matching hundreds of thousands of institutional banking entries against ERP general ledgers, it combines streaming polars ingestion, Fernet at-rest cryptography, distributed Celery task orchestration, and hybrid deterministic/probabilistic transaction matching.

```
                      +-----------------------------+
                      |   React 18 / Vite Client    |
                      |   (Modern Web Dashboard)    |
                      +--------------+--------------+
                                     |
                                REST API
                                     v
                      +-----------------------------+
                      |       FastAPI Gateway       |
                      |   (Bearer Auth & Routing)   |
                      +--------------+--------------+
                                     |
                      +--------------+--------------+
                      |                             |
                      v                             v
           +--------------------+         +--------------------+
           |  Streaming Ingest  |         |   Celery Workers   |
           |   (Polars Memory)  |         | (Parallel Matching)|
           +---------+----------+         +---------+----------+
                     |                              |
                     +--------------+---------------+
                                    |
                                    v
                     +------------------------------+
                     | PostgreSQL (Relational Core) |
                     | Redis (Task & Lock Broker)   |
                     +------------------------------+
```

---

## Key Features

1. **Streaming Polars Ingestion Pipeline**
   - High-performance, zero-copy ingestion of large multi-month bank statements (e.g. 50,000+ rows) without memory pressure spikes.
   - Dynamic memory-pressure evaluation adjusting insert chunk sizes in real time.
2. **At-Rest Fernet Cryptography**
   - Upload spool encryption using AES-128-CBC + HMAC-SHA256 authenticated tokens before raw transaction disk staging.
3. **Hybrid Multi-Stage Reconciliation Engine**
   - **Deterministic Tier**: Exact 1:1 matching across normalized transaction values, reference codes, and timestamps.
   - **Fuzzy / Heuristic Tier**: Tokenized Levenshtein distance, narrative similarity, and temporal tolerance windows.
   - **Machine Learning Classifiers**: LightGBM anomaly detection and automated vendor categorical encoding.
4. **Resilient Distributed Architecture**
   - Decoupled FastAPI web layer, asynchronous Celery workers, Redis queue with Dead Letter Queue (DLQ) isolation, and PostgreSQL persistent store.
5. **Modern Reactive Web Interface**
   - Built with React 18, Vite, TypeScript, Lucide Icons, and Tailwind CSS.
   - Interactive breakdown charts, manual reconciliation workbench, and audit log inspection.

---

## Repository Structure

```
bank-statement/
├── alembic/              # Database schema migrations
├── app/                  # FastAPI backend and Celery worker tasks
│   ├── main.py           # Application entrypoint and routers
│   ├── config.py         # App configuration & environment loader
│   ├── db.py             # SQLAlchemy session and engine management
│   ├── polars_memory.py  # Adaptive chunking and Polars ingestion
│   ├── task_ingestion.py # Distributed ingestion background tasks
│   └── task_inference.py # Matching and machine learning tasks
├── docs/                 # System architecture, deployment, and media
├── scripts/              # Validation, audit generation, and stress tests
├── src/                  # React 18 + Vite frontend source
│   ├── components/       # UI screens, dashboards, and navigation
│   └── App.tsx           # React root component
├── tests/                # Comprehensive unit, QA vector, and stress tests
├── docker-compose.yml    # Development stack orchestration
└── sample_sbi_bank_statement.csv # Scrubbed, jumbled benchmark dataset
```

---

## Quickstart

### 1. Prerequisites
- Docker Engine 24+ and Docker Compose v2
- Python 3.11+ (for local scripts)
- Node.js 18+ (for frontend development)

### 2. Environment Configuration
Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```
Generate an encryption key:
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 3. Launch Services with Docker Compose
```bash
docker compose up -d --build
```
The services will be available at:
- **Web Dashboard:** `http://localhost:5173` (or port 8000 in production build)
- **FastAPI Documentation:** `http://localhost:8000/docs`
- **PostgreSQL:** `localhost:5432`
- **Redis Broker:** `localhost:6379`

### 4. Run Automated Test Suite
```bash
pytest tests/
```

---

## Benchmark Sample Dataset

This repository includes `sample_sbi_bank_statement.csv`, a 25,250-row realistic State Bank of India format statement. All corporate identities, account numbers, and transaction IDs have been anonymized, and rows have been randomly jumbled to guarantee confidentiality while preserving real-world transactional patterns.

---

## Author & Attribution

- **Creator:** Joel Anthony ([@jvag15](https://github.com/jvag15))
- **Email:** `mail@uncoalesced.com`
- **License:** [MIT License](LICENSE)
