# Bank Reconciliation Statement (BRS) Engine — Technical Documentation

> **Architected & Developed by Joel Anthony ([@jvag15](https://github.com/jvag15))**  
> **Contact:** `mail@uncoalesced.com`  
> **Status:** Closed Portfolio Prototype (Unmaintained — Open for Forking)

---

## 1. Architectural Philosophy

Corporate bank reconciliation at scale suffers from three primary bottlenecks:
1. **Memory Exhaustion on Ingestion**: Naive dataframe loading of multi-hundred-megabyte bank statement exports causes Python out-of-memory (OOM) crashes in constrained container environments.
2. **Data Security Vulnerabilities**: Staging unencrypted financial transactions on local worker disks violates data governance standards.
3. **Fragile Deterministic Matching**: Financial narratives vary widely across banking networks (RTGS vs NEFT vs IMPS vs internal transfer tokens), causing naive string matching to fail on over 40% of real transactions.

The BRS Engine was engineered specifically to solve these challenges through a three-layer pipeline:

```
+--------------------------------------------------------------------------+
| Layer 1: Ingestion & Cryptography                                        |
| - Polars LazyFrame streaming                                             |
| - Adaptive parameter-buffer sizing based on Linux memory pressure        |
| - Symmetric AES-128-CBC + HMAC-SHA256 Fernet encrypted spooling           |
+--------------------------------------------------------------------------+
                                     |
                                     v
+--------------------------------------------------------------------------+
| Layer 2: Hybrid Multi-Tier Reconciliation Matching                        |
| - Tier 1: Deterministic 1:1 value + reference matching (100% confidence)  |
| - Tier 2: Levenshtein & narrative token cosine similarity (80-99% conf)  |
| - Tier 3: Temporal date windowing & partial sum subset matching          |
| - Tier 4: LightGBM classification for unclassified anomalous rows        |
+--------------------------------------------------------------------------+
                                     |
                                     v
+--------------------------------------------------------------------------+
| Layer 3: Presentation & Audit Governance                                 |
| - FastAPI REST endpoints with token authentication                       |
| - Audit job lifecycle state tracking (INGESTING -> MATCHING -> COMPLETE) |
| - React 18 reactive UI with transaction inspection workbench             |
+--------------------------------------------------------------------------+
```

---

## 2. Ingestion & Polars Memory Management

### Dynamic Adaptive Chunking (`app/polars_memory.py`)
Rather than loading an entire dataset into an unbounded list of dictionaries, the ingestion worker uses `polars.scan_csv()` combined with an adaptive chunk size evaluator.

- **Under Low Memory Pressure (< 65% memory usage):** Ingestion batches use up to 10,000 rows per database execute batch to maximize throughput.
- **Under High Memory Pressure (> 80% memory usage):** The chunk evaluator dynamically contracts the buffer to 1,000 rows, yielding memory to worker tasks and preventing container termination.

### Data at Rest Encryption
Uploaded chunks are staged in `var/uploads/` using Fernet symmetric encryption:
$$\text{Ciphertext} = \text{Fernet}_{\text{Key}}(\text{Raw CSV Chunk})$$
Only the active background Celery worker holds the key in memory during ingestion, deleting the decrypted bytes immediately after database flush.

---

## 3. Reconciliation Algorithm Specifications

Reconciliation proceeds through a prioritized waterfall:

| Waterfall Step | Matching Strategy | Match Criteria | Confidence |
|---|---|---|---|
| **Step 1** | Exact Reference | `bank.ref_no == ledger.ref_no AND bank.amount == ledger.amount` | 1.00 (100%) |
| **Step 2** | Normalized UTR / Cheque | Sanitized alphanumeric tokens match with identical date $\pm 1$ day | 0.98 (98%) |
| **Step 3** | Fuzzy Narrative Match | Tokenized cosine similarity $\ge 0.85$ and amount difference $= 0.00$ within $\pm 3$ days | 0.85 - 0.95 |
| **Step 4** | Anomaly & Review Flag | Low narrative similarity or mismatched amount tagged for human audit review | Flagged |

---

## 4. API Endpoints Contract

| Method | Endpoint | Description | Auth Required |
|---|---|---|---|
| `GET` | `/health` | Service health status and database connectivity | No |
| `POST` | `/api/v1/jobs/upload` | Upload bank statement / ledger file | Bearer Token |
| `GET` | `/api/v1/jobs/{job_id}/status` | Check job processing status and statistics | Bearer Token |
| `GET` | `/api/v1/jobs/{job_id}/results` | Fetch matched and unmatched transaction records | Bearer Token |
| `POST` | `/api/v1/jobs/{job_id}/export` | Export CSV summary report | Bearer Token |

---

## 5. Security & Deployment

- **Database:** PostgreSQL 16 with indexed relational constraints on `job_id`, `amount`, and `value_date`.
- **Worker Concurrency:** Celery pre-fork concurrency bounded to CPU cores with `-O fair` task scheduling.
- **Environment Isolation:** Zero credentials baked into Docker images; all configurations driven via environment variables.

---

*Author: Joel Anthony ([@jvag15](https://github.com/jvag15)) — Portfolio Release*
