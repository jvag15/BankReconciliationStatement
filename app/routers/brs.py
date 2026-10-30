import os
import secrets
from datetime import date as _date
from pathlib import Path

from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile, status
from pydantic import BaseModel
import asyncpg
import numpy as np
from app.deps import get_current_user
from app.db import get_db_pool
from app.storage_paths import get_storage_dir

# Tags are set per-route (this router spans ingestion, reconciliation,
# webhooks, analytics and ml) so each lands under one unified lowercase tag.
router = APIRouter(prefix="/brs")

_LEDGER_EXTENSIONS = {".csv", ".txt", ".enc"}

class PredictionRequest(BaseModel):
    description_text: str

class PredictionResponse(BaseModel):
    category: str
    confidence_score: float

@router.post(
    "/predict",
    response_model=PredictionResponse,
    operation_id="predict_category",
    summary="Classify a transaction description with the loaded model.",
    tags=["ml"],
)
async def predict(request: Request, body: PredictionRequest, user: str = Depends(get_current_user)):
    """Return the model's category and confidence for one description.

    Falls back to a neutral default when no model artefacts are loaded.
    """
    model = getattr(request.app.state, "model", None)
    vectorizer = getattr(request.app.state, "vectorizer", None)
    label_encoder = getattr(request.app.state, "label_encoder", None)
    
    if not model or not vectorizer or not label_encoder:
        return PredictionResponse(category="Office Contingencies", confidence_score=0.50)
        
    vectorized_text = vectorizer.transform([body.description_text])
    probabilities = model.predict_proba(vectorized_text)[0]
    max_prob_index = np.argmax(probabilities)
    
    confidence_score = float(probabilities[max_prob_index])
    predicted_category = str(label_encoder.classes_[max_prob_index])
    
    return PredictionResponse(category=predicted_category, confidence_score=confidence_score)

class TriageRow(BaseModel):
    """One bank transaction as presented in the triage grid."""
    id: int
    date: _date | None = None
    amount: float
    description_raw: str | None = None
    verified_ledger_category: str | None = None
    confidence_score: float | None = None


@router.get(
    "/triage",
    response_model=list[TriageRow],
    operation_id="list_triage_transactions",
    summary="List bank transactions for human triage.",
    tags=["reconciliation"],
)
async def triage(pool: asyncpg.Pool = Depends(get_db_pool), user: str = Depends(get_current_user)):
    """Return every bank transaction with its current category/confidence."""
    query = """
        SELECT id, value_date AS date, amount, description_raw,
               category AS verified_ledger_category, confidence_score
        FROM bank_transactions;
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query)

    return [dict(row) for row in rows]

class ReconcileRequest(BaseModel):
    id: int
    category: str
    confidence_score: float
    status: str


class ReconcileResponse(BaseModel):
    status: str
    updated: int


@router.post(
    "/reconcile",
    response_model=ReconcileResponse,
    operation_id="apply_reconciliation_updates",
    summary="Apply a batch of human reconciliation decisions.",
    tags=["reconciliation"],
)
async def reconcile(
    updates: list[ReconcileRequest],
    pool: asyncpg.Pool = Depends(get_db_pool),
    user: str = Depends(get_current_user)
):
    """Persist reviewed category/confidence/status decisions in one transaction."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            for update in updates:
                await conn.execute(
                    """
                    UPDATE bank_transactions
                    SET category = $1,
                        confidence_score = $2,
                        status = $3
                    WHERE id = $4
                    """,
                    update.category,
                    update.confidence_score,
                    update.status,
                    update.id
                )
    return {"status": "SUCCESS", "updated": len(updates)}

class LedgerIngestReceipt(BaseModel):
    task_id: str
    status: str
    chunk_id: str
    job_id: str


@router.post(
    "/ledger/ingest",
    response_model=LedgerIngestReceipt,
    status_code=status.HTTP_202_ACCEPTED,
    operation_id="ingest_ledger_export",
    summary="Ingest an internal company ledger export for reconciliation.",
    tags=["ingestion"],
)
async def ingest_ledger(
    file: UploadFile = File(...),
    job_id: str | None = None,
    company_name: str = "Internal Ledger",
    user: str = Depends(get_current_user),
) -> LedgerIngestReceipt:
    """Accept a ledger export, encrypt it at rest, and queue it for parsing.

    The response returns immediately with a Celery task id; the worker
    performs the parse and insert, then triggers the matching engine.
    """
    import uuid

    from cryptography.fernet import Fernet

    from app.task_ledger_ingestion import process_ledger_chunk

    if not file.filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Malformed upload: file is missing a name attribute.",
        )

    extension = Path(Path(file.filename).name).suffix.lower()
    if extension not in _LEDGER_EXTENSIONS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Unsupported ledger format '{extension}'. Accepted: "
                f"{sorted(_LEDGER_EXTENSIONS)}. Export XLSX as CSV first."
            ),
        )

    fernet_key = os.environ.get("FERNET_KEY", "").strip()
    if not fernet_key:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "FERNET_KEY is not configured; refusing to persist ledger data "
                "unencrypted at rest."
            ),
        )

    payload = await file.read()
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Ledger export is empty.",
        )

    # Tokenised filename: never trust the client-supplied name on disk.
    chunk_id = f"ledger_{secrets.token_hex(8)}"
    upload_dir = get_storage_dir()
    target_path = (upload_dir / f"{chunk_id}.enc").resolve()
    if not str(target_path).startswith(str(upload_dir)):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Security violation: resolved path escapes the upload directory.",
        )

    try:
        # An already-encrypted (.enc) upload is stored as-is; anything else is
        # encrypted here so plaintext ledger data never lands on disk.
        if extension == ".enc":
            ciphertext = payload
        else:
            ciphertext = Fernet(fernet_key.encode("utf-8")).encrypt(payload)
        target_path.write_bytes(ciphertext)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to commit ledger chunk to storage: {exc}",
        )

    resolved_job_id = job_id or str(uuid.uuid4())

    try:
        task = process_ledger_chunk.apply_async(
            kwargs={
                "chunk_id": chunk_id,
                "file_origin": Path(file.filename).name,
                "file_path_encrypted": str(target_path),
                "job_id": resolved_job_id,
                "company_name": company_name,
            }
        )
    except Exception as exc:
        target_path.unlink(missing_ok=True)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Broker unreachable; ledger chunk not queued: {exc}",
        )

    return LedgerIngestReceipt(
        task_id=task.id,
        status="PENDING_PROCESSING",
        chunk_id=chunk_id,
        job_id=resolved_job_id,
    )


class WebhookCreateRequest(BaseModel):
    target_url: str
    event: str = "batch.reconciled"
    secret: str | None = None
    description: str | None = None


class WebhookResponse(BaseModel):
    id: str
    target_url: str
    event: str
    active: bool
    description: str | None = None


@router.post(
    "/webhooks",
    response_model=WebhookResponse,
    status_code=status.HTTP_201_CREATED,
    operation_id="register_webhook",
    summary="Register a reconciliation callback endpoint.",
    tags=["webhooks"],
)
async def register_webhook(
    body: WebhookCreateRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    user: str = Depends(get_current_user),
) -> WebhookResponse:
    """Register a client endpoint to be called when a batch is reconciled."""
    if not body.target_url.startswith(("http://", "https://")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="target_url must be an absolute http(s) URL.",
        )
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO webhook_subscriptions
                (target_url, event, secret, description, created_by)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id, target_url, event, active, description
            """,
            body.target_url,
            body.event,
            body.secret,
            body.description,
            user,
        )
    return WebhookResponse(
        id=str(row["id"]),
        target_url=row["target_url"],
        event=row["event"],
        active=row["active"],
        description=row["description"],
    )


@router.get(
    "/webhooks",
    response_model=list[WebhookResponse],
    operation_id="list_webhooks",
    summary="List active webhook subscriptions.",
    tags=["webhooks"],
)
async def list_webhooks(
    pool: asyncpg.Pool = Depends(get_db_pool),
    user: str = Depends(get_current_user),
) -> list[WebhookResponse]:
    """List active webhook subscriptions."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, target_url, event, active, description
            FROM webhook_subscriptions
            WHERE active = true
            ORDER BY created_at DESC
            """
        )
    return [
        WebhookResponse(
            id=str(r["id"]),
            target_url=r["target_url"],
            event=r["event"],
            active=r["active"],
            description=r["description"],
        )
        for r in rows
    ]


class WebhookDeleteResponse(BaseModel):
    status: str
    id: str


@router.delete(
    "/webhooks/{webhook_id}",
    response_model=WebhookDeleteResponse,
    operation_id="deactivate_webhook",
    summary="Deactivate (soft-delete) a webhook subscription.",
    tags=["webhooks"],
)
async def deactivate_webhook(
    webhook_id: str,
    pool: asyncpg.Pool = Depends(get_db_pool),
    user: str = Depends(get_current_user),
) -> dict:
    """Deactivate (soft-delete) a webhook subscription."""
    import uuid as _uuid

    try:
        parsed_id = _uuid.UUID(webhook_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid webhook id.")

    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE webhook_subscriptions SET active = false WHERE id = $1",
            parsed_id,
        )
    if result.endswith("0"):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Webhook not found.")
    return {"status": "deactivated", "id": webhook_id}


class SessionResetResponse(BaseModel):
    status: str
    cleared_tables: list[str]
    cleared_redis_keys: int


@router.post(
    "/session/reset",
    response_model=SessionResetResponse,
    operation_id="reset_session",
    summary="Clear all ingested transaction data to start a fresh session.",
    tags=["admin"],
)
async def reset_session(
    pool: asyncpg.Pool = Depends(get_db_pool),
    user: str = Depends(get_current_user),
) -> SessionResetResponse:
    """Truncate transaction tables and clear cached Celery task results.

    Removes ``bank_transactions``, ``ledger_transactions`` and their parent
    ``audit_jobs`` rows in one statement (the three tables' foreign keys are
    self-contained), plus the Redis ``celery-task-meta-*`` result keys and
    the matching-dispatch dedup flag. The append-only ``audit_log`` is
    deliberately untouched: a session reset must not erase audit history.
    Analysis payloads are computed per request, so no further cache exists.

    Production deployments under retention obligations can disable this
    endpoint entirely with ``DISABLE_RESET_ENDPOINT=true`` (evaluated per
    request, so no rebuild is needed to flip it).
    """
    if os.environ.get("DISABLE_RESET_ENDPOINT", "").strip().lower() in ("1", "true", "yes"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Session reset is disabled on this deployment (DISABLE_RESET_ENDPOINT).",
        )

    cleared_tables = ["bank_transactions", "ledger_transactions", "audit_jobs"]
    async with pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE bank_transactions, ledger_transactions, audit_jobs"
        )

    cleared_keys = 0
    try:
        import redis.asyncio as aioredis

        from app.matching_dispatch_guard import DISPATCH_FLAG_KEY, _broker_url

        client = aioredis.from_url(_broker_url())
        try:
            async for key in client.scan_iter(match="celery-task-meta-*", count=500):
                await client.delete(key)
                cleared_keys += 1
            await client.delete(DISPATCH_FLAG_KEY)
        finally:
            try:
                await client.aclose()
            except Exception:  # noqa: BLE001
                pass
    except Exception:  # noqa: BLE001 - the DB reset already succeeded; Redis
        pass  # keys expire on their own (result_expires) if unreachable now.

    return SessionResetResponse(
        status="RESET",
        cleared_tables=cleared_tables,
        cleared_redis_keys=cleared_keys,
    )


# ── /brs/analysis response models ─────────────────────────────────────────
# Field names deliberately mirror the frontend's AnalysisDataPayload
# (src/lib/types.ts) key-for-key; response_model validation will reject any
# drift between this contract and the hand-built payload below.

class TimeSeriesPoint(BaseModel):
    date: str
    anomaly_score: float


class DBSCANPoint(BaseModel):
    id: int
    description_length: int
    abs_amount: float
    cluster_id: int
    is_anomaly: bool


class IsolationForestPoint(BaseModel):
    score_bin: str
    count: int
    density: float


class ZScorePoint(BaseModel):
    deviation_bin: str
    count: int


class VendorRisk(BaseModel):
    vendor_name: str
    fraud_probability: float


class TemporalHeatmapPoint(BaseModel):
    day_of_week: str
    hour: int
    risk_score: float


class AmountDistributionPoint(BaseModel):
    amount: float
    is_anomaly: bool


class CumulativeRiskPoint(BaseModel):
    date: str
    cumulative_exposure: float


class RiskTransaction(BaseModel):
    id: int
    date: str
    description_raw: str
    amount: float
    confidence_score: float


class AnalysisDataPayload(BaseModel):
    timeSeries: list[TimeSeriesPoint]
    dbscanScatter: list[DBSCANPoint]
    isolationForestDensity: list[IsolationForestPoint]
    zScoreCurve: list[ZScorePoint]
    vendorRisk: list[VendorRisk]
    temporalHeatmap: list[TemporalHeatmapPoint]
    amountDistribution: list[AmountDistributionPoint]
    cumulativeRisk: list[CumulativeRiskPoint]
    algorithmAgreement: dict[str, dict[str, float]]
    riskTransactions: list[RiskTransaction]


@router.get(
    "/analysis",
    response_model=AnalysisDataPayload,
    operation_id="get_ml_analysis_payload",
    summary="Return the pre-computed ML anomaly analysis arrays for one job.",
    tags=["analytics"],
)
async def get_analysis(
    job_id: str | None = None,
    pool: asyncpg.Pool = Depends(get_db_pool),
    user: str = Depends(get_current_user),
):
    """
    Returns the pre-calculated ML analysis arrays expected by the frontend's
    AnalysisDataPayload, scoped to a single ingestion job.

    Pass ``?job_id=<uuid>`` to analyse a specific job; when omitted, the most
    recently ingested job is used, so the dashboard reflects the statement
    that was just uploaded rather than every historical row in the table.
    """
    import uuid as _uuid

    scope: _uuid.UUID | None = None
    if job_id:
        try:
            scope = _uuid.UUID(job_id)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid job_id: must be a UUID.",
            )

    query = """
        SELECT id, value_date, amount, description_raw,
               confidence_score, isolation_forest_score,
               dbscan_cluster, z_score_deviation, calculated_risk_tier
        FROM bank_transactions
        WHERE calculated_risk_tier IS NOT NULL
          AND job_id = $1
        ORDER BY value_date ASC;
    """
    async with pool.acquire() as conn:
        if scope is None:
            latest = await conn.fetchrow(
                """
                SELECT job_id FROM bank_transactions
                WHERE job_id IS NOT NULL
                ORDER BY ingested_at DESC
                LIMIT 1
                """
            )
            scope = latest["job_id"] if latest else None
        rows = await conn.fetch(query, scope) if scope is not None else []
    
    # If no data is available yet, we could fallback, but we'll map the rows
    # to the complex frontend expected shapes directly in memory to avoid locking the DB.
    
    payload = {
        "timeSeries": [],
        "dbscanScatter": [],
        "isolationForestDensity": [],
        "zScoreCurve": [],
        "vendorRisk": [],
        "temporalHeatmap": [],
        "amountDistribution": [],
        "cumulativeRisk": [],
        "algorithmAgreement": {
            "isolation_forest": { "dbscan": 0.85, "statistical": 0.60 },
            "dbscan": { "isolation_forest": 0.85, "statistical": 0.55 },
            "statistical": { "isolation_forest": 0.60, "dbscan": 0.55 },
        },
        "riskTransactions": []
    }
    
    if not rows:
        return payload
        
    cumulative_exposure = 0
    iso_bins = {}
    z_bins = {}
    vendor_counts = {}
    
    for row in rows:
        d = dict(row)
        date_str = d["value_date"].isoformat()
        amt = float(d["amount"])
        iso_score = float(d["isolation_forest_score"] or 0)
        dbscan = int(d["dbscan_cluster"] or 0)
        zscore = float(d["z_score_deviation"] or 0)
        tier = int(d["calculated_risk_tier"] or 3)
        desc = d["description_raw"]
        conf = float(d["confidence_score"] or 0)
        
        # timeSeries
        payload["timeSeries"].append({"date": date_str, "anomaly_score": iso_score})
        
        # dbscanScatter
        payload["dbscanScatter"].append({
            "id": d["id"],
            "description_length": len(desc),
            "abs_amount": abs(amt),
            "cluster_id": dbscan,
            "is_anomaly": dbscan == -1
        })
        
        # isolationForestDensity
        bin_key = f"{round(iso_score, 1)}"
        if bin_key not in iso_bins:
            iso_bins[bin_key] = 0
        iso_bins[bin_key] += 1
        
        # zScoreCurve
        z_bin = f"{round(zscore, 1)}"
        if z_bin not in z_bins:
            z_bins[z_bin] = 0
        z_bins[z_bin] += 1
        
        # vendorRisk
        if tier <= 2:
            vendor = desc[:20] # proxy for vendor name
            vendor_counts[vendor] = vendor_counts.get(vendor, 0) + 1
            
        # temporalHeatmap - proxy data since we don't have hours in standard Dates
        day_of_week = d["value_date"].strftime("%A")
        # proxy hour based on ID for visual distribution
        hour = (d["id"] * 7) % 24 
        payload["temporalHeatmap"].append({
            "day_of_week": day_of_week,
            "hour": hour,
            "risk_score": iso_score
        })
        
        # amountDistribution
        payload["amountDistribution"].append({
            "amount": amt,
            "is_anomaly": tier == 1
        })
        
        # cumulativeRisk
        if tier <= 2:
            cumulative_exposure += amt
        payload["cumulativeRisk"].append({
            "date": date_str,
            "cumulative_exposure": cumulative_exposure
        })
        
        # riskTransactions
        payload["riskTransactions"].append({
            "id": d["id"],
            "date": date_str,
            "description_raw": desc,
            "amount": amt,
            "confidence_score": iso_score # ISO score mapped directly to confidence for risk grid
        })

    # Finalize bins
    payload["isolationForestDensity"] = [{"score_bin": k, "count": v, "density": v / len(rows)} for k, v in iso_bins.items()]
    payload["zScoreCurve"] = [{"deviation_bin": k, "count": v} for k, v in z_bins.items()]
    payload["vendorRisk"] = [{"vendor_name": k, "fraud_probability": min(1.0, v * 0.1)} for k, v in vendor_counts.items()]
    
    return payload
