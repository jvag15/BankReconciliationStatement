"""FastAPI Gateway interface for secure, low-latency bank statement processing."""

from __future__ import annotations

import os
from pathlib import Path
import secrets
from typing import Any, Optional
from datetime import datetime, timezone as _timezone
import json

from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, UploadFile, File, HTTPException, status, Depends, Request  # noqa: E402
from fastapi.exceptions import RequestValidationError  # noqa: E402
from fastapi.responses import FileResponse, JSONResponse  # noqa: E402
from fastapi.staticfiles import StaticFiles  # noqa: E402
from fastapi.security import APIKeyHeader  # noqa: E402
from loguru import logger  # noqa: E402
from pydantic import BaseModel  # noqa: E402
from starlette.exceptions import HTTPException as StarletteHTTPException  # noqa: E402
from sqlalchemy import create_engine, text  # noqa: E402
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from limits.storage import RedisStorage
from prometheus_fastapi_instrumentator import Instrumentator

from app.celery_app import celery_app  # noqa: E402

_broker_url = os.environ.get("CELERY_BROKER_URL", os.environ.get("REDIS_BROKER_URL"))
_result_backend = os.environ.get("POSTGRES_RESULT_BACKEND_URL")
if _result_backend and _result_backend.startswith("postgresql://"):
    _result_backend = _result_backend.replace("postgresql://", "db+postgresql://", 1)
elif _result_backend and _result_backend.startswith("postgresql+psycopg2://"):
    _result_backend = _result_backend.replace("postgresql+psycopg2://", "db+postgresql://", 1)

celery_app.conf.update(
    broker_url=_broker_url,
    result_backend=_result_backend,
)

from app.task_ingestion import process_statement_chunk  # noqa: E402

_OPENAPI_TAGS = [
    {"name": "ingestion", "description": "Encrypted statement/ledger upload and asynchronous chunk processing."},
    {"name": "reconciliation", "description": "Triage, manual reconciliation and the ADR-014 matching surface."},
    {"name": "analytics", "description": "Pre-computed ML anomaly analysis payloads for the dashboard."},
    {"name": "ml", "description": "Direct model inference utilities."},
    {"name": "webhooks", "description": "Reconciliation callback subscriptions."},
    {"name": "admin", "description": "Human-in-the-loop review and override operations."},
    {"name": "reports", "description": "Downloadable reconciliation summaries."},
    {"name": "events", "description": "Server-sent processing telemetry."},
    {"name": "auth", "description": "Ephemeral stream-token minting."},
    {"name": "health", "description": "Unauthenticated liveness and subsystem diagnostics."},
]

app = FastAPI(
    title="Automated Financial Reconciliation Engine",
    description="High-speed ingestion and LightGBM machine learning ledger classification pipeline.",
    version="1.0.0",
    openapi_tags=_OPENAPI_TAGS,
)


# ─── Standardized error envelope + global handlers ────────────────────────
#
# Every non-2xx the API itself produces is shaped as ErrorResponse so
# clients parse one schema. Handlers only fire for errors raised before a
# response starts; an error mid-SSE stream simply closes the stream.

class ErrorResponse(BaseModel):
    """Uniform error payload returned by all API error paths."""
    detail: str
    error_code: str
    timestamp: str


_ERROR_CODES = {
    400: "BAD_REQUEST",
    401: "UNAUTHORIZED",
    403: "FORBIDDEN",
    404: "NOT_FOUND",
    405: "METHOD_NOT_ALLOWED",
    409: "CONFLICT",
    422: "VALIDATION_ERROR",
    429: "RATE_LIMITED",
    500: "INTERNAL_SERVER_ERROR",
    503: "SERVICE_UNAVAILABLE",
}


def _utc_now_iso() -> str:
    return (
        datetime.now(_timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _error_json(status_code: int, detail: str, headers: dict | None = None) -> JSONResponse:
    body = ErrorResponse(
        detail=detail,
        error_code=_ERROR_CODES.get(status_code, f"HTTP_{status_code}"),
        timestamp=_utc_now_iso(),
    )
    return JSONResponse(status_code=status_code, content=body.model_dump(), headers=headers)


@app.exception_handler(StarletteHTTPException)
async def _http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
    """Envelope 4xx/5xx raised as HTTPException (covers routing 404/405 too)."""
    detail = exc.detail if isinstance(exc.detail, str) else str(exc.detail)
    return _error_json(exc.status_code, detail, headers=getattr(exc, "headers", None))


@app.exception_handler(RequestValidationError)
async def _validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """Envelope 422s with a compact, field-scoped description."""
    problems = []
    for err in exc.errors()[:10]:
        loc = ".".join(str(part) for part in err.get("loc", ()))
        problems.append(f"{loc}: {err.get('msg', 'invalid')}")
    return _error_json(422, "Validation failed: " + "; ".join(problems))


@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Catch-all 500: full detail server-side, generic non-leaking client body."""
    logger.exception(
        "Unhandled exception on {} {}: {}", request.method, request.url.path, exc,
    )
    return _error_json(500, "Internal server error.")

limiter_storage = RedisStorage(_broker_url)
limiter = Limiter(key_func=get_remote_address, storage_uri=_broker_url)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

Instrumentator().instrument(app).expose(app)

from app.storage_paths import get_storage_dir  # noqa: E402

_UPLOAD_DIR = get_storage_dir()

from app.deps import api_key_auth, api_key_header

from app.routers.brs import router as audit_router
app.include_router(audit_router)

from app.routers.health import router as health_router  # noqa: E402
app.include_router(health_router)

from app.routers.reports import router as reports_router  # noqa: E402
app.include_router(reports_router)

from app.routers.events import router as events_router  # noqa: E402
app.include_router(events_router)

from app.routers.auth import router as auth_router  # noqa: E402
app.include_router(auth_router)


class TaskReceipt(BaseModel):
    """Pydantic structural definition for downstream polling validation."""
    task_id: str
    status: str
    filename_assigned: str


@app.post(
    "/api/v1/statements/ingest",
    response_model=TaskReceipt,
    status_code=status.HTTP_202_ACCEPTED,
    operation_id="ingest_statement_chunk",
    summary="Ingest an encrypted statement chunk into the processing pipeline.",
    tags=["ingestion"],
    dependencies=[Depends(api_key_auth)],
    responses={
        400: {"model": ErrorResponse, "description": "Malformed upload or missing required CSV columns."},
        401: {"model": ErrorResponse},
        429: {"description": "Rate limited (INGEST_RATE_LIMIT)."},
        503: {"model": ErrorResponse, "description": "Broker unreachable; chunk not queued."},
    },
)
@limiter.limit(os.environ.get("INGEST_RATE_LIMIT", "10/minute"))
async def ingest_statement_chunk(request: Request, file: UploadFile = File(...)) -> Any:
    """
    Accepts a financial statement payload, secures it, and hands it off to Celery.
    
    Bypasses block storage locking delays by offloading Polars extraction and
    LightGBM classification to the background asynchronous worker thread matrix.
    """
    if not file.filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Malformed file upload payload. File missing a valid name attribute."
        )

    # 1. Enforce rigorous file type validations
    allowed_extensions = {".csv", ".txt", ".enc"}
    extracted_filename = Path(file.filename).name
    file_extension = Path(extracted_filename).suffix.lower()

    if file_extension not in allowed_extensions:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported file format. System restricts processing to: {allowed_extensions}"
        )

    # 2. Prevent directory traversal attacks via strict tokenized file renaming
    secure_token = secrets.token_hex(8)
    filename_assigned = f"chunk_tx_{secure_token}{file_extension}"
    
    target_storage_path = (_UPLOAD_DIR / filename_assigned).resolve()
    if not str(target_storage_path).startswith(str(_UPLOAD_DIR.resolve())):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Security violation: Invalid target destination path breakout detected."
        )

    # 3. Synchronous edge-case validation + at-rest encryption normalisation.
    #    - A payload that decrypts with FERNET_KEY gets the fast-path header
    #      check (400 on missing required columns) and is stored as received.
    #    - A raw .csv/.txt payload is encrypted server-side here, so the
    #      worker's decrypt_file_to_memory succeeds instead of routing every
    #      plaintext upload to the DLQ with InvalidToken. Header validation
    #      for raw payloads stays worker-side (schema check), matching the
    #      long-standing API contract that plaintext uploads are accepted.
    #    - A .enc upload that does NOT decrypt is stored byte-for-byte as
    #      received: failing its decrypt in the worker and routing to the
    #      DLQ is the deliberate contract for corrupt/foreign ciphertext.
    from cryptography.fernet import Fernet

    file_bytes = await file.read()

    fernet_key = os.environ.get("FERNET_KEY", "").strip().encode("utf-8")
    is_valid_ciphertext = False
    if fernet_key:
        try:
            plaintext = Fernet(fernet_key).decrypt(file_bytes)
            is_valid_ciphertext = True
        except Exception:
            plaintext = b""
        if is_valid_ciphertext:
            header_line = plaintext.split(b"\n")[0].decode("utf-8", errors="replace").lower()
            if "amount" not in header_line or "description_raw" not in header_line:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Malformed payload: CSV missing required columns (amount, description_raw)."
                )

    if is_valid_ciphertext or file_extension == ".enc":
        payload_at_rest = file_bytes
    else:
        # Raw statement export: never let plaintext touch disk.
        if not fernet_key:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=(
                    "FERNET_KEY is not configured; refusing to persist statement "
                    "data unencrypted at rest."
                ),
            )
        from app.storage_encryption import encrypt_bytes

        payload_at_rest = encrypt_bytes(file_bytes)

    # 4. Asynchronous Handoff
    try:
        target_storage_path.write_bytes(payload_at_rest)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to securely commit statement chunk to block storage: {str(exc)}"
        )

    # 3. Offload the processing logic directly into the active Redis queue channel via explicit kwargs
    try:
        celery_task = process_statement_chunk.apply_async(
            kwargs={
                "chunk_id": filename_assigned,
                "file_origin": "api_gateway_uploader",
                "file_path_encrypted": str(target_storage_path)
            }
        )
    except Exception as exc:
        if target_storage_path.exists():
            target_storage_path.unlink()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Broker messaging subsystem unreachable. Failed to queue task: {str(exc)}"
        )

    return TaskReceipt(
        task_id=celery_task.id,
        status="PENDING_PROCESSING",
        filename_assigned=filename_assigned
    )


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    result: Optional[Any] = None
    date_done: Optional[datetime] = None
    traceback: Optional[str] = None


@app.get(
    "/api/v1/statements/tasks/{task_id}",
    response_model=TaskStatusResponse,
    operation_id="get_task_status",
    summary="Poll the status of an asynchronous processing task.",
    tags=["ingestion"],
)
def get_task_status(task_id: str) -> Any:
    """
    Check the result backend to determine task execution state.
    Bypasses Celery app abstraction for raw, high-speed lookup directly against the PostgreSQL meta table.
    """
    db_url = os.environ.get("POSTGRES_RESULT_BACKEND_URL", "")
    if not db_url:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database URL not configured in environment."
        )

    if db_url.startswith("db+postgresql://"):
        db_url = db_url.replace("db+postgresql://", "postgresql://", 1)

    engine = create_engine(db_url)
    with engine.connect() as conn:
        query = text(
            "SELECT status, result, date_done, traceback "
            "FROM celery_taskmeta WHERE task_id = :task_id"
        )
        row = conn.execute(query, {"task_id": task_id}).fetchone()

    if not row:
        return TaskStatusResponse(task_id=task_id, status="PENDING")

    status_val, result_val, date_done, tb_val = row

    # Safely unpack binary encoded data via structured JSON deserialization only.
    # pickle.loads is explicitly banned (CWE-502: Unsafe Deserialization).
    parsed_result = None
    if result_val is not None:
        if isinstance(result_val, (bytes, bytearray, memoryview)):
            raw_bytes = bytes(result_val)
            try:
                parsed_result = json.loads(raw_bytes)
            except (json.JSONDecodeError, UnicodeDecodeError):
                try:
                    parsed_result = raw_bytes.decode("utf-8", errors="replace")
                except Exception:
                    parsed_result = str(result_val)
        else:
            parsed_result = result_val

    parsed_tb = None
    if tb_val is not None:
        if isinstance(tb_val, (bytes, bytearray, memoryview)):
            parsed_tb = bytes(tb_val).decode("utf-8", errors="replace")
        else:
            parsed_tb = str(tb_val)

    return TaskStatusResponse(
        task_id=task_id,
        status=status_val,
        result=parsed_result,
        date_done=date_done,
        traceback=parsed_tb
    )

class FlaggedTransaction(BaseModel):
    id: int
    description_raw: str
    amount: float
    confidence_score: Optional[float]
    category: str

@app.get(
    "/api/v1/admin/flagged_transactions",
    response_model=list[FlaggedTransaction],
    operation_id="list_flagged_transactions",
    summary="Fetch all transactions flagged for administrative review.",
    tags=["admin"],
    dependencies=[Depends(api_key_auth)],
    responses={401: {"model": ErrorResponse}},
)
def get_flagged_transactions() -> Any:
    """Retrieve rows explicitly flagged by the LightGBM confidence threshold."""
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database URL not configured in environment."
        )

    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "db+postgresql://", 1)
    elif db_url.startswith("postgresql+psycopg2://"):
        db_url = db_url.replace("postgresql+psycopg2://", "db+postgresql://", 1)
    elif db_url.startswith("db+postgresql://"):
        db_url = db_url.replace("db+postgresql://", "postgresql://", 1)

    engine = create_engine(db_url)
    with engine.connect() as conn:
        query = text(
            "SELECT id, description_raw, amount, confidence_score, category "
            "FROM bank_transactions "
            "WHERE category LIKE 'FLAGGED_FOR_REVIEW:%'"
        )
        rows = conn.execute(query).fetchall()

    return [
        FlaggedTransaction(
            id=row[0],
            description_raw=row[1],
            amount=float(row[2]) / 100 if row[2] is not None else 0.0,
            confidence_score=float(row[3]) if row[3] is not None else None,
            category=row[4]
        )
        for row in rows
    ]

class CategoryOverrideRequest(BaseModel):
    category: str

class CategoryOverrideResponse(BaseModel):
    status: str
    message: str

@app.put(
    "/api/v1/admin/transactions/{transaction_id}/category",
    response_model=CategoryOverrideResponse,
    operation_id="override_transaction_category",
    summary="Manually override the category of a transaction.",
    tags=["admin"],
    dependencies=[Depends(api_key_auth)],
    responses={401: {"model": ErrorResponse}, 404: {"model": ErrorResponse}},
)
def override_transaction_category(transaction_id: int, payload: CategoryOverrideRequest) -> Any:
    """Manually correct the category for a flagged transaction."""
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database URL not configured."
        )

    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "db+postgresql://", 1)
    elif db_url.startswith("postgresql+psycopg2://"):
        db_url = db_url.replace("postgresql+psycopg2://", "db+postgresql://", 1)
    elif db_url.startswith("db+postgresql://"):
        db_url = db_url.replace("db+postgresql://", "postgresql://", 1)

    engine = create_engine(db_url)
    with engine.begin() as conn:
        check_query = text("SELECT id FROM bank_transactions WHERE id = :id FOR UPDATE")
        row = conn.execute(check_query, {"id": transaction_id}).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Transaction not found.")
            
        update_query = text(
            "UPDATE bank_transactions "
            "SET category = :cat, match_rule_id = 'MANUAL_HITL' "
            "WHERE id = :id"
        )
        conn.execute(update_query, {"cat": payload.category, "id": transaction_id})

    return {"status": "success", "message": f"Transaction {transaction_id} assigned to {payload.category}"}


# ─── SPA serve (must be last so explicit API routes win) ───────────

_STATIC_DIR = os.path.join(os.path.dirname(__file__), "..", "static")

if os.path.isdir(_STATIC_DIR):
    app.mount(
        "/assets",
        StaticFiles(directory=os.path.join(_STATIC_DIR, "assets")),
        name="assets",
    )

    # Unknown paths under the API surface must 404 with the uniform error
    # envelope instead of silently serving the SPA shell (which would turn
    # every API typo into a 200 text/html response).
    _API_PREFIXES = ("api/", "brs/", "health", "metrics", "openapi.json", "docs", "redoc")

    @app.get("/{full_path:path}", include_in_schema=False)
    async def serve_spa(full_path: str) -> Any:
        if full_path.startswith(_API_PREFIXES):
            return _error_json(404, "Not found.")
        candidate = os.path.join(_STATIC_DIR, full_path)
        if os.path.isfile(candidate):
            return FileResponse(candidate)
        # no-cache: browser must revalidate index.html each load so it picks up the
        # latest hashed asset bundle after a deploy (hashed /assets stay cacheable).
        return FileResponse(
            os.path.join(_STATIC_DIR, "index.html"),
            headers={"Cache-Control": "no-cache"},
        )
