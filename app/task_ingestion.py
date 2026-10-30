import os
import uuid
import polars as pl
from decimal import Decimal
from datetime import datetime
from sqlalchemy import text
from celery import shared_task
from app.session import get_db_connection
from app.celery_app import celery_app as app
from app.task_base import DLQBackedTask
from app.polars_ingestion import ingest_bank_statement

def parse_date_safely(date_str):
    if not date_str:
        return datetime.utcnow().date()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(str(date_str).strip(), fmt).date()
        except ValueError:
            continue
    return datetime.utcnow().date()

def spool_audit_job(job_id: str, file_path: str) -> dict:
    """Entry point for local CLI processing pipelines."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Statement file target missing at: {file_path}")
        
    # Queue processing asynchronously via worker matrix
    task = process_statement_chunk.delay(
        chunk_id=f"cli_chunk_{uuid.uuid4().hex[:8]}",
        file_origin="local_cli_pipeline",
        file_path_encrypted=file_path,
        job_id=job_id
    )
    return {"status": "SPOOLED", "job_id": job_id, "task_id": task.id}

@app.task(base=DLQBackedTask, bind=True, name="brs_engine.tasks.process_statement_chunk")
def process_statement_chunk(self, chunk_id: str, file_origin: str, file_path_encrypted: str, job_id: str | None = None, **kwargs):
    if not job_id:
        job_id = str(uuid.uuid4())
        
    from app.storage_encryption import decrypt_file_to_memory
    from cryptography.fernet import InvalidToken
    from app.db_dlq import _write_to_dlq
    from app.events_publisher import publish_event
    from pathlib import Path

    publish_event(
        "BATCH_STARTED",
        {"chunk_id": chunk_id, "job_id": job_id, "file_origin": file_origin},
    )

    file_path_obj = Path(file_path_encrypted)
    try:
        decrypted_bytes = decrypt_file_to_memory(file_path_encrypted)
    except InvalidToken as exc:
        import traceback
        _write_to_dlq(
            raw_payload=file_path_encrypted,
            file_origin=file_origin,
            worker_id=self.request.id or "unknown",
            exception_class=type(exc).__name__,
            stack_trace=traceback.format_exc(),
        )
        if file_path_obj.exists():
            file_path_obj.unlink()
        publish_event(
            "BATCH_FAILED",
            {"chunk_id": chunk_id, "job_id": job_id, "reason": type(exc).__name__},
        )
        raise
        
    required_cols = ["date", "value_date", "description_raw", "amount"]
    lf = ingest_bank_statement(decrypted_bytes, required_cols)
    df = lf.collect()
    
    if file_path_obj.exists():
        file_path_obj.unlink()

    with get_db_connection() as conn:
        with conn.begin():
            # Guarantee relational parent existence safely
            conn.execute(
                text("""
                    INSERT INTO audit_jobs (job_id, company_name, status, file_origin)
                    VALUES (:job_id, 'Sample Corporate Group', 'INGESTING', :origin)
                    ON CONFLICT (job_id) DO NOTHING
                """),
                {"job_id": job_id, "origin": file_origin}
            )
            
            # Insert in memory-pressure-aware chunks rather than one giant
            # executemany: on a loaded worker under its cgroup limit a full
            # 500k-row parameter buffer can trip an OOM kill. The chunk size
            # is re-evaluated after each flush so it adapts as pressure moves.
            from app.polars_memory import get_adaptive_chunk_size

            insert_stmt = text(
                """
                INSERT INTO bank_transactions (job_id, source_chunk_id, file_origin, status, amount, value_date, description_raw)
                VALUES (:job_id, :source_chunk_id, :file_origin, :status, :amount, :value_date, :description_raw)
                """
            )

            batch_data: list[dict] = []
            rows_written = 0
            chunk_size = get_adaptive_chunk_size()
            for row in df.iter_rows(named=True):
                amt = float(row.get("amount") or 0.0)
                parsed_date = parse_date_safely(row.get("value_date") or row.get("date"))

                batch_data.append({
                    "job_id": job_id,
                    "source_chunk_id": chunk_id[:36],
                    "file_origin": file_origin,
                    "status": "UNPROCESSED",
                    "amount": amt,
                    "value_date": parsed_date,
                    "description_raw": str(row.get("description_raw") or "")
                })

                if len(batch_data) >= chunk_size:
                    conn.execute(insert_stmt, batch_data)
                    rows_written += len(batch_data)
                    batch_data = []  # release the buffer before refilling
                    chunk_size = get_adaptive_chunk_size()

            if batch_data:
                conn.execute(insert_stmt, batch_data)
                rows_written += len(batch_data)

    publish_event(
        "BATCH_COMPLETED",
        {"chunk_id": chunk_id, "job_id": job_id, "rows_written": rows_written},
    )

    # Automatically fire off downstream mathematical modeling tasks
    from app.execute_unsupervised_analysis import execute_unsupervised_analysis
    execute_unsupervised_analysis.delay(job_id=job_id)

    return {"status": "SUCCESS", "rows_written": rows_written, "job_id": job_id, "chunk_id": chunk_id}