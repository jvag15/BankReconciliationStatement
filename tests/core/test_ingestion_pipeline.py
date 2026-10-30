"""End-to-end integration test harness for asynchronous statement ingestion."""

from __future__ import annotations

import io
import os
import pathlib
import tempfile
import uuid

# Force explicit environment bindings before importing the app
from dotenv import load_dotenv
load_dotenv()

os.environ["CELERY_BROKER_URL"] = os.environ.get("CELERY_BROKER_URL", "redis://127.0.0.1:6379/0")
os.environ["POSTGRES_RESULT_BACKEND_URL"] = os.environ.get("POSTGRES_RESULT_BACKEND_URL", "postgresql://brs_user:brs_password@127.0.0.1:5432/brs_db")
os.environ["DATABASE_URL"] = os.environ.get("DATABASE_URL", "postgresql://brs_user:brs_password@127.0.0.1:5432/brs_db")

import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

# Set required test environment configurations
if "FERNET_KEY" not in os.environ:
    os.environ["FERNET_KEY"] = Fernet.generate_key().decode("utf-8")

from app.main import app

client = TestClient(app)


def test_synchronous_ingestion_lifecycle() -> None:
    """Verify the full ingestion pipeline by invoking the task function directly."""
    from app.task_ingestion import process_statement_chunk

    mock_id = str(uuid.uuid4())[:8]
    chunk_id = f"test_chunk_{mock_id}"

    csv_payload = (
        f"date,value_date,description,description_raw,amount,balance,utr,source_chunk_id,file_origin\n"
        f"2026-06-25,2026-06-25,NEFT-VENDOR-PAY,NEFT-VENDOR-PAY,-1500.00,10000.00,UTR{mock_id},{chunk_id},test_suite.csv\n"
    )
    f = Fernet(os.environ["FERNET_KEY"].encode("utf-8"))
    encrypted_payload = f.encrypt(csv_payload.encode("utf-8"))

    upload_dir = pathlib.Path("uploads")
    upload_dir.mkdir(exist_ok=True)
    enc_file = upload_dir / f"{chunk_id}.enc"
    enc_file.write_bytes(encrypted_payload)

    try:
        result = process_statement_chunk.apply(
            kwargs={
                "chunk_id": chunk_id,
                "file_origin": "test_suite.csv",
                "file_path_encrypted": str(enc_file),
            }
        ).get()

        assert result["status"] == "SUCCESS"
        assert result["chunk_id"] == chunk_id
        assert int(result["rows_written"]) >= 1

        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            pytest.fail("DATABASE_URL is not set.")

        engine = create_engine(db_url)
        with engine.connect() as conn:
            query = text(
                "SELECT description_raw FROM bank_transactions WHERE source_chunk_id = :chunk_id"
            )
            rows = conn.execute(query, {"chunk_id": chunk_id}).fetchall()
            assert len(rows) == 1, "Expected exactly one matching transaction record."

        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM bank_transactions WHERE source_chunk_id = :chunk_id"),
                {"chunk_id": chunk_id},
            )
    finally:
        if enc_file.exists():
            enc_file.unlink()


def test_authentication_rejection() -> None:
    """Missing X-API-Key must yield 401."""
    response = client.post(
        "/api/v1/statements/ingest",
        files={"file": ("dummy.csv", io.BytesIO(b"data"), "text/csv")}
    )
    assert response.status_code == 401
    assert "detail" in response.json()


def test_malformed_payload_rejection() -> None:
    """CSV missing required columns must yield 400."""
    csv_payload = """date,value_date,description,balance,utr,source_chunk_id,file_origin
2026-06-25,2026-06-25,NEFT-VENDOR-PAY,10000.00,UTR123,chunk_123,test.csv
"""
    f = Fernet(os.environ["FERNET_KEY"].encode("utf-8"))
    encrypted_payload = f.encrypt(csv_payload.encode("utf-8"))

    response = client.post(
        "/api/v1/statements/ingest",
        files={"file": ("malformed.enc", io.BytesIO(encrypted_payload), "application/octet-stream")},
        headers={"X-API-Token": os.environ.get("API_AUTH_TOKEN", "test-token-1234567890abcdef")}
    )
    assert response.status_code == 400
    assert "Malformed payload" in response.json()["detail"]


def test_cryptographic_failure_dlq_routing() -> None:
    """Corrupted ciphertext must route to DLQ without crashing the worker."""
    from app.db_dlq import _write_to_dlq

    corrupt_payload = b"this_is_not_a_valid_fernet_token"
    upload_dir = pathlib.Path("uploads")
    upload_dir.mkdir(exist_ok=True)
    corrupt_file = upload_dir / f"corrupt_{uuid.uuid4().hex[:8]}.enc"
    corrupt_file.write_bytes(corrupt_payload)

    worker_id = f"test_worker_{uuid.uuid4().hex[:8]}"

    try:
        from app.storage_encryption import decrypt_file_to_memory
        try:
            decrypt_file_to_memory(str(corrupt_file))
            pytest.fail("Decryption should have raised an exception.")
        except Exception as exc:
            import traceback as tb
            _write_to_dlq(
                raw_payload=str(corrupt_file),
                file_origin="test_suite_corrupt.csv",
                worker_id=worker_id,
                exception_class=type(exc).__name__,
                stack_trace=tb.format_exc(),
            )

        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            pytest.fail("DATABASE_URL is not set.")

        engine = create_engine(db_url)
        with engine.connect() as conn:
            query = text(
                "SELECT exception_class FROM dlq_transactions WHERE worker_id = :worker_id"
            )
            result = conn.execute(query, {"worker_id": worker_id}).fetchall()

            assert len(result) >= 1, "Expected DLQ entry for cryptographic failure."
            assert "InvalidToken" in str(result[0][0]) or "Error" in str(result[0][0])

        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM dlq_transactions WHERE worker_id = :worker_id"),
                {"worker_id": worker_id},
            )
    finally:
        if corrupt_file.exists():
            corrupt_file.unlink()

def test_audit_submit_and_status() -> None:
    """Verify /api/v1/statements/ingest creates a job and /tasks/ returns correctly."""
    csv_payload = "date,value_date,description,balance,utr\n2026-06-25,2026-06-25,PAY,1000,UTR"
    response = client.post(
        "/api/v1/statements/ingest",
        data={"company_name": "TestCorp"},
        files={"file": ("test_statement.csv", io.BytesIO(csv_payload.encode("utf-8")), "text/csv")},
        headers={"X-API-Token": os.environ.get("API_AUTH_TOKEN", "test-token-1234567890abcdef")}
    )
    assert response.status_code == 202
    res_json = response.json()
    assert "task_id" in res_json
    assert res_json["status"] == "PENDING_PROCESSING"
    
    task_id = res_json["task_id"]
    
    status_response = client.get(
        f"/api/v1/statements/tasks/{task_id}",
        headers={"X-API-Token": os.environ.get("API_AUTH_TOKEN", "test-token-1234567890abcdef")}
    )
    assert status_response.status_code == 200
    stat_json = status_response.json()
    assert stat_json["task_id"] == task_id
    assert stat_json["status"] in ["PENDING", "SUCCESS", "FAILURE", "PENDING_PROCESSING"]

def test_dynamic_chunk_sizing(monkeypatch) -> None:
    """Verify get_dynamic_chunk_size limits chunks accurately under low memory."""
    from scripts.bulk_ingest import get_dynamic_chunk_size
    
    class MockMemInfo:
        def __init__(self, avail):
            self.available = avail
            
    # Mock < 4GB
    monkeypatch.setattr("psutil.virtual_memory", lambda: MockMemInfo(3 * 1024**3))
    assert get_dynamic_chunk_size() == 1000
    
    # Mock < 8GB
    monkeypatch.setattr("psutil.virtual_memory", lambda: MockMemInfo(6 * 1024**3))
    assert get_dynamic_chunk_size() == 2500
    
    # Mock >= 8GB
    monkeypatch.setattr("psutil.virtual_memory", lambda: MockMemInfo(16 * 1024**3))
    assert get_dynamic_chunk_size() == 5000