import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from cryptography.fernet import InvalidToken

from app.task_ingestion import process_statement_chunk


def test_fernet_cipher_integrity_cleanup() -> None:
    """Micro-Phase 1.2 & 1.3: Assert decryption failure triggers unlink() and routes to DLQ."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".enc") as tmp:
        tmp.write(b"fake encrypted payload")
        mock_path = Path(tmp.name)
    # On Windows, we must close the file handle before it can be unlinked by the task
    tmp.close()

    with patch("app.storage_encryption.decrypt_file_to_memory", side_effect=InvalidToken("Corrupted token")), \
         patch("app.db_dlq._write_to_dlq") as mock_dlq:

        with pytest.raises(InvalidToken):
            process_statement_chunk(
                chunk_id="chunk_crypto",
                file_origin="test_crypto.csv",
                file_path_encrypted=str(mock_path),
                job_id="job_crypto_001"
            )

        # Assert unlink was called on the path to ensure zero footprint
        assert not mock_path.exists()
        
        # Verify DLQ routing
        mock_dlq.assert_called_once()
        args = mock_dlq.call_args.kwargs
        assert args["exception_class"] == "InvalidToken"
        assert args["worker_id"] == "unknown"

    # Cleanup the actual temp file
    if mock_path.exists():
        mock_path.unlink()

import io
import os
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_uvicorn_intake_malformed_fernet() -> None:
    """Micro-Phase 1.1: Inject malformed AES-128 Fernet streams into Uvicorn endpoint."""
    corrupt_payload = b"this_is_not_a_valid_fernet_token"
    
    response = client.post(
        "/api/v1/statements/ingest",
        files={"file": ("corrupt.enc", io.BytesIO(corrupt_payload), "application/octet-stream")},
        headers={"X-API-Token": os.environ.get("API_AUTH_TOKEN", "test-token-1234567890abcdef")}
    )
    
    # Uvicorn should accept it (202) and defer the decryption/failure to Celery
    assert response.status_code == 202

    # The defer-to-Celery contract: a tokenised spool filename is returned so
    # the worker can pick the payload up. Resolve the spool via the same helper
    # main.py uses, not the old hardcoded drive-letter path (which resolved
    # drive-relative on Linux).
    from app.storage_paths import get_storage_dir
    filename = response.json()["filename_assigned"]
    assert filename.startswith("chunk_tx_") and filename.endswith(".enc")

    # The file's on-disk lifetime is owned by the worker, which decrypts,
    # fails on the bad token, routes to the DLQ and unlinks it — so asserting
    # the file still exists here races the consumer. Just clean it up if the
    # worker has not already done so.
    file_path = get_storage_dir() / filename
    if file_path.exists():
        file_path.unlink()
