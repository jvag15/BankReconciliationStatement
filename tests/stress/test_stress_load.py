"""Gated high-volume stress test.

Deselected by default (see pyproject.toml `addopts` and conftest RUN_STRESS
gate). Run with:  RUN_STRESS=1 pytest -m stress tests/stress/

Exercises generation -> ingestion via the eager Celery path (like
tests/core/test_ingestion_pipeline.py, no HTTP/broker needed) at volume,
and asserts throughput and integrity thresholds. The full HTTP + matching
harness lives in scripts/run_stress_test.py for live-stack runs.
"""

from __future__ import annotations

import time
import uuid

import random

import pytest

from scripts.generate_stress_data import _build_statement_batch

# Module-level marker so `-m 'not stress'` (pyproject addopts) deselects the
# whole module by default. A conftest `pytestmark` does not apply to test
# modules, so the marker must live here for deselection to take effect; the
# conftest RUN_STRESS gate remains as a second guard for explicit `-m stress`.
pytestmark = pytest.mark.stress


# Assertion thresholds.
_MIN_ROWS = 10_000
_MAX_ANOMALY_DRIFT = 0.02  # observed anomaly rate must be within +/- of target


def test_generator_produces_valid_high_volume_statements() -> None:
    """10k+ rows generate with the API-required header and realistic amounts."""
    rng = random.Random(7)
    csv_bytes, records = _build_statement_batch(rng, _MIN_ROWS, anomaly_rate=0.05)

    header = csv_bytes.split(b"\n", 1)[0].decode()
    assert "amount" in header and "description_raw" in header
    assert len(records) == _MIN_ROWS

    observed_anomaly = sum(1 for r in records if r["anomaly"]) / len(records)
    assert abs(observed_anomaly - 0.05) < _MAX_ANOMALY_DRIFT

    # Amounts are realistic rupee magnitudes, both signs present.
    amounts = [r["amount"] for r in records]
    assert any(a > 0 for a in amounts) and any(a < 0 for a in amounts)
    assert max(abs(a) for a in amounts) <= 500_000.0


def test_eager_ingestion_at_volume() -> None:
    """A large encrypted chunk ingests end-to-end via the eager Celery path."""
    import os
    from cryptography.fernet import Fernet
    from sqlalchemy import text

    from app.session import get_db_connection
    from app.storage_paths import get_storage_dir

    # Fresh key for this run so we can decrypt what we encrypt.
    key = Fernet.generate_key()
    os.environ["FERNET_KEY"] = key.decode()

    rng = random.Random(11)
    csv_bytes, records = _build_statement_batch(rng, 5_000, anomaly_rate=0.05)
    enc = Fernet(key).encrypt(csv_bytes)

    chunk_id = f"stress_test_{uuid.uuid4().hex[:10]}"
    spool = get_storage_dir() / f"{chunk_id}.enc"
    spool.write_bytes(enc)

    from app.task_ingestion import process_statement_chunk

    t0 = time.perf_counter()
    result = process_statement_chunk.apply(kwargs={
        "chunk_id": chunk_id,
        "file_origin": f"stress_test::{chunk_id}",
        "file_path_encrypted": str(spool),
    }).get()
    elapsed = time.perf_counter() - t0

    assert result["status"] == "SUCCESS"
    assert result["rows_written"] == 5_000
    job_id = result["job_id"]

    # Integrity: exactly the rows we sent landed for this job, none lost.
    with get_db_connection() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM bank_transactions WHERE job_id = :j"),
            {"j": job_id},
        ).scalar()
    assert count == 5_000

    # Throughput floor (very lenient; guards against a catastrophic regression).
    assert elapsed < 60.0

    # Cleanup this run's rows.
    with get_db_connection() as conn:
        with conn.begin():
            conn.execute(text("DELETE FROM bank_transactions WHERE job_id = :j"), {"j": job_id})
            conn.execute(text("DELETE FROM audit_jobs WHERE job_id = :j"), {"j": job_id})
    if spool.exists():
        spool.unlink()
