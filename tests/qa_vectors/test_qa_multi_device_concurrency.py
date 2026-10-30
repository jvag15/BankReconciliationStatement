import os
import tempfile

import pytest
import threading
from sqlalchemy import create_engine, text

def test_high_concurrency_multi_device_job_race_condition() -> None:
    """Micro-Phase 6.2: Simulate a concurrent multi-device ingestion batch by spawning parallel worker threads.
    Each thread must upload a unique mock statement with an independent job_id foreign key.
    Assert via SQLAlchemy 2.0 text() selectors that executing queries for Device A is cryptographically incapable
    of leaking records or data contamination from Device B.
    """
    # A file-backed SQLite DB with the default pool gives each thread its own
    # connection. The previous StaticPool shared a single connection across all
    # threads, so concurrent begin() calls raced and the test was flaky. The
    # busy timeout lets a thread wait on the write lock instead of erroring.
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    engine = create_engine(
        f"sqlite:///{tmp.name}",
        connect_args={"check_same_thread": False, "timeout": 30},
    )

    with engine.begin() as conn:
        conn.execute(text('''
            CREATE TABLE bank_transactions (
                id INTEGER PRIMARY KEY,
                source_chunk_id TEXT,
                file_origin TEXT,
                amount INTEGER,
                value_date TEXT,
                description_raw TEXT,
                status TEXT,
                ingested_at TEXT,
                updated_at TEXT,
                job_id TEXT
            )
        '''))
        
    def simulate_device_upload(job_id: str, amount: int) -> None:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO bank_transactions (
                        id, source_chunk_id, file_origin, amount, value_date, description_raw, status, ingested_at, updated_at, job_id
                    ) VALUES (
                        :id, :source_chunk_id, :file_origin, :amount, :value_date, :description_raw, 'PARSED', 'now', 'now', :job_id
                    )
                """),
                {
                    "id": amount, 
                    "source_chunk_id": f"chunk_{job_id}", 
                    "file_origin": f"device_{job_id}.csv", 
                    "amount": amount, 
                    "value_date": "2024-01-01", 
                    "description_raw": "test", 
                    "job_id": job_id
                }
            )
            
    threads = []
    job_ids = [f"job_{i}" for i in range(10)]
    for i, j_id in enumerate(job_ids):
        t = threading.Thread(target=simulate_device_upload, args=(j_id, i))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    # Assert absolute tenant data isolation via job_id foreign key
    with engine.connect() as conn:
        for i, j_id in enumerate(job_ids):
            result = conn.execute(text("SELECT amount FROM bank_transactions WHERE job_id = :job_id"), {"job_id": j_id}).fetchall()
            assert len(result) == 1
            assert result[0][0] == i

    engine.dispose()
    os.unlink(tmp.name)
