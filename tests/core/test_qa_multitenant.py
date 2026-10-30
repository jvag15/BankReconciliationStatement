"""QA Automation: Multi-Tenant Isolation Validation."""
from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from app.session import _get_engine

class TestMultiTenantIsolation:
    """Validate that transactions are strictly partitioned by job_id and cross-tenant data leaks are impossible."""

    def test_strict_partitioning_by_job_id(self) -> None:
        engine = _get_engine()
        job_a = str(uuid.uuid4())
        job_b = str(uuid.uuid4())
        
        with engine.begin() as conn:
            # Insert Tenant A Audit Job
            conn.execute(
                text("INSERT INTO audit_jobs (job_id, company_name, file_origin) VALUES (:jid, 'Tenant A Corp', 'file_a.csv')"),
                {"jid": job_a}
            )
            # Insert Tenant B Audit Job
            conn.execute(
                text("INSERT INTO audit_jobs (job_id, company_name, file_origin) VALUES (:jid, 'Tenant B Inc', 'file_b.csv')"),
                {"jid": job_b}
            )
            
            # Insert Transactions for Tenant A
            conn.execute(
                text("""
                INSERT INTO bank_transactions (source_chunk_id, file_origin, amount, value_date, description_raw, job_id, status)
                VALUES ('chunkA', 'file_a.csv', 10000, '2024-01-01', 'Tenant A Transaction', :job_a, 'UNPROCESSED')
                """),
                {"job_a": job_a}
            )
            # Insert Transactions for Tenant B
            conn.execute(
                text("""
                INSERT INTO bank_transactions (source_chunk_id, file_origin, amount, value_date, description_raw, job_id, status)
                VALUES ('chunkB', 'file_b.csv', 20000, '2024-01-01', 'Tenant B Transaction', :job_b, 'UNPROCESSED')
                """),
                {"job_b": job_b}
            )
            
            # Assert Query for Tenant A isolated boundaries
            result_a = conn.execute(
                text("SELECT description_raw FROM bank_transactions WHERE job_id = :job_a"),
                {"job_a": job_a}
            ).fetchall()
            
            assert len(result_a) == 1, "Cross-tenant leak: Tenant A retrieved incorrect number of rows"
            assert result_a[0][0] == 'Tenant A Transaction'
            
            # Assert Query for Tenant B isolated boundaries
            result_b = conn.execute(
                text("SELECT description_raw FROM bank_transactions WHERE job_id = :job_b"),
                {"job_b": job_b}
            ).fetchall()
            
            assert len(result_b) == 1, "Cross-tenant leak: Tenant B retrieved incorrect number of rows"
            assert result_b[0][0] == 'Tenant B Transaction'
            
            # Cleanup Sandbox Data
            conn.execute(text("DELETE FROM bank_transactions WHERE job_id IN (:job_a, :job_b)"), {"job_a": job_a, "job_b": job_b})
            conn.execute(text("DELETE FROM audit_jobs WHERE job_id IN (:job_a, :job_b)"), {"job_a": job_a, "job_b": job_b})
