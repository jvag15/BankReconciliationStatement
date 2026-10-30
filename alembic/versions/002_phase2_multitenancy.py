"""Phase 2.1 — Multi-tenant audit_jobs table and job_id FK injection.

Revision ID: 002_phase2_multitenancy
Revises: 001_phase0
Create Date: 2026-06-24 16:44:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "002_phase2_multitenancy"
down_revision: Union[str, None] = "001_phase0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create audit_jobs table and add job_id FK to bank/ledger transactions."""

    # 1. Create the audit_jobs registry table
    op.create_table(
        "audit_jobs",
        sa.Column("job_id", sa.Uuid(), primary_key=True),
        sa.Column("company_name", sa.String(255), nullable=False),
        sa.Column(
            "status",
            sa.String(20),
            nullable=False,
            server_default="QUEUED",
        ),
        sa.Column("file_origin", sa.String(255), nullable=False),
        sa.Column("total_rows", sa.BigInteger(), nullable=True),
        sa.Column(
            "rows_processed",
            sa.BigInteger(),
            nullable=False,
            server_default="0",
        ),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "completed_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.CheckConstraint(
            "status IN ('QUEUED','INGESTING','RUNNING_ML','COMPLETED','FAILED')",
            name="ck_audit_jobs_status",
        ),
    )
    op.create_index("ix_jobs_company_created", "audit_jobs", ["company_name", "created_at"])
    op.create_index("ix_jobs_status", "audit_jobs", ["status"])

    # 2. Add job_id FK column to bank_transactions
    op.add_column(
        "bank_transactions",
        sa.Column("job_id", sa.Uuid(), nullable=True),
    )
    op.create_foreign_key(
        "fk_bank_transactions_job_id",
        "bank_transactions",
        "audit_jobs",
        ["job_id"],
        ["job_id"],
    )
    op.create_index(
        "ix_bank_job_id",
        "bank_transactions",
        ["job_id"],
        postgresql_where=sa.text("job_id IS NOT NULL"),
    )

    # 3. Add job_id FK column to ledger_transactions
    op.add_column(
        "ledger_transactions",
        sa.Column("job_id", sa.Uuid(), nullable=True),
    )
    op.create_foreign_key(
        "fk_ledger_transactions_job_id",
        "ledger_transactions",
        "audit_jobs",
        ["job_id"],
        ["job_id"],
    )
    op.create_index(
        "ix_ledger_job_id",
        "ledger_transactions",
        ["job_id"],
        postgresql_where=sa.text("job_id IS NOT NULL"),
    )


def downgrade() -> None:
    """Remove job_id FK columns and drop audit_jobs table."""

    # Reverse: ledger_transactions
    op.drop_index("ix_ledger_job_id", table_name="ledger_transactions")
    op.drop_constraint("fk_ledger_transactions_job_id", "ledger_transactions", type_="foreignkey")
    op.drop_column("ledger_transactions", "job_id")

    # Reverse: bank_transactions
    op.drop_index("ix_bank_job_id", table_name="bank_transactions")
    op.drop_constraint("fk_bank_transactions_job_id", "bank_transactions", type_="foreignkey")
    op.drop_column("bank_transactions", "job_id")

    # Reverse: audit_jobs
    op.drop_index("ix_jobs_status", table_name="audit_jobs")
    op.drop_index("ix_jobs_company_created", table_name="audit_jobs")
    op.drop_table("audit_jobs")
