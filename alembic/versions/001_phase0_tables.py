"""Initial migration — Phase 0 tables.

Revision ID: 001_phase0
Revises: None
Create Date: 2026-06-21 12:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "001_phase0"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create the 5 Phase 0 tables per the Data Dictionary."""
    op.create_table(
        "reconciliation_rules",
        sa.Column("rule_id", sa.String(50), primary_key=True),
        sa.Column("rule_name", sa.String(255), nullable=False),
        sa.Column("rule_description", sa.Text(), nullable=False),
        sa.Column(
            "match_type",
            sa.String(20),
            nullable=False,
        ),
        sa.Column(
            "confidence_default",
            sa.Numeric(5, 4),
            nullable=False,
        ),
        sa.Column(
            "amount_tolerance",
            sa.Numeric(20, 8),
            nullable=True,
        ),
        sa.Column(
            "date_tolerance_days",
            sa.Integer(),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.CheckConstraint(
            "match_type IN ("
            "'EXACT_UTR','EXACT_AMOUNT_DATE','1_N_SETTLEMENT',"
            "'FUZZY_NARRATION','MANUAL_HITL')",
            name="ck_reconciliation_rules_match_type",
        ),
    )
    op.create_index(
        "ix_recon_match_type", "reconciliation_rules", ["match_type"],
    )

    op.create_table(
        "ledger_transactions",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column(
            "source_chunk_id", sa.String(36), nullable=False, unique=True,
        ),
        sa.Column("file_origin", sa.String(255), nullable=False),
        sa.Column("ledger_date", sa.Date(), nullable=False),
        sa.Column("amount", sa.Numeric(20, 8), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("account_code", sa.String(50), nullable=False),
        sa.Column("cost_center", sa.String(50), nullable=True),
        sa.Column("voucher_number", sa.String(50), nullable=True),
        sa.Column("utr", sa.String(22), nullable=True),
        sa.Column("neft_ref", sa.String(30), nullable=True),
        sa.Column("rtgs_ref", sa.String(30), nullable=True),
        sa.Column("imps_ref", sa.String(12), nullable=True),
        sa.Column("upi_ref", sa.String(18), nullable=True),
        sa.Column("ifsc", sa.String(11), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ledger_date_amount",
        "ledger_transactions",
        ["ledger_date", "amount"],
    )
    op.create_index(
        "ix_ledger_utr",
        "ledger_transactions",
        ["utr"],
        postgresql_where=sa.text("utr IS NOT NULL"),
    )
    op.create_index(
        "ix_ledger_voucher",
        "ledger_transactions",
        ["voucher_number"],
        postgresql_where=sa.text("voucher_number IS NOT NULL"),
    )

    op.create_table(
        "bank_transactions",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column(
            "source_chunk_id", sa.String(36), nullable=False, unique=True,
        ),
        sa.Column("file_origin", sa.String(255), nullable=False),
        sa.Column(
            "status",
            sa.String(20),
            nullable=False,
            server_default="UNPROCESSED",
        ),
        sa.Column("amount", sa.Numeric(20, 8), nullable=False),
        sa.Column("value_date", sa.Date(), nullable=False),
        sa.Column("description_raw", sa.Text(), nullable=False),
        sa.Column("utr", sa.String(22), nullable=True),
        sa.Column("neft_ref", sa.String(30), nullable=True),
        sa.Column("rtgs_ref", sa.String(30), nullable=True),
        sa.Column("imps_ref", sa.String(12), nullable=True),
        sa.Column("upi_ref", sa.String(18), nullable=True),
        sa.Column("ifsc", sa.String(11), nullable=True),
        sa.Column("cin", sa.String(21), nullable=True),
        sa.Column("pan", sa.String(10), nullable=True),
        sa.Column("risk_score", sa.Numeric(5, 4), nullable=True),
        sa.Column("category", sa.String(50), nullable=True),
        sa.Column(
            "matched_ledger_id",
            sa.BigInteger(),
            sa.ForeignKey("ledger_transactions.id"),
            nullable=True,
        ),
        sa.Column("confidence_score", sa.Numeric(5, 4), nullable=True),
        sa.Column("match_rule_id", sa.String(50), nullable=True),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "status IN ("
            "'UNPROCESSED','PARSED','SCORED','PARTIAL_MATCH',"
            "'MATCHED','RECONCILED')",
            name="ck_bank_transactions_status",
        ),
    )
    op.create_index(
        "ix_bank_status_date",
        "bank_transactions",
        ["status", "value_date"],
    )
    op.create_index(
        "ix_bank_utr",
        "bank_transactions",
        ["utr"],
        postgresql_where=sa.text("utr IS NOT NULL"),
    )
    op.create_index(
        "ix_bank_amount_date",
        "bank_transactions",
        ["amount", "value_date"],
    )
    op.create_index(
        "ix_bank_matched_ledger",
        "bank_transactions",
        ["matched_ledger_id"],
        postgresql_where=sa.text("matched_ledger_id IS NOT NULL"),
    )
    op.create_index(
        "ix_bank_ingested_at",
        "bank_transactions",
        ["ingested_at"],
    )

    op.create_table(
        "dlq_transactions",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("raw_payload", sa.Text(), nullable=False),
        sa.Column("file_origin", sa.String(255), nullable=False),
        sa.Column("worker_id", sa.String(255), nullable=False),
        sa.Column("exception_class", sa.String(255), nullable=False),
        sa.Column("stack_trace", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_dlq_created_at", "dlq_transactions", ["created_at"],
    )
    op.create_index(
        "ix_dlq_exc_created",
        "dlq_transactions",
        ["exception_class", "created_at"],
    )

    op.create_table(
        "audit_log",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("transaction_id", sa.BigInteger(), nullable=False),
        sa.Column("from_status", sa.String(20), nullable=False),
        sa.Column("to_status", sa.String(20), nullable=False),
        sa.Column("worker_id", sa.String(255), nullable=False),
        sa.Column("rule_id", sa.String(50), nullable=True),
        sa.Column("confidence_score", sa.Numeric(5, 4), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "from_status IN ("
            "'UNPROCESSED','PARSED','SCORED','PARTIAL_MATCH',"
            "'MATCHED','RECONCILED')",
            name="ck_audit_from_status",
        ),
        sa.CheckConstraint(
            "to_status IN ("
            "'UNPROCESSED','PARSED','SCORED','PARTIAL_MATCH',"
            "'MATCHED','RECONCILED')",
            name="ck_audit_to_status",
        ),
        sa.CheckConstraint(
            "from_status != to_status",
            name="ck_audit_status_transition",
        ),
    )
    op.create_index(
        "ix_audit_tx_created", "audit_log", ["transaction_id", "created_at"],
    )
    op.create_index(
        "ix_audit_created_at", "audit_log", ["created_at"],
    )
    op.create_index(
        "ix_audit_rule_id",
        "audit_log",
        ["rule_id"],
        postgresql_where=sa.text("rule_id IS NOT NULL"),
    )

    # Append-only guard: prevent UPDATE and DELETE on audit_log
    op.execute(
        """
        CREATE RULE audit_log_no_update AS ON UPDATE TO audit_log
            DO INSTEAD NOTHING
        """
    )
    op.execute(
        """
        CREATE RULE audit_log_no_delete AS ON DELETE TO audit_log
            DO INSTEAD NOTHING
        """
    )


def downgrade() -> None:
    """Drop Phase 0 tables in reverse dependency order."""
    op.execute("DROP RULE IF EXISTS audit_log_no_delete ON audit_log")
    op.execute("DROP RULE IF EXISTS audit_log_no_update ON audit_log")
    op.drop_table("audit_log")
    op.drop_table("dlq_transactions")
    op.drop_table("bank_transactions")
    op.drop_table("ledger_transactions")
    op.drop_table("reconciliation_rules")
