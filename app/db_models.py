"""SQLAlchemy Core table definitions for BRS Engine."""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    Uuid,
    func,
    text,
)

metadata = MetaData()

_STATUS_ENUM = (
    "'UNPROCESSED','PARSED','SCORED','PARTIAL_MATCH','MATCHED','RECONCILED'"
)

_MATCH_TYPE_ENUM = (
    "'EXACT_UTR','EXACT_AMOUNT_DATE','1_N_SETTLEMENT',"
    "'FUZZY_NARRATION','MANUAL_HITL'"
)

_JOB_STATUS_ENUM = (
    "'QUEUED','INGESTING','RUNNING_ML','COMPLETED','FAILED'"
)

# ---------------------------------------------------------------------------
# audit_jobs — multi-tenant job tracking registry.
# Must precede transactions and ledger_transactions (FK reference).
# ---------------------------------------------------------------------------
audit_jobs = Table(
    "audit_jobs",
    metadata,
    Column("job_id", Uuid, primary_key=True),
    Column("company_name", String(255), nullable=False),
    Column(
        "status",
        String(20),
        nullable=False,
        server_default="QUEUED",
    ),
    Column("file_origin", String(255), nullable=False),
    Column("total_rows", BigInteger, nullable=True),
    Column("rows_processed", BigInteger, nullable=False, server_default="0"),
    Column("error_message", Text, nullable=True),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    Column(
        "updated_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    Column(
        "completed_at",
        DateTime(timezone=True),
        nullable=True,
    ),
    CheckConstraint(
        f"status IN ({_JOB_STATUS_ENUM})",
        name="ck_audit_jobs_status",
    ),
    Index("ix_jobs_company_created", "company_name", "created_at"),
    Index("ix_jobs_status", "status"),
)

# ---------------------------------------------------------------------------
# reconciliation_rules — must precede transactions (FK reference).
# ---------------------------------------------------------------------------
reconciliation_rules = Table(
    "reconciliation_rules",
    metadata,
    Column("rule_id", String(50), primary_key=True),
    Column("rule_name", String(255), nullable=False),
    Column("rule_description", Text, nullable=False),
    Column("match_type", String(20), nullable=False),
    Column("confidence_default", Numeric(5, 4), nullable=False),
    Column("amount_tolerance", Numeric(20, 8), nullable=True),
    Column("date_tolerance_days", Integer, nullable=True),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    CheckConstraint(
        f"match_type IN ({_MATCH_TYPE_ENUM})",
        name="ck_reconciliation_rules_match_type",
    ),
    Index("ix_recon_match_type", "match_type"),
)

# ---------------------------------------------------------------------------
# ledger_transactions — must precede transactions (FK reference).
# ---------------------------------------------------------------------------
ledger_transactions = Table(
    "ledger_transactions",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column(
        "job_id",
        Uuid,
        ForeignKey("audit_jobs.job_id"),
        nullable=True,
    ),
    Column("source_chunk_id", String(36), nullable=False),
    Column("file_origin", String(255), nullable=False),
    Column("ledger_date", Date, nullable=False),
    Column("amount", Numeric(20, 8), nullable=False),
    Column("description", Text, nullable=False),
    Column("account_code", String(50), nullable=False),
    Column("cost_center", String(50), nullable=True),
    Column("voucher_number", String(50), nullable=True),
    Column("utr", String(22), nullable=True),
    Column("neft_ref", String(30), nullable=True),
    Column("rtgs_ref", String(30), nullable=True),
    Column("imps_ref", String(12), nullable=True),
    Column("upi_ref", String(18), nullable=True),
    Column("ifsc", String(11), nullable=True),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    Column(
        "updated_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    Index("ix_ledger_date_amount", "ledger_date", "amount"),
    Index(
        "ix_ledger_utr",
        "utr",
        postgresql_where=text("utr IS NOT NULL"),
    ),
    Index(
        "ix_ledger_voucher",
        "voucher_number",
        postgresql_where=text("voucher_number IS NOT NULL"),
    ),
    Index(
        "ix_ledger_job_id",
        "job_id",
        postgresql_where=text("job_id IS NOT NULL"),
    ),
)

# ---------------------------------------------------------------------------
# transactions
# ---------------------------------------------------------------------------
transactions = Table(
    "bank_transactions",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column(
        "job_id",
        Uuid,
        ForeignKey("audit_jobs.job_id"),
        nullable=True,
    ),
    Column("source_chunk_id", String(36), nullable=False, index=True),
    Column("file_origin", String(255), nullable=False),
    Column(
        "status",
        String(20),
        nullable=False,
        server_default="UNPROCESSED",
    ),
    Column("amount", Numeric(20, 8), nullable=False),
    Column("value_date", Date, nullable=False),
    Column("description_raw", Text, nullable=False),
    Column("utr", String(22), nullable=True),
    Column("neft_ref", String(30), nullable=True),
    Column("rtgs_ref", String(30), nullable=True),
    Column("imps_ref", String(12), nullable=True),
    Column("upi_ref", String(18), nullable=True),
    Column("ifsc", String(11), nullable=True),
    Column("cin", String(21), nullable=True),
    Column("pan", String(10), nullable=True),
    Column("risk_score", Numeric(5, 4), nullable=True),
    Column("category", String(50), nullable=True),
    Column(
        "matched_ledger_id",
        BigInteger,
        ForeignKey("ledger_transactions.id"),
        nullable=True,
    ),
    Column("confidence_score", Numeric(5, 4), nullable=True),
    Column("isolation_forest_score", Numeric(5, 4), nullable=True),
    Column("dbscan_cluster", Integer, nullable=True),
    Column("z_score_deviation", Numeric(10, 4), nullable=True),
    Column("calculated_risk_tier", Integer, nullable=True),
    Column("match_rule_id", String(50), nullable=True),
    Column(
        "ingested_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    Column(
        "updated_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    CheckConstraint(
        f"status IN ({_STATUS_ENUM})",
        name="ck_transactions_status",
    ),
    Index("ix_bank_status_date", "status", "value_date"),
    Index(
        "ix_bank_utr",
        "utr",
        postgresql_where=text("utr IS NOT NULL"),
    ),
    Index("ix_bank_amount_date", "amount", "value_date"),
    Index(
        "ix_bank_matched_ledger",
        "matched_ledger_id",
        postgresql_where=text("matched_ledger_id IS NOT NULL"),
    ),
    Index("ix_bank_ingested_at", "ingested_at"),
    Index(
        "ix_bank_job_id",
        "job_id",
        postgresql_where=text("job_id IS NOT NULL"),
    ),
)

# ---------------------------------------------------------------------------
# dlq_transactions — insert-only dead letter queue.
# ---------------------------------------------------------------------------
dlq_transactions = Table(
    "dlq_transactions",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("raw_payload", Text, nullable=False),
    Column("file_origin", String(255), nullable=False),
    Column("worker_id", String(255), nullable=False),
    Column("exception_class", String(255), nullable=False),
    Column("stack_trace", Text, nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    # Recovery bookkeeping for app/task_dlq_recovery.py (migration 006).
    # PENDING -> REQUEUED (transient failure re-dispatched) | FAILED_UNRECOVERABLE
    Column(
        "recovery_status",
        String(30),
        nullable=False,
        server_default="PENDING",
    ),
    Column(
        "recovery_attempts",
        Integer,
        nullable=False,
        server_default="0",
    ),
    Column("last_recovery_at", DateTime(timezone=True), nullable=True),
    Index("ix_dlq_created_at", "created_at"),
    Index("ix_dlq_exc_created", "exception_class", "created_at"),
    Index("ix_dlq_recovery_status", "recovery_status"),
)

# ---------------------------------------------------------------------------
# audit_log — insert-only append-only event ledger.
# ---------------------------------------------------------------------------
audit_log = Table(
    "audit_log",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("transaction_id", BigInteger, nullable=False),
    Column("from_status", String(20), nullable=False),
    Column("to_status", String(20), nullable=False),
    Column("worker_id", String(255), nullable=False),
    Column("rule_id", String(50), nullable=True),
    Column("confidence_score", Numeric(5, 4), nullable=True),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    CheckConstraint(
        f"from_status IN ({_STATUS_ENUM})",
        name="ck_audit_from_status",
    ),
    CheckConstraint(
        f"to_status IN ({_STATUS_ENUM})",
        name="ck_audit_to_status",
    ),
    CheckConstraint(
        "from_status != to_status",
        name="ck_audit_status_transition",
    ),
    Index("ix_audit_tx_created", "transaction_id", "created_at"),
    Index("ix_audit_created_at", "created_at"),
    Index(
        "ix_audit_rule_id",
        "rule_id",
        postgresql_where=text("rule_id IS NOT NULL"),
    ),
)

# ---------------------------------------------------------------------------
# webhook_subscriptions — registered client callback endpoints.
# ---------------------------------------------------------------------------
webhook_subscriptions = Table(
    "webhook_subscriptions",
    metadata,
    Column(
        "id",
        Uuid,
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    ),
    Column("target_url", Text, nullable=False),
    Column(
        "event",
        String(50),
        nullable=False,
        server_default="batch.reconciled",
    ),
    Column("secret", String(128), nullable=True),
    Column("description", String(255), nullable=True),
    Column(
        "active",
        Boolean,
        nullable=False,
        server_default=text("true"),
    ),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    Column("created_by", String(255), nullable=True),
    Index("ix_webhook_event_active", "event", "active"),
)
