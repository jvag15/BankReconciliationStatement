"""Drop UNIQUE on source_chunk_id (one chunk holds many transactions)

Revision ID: 004_phase4_chunk_cardinality
Revises: 003_phase3_ml_vectors
Create Date: 2026-07-21 12:00:00.000000

Phase 0 declared ``source_chunk_id`` as UNIQUE on both transaction
tables.  That models the wrong cardinality: a chunk is one *file*, and
every row ingested from that file carries the same ``source_chunk_id``.
The batch INSERT in ``app/task_ingestion.py`` therefore violated the
constraint on its second row and rolled back the whole chunk, so no
statement with more than one row could ever be ingested.

The column stays indexed (non-unique) because it is the lookup key for
per-chunk reprocessing and DLQ replay.
"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '004_phase4_chunk_cardinality'
down_revision = '003_phase3_ml_vectors'
branch_labels = None
depends_on = None


def upgrade():
    # The Phase 0 constraint was emitted unnamed, so PostgreSQL auto-named
    # it "<table>_<column>_key".  Drop defensively: depending on the server
    # version it may be backed either by a constraint or a bare unique index.
    for table in ("bank_transactions", "ledger_transactions"):
        op.execute(
            f"ALTER TABLE {table} "
            f"DROP CONSTRAINT IF EXISTS {table}_source_chunk_id_key"
        )
        op.execute(
            f"DROP INDEX IF EXISTS {table}_source_chunk_id_key"
        )

    # Retain a non-unique lookup index on the bank side, matching the
    # index=True declaration in app/db_models.py.
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_bank_transactions_source_chunk_id "
        "ON bank_transactions (source_chunk_id)"
    )


def downgrade():
    # Reinstating UNIQUE will fail on any data ingested since the upgrade
    # (that is the point of this migration), so drop the rows' index first
    # and let the constraint creation surface the conflict loudly.
    op.execute("DROP INDEX IF EXISTS ix_bank_transactions_source_chunk_id")
    for table in ("bank_transactions", "ledger_transactions"):
        op.execute(
            f"ALTER TABLE {table} "
            f"ADD CONSTRAINT {table}_source_chunk_id_key "
            f"UNIQUE (source_chunk_id)"
        )
