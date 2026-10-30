"""Restore append-only rules on audit_log

Revision ID: 005_audit_log_append_only_rules
Revises: 004_phase4_chunk_cardinality
Create Date: 2026-07-22 00:00:00.000000

Migration 001 created two PostgreSQL rules making ``audit_log`` an
append-only ledger.  Databases that were bootstrapped from
``app/db_models.py`` via ``metadata.create_all()`` instead of the
migration chain silently lack them: SQLAlchemy's ``MetaData`` cannot
express ``CREATE RULE``, so the immutability guarantee was dropped
without any error surfacing.

That leaves audit rows freely UPDATE-able and DELETE-able, which
breaks the audit trail this table exists to provide.  This migration
re-applies the rules idempotently so it is safe on both provisioning
paths — a database already carrying them from 001 is left unchanged.
"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '005_audit_log_append_only_rules'
down_revision = '004_phase4_chunk_cardinality'
branch_labels = None
depends_on = None


def upgrade():
    # CREATE RULE has no IF NOT EXISTS form, and CREATE OR REPLACE RULE is
    # the supported idempotent spelling. Re-running it against a database
    # that already has the rule from 001 is a no-op.
    op.execute(
        "CREATE OR REPLACE RULE audit_log_no_update AS "
        "ON UPDATE TO audit_log DO INSTEAD NOTHING"
    )
    op.execute(
        "CREATE OR REPLACE RULE audit_log_no_delete AS "
        "ON DELETE TO audit_log DO INSTEAD NOTHING"
    )


def downgrade():
    # Dropping these makes the audit ledger mutable again — only correct
    # when deliberately rolling back past this revision.
    op.execute("DROP RULE IF EXISTS audit_log_no_delete ON audit_log")
    op.execute("DROP RULE IF EXISTS audit_log_no_update ON audit_log")
