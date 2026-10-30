"""Add recovery tracking columns to dlq_transactions

Revision ID: 006_dlq_recovery_tracking
Revises: 005_audit_log_append_only_rules
Create Date: 2026-07-22 00:00:00.000000

The dead-letter queue was insert-only with no state, so a failed chunk sat
there permanently with no way to distinguish a transient failure worth
retrying from a terminal one. app/task_dlq_recovery.py drives an automated
recovery pass; it needs somewhere to record its verdict.

Columns
-------
recovery_status    PENDING (default) | REQUEUED | FAILED_UNRECOVERABLE
recovery_attempts  monotonic counter; caps automatic re-enqueue to bound loops
last_recovery_at   timestamp of the most recent recovery pass over this row
"""
import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = '006_dlq_recovery_tracking'
down_revision = '005_audit_log_append_only_rules'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'dlq_transactions',
        sa.Column(
            'recovery_status',
            sa.String(length=30),
            nullable=False,
            server_default='PENDING',
        ),
    )
    op.add_column(
        'dlq_transactions',
        sa.Column(
            'recovery_attempts',
            sa.Integer(),
            nullable=False,
            server_default='0',
        ),
    )
    op.add_column(
        'dlq_transactions',
        sa.Column('last_recovery_at', sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        'ix_dlq_recovery_status', 'dlq_transactions', ['recovery_status'],
    )


def downgrade():
    op.drop_index('ix_dlq_recovery_status', table_name='dlq_transactions')
    op.drop_column('dlq_transactions', 'last_recovery_at')
    op.drop_column('dlq_transactions', 'recovery_attempts')
    op.drop_column('dlq_transactions', 'recovery_status')
