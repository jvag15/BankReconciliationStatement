"""Add webhook_subscriptions for reconciliation callbacks

Revision ID: 007_webhook_subscriptions
Revises: 006_dlq_recovery_tracking
Create Date: 2026-07-22 00:00:00.000000

Since the system is headless, clients cannot poll a UI for completion.
This table registers HTTP endpoints that the webhook dispatcher
(app/task_webhook.py) calls when a statement batch finishes
reconciliation, optionally HMAC-signing the payload with `secret`.
"""
import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = '007_webhook_subscriptions'
down_revision = '006_dlq_recovery_tracking'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'webhook_subscriptions',
        sa.Column(
            'id', sa.Uuid(), primary_key=True,
            server_default=sa.text('gen_random_uuid()'),
        ),
        sa.Column('target_url', sa.Text(), nullable=False),
        sa.Column(
            'event', sa.String(length=50), nullable=False,
            server_default='batch.reconciled',
        ),
        # Optional shared secret for HMAC-SHA256 request signing.
        sa.Column('secret', sa.String(length=128), nullable=True),
        sa.Column('description', sa.String(length=255), nullable=True),
        sa.Column(
            'active', sa.Boolean(), nullable=False,
            server_default=sa.text('true'),
        ),
        sa.Column(
            'created_at', sa.DateTime(timezone=True), nullable=False,
            server_default=sa.text('now()'),
        ),
        sa.Column('created_by', sa.String(length=255), nullable=True),
    )
    op.create_index(
        'ix_webhook_event_active',
        'webhook_subscriptions',
        ['event', 'active'],
    )


def downgrade():
    op.drop_index('ix_webhook_event_active', table_name='webhook_subscriptions')
    op.drop_table('webhook_subscriptions')
