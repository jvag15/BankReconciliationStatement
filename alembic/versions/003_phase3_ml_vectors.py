"""Add ML vectors to bank_transactions

Revision ID: 003_phase3_ml_vectors
Revises: 002_phase2_multitenancy
Create Date: 2026-07-16 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '003_phase3_ml_vectors'
down_revision = '002_phase2_multitenancy'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('bank_transactions', sa.Column('isolation_forest_score', sa.Numeric(precision=5, scale=4), nullable=True))
    op.add_column('bank_transactions', sa.Column('dbscan_cluster', sa.Integer(), nullable=True))
    op.add_column('bank_transactions', sa.Column('z_score_deviation', sa.Numeric(precision=10, scale=4), nullable=True))
    op.add_column('bank_transactions', sa.Column('calculated_risk_tier', sa.Integer(), nullable=True))


def downgrade():
    op.drop_column('bank_transactions', 'calculated_risk_tier')
    op.drop_column('bank_transactions', 'z_score_deviation')
    op.drop_column('bank_transactions', 'dbscan_cluster')
    op.drop_column('bank_transactions', 'isolation_forest_score')
