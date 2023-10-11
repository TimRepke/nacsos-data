"""lexisenum

Revision ID: b5d6e2a0e7ca
Revises: 1f57c6f42055
Create Date: 2023-10-11 18:54:34.723740

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'b5d6e2a0e7ca'
down_revision = '1f57c6f42055'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TYPE itemtype ADD VALUE IF NOT EXISTS 'lexis';")


def downgrade():
    pass  # vorwärts immer, rückwärts nimmer
