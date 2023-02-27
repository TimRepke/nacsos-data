"""add config to assignment scope

Revision ID: 1148d434d975
Revises: f72c47e6708d
Create Date: 2022-07-14 19:51:13.482580

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '1148d434d975'
down_revision = 'f72c47e6708d'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('assignment_scope', sa.Column('config', postgresql.JSONB(astext_type=sa.Text()), nullable=True))


def downgrade():
    op.drop_column('assignment_scope', 'config')
