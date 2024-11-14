"""prio app

Revision ID: 12ece8918eb8
Revises: 67c3e5f99233
Create Date: 2024-11-07 20:40:49.591508

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '12ece8918eb8'
down_revision = '67c3e5f99233'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('priorities', sa.Column('nql_parsed', postgresql.JSONB(astext_type=sa.Text()), nullable=True))


def downgrade():
    op.drop_column('priorities', 'nql_parsed')
