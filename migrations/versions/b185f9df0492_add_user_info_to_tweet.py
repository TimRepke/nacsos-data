"""add user info to tweet

Revision ID: b185f9df0492
Revises: 9cc6877cf04a
Create Date: 2022-08-12 19:12:21.654773

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'b185f9df0492'
down_revision = '9cc6877cf04a'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('twitter_item', sa.Column('user', postgresql.JSONB(astext_type=sa.Text()), nullable=True))


def downgrade():
    op.drop_column('twitter_item', 'user')
