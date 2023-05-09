"""assiscope ts not null

Revision ID: 07e7e605fc9f
Revises: d737a2c48c0b
Create Date: 2023-05-09 19:47:41.785042

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '07e7e605fc9f'
down_revision = 'd737a2c48c0b'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('assignment_scope', 'time_created',
               existing_type=postgresql.TIMESTAMP(timezone=True),
               nullable=False,
               existing_server_default=sa.text('now()'))


def downgrade():
    op.alter_column('assignment_scope', 'time_created',
               existing_type=postgresql.TIMESTAMP(timezone=True),
               nullable=True,
               existing_server_default=sa.text('now()'))