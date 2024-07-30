"""drop times

Revision ID: a68e73a7caa6
Revises: 33fe72326e3a
Create Date: 2024-07-29 13:24:14.074430

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'a68e73a7caa6'
down_revision = '33fe72326e3a'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_column('import', 'time_finished')
    op.drop_column('import', 'time_started')


def downgrade():
    op.add_column('import', sa.Column('time_started', postgresql.TIMESTAMP(timezone=True), autoincrement=False, nullable=True))
    op.add_column('import', sa.Column('time_finished', postgresql.TIMESTAMP(timezone=True), autoincrement=False, nullable=True))
