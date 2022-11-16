"""add multi_int

Revision ID: a8a67609c622
Revises: ac0d7784de10
Create Date: 2022-11-16 20:01:03.553483

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'a8a67609c622'
down_revision = 'ac0d7784de10'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('bot_annotation', sa.Column('multi_int', postgresql.ARRAY(sa.Integer()), nullable=True))


def downgrade():
    op.drop_column('bot_annotation', 'multi_int')
