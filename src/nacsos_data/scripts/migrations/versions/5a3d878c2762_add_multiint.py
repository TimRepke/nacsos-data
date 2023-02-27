"""add multiint

Revision ID: 5a3d878c2762
Revises: 734c0532d595
Create Date: 2022-11-08 15:13:40.012829

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '5a3d878c2762'
down_revision = '734c0532d595'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('annotation', sa.Column('multi_int', postgresql.ARRAY(sa.Integer()), nullable=True))
    op.create_check_constraint('annotation_has_value', 'annotation',
                               'num_nonnulls(value_bool, value_int, value_float, value_str, multi_int) = 1')


def downgrade():
    op.drop_column('annotation', 'multi_int')
    op.drop_constraint('annotation_has_value', 'annotation', type_='check')
