"""incl rule

Revision ID: 29ee854289d4
Revises: 00859c3f922e
Create Date: 2024-02-09 18:24:01.100710

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '29ee854289d4'
down_revision = '00859c3f922e'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('annotation_scheme', sa.Column('inclusion_rule', sa.String(), nullable=True))


def downgrade():
    op.drop_column('annotation_scheme', 'inclusion_rule')
