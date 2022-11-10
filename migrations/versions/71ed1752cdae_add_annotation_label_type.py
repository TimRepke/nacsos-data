"""add annotation_label type

Revision ID: 71ed1752cdae
Revises: 1068ffa0b6e1
Create Date: 2022-11-10 18:31:19.436734

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '71ed1752cdae'
down_revision = '1068ffa0b6e1'
branch_labels = None
depends_on = None


def upgrade():
    op.execute('CREATE TYPE annotation_label AS (key varchar, repeat integer);')


def downgrade():
    op.execute("DROP TYPE annotation_label;")
