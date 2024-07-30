"""add batchsize

Revision ID: 53d573bcffcb
Revises: a68e73a7caa6
Create Date: 2024-07-30 19:11:10.271159

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = '53d573bcffcb'
down_revision = 'a68e73a7caa6'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('annotation_tracker',
                  sa.Column('batch_size', sa.Integer(), nullable=False, server_default=text('100')))


def downgrade():
    op.drop_column('annotation_tracker', 'batch_size')
