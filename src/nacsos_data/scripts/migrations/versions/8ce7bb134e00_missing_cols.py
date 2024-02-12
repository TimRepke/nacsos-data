"""missing cols

Revision ID: 8ce7bb134e00
Revises: f30a4bd3720d
Create Date: 2024-02-12 20:27:59.305898

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '8ce7bb134e00'
down_revision = 'f30a4bd3720d'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('annotation_quality', sa.Column('precision', sa.Float(), nullable=True))
    op.add_column('annotation_quality', sa.Column('recall', sa.Float(), nullable=True))
    op.add_column('annotation_quality', sa.Column('f1', sa.Float(), nullable=True))
    op.add_column('annotation_quality', sa.Column('perc_agree', sa.Float(), nullable=True))


def downgrade():
    op.drop_column('annotation_quality', 'precision')
    op.drop_column('annotation_quality', 'recall')
    op.drop_column('annotation_quality', 'f1')
    op.drop_column('annotation_quality', 'perc_agree')
