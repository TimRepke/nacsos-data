"""Remove title index

Revision ID: a986dfc199b8
Revises: 2c270c20fa65
Create Date: 2022-12-14 17:15:33.420651

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a986dfc199b8'
down_revision = '2c270c20fa65'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('academic_item', sa.Column('pubmed_id', sa.String(), nullable=True))
    op.drop_index('ix_academic_item_title', table_name='academic_item')
    op.create_index(op.f('ix_academic_item_pubmed_id'), 'academic_item', ['pubmed_id'], unique=False)


def downgrade():
    op.drop_index(op.f('ix_academic_item_pubmed_id'), table_name='academic_item')
    op.create_index('ix_academic_item_title', 'academic_item', ['title'], unique=False)
    op.drop_column('academic_item', 'pubmed_id')
