"""add rev to aiv

Revision ID: cc7548db705c
Revises: 31af32aff187
Create Date: 2024-10-21 11:36:06.234977

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'cc7548db705c'
down_revision = '31af32aff187'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('academic_item_variant', sa.Column('import_revision_id', sa.UUID(), nullable=True))
    op.create_foreign_key(None, 'academic_item_variant', 'import_revision', ['import_revision_id'], ['import_revision_id'],
                          ondelete='CASCADE')


def downgrade():
    op.drop_constraint(None, 'academic_item_variant', type_='foreignkey')
    op.drop_column('academic_item_variant', 'import_revision_id')
