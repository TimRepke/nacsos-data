"""var imp cascade

Revision ID: d4673c319106
Revises: b3e78f4fa7ad
Create Date: 2023-05-12 20:06:29.450021

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd4673c319106'
down_revision = 'b3e78f4fa7ad'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint('academic_item_variant_import_id_fkey', 'academic_item_variant', type_='foreignkey')
    op.create_foreign_key(None, 'academic_item_variant', 'import', ['import_id'], ['import_id'], ondelete='CASCADE')


def downgrade():
    op.drop_constraint(None, 'academic_item_variant', type_='foreignkey')
    op.create_foreign_key('academic_item_variant_import_id_fkey', 'academic_item_variant', 'import', ['import_id'], ['import_id'])
