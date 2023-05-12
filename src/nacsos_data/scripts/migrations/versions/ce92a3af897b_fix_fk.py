"""fix fk

Revision ID: ce92a3af897b
Revises: a90b9a7e0aff
Create Date: 2023-05-11 20:20:39.958746

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ce92a3af897b'
down_revision = 'a90b9a7e0aff'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint('academic_item_variant_import_id_fkey', 'academic_item_variant', type_='foreignkey')
    op.create_foreign_key(None, 'academic_item_variant', 'import', ['import_id'], ['import_id'])


def downgrade():
    op.drop_constraint(None, 'academic_item_variant', type_='foreignkey')
    op.create_foreign_key('academic_item_variant_import_id_fkey', 'academic_item_variant', 'academic_item', ['import_id'], ['item_id'])
