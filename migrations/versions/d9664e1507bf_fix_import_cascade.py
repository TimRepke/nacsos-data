"""fix import cascade

Revision ID: d9664e1507bf
Revises: 27a1ef8e5c89
Create Date: 2023-02-17 17:33:58.874483

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'd9664e1507bf'
down_revision = '27a1ef8e5c89'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint('m2m_import_item_item_id_fkey', 'm2m_import_item', type_='foreignkey')
    op.drop_constraint('m2m_import_item_import_id_fkey', 'm2m_import_item', type_='foreignkey')
    op.create_foreign_key('m2m_import_item_item_id_fkey', 'm2m_import_item', 'item', ['item_id'], ['item_id'],
                          ondelete='cascade')
    op.create_foreign_key('m2m_import_item_import_id_fkey', 'm2m_import_item', 'import', ['import_id'], ['import_id'],
                          ondelete='cascade')


def downgrade():
    op.drop_constraint('m2m_import_item_item_id_fkey', 'm2m_import_item', type_='foreignkey')
    op.drop_constraint('m2m_import_item_import_id_fkey', 'm2m_import_item', type_='foreignkey')
    op.create_foreign_key('m2m_import_item_import_id_fkey', 'm2m_import_item', 'import', ['import_id'], ['import_id'])
    op.create_foreign_key('m2m_import_item_item_id_fkey', 'm2m_import_item', 'item', ['item_id'], ['item_id'])
