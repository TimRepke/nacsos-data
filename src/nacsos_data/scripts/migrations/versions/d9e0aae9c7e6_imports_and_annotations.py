"""imports and annotations

Revision ID: d9e0aae9c7e6
Revises: 91d5479e3a40
Create Date: 2023-09-11 10:57:30.804252

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd9e0aae9c7e6'
down_revision = '91d5479e3a40'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('bot_annotation', sa.Column('order', sa.Integer(), nullable=True))
    op.drop_constraint('m2m_import_item_item_id_fkey', 'm2m_import_item', type_='foreignkey')
    op.drop_constraint('m2m_import_item_import_id_fkey', 'm2m_import_item', type_='foreignkey')
    op.create_foreign_key(None, 'm2m_import_item', 'import', ['import_id'], ['import_id'])
    op.create_foreign_key(None, 'm2m_import_item', 'item', ['item_id'], ['item_id'])


def downgrade():
    op.drop_constraint(None, 'm2m_import_item', type_='foreignkey')
    op.drop_constraint(None, 'm2m_import_item', type_='foreignkey')
    op.create_foreign_key('m2m_import_item_import_id_fkey', 'm2m_import_item', 'import', ['import_id'], ['import_id'], ondelete='CASCADE')
    op.create_foreign_key('m2m_import_item_item_id_fkey', 'm2m_import_item', 'item', ['item_id'], ['item_id'], ondelete='CASCADE')
    op.drop_column('bot_annotation', 'order')
