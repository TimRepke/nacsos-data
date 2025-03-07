"""cascade item subclasses

Revision ID: deeb94c47382
Revises: 99c9410f5dda
Create Date: 2023-01-27 14:05:17.918976

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'deeb94c47382'
down_revision = '99c9410f5dda'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint('academic_item_item_id_fkey', 'academic_item', type_='foreignkey')
    op.create_foreign_key('academic_item_item_id_fkey', 'academic_item', 'item', ['item_id'], ['item_id'],
                          ondelete='CASCADE')
    op.drop_constraint('generic_item_item_id_fkey', 'generic_item', type_='foreignkey')
    op.create_foreign_key('generic_item_item_id_fkey', 'generic_item', 'item', ['item_id'], ['item_id'],
                          ondelete='CASCADE')
    op.drop_constraint('twitter_item_item_id_fkey', 'twitter_item', type_='foreignkey')
    op.create_foreign_key('twitter_item_item_id_fkey', 'twitter_item', 'item', ['item_id'], ['item_id'],
                          ondelete='CASCADE')


def downgrade():
    op.drop_constraint('twitter_item_item_id_fkey', 'twitter_item', type_='foreignkey')
    op.create_foreign_key('twitter_item_item_id_fkey', 'twitter_item', 'item', ['item_id'], ['item_id'])
    op.drop_constraint('generic_item_item_id_fkey', 'generic_item', type_='foreignkey')
    op.create_foreign_key('generic_item_item_id_fkey', 'generic_item', 'item', ['item_id'], ['item_id'])
    op.drop_constraint('academic_item_item_id_fkey', 'academic_item', type_='foreignkey')
    op.create_foreign_key('academic_item_item_id_fkey', 'academic_item', 'item', ['item_id'], ['item_id'])
