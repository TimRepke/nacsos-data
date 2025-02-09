"""cascade annotations, assignments

Revision ID: 99c9410f5dda
Revises: 0a7150badec2
Create Date: 2023-01-27 13:55:31.562707

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '99c9410f5dda'
down_revision = '0a7150badec2'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint('annotation_item_id_fkey', 'annotation', type_='foreignkey')
    op.create_foreign_key('annotation_item_id_fkey',
                          'annotation', 'item',
                          ['item_id'], ['item_id'],
                          ondelete='CASCADE')
    op.drop_constraint('assignment_item_id_fkey', 'assignment', type_='foreignkey')
    op.create_foreign_key('assignment_item_id_fkey',
                          'assignment', 'item',
                          ['item_id'], ['item_id'],
                          ondelete='CASCADE')
    op.drop_constraint('bot_annotation_item_id_fkey', 'bot_annotation', type_='foreignkey')
    op.create_foreign_key('bot_annotation_item_id_fkey',
                          'bot_annotation', 'item',
                          ['item_id'], ['item_id'],
                          ondelete='CASCADE')


def downgrade():
    op.drop_constraint('assignment_item_id_fkey', 'assignment', type_='foreignkey')
    op.create_foreign_key('assignment_item_id_fkey', 'assignment', 'item', ['item_id'], ['item_id'])
    op.drop_constraint('annotation_item_id_fkey', 'annotation', type_='foreignkey')
    op.create_foreign_key('annotation_item_id_fkey', 'annotation', 'item', ['item_id'], ['item_id'])
    op.drop_constraint('bot_annotation_item_id_fkey', 'bot_annotation', type_='foreignkey')
    op.create_foreign_key('bot_annotation_item_id_fkey', 'bot_annotation', 'item', ['item_id'], ['item_id'])
