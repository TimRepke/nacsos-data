"""add unique parent

Revision ID: cce19f8cf077
Revises: a8a67609c622
Create Date: 2022-11-16 20:18:47.573320

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'cce19f8cf077'
down_revision = 'a8a67609c622'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint('bot_annotation_bot_annotation_metadata_id_item_id_key_repea_key', 'bot_annotation',
                       type_='unique')
    op.create_unique_constraint(None, 'bot_annotation',
                                ['bot_annotation_metadata_id', 'item_id', 'parent', 'key', 'repeat'])


def downgrade():
    op.drop_constraint(None, 'bot_annotation', type_='unique')
    op.create_unique_constraint('bot_annotation_bot_annotation_metadata_id_item_id_key_repea_key', 'bot_annotation',
                                ['bot_annotation_metadata_id', 'item_id', 'key', 'repeat'])
