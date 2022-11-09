"""add parent bot annotation

Revision ID: b79b1378b6f5
Revises: 5a3d878c2762
Create Date: 2022-11-08 20:11:43.801208

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b79b1378b6f5'
down_revision = '5a3d878c2762'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('bot_annotation', sa.Column('parent', sa.UUID(), nullable=True))
    op.create_index(op.f('ix_bot_annotation_parent'), 'bot_annotation', ['parent'], unique=False)
    op.create_foreign_key(None, 'bot_annotation', 'bot_annotation', ['parent'], ['bot_annotation_id'])
    op.create_check_constraint('bot_annotation_has_value', 'annotation',
                               'num_nonnulls(value_bool, value_int, value_float, value_str, multi_int) = 1')


def downgrade():
    op.drop_constraint(None, 'bot_annotation', type_='foreignkey')
    op.drop_index(op.f('ix_bot_annotation_parent'), table_name='bot_annotation')
    op.drop_column('bot_annotation', 'parent')
    op.drop_constraint('bot_annotation_has_value', 'annotation', type_='check')
