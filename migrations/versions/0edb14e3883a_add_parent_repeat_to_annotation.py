"""add parent_repeat to annotation

Revision ID: 0edb14e3883a
Revises: fb4fd884cb46
Create Date: 2022-06-30 18:53:50.336100

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0edb14e3883a'
down_revision = 'fb4fd884cb46'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('annotation', sa.Column('parent_repeat', sa.Integer(), nullable=False))
    op.drop_constraint('annotation_unique_assignment_id_key_repeat_tuple',
                       'annotation', type_='unique')
    op.create_unique_constraint('annotation_unique_assignment_id_key_repeat_tuple',
                                'annotation',
                                ['assignment_id', 'key', 'parent_repeat', 'repeat'])
    # ### end Alembic commands ###


def downgrade():
    op.drop_constraint('annotation_unique_assignment_id_key_repeat_tuple',
                       'annotation',
                       type_='unique')
    op.create_unique_constraint('annotation_unique_assignment_id_key_repeat_tuple',
                                'annotation',
                                ['assignment_id', 'key', 'repeat'])
    op.drop_column('annotation', 'parent_repeat')
    # ### end Alembic commands ###
