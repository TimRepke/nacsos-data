"""add parent to annotation

Revision ID: df4d9483f6fe
Revises: 0edb14e3883a
Create Date: 2022-06-30 19:36:11.226250

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'df4d9483f6fe'
down_revision = '0edb14e3883a'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint('annotation_unique_assignment_id_key_repeat_tuple', 'annotation', type_='unique')
    op.drop_column('annotation', 'parent_repeat')

    op.add_column('annotation', sa.Column('parent', postgresql.UUID(), nullable=True))
    op.create_index(op.f('ix_annotation_parent'), 'annotation', ['parent'], unique=False)
    op.create_foreign_key(constraint_name='annotation_annotation_id_fkey',
                          source_table='annotation',
                          referent_table='annotation',
                          local_cols=['parent'],
                          remote_cols=['annotation_id'])

    op.create_unique_constraint('annotation_unique_assignment_id_key_repeat_tuple',
                                'annotation',
                                ['assignment_id', 'key', 'parent', 'repeat'])


def downgrade():
    op.drop_constraint('annotation_annotation_id_fkey', 'annotation', type_='foreignkey')
    op.drop_index(op.f('ix_annotation_parent'), table_name='annotation')
    op.create_unique_constraint('annotation_unique_assignment_id_key_repeat_tuple', 'annotation',
                                ['assignment_id', 'key', 'parent_repeat', 'repeat'])
    op.drop_column('annotation', 'parent')

    op.add_column('annotation', sa.Column('parent_repeat', sa.Integer(), nullable=False))
    op.drop_constraint('annotation_unique_assignment_id_key_repeat_tuple',
                       'annotation', type_='unique')
    op.create_unique_constraint('annotation_unique_assignment_id_key_repeat_tuple',
                                'annotation',
                                ['assignment_id', 'key', 'parent_repeat', 'repeat'])
