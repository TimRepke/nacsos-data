"""rename annotation task to scheme

Revision ID: 7410761c53c7
Revises: b9cc04e8a879
Create Date: 2022-08-12 11:28:39.051640

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '7410761c53c7'
down_revision = 'b9cc04e8a879'
branch_labels = None
depends_on = None


def upgrade():
    # drop all constraints and indices first
    op.drop_constraint('annotation_task_id_fkey', 'annotation', type_='foreignkey')
    op.drop_index('ix_annotation_task_id', table_name='annotation')
    op.drop_constraint('assignment_task_id_fkey', 'assignment', type_='foreignkey')
    op.drop_index('ix_assignment_task_id', table_name='assignment')
    op.drop_constraint('assignment_scope_task_id_fkey', 'assignment_scope', type_='foreignkey')
    op.drop_index('ix_assignment_scope_task_id', table_name='assignment_scope')
    op.drop_constraint('annotation_task_pkey', 'annotation_task')
    op.drop_index('ix_annotation_task_annotation_task_id', table_name='annotation_task')

    # rename the table and column
    op.rename_table('annotation_task', 'annotation_scheme')
    op.alter_column('annotation_scheme', 'annotation_task_id', new_column_name='annotation_scheme_id')

    # rename pkey column and update immediate indices
    op.create_index(op.f('ix_annotation_scheme_annotation_scheme_id'),
                    'annotation_scheme', ['annotation_scheme_id'], unique=True)
    op.create_primary_key('annotation_scheme_pkey', 'annotation_scheme', ['annotation_scheme_id'])

    # rename and adapt references in annotation table
    op.alter_column('annotation', 'task_id', new_column_name='annotation_scheme_id')
    op.create_index(op.f('ix_annotation_annotation_scheme_id'), 'annotation', ['annotation_scheme_id'], unique=False)
    op.create_foreign_key(None, 'annotation', 'annotation_scheme', ['annotation_scheme_id'], ['annotation_scheme_id'])

    # rename and adapt reference in assignment table
    op.alter_column('assignment', 'task_id', new_column_name='annotation_scheme_id')
    op.create_index(op.f('ix_assignment_annotation_scheme_id'), 'assignment', ['annotation_scheme_id'], unique=False)
    op.create_foreign_key(None, 'assignment', 'annotation_scheme', ['annotation_scheme_id'], ['annotation_scheme_id'])

    # rename and adapt reference in assignment_scope table
    op.alter_column('assignment_scope', 'task_id', new_column_name='annotation_scheme_id')
    op.create_index(op.f('ix_assignment_scope_annotation_scheme_id'),
                    'assignment_scope', ['annotation_scheme_id'], unique=False)
    op.create_foreign_key(None, 'assignment_scope', 'annotation_scheme',
                          ['annotation_scheme_id'], ['annotation_scheme_id'])


def downgrade():
    op.drop_constraint('annotation_annotation_scheme_id_fkey', 'annotation', type_='foreignkey')
    op.drop_index('ix_annotation_annotation_scheme_id', table_name='annotation')
    op.drop_constraint('assignment_annotation_scheme_id_fkey', 'assignment', type_='foreignkey')
    op.drop_index('ix_assignment_annotation_scheme_id', table_name='assignment')
    op.drop_constraint('assignment_scope_annotation_scheme_id_fkey', 'assignment_scope', type_='foreignkey')
    op.drop_index('ix_assignment_scope_annotation_scheme_id', table_name='assignment_scope')
    op.drop_constraint('annotation_scheme_pkey', 'annotation_scheme')
    op.drop_index('ix_annotation_scheme_annotation_scheme_id', table_name='annotation_scheme')

    # rename the table
    op.rename_table('annotation_scheme', 'annotation_task')

    # rename pkey column and update immediate indices
    op.alter_column('annotation_task', 'annotation_scheme_id', new_column_name='annotation_task_id')
    op.create_index(op.f('ix_annotation_task_annotation_task_id'),
                    'annotation_task', ['annotation_task_id'], unique=True)
    op.create_primary_key('annotation_task_pkey', 'annotation_task', ['annotation_task_id'])

    # rename and adapt reference in assignment_scope table
    op.alter_column('assignment_scope', 'annotation_scheme_id', new_column_name='task_id')
    op.create_index(op.f('ix_assignment_scope_task_id'),
                    'assignment_scope', ['task_id'], unique=False)
    op.create_foreign_key(None, 'assignment_scope', 'annotation_task', ['task_id'], ['annotation_task_id'])

    # rename and adapt reference in assignment table
    op.alter_column('assignment', 'annotation_scheme_id', new_column_name='task_id')
    op.create_index(op.f('ix_assignment_task_id'), 'assignment', ['task_id'], unique=False)
    op.create_foreign_key(None, 'assignment', 'annotation_task', ['task_id'], ['annotation_task_id'])

    # rename and adapt references in annotation table
    op.alter_column('annotation', 'annotation_scheme_id', new_column_name='task_id')
    op.create_index(op.f('ix_annotation_task_id'), 'annotation', ['task_id'], unique=False)
    op.create_foreign_key(None, 'annotation', 'annotation_task', ['task_id'], ['annotation_task_id'])
