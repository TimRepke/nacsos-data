"""add move pipeline id

Revision ID: fc601d622855
Revises: ac3dc3438b8c
Create Date: 2024-10-17 19:41:05.218393

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'fc601d622855'
down_revision = 'ac3dc3438b8c'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('import_revision', sa.Column('pipeline_task_id', sa.String(), nullable=True))
    op.execute('''
    UPDATE import_revision
    SET pipeline_task_id = import.pipeline_task_id
    FROM import
    WHERE import.import_id = import_revision.import_id AND import_revision.import_revision_counter = 1
    ''')

    op.drop_index('ix_import_pipeline_task_id', table_name='import')
    op.drop_column('import', 'pipeline_task_id')


def downgrade():
    op.drop_column('import_revision', 'pipeline_task_id')
    op.add_column('import', sa.Column('pipeline_task_id', sa.VARCHAR(), autoincrement=False, nullable=True))
    op.create_index('ix_import_pipeline_task_id', 'import', ['pipeline_task_id'], unique=False)
