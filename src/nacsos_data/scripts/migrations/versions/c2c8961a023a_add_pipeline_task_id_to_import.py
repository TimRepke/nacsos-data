"""add pipeline_task_id to import

Revision ID: c2c8961a023a
Revises: b185f9df0492
Create Date: 2022-08-16 18:12:31.765095

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c2c8961a023a'
down_revision = 'b185f9df0492'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('import', sa.Column('pipeline_task_id', sa.String(), nullable=True))
    op.create_index(op.f('ix_import_pipeline_task_id'), 'import', ['pipeline_task_id'], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_import_pipeline_task_id'), table_name='import')
    op.drop_column('import', 'pipeline_task_id')
    # ### end Alembic commands ###
