"""simplify task

Revision ID: 33fe72326e3a
Revises: 8ce7bb134e00
Create Date: 2024-04-17 19:17:00.501661

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '33fe72326e3a'
down_revision = '8ce7bb134e00'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('tasks', sa.Column('message_id', sa.String(), nullable=True))
    op.drop_column('tasks', 'location')
    op.drop_column('tasks', 'dependencies')
    op.drop_column('tasks', 'est_runtime')
    op.drop_column('tasks', 'est_cpu_load')
    op.drop_column('tasks', 'est_memory')


def downgrade():
    op.add_column('tasks', sa.Column('est_memory', sa.INTEGER(), autoincrement=False, nullable=True))
    op.add_column('tasks', sa.Column('est_cpu_load', postgresql.ENUM('VHIGH', 'HIGH', 'MEDIUM', 'LOW', 'MINIMAL', name='cpuloadclassification'), server_default=sa.text("'MEDIUM'::cpuloadclassification"), autoincrement=False, nullable=False))
    op.add_column('tasks', sa.Column('est_runtime', sa.INTEGER(), autoincrement=False, nullable=True))
    op.add_column('tasks', sa.Column('dependencies', postgresql.ARRAY(sa.UUID()), autoincrement=False, nullable=True))
    op.add_column('tasks', sa.Column('location', postgresql.ENUM('LOCAL', 'PIK', name='executionlocation'), server_default=sa.text("'LOCAL'::executionlocation"), autoincrement=False, nullable=False))
    op.drop_column('tasks', 'message_id')
