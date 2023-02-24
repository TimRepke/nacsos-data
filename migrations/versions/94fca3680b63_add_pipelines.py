"""add pipelines

Revision ID: 94fca3680b63
Revises: 235b87457c39
Create Date: 2023-02-24 17:02:03.327109

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '94fca3680b63'
down_revision = '235b87457c39'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('tasks',
                    sa.Column('task_id', sa.UUID(), nullable=False),
                    sa.Column('user_id', sa.UUID(), nullable=True),
                    sa.Column('project_id', sa.UUID(), nullable=False),
                    sa.Column('fingerprint', sa.String(), nullable=False),
                    sa.Column('function_name', sa.String(), nullable=False),
                    sa.Column('dependencies', sa.ARRAY(sa.UUID()), nullable=True),
                    sa.Column('status',
                              sa.Enum('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED', name='taskstatus'),
                              server_default='PENDING', nullable=False),
                    sa.Column('location', sa.Enum('LOCAL', 'PIK', name='executionlocation'), server_default='LOCAL',
                              nullable=False),
                    sa.Column('params', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
                    sa.Column('comment', sa.String(), nullable=True),
                    sa.Column('time_created', sa.DateTime(timezone=True), server_default=sa.text('now()'),
                              nullable=True),
                    sa.Column('time_started', sa.DateTime(timezone=True), nullable=True),
                    sa.Column('time_finished', sa.DateTime(timezone=True), nullable=True),
                    sa.Column('est_runtime', sa.Integer(), nullable=True),
                    sa.Column('est_memory', sa.Integer(), nullable=True),
                    sa.Column('est_cpu_load',
                              sa.Enum('VHIGH', 'HIGH', 'MEDIUM', 'LOW', 'MINIMAL', name='cpuloadclassification'),
                              server_default='MEDIUM', nullable=False),
                    sa.Column('rec_expunge', sa.DateTime(timezone=True), nullable=True),
                    sa.ForeignKeyConstraint(['project_id'], ['project.project_id'], ),
                    sa.ForeignKeyConstraint(['user_id'], ['user.user_id'], ),
                    sa.PrimaryKeyConstraint('task_id')
                    )
    op.create_index(op.f('ix_tasks_fingerprint'), 'tasks', ['fingerprint'], unique=False)
    op.create_index(op.f('ix_tasks_function_name'), 'tasks', ['function_name'], unique=False)
    op.create_index(op.f('ix_tasks_project_id'), 'tasks', ['project_id'], unique=False)
    op.create_index(op.f('ix_tasks_task_id'), 'tasks', ['task_id'], unique=True)
    op.create_index(op.f('ix_tasks_user_id'), 'tasks', ['user_id'], unique=False)


def downgrade():
    op.drop_index(op.f('ix_tasks_user_id'), table_name='tasks')
    op.drop_index(op.f('ix_tasks_task_id'), table_name='tasks')
    op.drop_index(op.f('ix_tasks_project_id'), table_name='tasks')
    op.drop_index(op.f('ix_tasks_function_name'), table_name='tasks')
    op.drop_index(op.f('ix_tasks_fingerprint'), table_name='tasks')
    op.drop_table('tasks')
