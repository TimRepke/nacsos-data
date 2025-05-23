"""Initial

Revision ID: 77b486e09680
Revises: 
Create Date: 2022-05-13 18:07:28.299161

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '77b486e09680'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('item',
                    sa.Column('item_id', postgresql.UUID(as_uuid=True), nullable=False),
                    sa.Column('text', sa.String(), nullable=False),
                    sa.PrimaryKeyConstraint('item_id')
                    )
    op.create_index(op.f('ix_item_item_id'), 'item', ['item_id'], unique=True)
    op.create_table('project',
                    sa.Column('project_id', postgresql.UUID(as_uuid=True), nullable=False),
                    sa.Column('name', sa.String(), nullable=False),
                    sa.Column('description', sa.String(), nullable=True),
                    sa.PrimaryKeyConstraint('project_id'),
                    sa.UniqueConstraint('name')
                    )
    op.create_index(op.f('ix_project_project_id'), 'project', ['project_id'], unique=True)
    op.create_table('user',
                    sa.Column('user_id', postgresql.UUID(as_uuid=True), nullable=False),
                    sa.Column('username', sa.String(), nullable=False),
                    sa.Column('email', sa.String(), nullable=False),
                    sa.Column('full_name', sa.String(), nullable=False),
                    sa.Column('affiliation', sa.String(), nullable=True),
                    sa.Column('password', sa.String(), nullable=False),
                    sa.Column('is_superuser', sa.Boolean(), nullable=False),
                    sa.Column('is_active', sa.Boolean(), nullable=False),
                    sa.PrimaryKeyConstraint('user_id'),
                    sa.UniqueConstraint('email'),
                    sa.UniqueConstraint('username')
                    )
    op.create_index(op.f('ix_user_user_id'), 'user', ['user_id'], unique=True)
    op.create_table('annotation_task',
                    sa.Column('annotation_task_id', postgresql.UUID(as_uuid=True), nullable=False),
                    sa.Column('project_id', postgresql.UUID(as_uuid=True), nullable=False),
                    sa.Column('name', sa.String(), nullable=False),
                    sa.Column('description', sa.String(), nullable=True),
                    sa.Column('labels', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
                    sa.ForeignKeyConstraint(['project_id'], ['project.project_id'], ),
                    sa.PrimaryKeyConstraint('annotation_task_id')
                    )
    op.create_index(op.f('ix_annotation_task_annotation_task_id'), 'annotation_task', ['annotation_task_id'],
                    unique=True)
    op.create_table('project_permissions',
                    sa.Column('project_permission_id', postgresql.UUID(as_uuid=True), nullable=False),
                    sa.Column('project_id', postgresql.UUID(as_uuid=True), nullable=False),
                    sa.Column('user_id', postgresql.UUID(as_uuid=True), nullable=False),
                    sa.Column('owner', sa.Boolean(), nullable=False),
                    sa.Column('dataset_read', sa.Boolean(), nullable=False),
                    sa.Column('dataset_edit', sa.Boolean(), nullable=False),
                    sa.Column('queries_read', sa.Boolean(), nullable=False),
                    sa.Column('queries_edit', sa.Boolean(), nullable=False),
                    sa.Column('annotations_read', sa.Boolean(), nullable=False),
                    sa.Column('annotations_edit', sa.Boolean(), nullable=False),
                    sa.Column('pipelines_read', sa.Boolean(), nullable=False),
                    sa.Column('pipelines_edit', sa.Boolean(), nullable=False),
                    sa.Column('artefacts_read', sa.Boolean(), nullable=False),
                    sa.Column('artefacts_edit', sa.Boolean(), nullable=False),
                    sa.ForeignKeyConstraint(['project_id'], ['project.project_id'], ),
                    sa.ForeignKeyConstraint(['user_id'], ['user.user_id'], ),
                    sa.PrimaryKeyConstraint('project_permission_id')
                    )
    op.create_index(op.f('ix_project_permissions_project_permission_id'), 'project_permissions',
                    ['project_permission_id'], unique=True)
    op.create_index(op.f('ix_project_permissions_user_id'), 'project_permissions', ['user_id'], unique=False)
    op.create_table('annotation',
                    sa.Column('annotation_id', postgresql.UUID(as_uuid=True), nullable=False),
                    sa.Column('time_created', sa.DateTime(timezone=True), server_default=sa.text('now()'),
                              nullable=True),
                    sa.Column('time_updated', sa.DateTime(timezone=True), nullable=True),
                    sa.Column('assignment_id', postgresql.UUID(as_uuid=True), nullable=False),
                    sa.Column('user_id', postgresql.UUID(as_uuid=True), nullable=False),
                    sa.Column('item_id', postgresql.UUID(as_uuid=True), nullable=False),
                    sa.Column('task_id', postgresql.UUID(as_uuid=True), nullable=False),
                    sa.Column('key', sa.String(), nullable=False),
                    sa.Column('repeat', sa.Integer(), nullable=False),
                    sa.Column('value_bool', sa.Boolean(), nullable=True),
                    sa.Column('value_int', sa.Integer(), nullable=True),
                    sa.Column('value_float', sa.Float(), nullable=True),
                    sa.Column('value_str', sa.String(), nullable=True),
                    sa.Column('text_offset_start', sa.Integer(), nullable=True),
                    sa.Column('text_offset_stop', sa.Integer(), nullable=True),
                    sa.ForeignKeyConstraint(['assignment_id'], ['annotation_task.annotation_task_id'], ),
                    sa.ForeignKeyConstraint(['item_id'], ['item.item_id'], ),
                    sa.ForeignKeyConstraint(['task_id'], ['annotation_task.annotation_task_id'], ),
                    sa.ForeignKeyConstraint(['user_id'], ['user.user_id'], ),
                    sa.PrimaryKeyConstraint('annotation_id')
                    )
    op.create_index(op.f('ix_annotation_annotation_id'), 'annotation', ['annotation_id'], unique=True)
    op.create_index(op.f('ix_annotation_assignment_id'), 'annotation', ['assignment_id'], unique=False)
    op.create_index(op.f('ix_annotation_item_id'), 'annotation', ['item_id'], unique=False)
    op.create_index(op.f('ix_annotation_task_id'), 'annotation', ['task_id'], unique=False)
    op.create_index(op.f('ix_annotation_user_id'), 'annotation', ['user_id'], unique=False)
    op.create_table('assignment_scope',
                    sa.Column('assignment_scope_id', postgresql.UUID(as_uuid=True), nullable=False),
                    sa.Column('task_id', postgresql.UUID(as_uuid=True), nullable=False),
                    sa.Column('name', sa.String(), nullable=False),
                    sa.Column('description', sa.String(), nullable=True),
                    sa.ForeignKeyConstraint(['task_id'], ['annotation_task.annotation_task_id'], ),
                    sa.PrimaryKeyConstraint('assignment_scope_id')
                    )
    op.create_index(op.f('ix_assignment_scope_assignment_scope_id'), 'assignment_scope', ['assignment_scope_id'],
                    unique=True)
    op.create_index(op.f('ix_assignment_scope_task_id'), 'assignment_scope', ['task_id'], unique=False)
    op.create_table('assignment',
                    sa.Column('assignment_id', postgresql.UUID(as_uuid=True), nullable=False),
                    sa.Column('user_id', postgresql.UUID(as_uuid=True), nullable=False),
                    sa.Column('item_id', postgresql.UUID(as_uuid=True), nullable=False),
                    sa.Column('task_id', postgresql.UUID(as_uuid=True), nullable=False),
                    sa.Column('assignment_scope_id', postgresql.UUID(as_uuid=True), nullable=True),
                    sa.ForeignKeyConstraint(['assignment_scope_id'], ['assignment_scope.assignment_scope_id'], ),
                    sa.ForeignKeyConstraint(['item_id'], ['item.item_id'], ),
                    sa.ForeignKeyConstraint(['task_id'], ['annotation_task.annotation_task_id'], ),
                    sa.ForeignKeyConstraint(['user_id'], ['user.user_id'], ),
                    sa.PrimaryKeyConstraint('assignment_id')
                    )
    op.create_index(op.f('ix_assignment_assignment_id'), 'assignment', ['assignment_id'], unique=True)
    op.create_index(op.f('ix_assignment_assignment_scope_id'), 'assignment', ['assignment_scope_id'], unique=False)
    op.create_index(op.f('ix_assignment_item_id'), 'assignment', ['item_id'], unique=False)
    op.create_index(op.f('ix_assignment_task_id'), 'assignment', ['task_id'], unique=False)
    op.create_index(op.f('ix_assignment_user_id'), 'assignment', ['user_id'], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_assignment_user_id'), table_name='assignment')
    op.drop_index(op.f('ix_assignment_task_id'), table_name='assignment')
    op.drop_index(op.f('ix_assignment_item_id'), table_name='assignment')
    op.drop_index(op.f('ix_assignment_assignment_scope_id'), table_name='assignment')
    op.drop_index(op.f('ix_assignment_assignment_id'), table_name='assignment')
    op.drop_table('assignment')
    op.drop_index(op.f('ix_assignment_scope_task_id'), table_name='assignment_scope')
    op.drop_index(op.f('ix_assignment_scope_assignment_scope_id'), table_name='assignment_scope')
    op.drop_table('assignment_scope')
    op.drop_index(op.f('ix_annotation_user_id'), table_name='annotation')
    op.drop_index(op.f('ix_annotation_task_id'), table_name='annotation')
    op.drop_index(op.f('ix_annotation_item_id'), table_name='annotation')
    op.drop_index(op.f('ix_annotation_assignment_id'), table_name='annotation')
    op.drop_index(op.f('ix_annotation_annotation_id'), table_name='annotation')
    op.drop_table('annotation')
    op.drop_index(op.f('ix_project_permissions_user_id'), table_name='project_permissions')
    op.drop_index(op.f('ix_project_permissions_project_permission_id'), table_name='project_permissions')
    op.drop_table('project_permissions')
    op.drop_index(op.f('ix_annotation_task_annotation_task_id'), table_name='annotation_task')
    op.drop_table('annotation_task')
    op.drop_index(op.f('ix_user_user_id'), table_name='user')
    op.drop_table('user')
    op.drop_index(op.f('ix_project_project_id'), table_name='project')
    op.drop_table('project')
    op.drop_index(op.f('ix_item_item_id'), table_name='item')
    op.drop_table('item')
    # ### end Alembic commands ###
