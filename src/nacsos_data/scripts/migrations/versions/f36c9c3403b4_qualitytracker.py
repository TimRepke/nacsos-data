"""qualitytracker

Revision ID: f36c9c3403b4
Revises: b5d6e2a0e7ca
Create Date: 2023-10-26 21:52:34.664107

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'f36c9c3403b4'
down_revision = 'b5d6e2a0e7ca'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('annotation_quality',
                    sa.Column('annotation_quality_id', sa.UUID(), nullable=False),
                    sa.Column('project_id', sa.UUID(), nullable=False),
                    sa.Column('assignment_scope_id', sa.UUID(), nullable=False),
                    sa.Column('user_base', sa.UUID(), nullable=True),
                    sa.Column('annotations_base', postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
                              nullable=True),
                    sa.Column('user_target', sa.UUID(), nullable=True),
                    sa.Column('annotations_target', postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
                              nullable=True),
                    sa.Column('label_path_key', sa.String(), nullable=False),
                    sa.Column('label_path', postgresql.JSONB(none_as_null=True, astext_type=sa.Text()), nullable=True),
                    sa.Column('label_key', sa.String(), nullable=False),
                    sa.Column('cohen', sa.Float(), nullable=True),
                    sa.Column('fleiss', sa.Float(), nullable=True),
                    sa.Column('randolph', sa.Float(), nullable=True),
                    sa.Column('krippendorff', sa.Float(), nullable=True),
                    sa.Column('pearson', sa.Float(), nullable=True),
                    sa.Column('pearson_p', sa.Float(), nullable=True),
                    sa.Column('kendall', sa.Float(), nullable=True),
                    sa.Column('kendall_p', sa.Float(), nullable=True),
                    sa.Column('spearman', sa.Float(), nullable=True),
                    sa.Column('spearman_p', sa.Float(), nullable=True),
                    sa.Column('num_items', sa.Integer(), nullable=True),
                    sa.Column('num_overlap', sa.Integer(), nullable=True),
                    sa.Column('num_agree', sa.Integer(), nullable=True),
                    sa.Column('num_disagree', sa.Integer(), nullable=True),
                    sa.Column('time_created', sa.DateTime(timezone=True), server_default=sa.text('now()'),
                              nullable=False),
                    sa.Column('time_updated', sa.DateTime(timezone=True), nullable=True),
                    sa.ForeignKeyConstraint(['assignment_scope_id'], ['assignment_scope.assignment_scope_id'],
                                            ondelete='CASCADE'),
                    sa.ForeignKeyConstraint(['project_id'], ['project.project_id'], ondelete='CASCADE'),
                    sa.ForeignKeyConstraint(['user_base'], ['user.user_id'], ondelete='CASCADE'),
                    sa.ForeignKeyConstraint(['user_target'], ['user.user_id'], ondelete='CASCADE'),
                    sa.PrimaryKeyConstraint('annotation_quality_id'),
                    sa.UniqueConstraint('assignment_scope_id', 'label_path_key', 'user_base', 'user_target')
                    )
    op.create_index(op.f('ix_annotation_quality_annotation_quality_id'), 'annotation_quality',
                    ['annotation_quality_id'], unique=True)
    op.create_index(op.f('ix_annotation_quality_assignment_scope_id'), 'annotation_quality', ['assignment_scope_id'],
                    unique=False)
    op.create_index(op.f('ix_annotation_quality_project_id'), 'annotation_quality', ['project_id'], unique=False)


def downgrade():
    op.drop_index(op.f('ix_annotation_quality_project_id'), table_name='annotation_quality')
    op.drop_index(op.f('ix_annotation_quality_assignment_scope_id'), table_name='annotation_quality')
    op.drop_index(op.f('ix_annotation_quality_annotation_quality_id'), table_name='annotation_quality')
    op.drop_table('annotation_quality')
