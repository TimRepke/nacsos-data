"""annotation_tracker

Revision ID: 395f96022ce9
Revises: d9e0aae9c7e6
Create Date: 2023-09-26 13:18:15.852630

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '395f96022ce9'
down_revision = 'd9e0aae9c7e6'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('annotation_tracker',
                    sa.Column('annotation_tracking_id', sa.UUID(), nullable=False),
                    sa.Column('project_id', sa.UUID(), nullable=False),
                    sa.Column('name', sa.String(), nullable=False),
                    sa.Column('inclusion_rule', sa.String(), nullable=False),
                    sa.Column('majority', sa.Boolean(), nullable=False),
                    sa.Column('n_items_total', sa.Integer(), nullable=False),
                    sa.Column('recall_target', sa.Float(), nullable=False),
                    sa.Column('source_ids', sa.ARRAY(sa.UUID()), nullable=True),
                    sa.Column('labels', sa.ARRAY(sa.Integer()), nullable=True),
                    sa.Column('recall', sa.ARRAY(sa.Float()), nullable=True),
                    sa.Column('buscar',
                              postgresql.JSONB(none_as_null=True, astext_type=sa.Text()), nullable=True),
                    sa.Column('time_created',
                              sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
                    sa.Column('time_updated', sa.DateTime(timezone=True), nullable=True),
                    sa.ForeignKeyConstraint(['project_id'], ['project.project_id'], ondelete='CASCADE'),
                    sa.PrimaryKeyConstraint('annotation_tracking_id')
                    )
    op.create_index(op.f('ix_annotation_tracker_annotation_tracking_id'), 'annotation_tracker',
                    ['annotation_tracking_id'], unique=True)
    op.create_index(op.f('ix_annotation_tracker_project_id'), 'annotation_tracker', ['project_id'], unique=False)


def downgrade():
    op.drop_index(op.f('ix_annotation_tracker_project_id'), table_name='annotation_tracker')
    op.drop_index(op.f('ix_annotation_tracker_annotation_tracking_id'), table_name='annotation_tracker')
    op.drop_table('annotation_tracker')
