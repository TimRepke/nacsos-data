"""prio

Revision ID: 67c3e5f99233
Revises: fabb66a2156b
Create Date: 2024-11-06 17:39:25.642671

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '67c3e5f99233'
down_revision = 'fabb66a2156b'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('priorities',
                    sa.Column('priority_id', sa.UUID(), nullable=False),
                    sa.Column('project_id', sa.UUID(), nullable=False),
                    sa.Column('name', sa.String(), nullable=False),
                    sa.Column('time_created', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
                    sa.Column('time_started', sa.DateTime(timezone=True), nullable=True),
                    sa.Column('time_ready', sa.DateTime(timezone=True), nullable=True),
                    sa.Column('time_assigned', sa.DateTime(timezone=True), nullable=True),
                    sa.Column('source_scopes', postgresql.ARRAY(sa.UUID()), nullable=False),
                    sa.Column('nql', sa.String(), nullable=True),
                    sa.Column('incl_rule', sa.String(), nullable=False),
                    sa.Column('incl_field', sa.String(), nullable=False),
                    sa.Column('incl_pred_field', sa.String(), nullable=False),
                    sa.Column('train_split', sa.Float(), nullable=False),
                    sa.Column('n_predictions', sa.Integer(), nullable=False),
                    sa.Column('config', postgresql.JSONB(astext_type=sa.Text(), none_as_null=True), nullable=False),
                    sa.Column('prioritised_ids', postgresql.ARRAY(sa.UUID()), nullable=True),
                    sa.ForeignKeyConstraint(['project_id'], ['project.project_id'], ondelete='CASCADE'),
                    sa.PrimaryKeyConstraint('priority_id')
                    )
    op.create_index(op.f('ix_priorities_priority_id'), 'priorities', ['priority_id'], unique=True)
    op.create_index(op.f('ix_priorities_project_id'), 'priorities', ['project_id'], unique=False)


def downgrade():
    op.drop_index(op.f('ix_priorities_project_id'), table_name='priorities')
    op.drop_index(op.f('ix_priorities_priority_id'), table_name='priorities')
    op.drop_table('priorities')
