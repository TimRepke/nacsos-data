"""datetimes

Revision ID: d737a2c48c0b
Revises: c32ef4fe0edb
Create Date: 2023-05-08 20:00:36.564538

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd737a2c48c0b'
down_revision = 'c32ef4fe0edb'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('annotation_scheme', sa.Column('time_created', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False))
    op.add_column('annotation_scheme', sa.Column('time_updated', sa.DateTime(timezone=True), nullable=True))
    op.add_column('auth_tokens', sa.Column('time_created', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False))
    op.add_column('auth_tokens', sa.Column('time_updated', sa.DateTime(timezone=True), nullable=True))
    op.add_column('project', sa.Column('time_created', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False))
    op.add_column('user', sa.Column('time_created', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False))
    op.add_column('user', sa.Column('time_updated', sa.DateTime(timezone=True), nullable=True))


def downgrade():
    op.drop_column('user', 'time_updated')
    op.drop_column('user', 'time_created')
    op.drop_column('project', 'time_created')
    op.drop_column('auth_tokens', 'time_updated')
    op.drop_column('auth_tokens', 'time_created')
    op.drop_column('annotation_scheme', 'time_updated')
    op.drop_column('annotation_scheme', 'time_created')
