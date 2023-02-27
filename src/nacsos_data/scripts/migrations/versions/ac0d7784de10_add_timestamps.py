"""add timestamps

Revision ID: ac0d7784de10
Revises: 71ed1752cdae
Create Date: 2022-11-14 11:08:49.405225

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ac0d7784de10'
down_revision = '71ed1752cdae'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('bot_annotation_metadata', sa.Column('time_created', sa.DateTime(timezone=True),
                                                       server_default=sa.text('now()'), nullable=True))
    op.add_column('bot_annotation_metadata', sa.Column('time_updated', sa.DateTime(timezone=True),
                                                       nullable=True))


def downgrade():
    op.drop_column('bot_annotation_metadata', 'time_updated')
    op.drop_column('bot_annotation_metadata', 'time_created')
