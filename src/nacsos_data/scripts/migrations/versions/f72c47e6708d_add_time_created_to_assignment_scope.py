"""add time_created to assignment scope

Revision ID: f72c47e6708d
Revises: 7944d3d6b325
Create Date: 2022-07-11 19:08:14.590281

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'f72c47e6708d'
down_revision = '7944d3d6b325'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('assignment_scope',
                  sa.Column('time_created', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True))


def downgrade():
    op.drop_column('assignment_scope', 'time_created')
