"""add news

Revision ID: f66404ac3cd0
Revises: 53d573bcffcb
Create Date: 2024-09-02 20:21:13.076177

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'f66404ac3cd0'
down_revision = '53d573bcffcb'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('user', sa.Column('setting_newsletter', sa.Boolean(),
                                    server_default=sa.text('false'), nullable=False))


def downgrade():
    op.drop_column('user', 'setting_newsletter')
