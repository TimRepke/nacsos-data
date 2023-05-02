"""add project setting

Revision ID: b1a5be48fda7
Revises: 40e485a48a92
Create Date: 2023-05-02 13:25:51.772798

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b1a5be48fda7'
down_revision = '40e485a48a92'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('project', sa.Column('setting_motivational_quotes', sa.Boolean(), server_default=sa.text('true'), nullable=False))


def downgrade():
    op.drop_column('project', 'setting_motivational_quotes')
