"""empty pw

Revision ID: 222054e4b214
Revises: f36c9c3403b4
Create Date: 2023-11-01 15:40:56.247919

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '222054e4b214'
down_revision = 'f36c9c3403b4'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('user', 'password',
                    existing_type=sa.VARCHAR(),
                    nullable=True)


def downgrade():
    op.alter_column('user', 'password',
                    existing_type=sa.VARCHAR(),
                    nullable=False)