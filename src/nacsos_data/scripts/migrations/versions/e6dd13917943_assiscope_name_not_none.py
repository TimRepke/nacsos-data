"""assiscope name not none


Revision ID: e6dd13917943
Revises: 07e7e605fc9f
Create Date: 2023-05-11 13:35:02.806748

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e6dd13917943'
down_revision = '07e7e605fc9f'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('assignment_scope', 'description',
               existing_type=sa.VARCHAR(),
               nullable=False)


def downgrade():
    op.alter_column('assignment_scope', 'description',
               existing_type=sa.VARCHAR(),
               nullable=True)