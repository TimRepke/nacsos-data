"""rename query to imports

Revision ID: da16cf5cfd96
Revises: 9f5ab90ded04
Create Date: 2022-07-21 18:28:47.496746

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'da16cf5cfd96'
down_revision = '9f5ab90ded04'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('project_permissions', 'queries_read', new_column_name='imports_read')
    op.alter_column('project_permissions', 'queries_edit', new_column_name='imports_edit')


def downgrade():
    op.alter_column('project_permissions', 'imports_read', new_column_name='queries_read')
    op.alter_column('project_permissions', 'imports_edit', new_column_name='queries_edit')
