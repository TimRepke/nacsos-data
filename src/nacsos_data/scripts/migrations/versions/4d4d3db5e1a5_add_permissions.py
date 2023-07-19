"""add_permissions

Revision ID: 4d4d3db5e1a5
Revises: d4673c319106
Create Date: 2023-07-19 17:06:00.385207

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = '4d4d3db5e1a5'
down_revision = 'd4673c319106'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('academic_item', sa.Column('dimensions_id', sa.String(), nullable=True))
    op.create_index(op.f('ix_academic_item_dimensions_id'), 'academic_item', ['dimensions_id'], unique=False)
    op.create_unique_constraint(None, 'academic_item', ['dimensions_id', 'project_id'])

    op.add_column('academic_item_variant', sa.Column('dimensions_id', sa.String(), nullable=True))
    op.create_unique_constraint(None, 'academic_item_variant', ['item_id', 'dimensions_id'])

    op.add_column('project_permissions', sa.Column('search_dimensions', sa.Boolean(),
                                                   nullable=False, server_default=text('FALSE')))
    op.add_column('project_permissions', sa.Column('search_oa', sa.Boolean(),
                                                   nullable=False, server_default=text('FALSE')))
    op.add_column('project_permissions', sa.Column('import_limit_oa', sa.Integer(),
                                                   nullable=False, server_default=text('0')))


def downgrade():
    op.drop_column('project_permissions', 'import_limit_oa')
    op.drop_column('project_permissions', 'search_oa')
    op.drop_column('project_permissions', 'search_dimensions')

    op.drop_constraint(None, 'academic_item_variant', type_='unique')
    op.drop_column('academic_item_variant', 'dimensions_id')

    op.drop_constraint(None, 'academic_item', type_='unique')
    op.drop_index(op.f('ix_academic_item_dimensions_id'), table_name='academic_item')
    op.drop_column('academic_item', 'dimensions_id')
