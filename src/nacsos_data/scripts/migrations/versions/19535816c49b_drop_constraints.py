"""drop constraints

Revision ID: 19535816c49b
Revises: e6dd13917943
Create Date: 2023-05-11 13:42:03.612961

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '19535816c49b'
down_revision = 'e6dd13917943'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint('academic_item_doi_project_id_key', 'academic_item', type_='unique')
    op.drop_constraint('academic_item_openalex_id_project_id_key', 'academic_item', type_='unique')


def downgrade():
    op.create_unique_constraint('academic_item_openalex_id_project_id_key', 'academic_item', ['openalex_id', 'project_id'])
    op.create_unique_constraint('academic_item_doi_project_id_key', 'academic_item', ['doi', 'project_id'])
