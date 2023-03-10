"""drop title slug constraint

Revision ID: 97ba1f870753
Revises: 2776318df1b7
Create Date: 2023-03-10 19:51:38.451367

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '97ba1f870753'
down_revision = '2776318df1b7'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint('academic_item_title_slug_project_id_key', 'academic_item', type_='unique')


def downgrade():
    op.create_unique_constraint('academic_item_title_slug_project_id_key', 'academic_item',
                                ['title_slug', 'project_id'])
