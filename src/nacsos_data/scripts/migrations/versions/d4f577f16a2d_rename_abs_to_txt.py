"""rename abs to txt

Revision ID: d4f577f16a2d
Revises: fc601d622855
Create Date: 2024-10-17 23:14:30.341724

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd4f577f16a2d'
down_revision = 'fc601d622855'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('academic_item_variant', 'abstract', new_column_name='text')


def downgrade():
    op.alter_column('academic_item_variant', 'text', new_column_name='abstract')
