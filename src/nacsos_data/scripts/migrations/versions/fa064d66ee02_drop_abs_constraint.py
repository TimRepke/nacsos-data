"""drop abs constraint

Revision ID: fa064d66ee02
Revises: ce92a3af897b
Create Date: 2023-05-11 21:55:49.644690

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'fa064d66ee02'
down_revision = 'ce92a3af897b'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint('academic_item_variant_item_id_abstract_key', 'academic_item_variant', type_='unique')


def downgrade():
    op.create_unique_constraint('academic_item_variant_item_id_abstract_key', 'academic_item_variant',
                                ['item_id', 'abstract'])
