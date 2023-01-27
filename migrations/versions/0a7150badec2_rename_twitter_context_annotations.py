"""rename twitter context annotations

Revision ID: 0a7150badec2
Revises: ce20ca479173
Create Date: 2023-01-27 13:23:08.140636

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '0a7150badec2'
down_revision = 'ce20ca479173'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('twitter_item', 'annotations', new_column_name='context_annotations')


def downgrade():
    op.alter_column('twitter_item', 'context_annotations', new_column_name="annotations")
