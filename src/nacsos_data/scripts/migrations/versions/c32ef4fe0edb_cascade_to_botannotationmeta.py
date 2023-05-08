"""cascade to botannotationmeta

Revision ID: c32ef4fe0edb
Revises: b1a5be48fda7
Create Date: 2023-05-08 13:13:33.486488

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c32ef4fe0edb'
down_revision = 'b1a5be48fda7'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint('bot_annotation_metadata_annotation_scheme_id_fkey', 'bot_annotation_metadata', type_='foreignkey')
    op.create_foreign_key(None, 'bot_annotation_metadata', 'annotation_scheme', ['annotation_scheme_id'], ['annotation_scheme_id'], ondelete='CASCADE')


def downgrade():
    op.drop_constraint(None, 'bot_annotation_metadata', type_='foreignkey')
    op.create_foreign_key('bot_annotation_metadata_annotation_scheme_id_fkey', 'bot_annotation_metadata', 'annotation_scheme', ['annotation_scheme_id'], ['annotation_scheme_id'])
