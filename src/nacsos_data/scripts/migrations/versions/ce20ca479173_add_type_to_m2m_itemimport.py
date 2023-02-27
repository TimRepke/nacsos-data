"""add type to m2m_itemimport

Revision ID: ce20ca479173
Revises: a986dfc199b8
Create Date: 2022-12-20 14:31:08.312600

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ce20ca479173'
down_revision = 'a986dfc199b8'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE TYPE m2mimportitemtype as enum ('explicit', 'implicit');")
    op.add_column('m2m_import_item', sa.Column('type', sa.Enum('explicit', 'implicit', name='m2mimportitemtype'),
                                               server_default='explicit', nullable=False))


def downgrade():
    op.drop_column('m2m_import_item', 'type')
