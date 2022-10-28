"""change bitannotationkind

Revision ID: 734c0532d595
Revises: f05e86f4be30
Create Date: 2022-10-28 15:08:20.338667

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '734c0532d595'
down_revision = 'f05e86f4be30'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TYPE botkind RENAME VALUE 'CONSOLIDATE' TO 'RESOLVE';")


def downgrade():
    op.execute("ALTER TYPE botkind RENAME VALUE 'RESOLVE' TO 'CONSOLIDATE';")
