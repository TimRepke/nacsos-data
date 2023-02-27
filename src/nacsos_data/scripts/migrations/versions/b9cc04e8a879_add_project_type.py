"""add project type

Revision ID: b9cc04e8a879
Revises: da16cf5cfd96
Create Date: 2022-08-01 15:38:21.697484

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b9cc04e8a879'
down_revision = 'da16cf5cfd96'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TYPE projecttype ADD VALUE 'basic' BEFORE 'twitter'")


def downgrade():
    op.execute("ALTER TYPE projecttye DROP ATTRIBUTE IF EXISTS 'basic'")
