"""merge academic branches

Revision ID: 2c270c20fa65
Revises: cce19f8cf077, 731be1ea0af7
Create Date: 2022-12-14 17:14:58.889611

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2c270c20fa65'
down_revision = ('cce19f8cf077', '731be1ea0af7')
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
