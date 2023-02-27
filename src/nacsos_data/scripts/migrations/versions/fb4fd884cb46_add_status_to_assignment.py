"""add status to assignment

Revision ID: fb4fd884cb46
Revises: 6b1c390079fc
Create Date: 2022-06-29 21:35:27.243267

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'fb4fd884cb46'
down_revision = '6b1c390079fc'
branch_labels = None
depends_on = None

FULL: 'FULL'  # This assignment was fully and correctly fulfilled
PARTIAL: 'PARTIAL'  # This assignment was partially fulfilled
OPEN: 'OPEN'  # This assignment was not attempted
INVALID: 'INVALID'  # Something does not comply with the annotation scheme and is thus invalid


def upgrade():
    op.execute("CREATE TYPE assignmentstatus AS ENUM('FULL', 'PARTIAL', 'OPEN', 'INVALID')")
    op.add_column('assignment',
                  sa.Column('status', sa.Enum('FULL', 'PARTIAL', 'OPEN', 'INVALID', name='assignmentstatus'),
                            nullable=False,
                            server_default='OPEN'))
    # ### end Alembic commands ###


def downgrade():
    op.drop_column('assignment', 'status')
    op.execute('DROP TYPE assignmentstatus')
    # ### end Alembic commands ###
