"""add order to assignments

Revision ID: 6b1c390079fc
Revises: c54713d59de5
Create Date: 2022-06-03 13:41:28.054981

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '6b1c390079fc'
down_revision = 'c54713d59de5'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('assignment', sa.Column('order', sa.Integer(), sa.Identity(always=False), nullable=False))
    op.alter_column('assignment', 'assignment_scope_id',
                    existing_type=postgresql.UUID(),
                    nullable=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('assignment', 'assignment_scope_id',
                    existing_type=postgresql.UUID(),
                    nullable=True)
    op.drop_column('assignment', 'order')
    # ### end Alembic commands ###