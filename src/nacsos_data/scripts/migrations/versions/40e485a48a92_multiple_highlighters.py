"""empty message

Revision ID: 40e485a48a92
Revises: 759bd9e10fae
Create Date: 2023-04-19 15:15:50.726157

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '40e485a48a92'
down_revision = '759bd9e10fae'
branch_labels = None
depends_on = None


def upgrade():
    # create new column
    op.add_column('assignment_scope', sa.Column('highlighter_ids', postgresql.ARRAY(sa.UUID()), nullable=True))
    # transfer data
    op.execute('UPDATE assignment_scope SET highlighter_ids = ARRAY[highlighter_id] WHERE highlighter_id IS NOT NULL;')
    # drop old column
    op.drop_constraint('assignment_scope_highlighter_id_fkey', 'assignment_scope', type_='foreignkey')
    op.drop_column('assignment_scope', 'highlighter_id')

    # some follow-up update
    op.alter_column('highlighters', 'name', existing_type=sa.VARCHAR(), nullable=False)


def downgrade():
    op.alter_column('highlighters', 'name', existing_type=sa.VARCHAR(), nullable=True)
    op.add_column('assignment_scope', sa.Column('highlighter_id', sa.UUID(), autoincrement=False, nullable=True))
    op.create_foreign_key('assignment_scope_highlighter_id_fkey', 'assignment_scope', 'highlighters',
                          ['highlighter_id'], ['highlighter_id'])
    op.execute('UPDATE assignment_scope SET highlighter_id = highlighter_ids[0] WHERE highlighter_ids IS NOT NULL;')
    op.drop_column('assignment_scope', 'highlighter_ids')
