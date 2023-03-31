"""add highlighter

Revision ID: 6e1c9891aeef
Revises: 97ba1f870753
Create Date: 2023-03-31 14:57:50.186202

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '6e1c9891aeef'
down_revision = '97ba1f870753'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('highlighters',
                    sa.Column('highlighter_id', sa.UUID(), nullable=False),
                    sa.Column('project_id', sa.UUID(), nullable=False),
                    sa.Column('name', sa.String(), nullable=False),
                    sa.Column('keywords', postgresql.ARRAY(sa.String()), nullable=False),
                    sa.Column('style', sa.String(), nullable=True),
                    sa.ForeignKeyConstraint(['project_id'], ['project.project_id'], ),
                    sa.PrimaryKeyConstraint('highlighter_id')
                    )
    op.create_index(op.f('ix_highlighters_highlighter_id'), 'highlighters', ['highlighter_id'], unique=True)
    op.create_index(op.f('ix_highlighters_project_id'), 'highlighters', ['project_id'], unique=False)
    op.add_column('assignment_scope', sa.Column('highlighter_id', sa.UUID(), nullable=True))
    op.create_foreign_key(None, 'assignment_scope', 'highlighters', ['highlighter_id'], ['highlighter_id'])


def downgrade():
    op.drop_constraint(None, 'assignment_scope', type_='foreignkey')
    op.drop_column('assignment_scope', 'highlighter_id')
    op.drop_index(op.f('ix_highlighters_project_id'), table_name='highlighters')
    op.drop_index(op.f('ix_highlighters_highlighter_id'), table_name='highlighters')
    op.drop_table('highlighters')
