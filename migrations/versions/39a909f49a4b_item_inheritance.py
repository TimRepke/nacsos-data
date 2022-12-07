"""item inheritance

Revision ID: 39a909f49a4b
Revises: b79b1378b6f5
Create Date: 2022-11-09 13:42:24.737694

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '39a909f49a4b'
down_revision = 'b79b1378b6f5'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('item', sa.Column('type', sa.Enum('generic', 'twitter', 'academic', 'patents', name='projecttype'),
                                    nullable=False, server_default='twitter'))
    op.alter_column('item', 'type', server_default=None)
    op.drop_column('item', 'meta')
    op.drop_column('twitter_item', 'status')
    op.execute("ALTER TYPE projecttype RENAME VALUE 'basic' TO 'generic';")


def downgrade():
    op.add_column('twitter_item', sa.Column('status', sa.VARCHAR(), autoincrement=False, nullable=False))
    op.add_column('item', sa.Column('meta', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True))
    op.drop_column('item', 'type')
    op.execute("ALTER TYPE projecttype RENAME VALUE 'generic' TO 'basic';")
