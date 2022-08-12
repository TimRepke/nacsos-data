"""add annotations to tweet item

Revision ID: 99a0647bb486
Revises: 7410761c53c7
Create Date: 2022-08-12 15:44:58.023556

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '99a0647bb486'
down_revision = '7410761c53c7'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('twitter_item', sa.Column('annotations', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.drop_index('ix_twitter_item_twitter_id', table_name='twitter_item')
    op.create_index(op.f('ix_twitter_item_twitter_id'), 'twitter_item', ['twitter_id'], unique=False)


def downgrade():
    op.drop_index(op.f('ix_twitter_item_twitter_id'), table_name='twitter_item')
    op.create_index('ix_twitter_item_twitter_id', 'twitter_item', ['twitter_id'], unique=False)
    op.drop_column('twitter_item', 'annotations')
