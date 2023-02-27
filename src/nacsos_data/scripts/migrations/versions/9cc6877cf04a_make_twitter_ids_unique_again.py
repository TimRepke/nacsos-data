"""make twitter ids unique again

Revision ID: 9cc6877cf04a
Revises: 99a0647bb486
Create Date: 2022-08-12 18:32:28.879389

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9cc6877cf04a'
down_revision = '99a0647bb486'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_index('ix_twitter_item_twitter_id', table_name='twitter_item')
    op.create_index(op.f('ix_twitter_item_twitter_id'), 'twitter_item', ['twitter_id'], unique=True)
    op.create_foreign_key(None, 'twitter_item', 'item', ['item_id'], ['item_id'])


def downgrade():
    op.drop_constraint(None, 'twitter_item', type_='foreignkey')
    op.drop_index(op.f('ix_twitter_item_twitter_id'), table_name='twitter_item')
    op.create_index('ix_twitter_item_twitter_id', 'twitter_item', ['twitter_id'], unique=False)
