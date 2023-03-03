"""add auth tokens

Revision ID: 2776318df1b7
Revises: 94fca3680b63
Create Date: 2023-03-03 11:08:37.542444

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '2776318df1b7'
down_revision = '94fca3680b63'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('auth_tokens',
                    sa.Column('token_id', sa.UUID(), nullable=False),
                    sa.Column('username', sa.String(), nullable=False),
                    sa.Column('valid_till', sa.DateTime(timezone=True), nullable=True),
                    sa.ForeignKeyConstraint(['username'], ['user.username'], ),
                    sa.PrimaryKeyConstraint('token_id'))
    op.create_index(op.f('ix_auth_tokens_token_id'), 'auth_tokens', ['token_id'], unique=True)
    op.create_index(op.f('ix_auth_tokens_username'), 'auth_tokens', ['username'], unique=False)

    # op.drop_constraint('user_email_key', 'user', type_='unique')
    # op.drop_constraint('user_username_key', 'user', type_='unique')
    op.create_index(op.f('ix_user_email'), 'user', ['email'], unique=True)
    op.create_index(op.f('ix_user_username'), 'user', ['username'], unique=True)


def downgrade():
    op.drop_index(op.f('ix_user_username'), table_name='user')
    op.drop_index(op.f('ix_user_email'), table_name='user')
    op.create_unique_constraint('user_username_key', 'user', ['username'])
    op.create_unique_constraint('user_email_key', 'user', ['email'])
    op.drop_index(op.f('ix_auth_tokens_username'), table_name='auth_tokens')
    op.drop_index(op.f('ix_auth_tokens_token_id'), table_name='auth_tokens')
    op.drop_table('auth_tokens')
