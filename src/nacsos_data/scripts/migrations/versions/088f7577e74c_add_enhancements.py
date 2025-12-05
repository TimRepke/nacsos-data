"""add enhancements

Revision ID: 088f7577e74c
Revises: b58d4f0b2b3c
Create Date: 2025-12-05 18:09:34.374939

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '088f7577e74c'
down_revision = 'b58d4f0b2b3c'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('enhancement',
                    sa.Column('enhancement_id', sa.UUID(), nullable=False),
                    sa.Column('item_id', sa.UUID(), nullable=False),
                    sa.Column('time_created', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
                    sa.Column('key', sa.String(), nullable=False),
                    sa.Column('payload', postgresql.JSONB(none_as_null=True, astext_type=sa.Text()), nullable=True),
                    sa.ForeignKeyConstraint(['item_id'], ['item.item_id'], ondelete='CASCADE'),
                    sa.PrimaryKeyConstraint('enhancement_id'),
                    )
    op.create_index(op.f('ix_enhancement_enhancement_id'), 'enhancement', ['enhancement_id'], unique=True)
    op.create_index(op.f('ix_enhancement_item_id'), 'enhancement', ['item_id'], unique=False)
    op.create_index(op.f('ix_enhancement_key'), 'enhancement', ['key'], unique=False)


def downgrade():
    op.drop_index(op.f('ix_enhancement_key'), table_name='enhancement')
    op.drop_index(op.f('ix_enhancement_item_id'), table_name='enhancement')
    op.drop_index(op.f('ix_enhancement_enhancement_id'), table_name='enhancement')
    op.drop_table('enhancement')
