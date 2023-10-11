"""lexisnexis

Revision ID: 1f57c6f42055
Revises: 0add248a173e
Create Date: 2023-10-11 18:01:25.912377

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '1f57c6f42055'
down_revision = '0add248a173e'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('lexis_item',
                    sa.Column('item_id', sa.UUID(), nullable=False),
                    sa.Column('project_id', sa.UUID(), nullable=False),
                    sa.Column('teaser', sa.String(), nullable=True),
                    sa.Column('authors', sa.ARRAY(sa.String()), nullable=True),
                    sa.ForeignKeyConstraint(['item_id'], ['item.item_id'], ondelete='CASCADE'),
                    sa.ForeignKeyConstraint(['project_id'], ['project.project_id'], ondelete='cascade'),
                    sa.PrimaryKeyConstraint('item_id')
                    )
    op.create_index(op.f('ix_lexis_item_item_id'), 'lexis_item', ['item_id'], unique=True)
    op.create_index(op.f('ix_lexis_item_project_id'), 'lexis_item', ['project_id'], unique=False)

    op.create_table('lexis_item_source',
                    sa.Column('item_source_id', sa.UUID(), nullable=False),
                    sa.Column('item_id', sa.UUID(), nullable=False),
                    sa.Column('lexis_id', sa.String(), nullable=False),
                    sa.Column('name', sa.String(), nullable=True),
                    sa.Column('title', sa.String(), nullable=True),
                    sa.Column('section', sa.String(), nullable=True),
                    sa.Column('jurisdiction', sa.String(), nullable=True),
                    sa.Column('location', sa.String(), nullable=True),
                    sa.Column('content_type', sa.String(), nullable=True),
                    sa.Column('published_at', sa.DateTime(timezone=True), nullable=True),
                    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
                    sa.Column('meta', postgresql.JSONB(none_as_null=True, astext_type=sa.Text()), nullable=True),
                    sa.ForeignKeyConstraint(['item_id'], ['lexis_item.item_id'], ondelete='CASCADE'),
                    sa.PrimaryKeyConstraint('item_source_id'),
                    sa.UniqueConstraint('lexis_id', 'item_id')
                    )
    op.create_index(op.f('ix_lexis_item_source_item_id'), 'lexis_item_source', ['item_id'], unique=False)
    op.create_index(op.f('ix_lexis_item_source_item_source_id'), 'lexis_item_source', ['item_source_id'], unique=True)
    op.create_index(op.f('ix_lexis_item_source_lexis_id'), 'lexis_item_source', ['lexis_id'], unique=False)

    op.drop_column('bot_annotation', 'order')
    op.add_column('bot_annotation',
                  sa.Column('order', sa.Integer(), sa.Identity(always=False), nullable=False))
    # op.alter_column('bot_annotation', 'order',
    #                 existing_type=sa.INTEGER(),
    #                 existing_nullable=True,
    #                 server_default=sa.Identity(always=False))
    op.create_index(op.f('ix_bot_annotation_key'), 'bot_annotation', ['key'], unique=False)

    op.create_index(op.f('ix_annotation_key'), 'annotation', ['key'], unique=False)


def downgrade():
    op.drop_index(op.f('ix_bot_annotation_key'), table_name='bot_annotation')
    op.alter_column('bot_annotation', 'order',
                    existing_type=sa.INTEGER(),
                    server_default=None,
                    existing_nullable=True)
    op.drop_index(op.f('ix_annotation_key'), table_name='annotation')
    op.drop_index(op.f('ix_lexis_item_source_lexis_id'), table_name='lexis_item_source')
    op.drop_index(op.f('ix_lexis_item_source_item_source_id'), table_name='lexis_item_source')
    op.drop_index(op.f('ix_lexis_item_source_item_id'), table_name='lexis_item_source')
    op.drop_table('lexis_item_source')
    op.drop_index(op.f('ix_lexis_item_project_id'), table_name='lexis_item')
    op.drop_index(op.f('ix_lexis_item_item_id'), table_name='lexis_item')
    op.drop_table('lexis_item')
