"""add variants

Revision ID: a90b9a7e0aff
Revises: 19535816c49b
Create Date: 2023-05-11 16:17:51.784049

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'a90b9a7e0aff'
down_revision = '19535816c49b'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('academic_item_variant',
    sa.Column('item_variant_id', sa.UUID(), nullable=False),
    sa.Column('item_id', sa.UUID(), nullable=False),
    sa.Column('import_id', sa.UUID(), nullable=True),
    sa.Column('doi', sa.String(), nullable=True),
    sa.Column('wos_id', sa.String(), nullable=True),
    sa.Column('scopus_id', sa.String(), nullable=True),
    sa.Column('openalex_id', sa.String(), nullable=True),
    sa.Column('s2_id', sa.String(), nullable=True),
    sa.Column('pubmed_id', sa.String(), nullable=True),
    sa.Column('title', sa.String(), nullable=True),
    sa.Column('publication_year', sa.Integer(), nullable=True),
    sa.Column('source', sa.String(), nullable=True),
    sa.Column('keywords', postgresql.JSONB(none_as_null=True, astext_type=sa.Text()), nullable=True),
    sa.Column('authors', postgresql.JSONB(none_as_null=True, astext_type=sa.Text()), nullable=True),
    sa.Column('abstract', sa.String(), nullable=True),
    sa.Column('meta', postgresql.JSONB(none_as_null=True, astext_type=sa.Text()), nullable=True),
    sa.ForeignKeyConstraint(['import_id'], ['academic_item.item_id'], ),
    sa.ForeignKeyConstraint(['item_id'], ['academic_item.item_id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('item_variant_id'),
    sa.UniqueConstraint('item_id', 'abstract'),
    sa.UniqueConstraint('item_id', 'doi'),
    sa.UniqueConstraint('item_id', 'openalex_id'),
    sa.UniqueConstraint('item_id', 'publication_year'),
    sa.UniqueConstraint('item_id', 'pubmed_id'),
    sa.UniqueConstraint('item_id', 's2_id'),
    sa.UniqueConstraint('item_id', 'scopus_id'),
    sa.UniqueConstraint('item_id', 'source'),
    sa.UniqueConstraint('item_id', 'title'),
    sa.UniqueConstraint('item_id', 'wos_id')
    )
    op.create_index(op.f('ix_academic_item_variant_item_id'), 'academic_item_variant', ['item_id'], unique=False)
    op.create_index(op.f('ix_academic_item_variant_item_variant_id'), 'academic_item_variant', ['item_variant_id'], unique=True)


def downgrade():
    op.drop_index(op.f('ix_academic_item_variant_item_variant_id'), table_name='academic_item_variant')
    op.drop_index(op.f('ix_academic_item_variant_item_id'), table_name='academic_item_variant')
    op.drop_table('academic_item_variant')