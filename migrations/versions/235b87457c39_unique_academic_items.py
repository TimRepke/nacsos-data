"""unique academic items

Revision ID: 235b87457c39
Revises: d9664e1507bf
Create Date: 2023-02-17 17:46:57.942916

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '235b87457c39'
down_revision = 'd9664e1507bf'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('academic_item', sa.Column('project_id', sa.UUID(), nullable=False))
    op.create_index(op.f('ix_academic_item_item_id'), 'academic_item', ['item_id'], unique=True)
    op.create_index(op.f('ix_academic_item_project_id'), 'academic_item', ['project_id'], unique=False)
    op.create_unique_constraint('academic_item_scopus_id_project_id_key', 'academic_item', ['scopus_id', 'project_id'])
    op.create_unique_constraint('academic_item_s2_id_project_id_key', 'academic_item', ['s2_id', 'project_id'])
    op.create_unique_constraint('academic_item_doi_project_id_key', 'academic_item', ['doi', 'project_id'])
    op.create_unique_constraint('academic_item_openalex_id_project_id_key', 'academic_item',
                                ['openalex_id', 'project_id'])
    op.create_unique_constraint('academic_item_pubmed_id_project_id_key', 'academic_item', ['pubmed_id', 'project_id'])
    op.create_unique_constraint('academic_item_wos_id_project_id_key', 'academic_item', ['wos_id', 'project_id'])
    op.create_unique_constraint('academic_item_title_slug_project_id_key', 'academic_item',
                                ['title_slug', 'project_id'])
    op.create_foreign_key('academic_item_project_id_fkey', 'academic_item', 'project', ['project_id'], ['project_id'],
                          ondelete='cascade')
    op.create_index(op.f('ix_generic_item_item_id'), 'generic_item', ['item_id'], unique=True)


def downgrade():
    op.drop_index(op.f('ix_generic_item_item_id'), table_name='generic_item')
    op.drop_constraint('academic_item_project_id_fkey', 'academic_item', type_='foreignkey')
    op.drop_constraint('academic_item_title_slug_project_id_key', 'academic_item', type_='unique')
    op.drop_constraint('academic_item_wos_id_project_id_key', 'academic_item', type_='unique')
    op.drop_constraint('academic_item_scopus_id_project_id_key', 'academic_item', type_='unique')
    op.drop_constraint('academic_item_pubmed_id_project_id_key', 'academic_item', type_='unique')
    op.drop_constraint('academic_item_openalex_id_project_id_key', 'academic_item', type_='unique')
    op.drop_constraint('academic_item_doi_project_id_key', 'academic_item', type_='unique')
    op.drop_constraint('academic_item_s2_id_project_id_key', 'academic_item', type_='unique')
    op.drop_index(op.f('ix_academic_item_project_id'), table_name='academic_item')
    op.drop_index(op.f('ix_academic_item_item_id'), table_name='academic_item')
    op.drop_column('academic_item', 'project_id')
