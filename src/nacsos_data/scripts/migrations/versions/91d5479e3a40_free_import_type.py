"""free import type

Revision ID: 91d5479e3a40
Revises: 4d4d3db5e1a5
Create Date: 2023-08-17 16:18:09.440229

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '91d5479e3a40'
down_revision = '4d4d3db5e1a5'
branch_labels = None
depends_on = None


def upgrade():
    op.execute('ALTER TABLE import ALTER COLUMN type TYPE varchar USING type::text;')
    op.execute('DROP TYPE IF EXISTS importtype;')


def downgrade():
    op.execute("CREATE TYPE importtype AS enum ("
               "'ris', 'csv', 'jsonl', 'wos', 'scopus', 'ebsco', "
               "'jstor', 'ovid', 'pop', 'twitter', 'script');")
    op.execute('ALTER TABLE import ALTER COLUMN type TYPE importtype USING type::text::importtype;')
