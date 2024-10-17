"""add import revisions

Revision ID: c5fc02b9d0bb
Revises: f66404ac3cd0
Create Date: 2024-10-16 18:01:00.105852

"""
import uuid

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'c5fc02b9d0bb'
down_revision = 'f66404ac3cd0'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_column('m2m_import_item', 'time_created')

    op.create_table('import_revision',
                    sa.Column('import_revision_id', sa.UUID(), nullable=False),
                    sa.Column('import_revision_counter', sa.Integer(), nullable=False),
                    sa.Column('time_created', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
                    sa.Column('import_id', sa.UUID(), nullable=False),
                    sa.Column('num_items_retrieved', sa.Integer(), nullable=True),
                    sa.Column('num_items', sa.Integer(), nullable=True),
                    sa.Column('num_items_new', sa.Integer(), nullable=True),
                    sa.Column('num_items_updated', sa.Integer(), nullable=True),
                    sa.Column('num_items_removed', sa.Integer(), nullable=True),
                    sa.ForeignKeyConstraint(['import_id'], ['import.import_id'], ),
                    sa.PrimaryKeyConstraint('import_revision_id')
                    )
    op.create_index(op.f('ix_import_revision_import_id'), 'import_revision', ['import_id'], unique=False)
    op.create_index(op.f('ix_import_revision_import_revision_id'), 'import_revision', ['import_revision_id'], unique=True)
    op.create_unique_constraint('import_revision_import_id_import_revision_id_key', 'import_revision', ['import_id', 'import_revision_counter'])

    # For each existing import, create revision 1
    conn = op.get_bind()
    imports = conn.execute(sa.text('SELECT * FROM import;')).mappings().all()
    for imp in imports:
        num_items = conn.execute(sa.text('SELECT count(1) FROM m2m_import_item WHERE import_id = :import_id;'),
                                 {'import_id': imp['import_id']}).scalar()

        conn.execute(sa.text('INSERT INTO import_revision ('
                             '   import_revision_id, import_revision_counter, import_id, num_items, '
                             '   num_items_new, num_items_updated, num_items_removed) '
                             'VALUES (:import_revision_id, :import_revision_counter, :import_id, :num_items, '
                             '        :num_items_new, :num_items_updated, :num_items_removed);'),
                     {
                         'import_revision_id': uuid.uuid4(),
                         'import_revision_counter': 1,
                         'import_id': imp['import_id'],
                         'num_items': num_items,
                         # 'num_items_retrieved': num_items,
                         'num_items_new': num_items,
                         'num_items_updated': 0,
                         'num_items_removed': 0
                     })

    op.add_column('m2m_import_item', sa.Column('first_revision', sa.Integer(), nullable=False, server_default='1'))
    op.add_column('m2m_import_item', sa.Column('latest_revision', sa.Integer(), nullable=False, server_default='1'))
    op.create_foreign_key(None, 'm2m_import_item', 'import_revision', ['import_id', 'latest_revision'],
                          ['import_id', 'import_revision_counter'])

#  ALTER TABLE import_revision ADD CONSTRAINT import_revision_import_id_import_revision_id_key UNIQUE (import_id, import_revision_id)
def downgrade():
    op.add_column('m2m_import_item',
                  sa.Column('time_created', postgresql.TIMESTAMP(timezone=True), server_default=sa.text('now()'), autoincrement=False,
                            nullable=True))
    op.drop_constraint(None, 'm2m_import_item', type_='foreignkey')
    op.drop_column('m2m_import_item', 'latest_revision')
    op.drop_column('m2m_import_item', 'first_revision')

    op.drop_constraint('import_revision_import_id_import_revision_id_key', 'import_revision', type_='unique')
    op.drop_index(op.f('ix_import_revision_import_revision_id'), table_name='import_revision')
    op.drop_index(op.f('ix_import_revision_import_id'), table_name='import_revision')
    op.drop_table('import_revision')
