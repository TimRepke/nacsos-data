"""tweet project id

Revision ID: 1068ffa0b6e1
Revises: 39a909f49a4b
Create Date: 2022-11-09 17:25:07.996865

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '1068ffa0b6e1'
down_revision = '39a909f49a4b'
branch_labels = None
depends_on = None


def upgrade():
    # add explicit generic item (instead of relying on baseitem to be the generic item)
    op.create_table('generic_item',
                    sa.Column('item_id', sa.UUID(), nullable=False),
                    sa.Column('meta', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
                    sa.ForeignKeyConstraint(['item_id'], ['item.item_id'], ),
                    sa.PrimaryKeyConstraint('item_id')
                    )

    # remove M2MProjectItem
    op.drop_index('ix_m2m_project_item_item_id', table_name='m2m_project_item')
    op.drop_index('ix_m2m_project_item_project_id', table_name='m2m_project_item')
    op.drop_table('m2m_project_item')

    # add project_id reference to item (after dropping m2m_project_item in favour of 1:N relationship)
    op.add_column('item', sa.Column('project_id', sa.UUID(), nullable=False,
                                    server_default='59577b91-5d6d-4460-9074-4cf2e4bd748c'))
    op.execute("ALTER TABLE item ALTER  COLUMN project_id DROP DEFAULT;")
    op.create_index(op.f('ix_item_project_id'), 'item', ['project_id'], unique=False)
    op.create_foreign_key(None, 'item', 'project', ['project_id'], ['project_id'], ondelete='cascade')

    # rename projecttype to itemtype
    op.execute("CREATE TYPE itemtype as enum ('generic', 'twitter', 'academic', 'patents');")
    op.execute("ALTER TABLE project ALTER COLUMN type TYPE itemtype USING 'twitter'::itemtype;")
    op.execute("ALTER TABLE item ALTER  COLUMN type DROP DEFAULT;")
    op.execute("ALTER TABLE item ALTER COLUMN type TYPE itemtype USING 'twitter'::itemtype;")
    op.execute("DROP TYPE projecttype;")

    # add index on ProjectPermissions.project_id (was missing for some reason) and
    # add UniqueConstraint(project_id, twitter_id)
    op.create_index(op.f('ix_project_permissions_project_id'), 'project_permissions', ['project_id'], unique=False)
    op.create_unique_constraint(None, 'project_permissions', ['user_id', 'project_id'])

    # add uniqueness constraint within project for tweets on twitter_id
    # add required indices and (mirrored Item.project_id) FK to do so first
    op.add_column('twitter_item', sa.Column('project_id', sa.UUID(), nullable=False,
                                            server_default='59577b91-5d6d-4460-9074-4cf2e4bd748c'))
    op.execute("ALTER TABLE twitter_item ALTER  COLUMN project_id DROP DEFAULT;")
    op.drop_index('ix_twitter_item_twitter_id', table_name='twitter_item')
    op.create_index(op.f('ix_twitter_item_twitter_id'), 'twitter_item', ['twitter_id'], unique=False)
    op.create_index(op.f('ix_twitter_item_project_id'), 'twitter_item', ['project_id'], unique=False)
    op.create_unique_constraint(None, 'twitter_item', ['twitter_id', 'project_id'])
    op.create_foreign_key(None, 'twitter_item', 'project', ['project_id'], ['project_id'], ondelete='cascade')


def downgrade():
    op.drop_constraint(None, 'twitter_item', type_='foreignkey')
    op.drop_constraint(None, 'twitter_item', type_='unique')
    op.drop_index(op.f('ix_twitter_item_project_id'), table_name='twitter_item')
    op.drop_index(op.f('ix_twitter_item_twitter_id'), table_name='twitter_item')
    op.create_index('ix_twitter_item_twitter_id', 'twitter_item', ['twitter_id'], unique=False)
    op.drop_column('twitter_item', 'project_id')
    op.drop_index(op.f('ix_project_permissions_project_id'), table_name='project_permissions')
    op.drop_constraint(None, 'item', type_='foreignkey')
    op.drop_index(op.f('ix_item_project_id'), table_name='item')
    op.drop_column('item', 'project_id')
    op.create_table('m2m_project_item',
                    sa.Column('item_id', sa.UUID(), autoincrement=False, nullable=False),
                    sa.Column('project_id', sa.UUID(), autoincrement=False, nullable=False),
                    sa.ForeignKeyConstraint(['item_id'], ['item.item_id'], name='m2m_project_item_item_id_fkey'),
                    sa.ForeignKeyConstraint(['project_id'], ['project.project_id'],
                                            name='m2m_project_item_project_id_fkey'),
                    sa.PrimaryKeyConstraint('item_id', 'project_id', name='m2m_project_item_pkey')
                    )
    op.create_index('ix_m2m_project_item_project_id', 'm2m_project_item', ['project_id'], unique=False)
    op.create_index('ix_m2m_project_item_item_id', 'm2m_project_item', ['item_id'], unique=False)
    op.drop_table('generic_item')

    op.execute("CREATE TYPE projecttype as enum ('generic', 'twitter', 'academic', 'patents');")
    op.execute("ALTER TABLE project ALTER COLUMN type TYPE projecttype;")
    op.execute("ALTER TABLE item ALTER COLUMN type TYPE projecttype;")
    op.execute("DROP TYPE itemtype;")
