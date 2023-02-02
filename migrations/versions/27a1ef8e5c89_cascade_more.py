"""cascade more

Revision ID: 27a1ef8e5c89
Revises: deeb94c47382
Create Date: 2023-02-02 14:56:47.581445

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '27a1ef8e5c89'
down_revision = 'deeb94c47382'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint('assignment_assignment_scope_id_fkey', 'assignment', type_='foreignkey')
    op.create_foreign_key('assignment_assignment_scope_id_fkey',
                          'assignment', 'assignment_scope',
                          ['assignment_scope_id'], ['assignment_scope_id'],
                          ondelete='CASCADE')
    op.drop_constraint('assignment_scope_annotation_scheme_id_fkey', 'assignment_scope', type_='foreignkey')
    op.create_foreign_key('assignment_scope_annotation_scheme_id_fkey',
                          'assignment_scope', 'annotation_scheme',
                          ['annotation_scheme_id'], ['annotation_scheme_id'],
                          ondelete='CASCADE')
    op.drop_constraint('bot_annotation_bot_annotation_metadata_id_fkey', 'bot_annotation', type_='foreignkey')
    op.create_foreign_key('bot_annotation_bot_annotation_metadata_id_fkey',
                          'bot_annotation', 'bot_annotation_metadata',
                          ['bot_annotation_metadata_id'], ['bot_annotation_metadata_id'],
                          ondelete='CASCADE')
    op.create_index(op.f('ix_annotation_value_int'), 'annotation', ['value_int'], unique=False)
    op.create_index(op.f('ix_bot_annotation_value_int'), 'bot_annotation', ['value_int'], unique=False)


def downgrade():
    op.drop_constraint('bot_annotation_bot_annotation_metadata_id_fkey', 'bot_annotation', type_='foreignkey')
    op.create_foreign_key('bot_annotation_bot_annotation_metadata_id_fkey', 'bot_annotation', 'bot_annotation_metadata',
                          ['bot_annotation_metadata_id'], ['bot_annotation_metadata_id'])
    op.drop_constraint('assignment_scope_annotation_scheme_id_fkey', 'assignment_scope', type_='foreignkey')
    op.create_foreign_key('assignment_scope_annotation_scheme_id_fkey', 'assignment_scope', 'annotation_scheme',
                          ['annotation_scheme_id'], ['annotation_scheme_id'])
    op.drop_constraint('assignment_assignment_scope_id_fkey', 'assignment', type_='foreignkey')
    op.create_foreign_key('assignment_assignment_scope_id_fkey', 'assignment', 'assignment_scope',
                          ['assignment_scope_id'], ['assignment_scope_id'])
    op.drop_index(op.f('ix_annotation_value_int'), table_name='annotation')
    op.drop_index(op.f('ix_bot_annotation_value_int'), table_name='bot_annotation')
