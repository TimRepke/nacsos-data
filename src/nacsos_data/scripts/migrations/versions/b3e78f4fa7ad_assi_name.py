"""assi name

Revision ID: b3e78f4fa7ad
Revises: fa064d66ee02
Create Date: 2023-05-12 16:37:21.384289

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b3e78f4fa7ad'
down_revision = 'fa064d66ee02'
branch_labels = None
depends_on = None


def upgrade():
    # it's fine to just drop it, because the column is empty at this point
    op.add_column('bot_annotation_metadata', sa.Column('assignment_scope_id', sa.UUID(), nullable=True))
    op.drop_index('ix_bot_annotation_metadata_annotation_scope_id', table_name='bot_annotation_metadata')
    op.create_index(op.f('ix_bot_annotation_metadata_assignment_scope_id'), 'bot_annotation_metadata', ['assignment_scope_id'], unique=False)
    op.drop_constraint('bot_annotation_metadata_annotation_scope_id_fkey', 'bot_annotation_metadata', type_='foreignkey')
    op.create_foreign_key(None, 'bot_annotation_metadata', 'assignment_scope', ['assignment_scope_id'], ['assignment_scope_id'])
    op.drop_column('bot_annotation_metadata', 'annotation_scope_id')


def downgrade():
    op.add_column('bot_annotation_metadata', sa.Column('annotation_scope_id', sa.UUID(), autoincrement=False, nullable=True))
    op.drop_constraint(None, 'bot_annotation_metadata', type_='foreignkey')
    op.create_foreign_key('bot_annotation_metadata_annotation_scope_id_fkey', 'bot_annotation_metadata', 'assignment_scope', ['annotation_scope_id'], ['assignment_scope_id'])
    op.drop_index(op.f('ix_bot_annotation_metadata_assignment_scope_id'), table_name='bot_annotation_metadata')
    op.create_index('ix_bot_annotation_metadata_annotation_scope_id', 'bot_annotation_metadata', ['annotation_scope_id'], unique=False)
    op.drop_column('bot_annotation_metadata', 'assignment_scope_id')
