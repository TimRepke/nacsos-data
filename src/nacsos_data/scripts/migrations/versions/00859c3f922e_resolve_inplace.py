"""resolve inplace

Revision ID: 00859c3f922e
Revises: 222054e4b214
Create Date: 2024-02-07 14:35:51.863384

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '00859c3f922e'
down_revision = '222054e4b214'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('snippet',
                    sa.Column('snippet_id', sa.UUID(), nullable=False),
                    sa.Column('item_id', sa.UUID(), nullable=False),
                    sa.Column('offset_start', sa.Integer(), nullable=False),
                    sa.Column('offset_stop', sa.Integer(), nullable=False),
                    sa.Column('snippet', sa.String(), nullable=False),
                    sa.ForeignKeyConstraint(['item_id'], ['item.item_id'], ondelete='CASCADE'),
                    sa.PrimaryKeyConstraint('snippet_id')
                    )
    op.create_index(op.f('ix_snippet_item_id'), 'snippet', ['item_id'], unique=False)
    op.create_index(op.f('ix_snippet_snippet_id'), 'snippet', ['snippet_id'], unique=True)
    op.add_column('annotation', sa.Column('snippet_id', sa.UUID(), nullable=True))
    op.create_index(op.f('ix_annotation_snippet_id'), 'annotation', ['snippet_id'], unique=False)
    op.create_foreign_key(None, 'annotation', 'snippet', ['snippet_id'], ['snippet_id'])
    op.drop_column('annotation', 'text_offset_stop')
    op.drop_column('annotation', 'text_offset_start')
    op.add_column('annotation_quality', sa.Column('label_value', sa.Integer(), nullable=True))
    op.add_column('annotation_quality', sa.Column('multi_overlap_mean', sa.Float(), nullable=True))
    op.add_column('annotation_quality', sa.Column('multi_overlap_median', sa.Float(), nullable=True))
    op.add_column('annotation_quality', sa.Column('multi_overlap_std', sa.Float(), nullable=True))
    op.alter_column('annotation_quality', 'label_key',
                    existing_type=sa.VARCHAR(),
                    nullable=True)
    op.drop_constraint('annotation_quality_assignment_scope_id_label_path_key_user__key', 'annotation_quality',
                       type_='unique')
    op.create_unique_constraint(None, 'annotation_quality',
                                ['assignment_scope_id', 'label_key', 'label_value', 'user_base', 'user_target'])
    op.drop_column('annotation_quality', 'label_path')
    op.drop_column('annotation_quality', 'label_path_key')


def downgrade():
    op.add_column('annotation_quality', sa.Column('label_path_key', sa.VARCHAR(), autoincrement=False, nullable=False))
    op.add_column('annotation_quality',
                  sa.Column('label_path', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True))
    op.drop_constraint(None, 'annotation_quality', type_='unique')
    op.create_unique_constraint('annotation_quality_assignment_scope_id_label_path_key_user__key', 'annotation_quality',
                                ['assignment_scope_id', 'label_path_key', 'user_base', 'user_target'])
    op.alter_column('annotation_quality', 'label_key',
                    existing_type=sa.VARCHAR(),
                    nullable=False)
    op.drop_column('annotation_quality', 'multi_overlap_std')
    op.drop_column('annotation_quality', 'multi_overlap_median')
    op.drop_column('annotation_quality', 'multi_overlap_mean')
    op.drop_column('annotation_quality', 'label_value')
    op.add_column('annotation', sa.Column('text_offset_start', sa.INTEGER(), autoincrement=False, nullable=True))
    op.add_column('annotation', sa.Column('text_offset_stop', sa.INTEGER(), autoincrement=False, nullable=True))
    op.drop_constraint(None, 'annotation', type_='foreignkey')
    op.drop_index(op.f('ix_annotation_snippet_id'), table_name='annotation')
    op.drop_column('annotation', 'snippet_id')
    op.drop_index(op.f('ix_snippet_snippet_id'), table_name='snippet')
    op.drop_index(op.f('ix_snippet_item_id'), table_name='snippet')
    op.drop_table('snippet')
