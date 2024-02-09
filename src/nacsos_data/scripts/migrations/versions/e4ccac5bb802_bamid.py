"""bamid

Revision ID: e4ccac5bb802
Revises: 33c268ec1590
Create Date: 2024-02-09 22:00:17.559542

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e4ccac5bb802'
down_revision = '33c268ec1590'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('annotation_quality', 'bot_annotation_metadata_id',
               existing_type=sa.UUID(),
               nullable=True)
    op.drop_constraint('annotation_quality_assignment_scope_id_label_key_label_valu_key', 'annotation_quality', type_='unique')
    op.create_unique_constraint(None, 'annotation_quality', ['assignment_scope_id', 'bot_annotation_metadata_id', 'label_key', 'label_value', 'user_base', 'user_target'])


def downgrade():
    op.drop_constraint(None, 'annotation_quality', type_='unique')
    op.create_unique_constraint('annotation_quality_assignment_scope_id_label_key_label_valu_key', 'annotation_quality', ['assignment_scope_id', 'label_key', 'label_value', 'user_base', 'user_target'])
    op.alter_column('annotation_quality', 'bot_annotation_metadata_id',
               existing_type=sa.UUID(),
               nullable=False)
