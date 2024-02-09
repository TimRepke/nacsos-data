"""float to int

Revision ID: dc8ec86fa262
Revises: 29ee854289d4
Create Date: 2024-02-09 21:08:05.387044

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'dc8ec86fa262'
down_revision = '29ee854289d4'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('annotation_quality', 'num_items',
                    existing_type=sa.INTEGER(),
                    type_=sa.Float(),
                    existing_nullable=True)
    op.alter_column('annotation_quality', 'num_overlap',
                    existing_type=sa.INTEGER(),
                    type_=sa.Float(),
                    existing_nullable=True)
    op.alter_column('annotation_quality', 'num_agree',
                    existing_type=sa.INTEGER(),
                    type_=sa.Float(),
                    existing_nullable=True)
    op.alter_column('annotation_quality', 'num_disagree',
                    existing_type=sa.INTEGER(),
                    type_=sa.Float(),
                    existing_nullable=True)


def downgrade():
    op.alter_column('annotation_quality', 'num_disagree',
                    existing_type=sa.Float(),
                    type_=sa.INTEGER(),
                    existing_nullable=True)
    op.alter_column('annotation_quality', 'num_agree',
                    existing_type=sa.Float(),
                    type_=sa.INTEGER(),
                    existing_nullable=True)
    op.alter_column('annotation_quality', 'num_overlap',
                    existing_type=sa.Float(),
                    type_=sa.INTEGER(),
                    existing_nullable=True)
    op.alter_column('annotation_quality', 'num_items',
                    existing_type=sa.Float(),
                    type_=sa.INTEGER(),
                    existing_nullable=True)
