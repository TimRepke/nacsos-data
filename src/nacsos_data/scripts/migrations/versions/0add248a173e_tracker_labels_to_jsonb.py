"""tracker labels to jsonb

Revision ID: 0add248a173e
Revises: 395f96022ce9
Create Date: 2023-10-02 14:58:23.420774

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '0add248a173e'
down_revision = '395f96022ce9'
branch_labels = None
depends_on = None


def upgrade():
    op.execute('ALTER TABLE annotation_tracker ALTER COLUMN labels TYPE JSONB USING (labels::text::jsonb);')
    op.execute('ALTER TABLE annotation_tracker ALTER COLUMN recall TYPE JSONB USING (recall::text::jsonb);')


def downgrade():
    pass
    # FIXME no downgrading here
