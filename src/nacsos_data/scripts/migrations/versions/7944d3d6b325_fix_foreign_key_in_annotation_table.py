"""fix foreign key in annotation table

Revision ID: 7944d3d6b325
Revises: df4d9483f6fe
Create Date: 2022-07-08 17:58:44.863816

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '7944d3d6b325'
down_revision = 'df4d9483f6fe'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('annotation_assignment_id_fkey', 'annotation', type_='foreignkey')
    op.create_foreign_key(None, 'annotation', 'assignment', ['assignment_id'], ['assignment_id'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'annotation', type_='foreignkey')
    op.create_foreign_key('annotation_assignment_id_fkey', 'annotation', 'annotation_task', ['assignment_id'], ['annotation_task_id'])
    # ### end Alembic commands ###
