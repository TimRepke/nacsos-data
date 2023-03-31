"""more cascades

Revision ID: 759bd9e10fae
Revises: 6e1c9891aeef
Create Date: 2023-03-31 15:17:01.853304

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '759bd9e10fae'
down_revision = '6e1c9891aeef'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint('annotation_user_id_fkey', 'annotation', type_='foreignkey')
    op.drop_constraint('annotation_assignment_id_fkey', 'annotation', type_='foreignkey')
    op.drop_constraint('annotation_annotation_scheme_id_fkey', 'annotation', type_='foreignkey')
    op.drop_constraint('annotation_annotation_id_fkey', 'annotation', type_='foreignkey')
    op.create_foreign_key('annotation_annotation_id_fkey', 'annotation', 'annotation', ['parent'], ['annotation_id'],
                          ondelete='CASCADE')
    op.create_foreign_key('annotation_annotation_scheme_id_fkey', 'annotation', 'annotation_scheme',
                          ['annotation_scheme_id'], ['annotation_scheme_id'],
                          ondelete='CASCADE')
    op.create_foreign_key('annotation_user_id_fkey', 'annotation', 'user', ['user_id'], ['user_id'], ondelete='CASCADE')
    op.create_foreign_key('annotation_assignment_id_fkey', 'annotation', 'assignment', ['assignment_id'],
                          ['assignment_id'], ondelete='CASCADE')
    op.drop_constraint('annotation_task_project_id_fkey', 'annotation_scheme', type_='foreignkey')
    op.create_foreign_key('annotation_scheme_project_id_fkey', 'annotation_scheme', 'project', ['project_id'],
                          ['project_id'], ondelete='CASCADE')
    op.drop_constraint('bot_annotation_parent_fkey', 'bot_annotation', type_='foreignkey')
    op.create_foreign_key('bot_annotation_parent_fkey', 'bot_annotation', 'bot_annotation', ['parent'],
                          ['bot_annotation_id'],
                          ondelete='CASCADE')
    op.drop_constraint('bot_annotation_metadata_project_id_fkey', 'bot_annotation_metadata', type_='foreignkey')
    op.create_foreign_key('bot_annotation_metadata_project_id_fkey', 'bot_annotation_metadata', 'project',
                          ['project_id'], ['project_id'],
                          ondelete='CASCADE')
    op.drop_constraint('highlighters_project_id_fkey', 'highlighters', type_='foreignkey')
    op.create_foreign_key('highlighters_project_id_fkey', 'highlighters', 'project', ['project_id'], ['project_id'],
                          ondelete='CASCADE')
    op.drop_constraint('import_project_id_fkey', 'import', type_='foreignkey')
    op.create_foreign_key('import_project_id_fkey', 'import', 'project', ['project_id'], ['project_id'],
                          ondelete='CASCADE')
    op.drop_constraint('tasks_project_id_fkey', 'tasks', type_='foreignkey')
    op.create_foreign_key('tasks_project_id_fkey', 'tasks', 'project', ['project_id'], ['project_id'],
                          ondelete='CASCADE')


def downgrade():
    op.drop_constraint(None, 'tasks', type_='foreignkey')
    op.create_foreign_key('tasks_project_id_fkey', 'tasks', 'project', ['project_id'], ['project_id'])
    op.drop_constraint(None, 'import', type_='foreignkey')
    op.create_foreign_key('import_project_id_fkey', 'import', 'project', ['project_id'], ['project_id'])
    op.drop_constraint(None, 'highlighters', type_='foreignkey')
    op.create_foreign_key('highlighters_project_id_fkey', 'highlighters', 'project', ['project_id'], ['project_id'])
    op.drop_constraint(None, 'bot_annotation_metadata', type_='foreignkey')
    op.create_foreign_key('bot_annotation_metadata_project_id_fkey', 'bot_annotation_metadata', 'project',
                          ['project_id'], ['project_id'])
    op.drop_constraint(None, 'bot_annotation', type_='foreignkey')
    op.create_foreign_key('bot_annotation_parent_fkey', 'bot_annotation', 'bot_annotation', ['parent'],
                          ['bot_annotation_id'])
    op.drop_constraint(None, 'annotation_scheme', type_='foreignkey')
    op.create_foreign_key('annotation_task_project_id_fkey', 'annotation_scheme', 'project', ['project_id'],
                          ['project_id'])
    op.drop_constraint(None, 'annotation', type_='foreignkey')
    op.drop_constraint(None, 'annotation', type_='foreignkey')
    op.drop_constraint(None, 'annotation', type_='foreignkey')
    op.drop_constraint(None, 'annotation', type_='foreignkey')
    op.create_foreign_key('annotation_annotation_id_fkey', 'annotation', 'annotation', ['parent'], ['annotation_id'])
    op.create_foreign_key('annotation_annotation_scheme_id_fkey', 'annotation', 'annotation_scheme',
                          ['annotation_scheme_id'], ['annotation_scheme_id'])
    op.create_foreign_key('annotation_assignment_id_fkey', 'annotation', 'assignment', ['assignment_id'],
                          ['assignment_id'])
    op.create_foreign_key('annotation_user_id_fkey', 'annotation', 'user', ['user_id'], ['user_id'])
