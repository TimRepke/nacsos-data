BEGIN;

-- Disable triggers/foreign key checks during import if needed
SET CONSTRAINTS ALL DEFERRED;


\echo "Importing base data..."
\copy alembic_version FROM 'alembic.csv' WITH (FORMAT csv, HEADER)
\copy project FROM 'project.csv' WITH (FORMAT csv, HEADER)
\copy "user" FROM 'user.csv' WITH (FORMAT csv, HEADER)
\copy project_permissions FROM 'project_permissions.csv' WITH (FORMAT csv, HEADER)


\echo "Importing imports..."
\copy import FROM 'import.csv' WITH (FORMAT csv, HEADER)
\copy import_revision FROM 'import_revision.csv' WITH (FORMAT csv, HEADER)

\echo "Importing items..."
\copy item FROM 'item.csv' WITH (FORMAT csv, HEADER)
\copy academic_item FROM 'academic_item.csv' WITH (FORMAT csv, HEADER)
\copy academic_item_variant FROM 'academic_item_variant.csv' WITH (FORMAT csv, HEADER)
\copy lexis_item FROM 'lexis_item.csv' WITH (FORMAT csv, HEADER)
\copy lexis_item_source FROM 'lexis_item_source.csv' WITH (FORMAT csv, HEADER)
\copy twitter_item FROM 'twitter_item.csv' WITH (FORMAT csv, HEADER)
\copy generic_item FROM 'generic_item.csv' WITH (FORMAT csv, HEADER)

\echo "Importing imports <-> items m2m..."
\copy m2m_import_item FROM 'm2m_import_item.csv' WITH (FORMAT csv, HEADER)

\echo "Importing annotations..."
\copy annotation_scheme FROM 'annotation_scheme.csv' WITH (FORMAT csv, HEADER)
\copy assignment_scope FROM 'assignment_scope.csv' WITH (FORMAT csv, HEADER)
\copy assignment FROM 'assignment.csv' WITH (FORMAT csv, HEADER)
\copy annotation FROM 'annotation.csv' WITH (FORMAT csv, HEADER)

\echo "Importing bot annotations..."
\copy bot_annotation_metadata FROM 'bot_annotation_metadata.csv' WITH (FORMAT csv, HEADER)
\copy bot_annotation FROM 'bot_annotation.csv' WITH (FORMAT csv, HEADER)

\echo "Importing other things..."
\copy enhancement FROM 'enhancements.csv' WITH (FORMAT csv, HEADER)
\copy annotation_tracker FROM 'annotation_tracker.csv' WITH (FORMAT csv, HEADER)
\copy annotation_quality FROM 'annotation_quality.csv' WITH (FORMAT csv, HEADER)

\copy priorities FROM 'priorities.csv' WITH (FORMAT csv, HEADER)
\copy highlighters FROM 'highlighters.csv' WITH (FORMAT csv, HEADER)
\copy tasks FROM 'tasks.csv' WITH (FORMAT csv, HEADER)
\copy snippet FROM 'snippets.csv' WITH (FORMAT csv, HEADER)

COMMIT;