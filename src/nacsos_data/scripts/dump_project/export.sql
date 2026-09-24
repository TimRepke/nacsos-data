-- export_project.sql
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;

-- project
\echo '--> Collecting data from project table...'
CREATE TEMP TABLE temp_export_alembic AS (
    SELECT *
    FROM alembic_version);

-- project
\echo '--> Collecting data from project table...'
CREATE TEMP TABLE temp_export_project AS (
    SELECT *
    FROM project
    WHERE project_id = :'project_id');

-- users
\echo '--> Collecting data from users table...'
CREATE TEMP TABLE temp_export_users AS (
WITH
    uids AS (
        SELECT user_id
        FROM project_permissions pp
        WHERE project_id = :'project_id'
        UNION
        SELECT DISTINCT user_id
        FROM assignment ass
             JOIN item i ON ass.item_id = i.item_id
        WHERE project_id = :'project_id'
            UNION
        SELECT DISTINCT user_id
        FROM annotation ann
             JOIN item i ON ann.item_id = i.item_id
        WHERE project_id = :'project_id'
            UNION
        SELECT DISTINCT user_id
        FROM import
        WHERE project_id = :'project_id')
SELECT distinct u.user_id,
                u.username,
                u.email,
                u.full_name,
                u.affiliation,
                -- setting all user passwords to "demo"
                -- bcrypt.hashpw('demo'.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                '$2b$12$yQcXvbKgi7j2CN7iQw3EteevGWxntMaxWJ9ZbU6PHW7UVYxvOBF6m' as password,
                --u.password,
                u.is_superuser,
                u.is_active,
                u.time_created,
                u.time_updated,
                u.setting_newsletter
FROM "user" u
     JOIN uids ON uids.user_id = u.user_id );

-- permissions
\echo '--> Collecting data from permissions table...'
CREATE TEMP TABLE temp_export_permissions AS (
    SELECT *
    FROM project_permissions
    WHERE project_id = :'project_id');
-- import
\echo '--> Collecting data from import table...'
CREATE TEMP TABLE temp_export_import AS (
    SELECT *
    FROM import
    WHERE project_id = :'project_id');
-- import_revision
\echo '--> Collecting data from revisions table...'
CREATE TEMP TABLE temp_export_import_revision AS (
    SELECT ir.*
    FROM import_revision ir
         JOIN import i ON ir.import_id = i.import_id
    WHERE project_id = :'project_id');
-- m2m_import_item
\echo '--> Collecting data from m2m table...'
CREATE TEMP TABLE temp_export_m2m AS (
    SELECT m2m.*
    FROM m2m_import_item m2m
         JOIN import i ON m2m.import_id = i.import_id
    WHERE project_id = :'project_id');
-- item
\echo '--> Collecting data from items table...'
CREATE TEMP TABLE temp_export_item AS (
    SELECT *
    FROM item
    WHERE project_id = :'project_id');
-- academic_item
\echo '--> Collecting data from academic items table...'
CREATE TEMP TABLE temp_export_ai AS (
    SELECT *
    FROM academic_item
    WHERE project_id = :'project_id');
-- academic_item_variant
\echo '--> Collecting data from aiv table...'
CREATE TEMP TABLE temp_export_aiv AS (
    SELECT aiv.*
    FROM academic_item_variant aiv
         JOIN item i ON aiv.item_id = i.item_id
    WHERE project_id = :'project_id');
-- annotation_scheme
\echo '--> Collecting data from scheme table...'
CREATE TEMP TABLE temp_export_scheme AS (
    SELECT *
    FROM annotation_scheme
    WHERE project_id = :'project_id');
-- assignment_scope
\echo '--> Collecting data from scope table...'
CREATE TEMP TABLE temp_export_scope AS (
    SELECT scope.*
    FROM assignment_scope scope
         JOIN annotation_scheme scheme ON scope.annotation_scheme_id = scheme.annotation_scheme_id
    WHERE project_id = :'project_id');
-- assignment
-- NOTE: in theory, one could have assignments without a scope, so maybe add a UNION which selects assignments joined on item
\echo '--> Collecting data from assignment table...'
CREATE TEMP TABLE temp_export_ass AS (
    SELECT ass.*
    FROM assignment ass
         JOIN item i ON ass.item_id = i.item_id
    WHERE project_id = :'project_id');
-- annotation
-- NOTE: in theory, one could have annotations without a scope or scheme, so maybe add a UNION which selects annotations joined on item
\echo '--> Collecting data from annotation table...'
CREATE TEMP TABLE temp_export_ann AS (
    SELECT ann.*
    FROM annotation ann
         JOIN item i ON ann.item_id = i.item_id
    WHERE project_id = :'project_id');
-- priorities
\echo '--> Collecting data from prioritisation table...'
CREATE TEMP TABLE temp_export_prio AS (
    SELECT *
    FROM priorities
    WHERE project_id = :'project_id');

-- bot_annotation_metadata
\echo '--> Collecting data from bam table...'
CREATE TEMP TABLE temp_export_bam AS (
    SELECT *
    FROM bot_annotation_metadata
    WHERE project_id = :'project_id');
-- bot_annotation
-- NOTE: we might also want to join this on bot_annotation_metadata, but on item should be more inclusive
\echo '--> Collecting data from ba table...'
CREATE TEMP TABLE temp_export_ba AS (
    SELECT ba.*
    FROM bot_annotation ba
         JOIN item ON ba.item_id = item.item_id
    WHERE project_id = :'project_id');
-- enhancements
\echo '--> Collecting data from enhancements table...'
CREATE TEMP TABLE temp_export_en AS (
    SELECT en.*
    FROM enhancement en
         JOIN item ON en.item_id = item.item_id
    WHERE project_id = :'project_id');

-- annotation_tracker
\echo '--> Collecting data from tracker table...'
CREATE TEMP TABLE temp_export_at AS (
    SELECT *
    FROM annotation_tracker
    WHERE project_id = :'project_id');
-- annotation_quality
\echo '--> Collecting data from quality table...'
CREATE TEMP TABLE temp_export_aq AS (
    SELECT *
    FROM annotation_quality
    WHERE project_id = :'project_id');
-- highlighters
\echo '--> Collecting data from highlighters table...'
CREATE TEMP TABLE temp_export_highlighters AS (
    SELECT *
    FROM highlighters
    WHERE project_id = :'project_id');

-- generic_item
\echo '--> Collecting data from generic table...'
CREATE TEMP TABLE temp_export_gi AS (
    SELECT gi.*
    FROM generic_item gi
         JOIN item ON gi.item_id = item.item_id
    WHERE project_id = :'project_id');
-- twitter_item
\echo '--> Collecting data from twitter table...'
CREATE TEMP TABLE temp_export_ti AS (
    SELECT *
    FROM twitter_item
    WHERE project_id = :'project_id');
-- lexis_item
\echo '--> Collecting data from lexis table...'
CREATE TEMP TABLE temp_export_li AS (
    SELECT *
    FROM lexis_item
    WHERE project_id = :'project_id');
-- lexis_item_source
\echo '--> Collecting data from lexis sources table...'
CREATE TEMP TABLE temp_export_lis AS (
    SELECT lis.*
    FROM lexis_item_source lis
         JOIN item ON lis.item_id = item.item_id
    WHERE project_id = :'project_id');

-- tasks
\echo '--> Collecting data from tasks table...'
CREATE TEMP TABLE temp_export_tasks AS (
    SELECT *
    FROM tasks
    WHERE project_id = :'project_id');
-- snippet
\echo '--> Collecting data from snippet table...'
CREATE TEMP TABLE temp_export_snippet AS (
    SELECT s.*
    FROM snippet s
         JOIN item ON s.item_id = item.item_id
    WHERE project_id = :'project_id');

-- 4. Export the temp tables to CSV files
\echo '-----------------'
\echo '--> Writing to CSV files...'
\copy temp_export_alembic TO 'alembic.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_project TO 'project.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_users TO 'user.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_permissions TO 'project_permissions.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_import TO 'import.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_import_revision TO 'import_revision.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_m2m TO 'm2m_import_item.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_item TO 'item.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_ai TO 'academic_item.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_aiv TO 'academic_item_variant.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_scheme TO 'annotation_scheme.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_scope TO 'assignment_scope.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_ass TO 'assignment.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_ann TO 'annotation.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_prio TO 'priorities.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_bam TO 'bot_annotation_metadata.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_ba TO 'bot_annotation.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_en TO 'enhancements.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_at TO 'annotation_tracker.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_aq TO 'annotation_quality.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_highlighters TO 'highlighters.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_gi TO 'generic_item.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_li TO 'lexis_item.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_lis TO 'lexis_item_source.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_ti TO 'twitter_item.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_tasks TO 'tasks.csv' WITH (FORMAT csv, HEADER)
\copy temp_export_snippet TO 'snippets.csv' WITH (FORMAT csv, HEADER)

ROLLBACK; -- Cleanly closes the transaction and drops all temp tables automatically