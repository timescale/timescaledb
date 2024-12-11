-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Setup some variables
SELECT
    format('\! zcat < include/data/cagg_migrate_%1$s.sql.gz > %2$s/results/cagg_migrate_%1$s.sql', lower(:'TIME_DIMENSION_DATATYPE'), :'TEST_OUTPUT_DIR') AS "ZCAT_CMD",
    format('%2$s/results/cagg_migrate_%1$s.sql', lower(:'TIME_DIMENSION_DATATYPE'), :'TEST_OUTPUT_DIR') AS "TEST_SCHEMA_FILE"
\gset

-- decompress dump file
:ZCAT_CMD

-- restore dump
SELECT timescaledb_pre_restore();
\ir :TEST_SCHEMA_FILE
SELECT timescaledb_post_restore();

-- Make sure no scheduled job will be executed during the regression tests
SELECT _timescaledb_functions.stop_background_workers();

\if :IS_TIME_DIMENSION
    INSERT INTO conditions ("time", temperature)
    SELECT
        generate_series('2022-01-01 00:00:00-00'::timestamptz, '2022-12-31 23:59:59-00'::timestamptz, '1 hour'),
        0.25;
\else
    CREATE OR REPLACE FUNCTION integer_now()
    RETURNS :TIME_DIMENSION_DATATYPE LANGUAGE SQL STABLE AS
    $$
        SELECT coalesce(max(time), 0)
        FROM public.conditions
    $$;

    INSERT INTO conditions ("time", temperature)
    SELECT
        generate_series(1, 1000, 1),
        0.25;
\endif

CALL refresh_continuous_aggregate('conditions_summary_daily', NULL, NULL);
CALL refresh_continuous_aggregate('conditions_summary_weekly', NULL, NULL);

\set ON_ERROR_STOP 0
-- should fail because we don't need to migrate finalized caggs
CALL cagg_migrate('conditions_summary_daily_new');

-- should fail relation does not exist
CALL cagg_migrate('conditions_summary_not_cagg');

CREATE TABLE conditions_summary_not_cagg();

-- should fail continuous agg does not exist
CALL cagg_migrate('conditions_summary_not_cagg');
\set ON_ERROR_STOP 1

DROP TABLE conditions_summary_not_cagg;

SELECT
    ca.raw_hypertable_id AS "RAW_HYPERTABLE_ID",
    h.schema_name AS "MAT_SCHEMA_NAME",
    h.table_name AS "MAT_TABLE_NAME",
    partial_view_name AS "PART_VIEW_NAME",
    partial_view_schema AS "PART_VIEW_SCHEMA",
    direct_view_name AS "DIR_VIEW_NAME",
    direct_view_schema AS "DIR_VIEW_SCHEMA"
FROM
    _timescaledb_catalog.continuous_agg ca
    JOIN _timescaledb_catalog.hypertable h ON (h.id = ca.mat_hypertable_id)
WHERE
    user_view_name = 'conditions_summary_daily'
\gset

\set ON_ERROR_STOP 0
-- should fail because the new cagg with suffix '_new' already exists
CALL cagg_migrate('conditions_summary_daily');
\set ON_ERROR_STOP 1

-- remove the new cagg to execute the migration
DROP MATERIALIZED VIEW conditions_summary_daily_new;

-- get and set all the cagg data
SELECT
    _timescaledb_functions.cagg_migrate_pre_validation(
        'public',
        'conditions_summary_daily',
        'conditions_summary_daily_new'
    ) AS "CAGG_DATA"
\gset

CALL _timescaledb_functions.cagg_migrate_create_plan(:'CAGG_DATA', 'conditions_summary_daily_new');
\x on
SELECT mat_hypertable_id, user_view_definition FROM _timescaledb_catalog.continuous_agg_migrate_plan;
\x off
SELECT mat_hypertable_id, step_id, status, type, config FROM _timescaledb_catalog.continuous_agg_migrate_plan_step ORDER BY step_id;

-- should resume the execution
CALL cagg_migrate('conditions_summary_daily');
SELECT mat_hypertable_id, step_id, status, type, config FROM _timescaledb_catalog.continuous_agg_migrate_plan_step ORDER BY step_id;

\set ON_ERROR_STOP 0
-- should error because plan already exists
CALL _timescaledb_functions.cagg_migrate_create_plan(:'CAGG_DATA', 'conditions_summary_daily_new');
CALL cagg_migrate('conditions_summary_daily');
\set ON_ERROR_STOP 1

-- policies for test
ALTER MATERIALIZED VIEW conditions_summary_daily SET (timescaledb.compress=true);

\if :IS_TIME_DIMENSION
SELECT add_retention_policy('conditions_summary_daily', '30 days'::interval);
SELECT add_continuous_aggregate_policy('conditions_summary_daily', '30 days'::interval, '1 day'::interval, '1 hour'::interval);
SELECT add_compression_policy('conditions_summary_daily', '45 days'::interval);
\else
SELECT add_retention_policy('conditions_summary_daily', '400'::integer);
SELECT add_continuous_aggregate_policy('conditions_summary_daily', '50'::integer, '1'::integer, '1 hour'::interval);
SELECT add_compression_policy('conditions_summary_daily', '100'::integer);
\endif

SELECT *
FROM timescaledb_information.jobs
WHERE hypertable_schema = :'MAT_SCHEMA_NAME'
AND hypertable_name = :'MAT_TABLE_NAME'
AND job_id >= 1000;

-- execute the migration
DROP MATERIALIZED VIEW conditions_summary_daily_new;
TRUNCATE _timescaledb_catalog.continuous_agg_migrate_plan RESTART IDENTITY CASCADE;
CALL cagg_migrate('conditions_summary_daily');

SELECT
    ca.raw_hypertable_id AS "NEW_RAW_HYPERTABLE_ID",
    h.schema_name AS "NEW_MAT_SCHEMA_NAME",
    h.table_name AS "NEW_MAT_TABLE_NAME",
    partial_view_name AS "NEW_PART_VIEW_NAME",
    partial_view_schema AS "NEW_PART_VIEW_SCHEMA",
    direct_view_name AS "NEW_DIR_VIEW_NAME",
    direct_view_schema AS "NEW_DIR_VIEW_SCHEMA"
FROM
    _timescaledb_catalog.continuous_agg ca
    JOIN _timescaledb_catalog.hypertable h ON (h.id = ca.mat_hypertable_id)
WHERE
    user_view_name = 'conditions_summary_daily_new'
\gset

\d+ conditions_summary_daily_new

SELECT *
FROM timescaledb_information.jobs
WHERE hypertable_schema = :'NEW_MAT_SCHEMA_NAME'
AND hypertable_name = :'NEW_MAT_TABLE_NAME'
AND job_id >= 1000;

SELECT mat_hypertable_id, step_id, status, type, config FROM _timescaledb_catalog.continuous_agg_migrate_plan_step ORDER BY step_id;

-- check migrated data. should return 0 (zero) rows
SELECT * FROM conditions_summary_daily
EXCEPT
SELECT * FROM conditions_summary_daily_new;

-- compress both caggs
SELECT compress_chunk(c) FROM show_chunks('conditions_summary_daily') c ORDER BY c::regclass::text;
SELECT compress_chunk(c) FROM show_chunks('conditions_summary_daily_new') c ORDER BY c::regclass::text;

-- check migrated data after compression. should return 0 (zero) rows
SELECT * FROM conditions_summary_daily
EXCEPT
SELECT * FROM conditions_summary_daily_new;

CREATE OR REPLACE VIEW cagg_jobs AS
SELECT user_view_schema AS schema, user_view_name AS name, bgw_job.*
FROM _timescaledb_config.bgw_job
JOIN _timescaledb_catalog.continuous_agg ON mat_hypertable_id = hypertable_id
ORDER BY bgw_job.id;

-- test migration overriding the new cagg and keeping the old
DROP MATERIALIZED VIEW conditions_summary_daily_new;
TRUNCATE _timescaledb_catalog.continuous_agg_migrate_plan RESTART IDENTITY CASCADE;
-- check policies before the migration
SELECT * FROM cagg_jobs WHERE schema = 'public' AND name = 'conditions_summary_daily';
CALL cagg_migrate('conditions_summary_daily', override => TRUE);
-- cagg with the new format because it was overriden
\d+ conditions_summary_daily
-- cagg with the old format because it was overriden
\d+ conditions_summary_daily_old
\set ON_ERROR_STOP 0
-- should fail because the cagg was overriden
SELECT * FROM conditions_summary_daily_new;
\set ON_ERROR_STOP 1
-- check policies after the migration
SELECT * FROM cagg_jobs WHERE schema = 'public' AND name = 'conditions_summary_daily';
-- should return the old cagg jobs
SELECT * FROM cagg_jobs WHERE schema = 'public' AND name = 'conditions_summary_daily_old';
-- should return no rows because the cagg was overwritten
SELECT * FROM cagg_jobs WHERE schema = 'public' AND name = 'conditions_summary_daily_new';

-- test migration overriding the new cagg and removing the old
TRUNCATE _timescaledb_catalog.continuous_agg_migrate_plan RESTART IDENTITY CASCADE;
DROP MATERIALIZED VIEW conditions_summary_daily;
ALTER MATERIALIZED VIEW conditions_summary_daily_old RENAME TO conditions_summary_daily;
-- check policies before the migration
SELECT * FROM cagg_jobs WHERE schema = 'public' AND name = 'conditions_summary_daily';
CALL cagg_migrate('conditions_summary_daily', override => TRUE, drop_old => TRUE);
-- cagg with the new format because it was overriden
\d+ conditions_summary_daily
\set ON_ERROR_STOP 0
-- should fail because the cagg was overriden
SELECT * FROM conditions_summary_daily_new;
-- should fail because the old cagg was removed
SELECT * FROM conditions_summary_daily_old;
\set ON_ERROR_STOP 1
-- check policies after the migration
SELECT * FROM cagg_jobs WHERE schema = 'public' AND name = 'conditions_summary_daily';
-- should return no rows because the old cagg was removed
SELECT * FROM cagg_jobs WHERE schema = 'public' AND name = 'conditions_summary_daily_old';
-- should return no rows because the cagg was overwritten
SELECT * FROM cagg_jobs WHERE schema = 'public' AND name = 'conditions_summary_daily_new';

-- permission tests
TRUNCATE _timescaledb_catalog.continuous_agg_migrate_plan RESTART IDENTITY CASCADE;
GRANT ALL ON TABLE conditions TO :ROLE_DEFAULT_PERM_USER;
ALTER MATERIALIZED VIEW conditions_summary_weekly OWNER TO :ROLE_DEFAULT_PERM_USER;
SET ROLE :ROLE_DEFAULT_PERM_USER;

\set ON_ERROR_STOP 0
-- should fail because the lack of permissions on 'continuous_agg_migrate_plan' catalog table
CALL cagg_migrate('conditions_summary_weekly');
\set ON_ERROR_STOP 1

RESET ROLE;
GRANT SELECT, INSERT, UPDATE ON TABLE _timescaledb_catalog.continuous_agg_migrate_plan TO :ROLE_DEFAULT_PERM_USER;

SET ROLE :ROLE_DEFAULT_PERM_USER;

\set ON_ERROR_STOP 0
-- should fail because the lack of permissions on 'continuous_agg_migrate_plan_step' catalog table
CALL cagg_migrate('conditions_summary_weekly');
\set ON_ERROR_STOP 1

RESET ROLE;
GRANT SELECT, INSERT, UPDATE ON TABLE _timescaledb_catalog.continuous_agg_migrate_plan_step TO :ROLE_DEFAULT_PERM_USER;

SET ROLE :ROLE_DEFAULT_PERM_USER;

\set ON_ERROR_STOP 0
-- should fail because the lack of permissions on 'continuous_agg_migrate_plan_step_step_id_seq' catalog sequence
CALL cagg_migrate('conditions_summary_weekly');
\set ON_ERROR_STOP 1

RESET ROLE;
GRANT USAGE ON SEQUENCE _timescaledb_catalog.continuous_agg_migrate_plan_step_step_id_seq TO :ROLE_DEFAULT_PERM_USER;

SET ROLE :ROLE_DEFAULT_PERM_USER;

-- all necessary permissions granted
CALL cagg_migrate('conditions_summary_weekly');

-- check migrated data. should return 0 (zero) rows
SELECT * FROM conditions_summary_weekly
EXCEPT
SELECT * FROM conditions_summary_weekly_new;

SELECT mat_hypertable_id, step_id, status, type, config FROM _timescaledb_catalog.continuous_agg_migrate_plan_step ORDER BY step_id;

RESET ROLE;

-- according to the official documentation trying to execute a procedure with
-- transaction control statements inside an explicit transaction should fail:
-- https://www.postgresql.org/docs/current/sql-call.html
-- `If CALL is executed in a transaction block, then the called procedure cannot
--  execute transaction control statements. Transaction control statements are only
--  allowed if CALL is executed in its own transaction.`
TRUNCATE _timescaledb_catalog.continuous_agg_migrate_plan RESTART IDENTITY CASCADE;
DROP MATERIALIZED VIEW conditions_summary_weekly_new;

\set ON_ERROR_STOP 0
BEGIN;
-- should fail with `invalid transaction termination`
CALL cagg_migrate('conditions_summary_weekly');
ROLLBACK;
\set ON_ERROR_STOP 1

CREATE FUNCTION execute_migration() RETURNS void AS
$$
BEGIN
    CALL cagg_migrate('conditions_summary_weekly');
    RETURN;
END;
$$
LANGUAGE plpgsql;

\set ON_ERROR_STOP 0
-- execute migration inside a plpgsql function
BEGIN;
-- should fail with `invalid transaction termination`
SELECT execute_migration();
ROLLBACK;
\set ON_ERROR_STOP 1

--
-- test dropping chunks
--

-- no chunks marked as dropped
SELECT
   c.table_name as chunk_name,
   c.dropped
FROM _timescaledb_catalog.hypertable h, _timescaledb_catalog.chunk c
WHERE h.id = c.hypertable_id AND h.table_name = 'conditions' AND c.dropped
ORDER BY 1;

-- drop 1 chunk
\if :IS_TIME_DIMENSION
    SELECT drop_chunks('conditions', older_than => CAST('2022-01-08 00:00:00-00' AS :TIME_DIMENSION_DATATYPE));
\else
    SELECT drop_chunks('conditions', older_than => 10);
\endif

-- now he have one chunk marked as dropped
SELECT
   c.table_name as chunk_name,
   c.dropped
FROM _timescaledb_catalog.hypertable h, _timescaledb_catalog.chunk c
WHERE h.id = c.hypertable_id AND h.table_name = 'conditions' AND c.dropped
ORDER BY 1;

-- this migration should remove the chunk metadata marked as dropped
CALL cagg_migrate('conditions_summary_weekly', override => TRUE, drop_old => TRUE);

-- no chunks marked as dropped
SELECT
   c.table_name as chunk_name,
   c.dropped
FROM _timescaledb_catalog.hypertable h, _timescaledb_catalog.chunk c
WHERE h.id = c.hypertable_id AND h.table_name = 'conditions' AND c.dropped
ORDER BY 1;

-- cleanup
DROP FUNCTION execute_migration();
REVOKE SELECT, INSERT, UPDATE ON TABLE _timescaledb_catalog.continuous_agg_migrate_plan FROM :ROLE_DEFAULT_PERM_USER;
REVOKE USAGE ON SEQUENCE _timescaledb_catalog.continuous_agg_migrate_plan_step_step_id_seq FROM :ROLE_DEFAULT_PERM_USER;
TRUNCATE _timescaledb_catalog.continuous_agg_migrate_plan RESTART IDENTITY CASCADE;
DROP MATERIALIZED VIEW conditions_summary_daily;
DROP MATERIALIZED VIEW conditions_summary_weekly;
DROP TABLE conditions;

SELECT _timescaledb_functions.start_background_workers();
