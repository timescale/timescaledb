-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER

\if :IS_DISTRIBUTED
\echo 'Running distributed hypertable tests'
\else
\echo 'Running local hypertable tests'
\endif

CREATE TABLE conditions (
    "time" :TIME_DIMENSION_DATATYPE NOT NULL,
    temperature NUMERIC
);

\if :IS_DISTRIBUTED
    \if :IS_TIME_DIMENSION
        SELECT table_name FROM create_distributed_hypertable('conditions', 'time', replication_factor => 2);
    \else
        SELECT table_name FROM create_distributed_hypertable('conditions', 'time', chunk_time_interval => 10, replication_factor => 2);
    \endif
\else
    \if :IS_TIME_DIMENSION
        SELECT table_name FROM create_hypertable('conditions', 'time');
    \else
        SELECT table_name FROM create_hypertable('conditions', 'time', chunk_time_interval => 10);
    \endif
\endif

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

    \if :IS_DISTRIBUTED
        SELECT
            'CREATE OR REPLACE FUNCTION integer_now() RETURNS '||:'TIME_DIMENSION_DATATYPE'||' LANGUAGE SQL STABLE AS $$ SELECT coalesce(max(time), 0) FROM public.conditions $$;' AS "STMT"
            \gset
        CALL distributed_exec (:'STMT');
    \endif

    SELECT set_integer_now_func('conditions', 'integer_now');

    INSERT INTO conditions ("time", temperature)
    SELECT
        generate_series(1, 1000, 1),
        0.25;
\endif


\set ON_ERROR_STOP 0
-- should fail relation does not exist
CALL cagg_migrate('conditions_summary_daily');
CREATE TABLE conditions_summary_daily();
-- should fail continuous agg does not exist
CALL cagg_migrate('conditions_summary_daily');
\set ON_ERROR_STOP 1

DROP TABLE conditions_summary_daily;

CREATE MATERIALIZED VIEW conditions_summary_daily_new
WITH (timescaledb.continuous) AS
SELECT
\if :IS_TIME_DIMENSION
    time_bucket(INTERVAL '1 day', "time") AS bucket,
\else
    time_bucket(INTEGER '24', "time") AS bucket,
\endif
    MIN(temperature),
    MAX(temperature),
    AVG(temperature),
    SUM(temperature)
FROM
    conditions
GROUP BY
    bucket
WITH NO DATA;

\set ON_ERROR_STOP 0
-- should fail because we don't need to migrate finalized caggs
CALL cagg_migrate('conditions_summary_daily_new');
\set ON_ERROR_STOP 1

-- older continuous aggregate to be migrated
CREATE MATERIALIZED VIEW conditions_summary_daily
WITH (timescaledb.continuous, timescaledb.finalized=false) AS
SELECT
\if :IS_TIME_DIMENSION
    time_bucket(INTERVAL '1 day', "time") AS bucket,
\else
    time_bucket(INTEGER '24', "time") AS bucket,
\endif
    MIN(temperature),
    MAX(temperature),
    AVG(temperature),
    SUM(temperature)
FROM
    conditions
GROUP BY
    bucket;

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
    _timescaledb_internal.cagg_migrate_pre_validation(
        'public',
        'conditions_summary_daily',
        'conditions_summary_daily_new'
    ) AS "CAGG_DATA"
\gset

CALL _timescaledb_internal.cagg_migrate_create_plan(:'CAGG_DATA', 'conditions_summary_daily_new');
SELECT mat_hypertable_id FROM _timescaledb_catalog.continuous_agg_migrate_plan;
SELECT mat_hypertable_id, step_id, status, type, config FROM _timescaledb_catalog.continuous_agg_migrate_plan_step ORDER BY step_id;

-- should resume the execution
CALL cagg_migrate('conditions_summary_daily');
SELECT mat_hypertable_id, step_id, status, type, config FROM _timescaledb_catalog.continuous_agg_migrate_plan_step ORDER BY step_id;

\set ON_ERROR_STOP 0
-- should error because plan already exists
CALL _timescaledb_internal.cagg_migrate_create_plan(:'CAGG_DATA', 'conditions_summary_daily_new');
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

-- permissions test
TRUNCATE _timescaledb_catalog.continuous_agg_migrate_plan RESTART IDENTITY CASCADE;
DROP MATERIALIZED VIEW conditions_summary_daily;
GRANT ALL ON TABLE conditions TO :ROLE_DEFAULT_PERM_USER;
SET ROLE :ROLE_DEFAULT_PERM_USER;

CREATE MATERIALIZED VIEW conditions_summary_daily
WITH (timescaledb.continuous, timescaledb.finalized=false) AS
SELECT
\if :IS_TIME_DIMENSION
    time_bucket(INTERVAL '1 day', "time") AS bucket,
\else
    time_bucket(INTEGER '24', "time") AS bucket,
\endif
    MIN(temperature),
    MAX(temperature),
    AVG(temperature),
    SUM(temperature)
FROM
    conditions
GROUP BY
    bucket;

\set ON_ERROR_STOP 0
-- should fail because the lack of permissions on 'continuous_agg_migrate_plan' catalog table
CALL cagg_migrate('conditions_summary_daily');
\set ON_ERROR_STOP 1

RESET ROLE;
GRANT SELECT, INSERT, UPDATE ON TABLE _timescaledb_catalog.continuous_agg_migrate_plan TO :ROLE_DEFAULT_PERM_USER;

SET ROLE :ROLE_DEFAULT_PERM_USER;

\set ON_ERROR_STOP 0
-- should fail because the lack of permissions on 'continuous_agg_migrate_plan_step' catalog table
CALL cagg_migrate('conditions_summary_daily');
\set ON_ERROR_STOP 1

RESET ROLE;
GRANT SELECT, INSERT, UPDATE ON TABLE _timescaledb_catalog.continuous_agg_migrate_plan_step TO :ROLE_DEFAULT_PERM_USER;

SET ROLE :ROLE_DEFAULT_PERM_USER;

\set ON_ERROR_STOP 0
-- should fail because the lack of permissions on 'continuous_agg_migrate_plan_step_step_id_seq' catalog sequence
CALL cagg_migrate('conditions_summary_daily');
\set ON_ERROR_STOP 1

RESET ROLE;
GRANT USAGE ON SEQUENCE _timescaledb_catalog.continuous_agg_migrate_plan_step_step_id_seq TO :ROLE_DEFAULT_PERM_USER;

SET ROLE :ROLE_DEFAULT_PERM_USER;

-- all necessary permissions granted
CALL cagg_migrate('conditions_summary_daily');

-- check migrated data. should return 0 (zero) rows
SELECT * FROM conditions_summary_daily
EXCEPT
SELECT * FROM conditions_summary_daily_new;

SELECT mat_hypertable_id, step_id, status, type, config FROM _timescaledb_catalog.continuous_agg_migrate_plan_step ORDER BY step_id;

RESET ROLE;

-- according to the official documentation trying to execute a procedure with
-- transaction control statements inside an explicit transaction should fail:
-- https://www.postgresql.org/docs/current/sql-call.html
-- `If CALL is executed in a transaction block, then the called procedure cannot
--  execute transaction control statements. Transaction control statements are only
--  allowed if CALL is executed in its own transaction.`
TRUNCATE _timescaledb_catalog.continuous_agg_migrate_plan RESTART IDENTITY CASCADE;
DROP MATERIALIZED VIEW conditions_summary_daily_new;

\set ON_ERROR_STOP 0
BEGIN;
-- should fail with `invalid transaction termination`
CALL cagg_migrate('conditions_summary_daily');
ROLLBACK;
\set ON_ERROR_STOP 1

CREATE OR REPLACE FUNCTION execute_migration() RETURNS void AS
$$
BEGIN
    CALL cagg_migrate('conditions_summary_daily');
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

-- cleanup
REVOKE SELECT, INSERT, UPDATE ON TABLE _timescaledb_catalog.continuous_agg_migrate_plan FROM :ROLE_DEFAULT_PERM_USER;
REVOKE USAGE ON SEQUENCE _timescaledb_catalog.continuous_agg_migrate_plan_step_step_id_seq FROM :ROLE_DEFAULT_PERM_USER;
TRUNCATE _timescaledb_catalog.continuous_agg_migrate_plan RESTART IDENTITY CASCADE;
DROP MATERIALIZED VIEW conditions_summary_daily;
DROP TABLE conditions;
