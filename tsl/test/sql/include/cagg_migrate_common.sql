-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER

\if :IS_DISTRIBUTED
\echo 'Running distributed hypertable tests'
\else
\echo 'Running local hypertable tests'
\endif

\if :IS_TIME_DIMENSION
    \set TIME_DATATYPE TIMESTAMPTZ
\else
    \set TIME_DATATYPE INTEGER
\endif

CREATE TABLE conditions (
    "time" :TIME_DATATYPE NOT NULL,
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
    RETURNS integer LANGUAGE SQL STABLE AS
    $$
        SELECT coalesce(max(time), 0)
        FROM conditions
    $$;

    \if :IS_DISTRIBUTED
        CALL distributed_exec (
            $DIST$
            CREATE OR REPLACE FUNCTION integer_now() RETURNS integer LANGUAGE SQL STABLE AS $$ SELECT coalesce(max(time), 0) FROM conditions $$;
            $DIST$
        );
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

\set ON_ERROR_STOP 0
-- should error because plan already exists
CALL _timescaledb_internal.cagg_migrate_create_plan(:'CAGG_DATA', 'conditions_summary_daily_new');
\set ON_ERROR_STOP 1

DELETE FROM _timescaledb_catalog.continuous_agg_migrate_plan;
ALTER SEQUENCE _timescaledb_catalog.continuous_agg_migrate_plan_step_step_id_seq RESTART;

CALL _timescaledb_internal.cagg_migrate_create_plan(:'CAGG_DATA', 'conditions_summary_daily_new');
SELECT mat_hypertable_id, step_id, status, type, config FROM _timescaledb_catalog.continuous_agg_migrate_plan_step ORDER BY step_id;

-- policy for test
\if :IS_TIME_DIMENSION
SELECT add_retention_policy('conditions_summary_daily', '30 days'::interval);
\else
SELECT add_retention_policy('conditions_summary_daily', '30'::integer);
\endif

SELECT job_id, application_name, proc_schema, proc_name, scheduled, hypertable_schema, hypertable_name, config
FROM timescaledb_information.jobs
WHERE hypertable_schema = :'MAT_SCHEMA_NAME'
AND hypertable_name = :'MAT_TABLE_NAME'
AND job_id >= 1000;

-- execute the migration
DELETE FROM _timescaledb_catalog.continuous_agg_migrate_plan;
ALTER SEQUENCE _timescaledb_catalog.continuous_agg_migrate_plan_step_step_id_seq RESTART;
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

SELECT job_id, application_name, proc_schema, proc_name, scheduled, hypertable_schema, hypertable_name, config
FROM timescaledb_information.jobs
WHERE hypertable_schema = :'NEW_MAT_SCHEMA_NAME'
AND hypertable_name = :'NEW_MAT_TABLE_NAME'
AND job_id >= 1000;

SELECT mat_hypertable_id, step_id, status, type, config FROM _timescaledb_catalog.continuous_agg_migrate_plan_step ORDER BY step_id;

-- check migrated data. should return 0 (zero) rows
SELECT * FROM conditions_summary_daily
EXCEPT
SELECT * FROM conditions_summary_daily_new;

-- test migration overriding the new cagg and keeping the old
DROP MATERIALIZED VIEW conditions_summary_daily_new;
DELETE FROM _timescaledb_catalog.continuous_agg_migrate_plan;
ALTER SEQUENCE _timescaledb_catalog.continuous_agg_migrate_plan_step_step_id_seq RESTART;
CALL cagg_migrate('conditions_summary_daily', override => TRUE);
-- cagg with the new format because it was overriden
\d+ conditions_summary_daily
-- cagg with the old format because it was overriden
\d+ conditions_summary_daily_old
\set ON_ERROR_STOP 0
-- should fail because the cagg was overriden
SELECT * FROM conditions_summary_daily_new;
\set ON_ERROR_STOP 1

-- test migration overriding the new cagg and removing the old
DELETE FROM _timescaledb_catalog.continuous_agg_migrate_plan;
ALTER SEQUENCE _timescaledb_catalog.continuous_agg_migrate_plan_step_step_id_seq RESTART;
DROP MATERIALIZED VIEW conditions_summary_daily;
ALTER MATERIALIZED VIEW conditions_summary_daily_old RENAME TO conditions_summary_daily;
CALL cagg_migrate('conditions_summary_daily', override => TRUE, drop_old => TRUE);
-- cagg with the new format because it was overriden
\d+ conditions_summary_daily
\set ON_ERROR_STOP 0
-- should fail because the cagg was overriden
SELECT * FROM conditions_summary_daily_new;
-- should fail because the old cagg was removed
SELECT * FROM conditions_summary_daily_old;
\set ON_ERROR_STOP 1

-- permissions test
DELETE FROM _timescaledb_catalog.continuous_agg_migrate_plan;
ALTER SEQUENCE _timescaledb_catalog.continuous_agg_migrate_plan_step_step_id_seq RESTART;
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
