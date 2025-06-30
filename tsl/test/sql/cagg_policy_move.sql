-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE VIEW hypertable_invalidation_thresholds AS
SELECT format('%I.%I', ht.schema_name, ht.table_name)::regclass AS hypertable,
       watermark AS threshold
  FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
  JOIN _timescaledb_catalog.hypertable ht
    ON hypertable_id = ht.id
ORDER BY 1;

CREATE VIEW hypertable_invalidations AS
SELECT format('%I.%I', ht.schema_name, ht.table_name)::regclass AS hypertable,
       hypertable_id,
       lowest_modified_value,
       greatest_modified_value
  FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
  JOIN _timescaledb_catalog.hypertable ht
    ON hypertable_id = ht.id
ORDER BY 1, 2, 3;

CREATE FUNCTION get_job_id_for(REGCLASS) RETURNS INTEGER AS $$
SELECT DISTINCT job.id AS job_id
  FROM _timescaledb_catalog.hypertable AS ht
  JOIN _timescaledb_config.bgw_job AS job
    ON hypertable_id = ht.id
 WHERE format('%I.%I', schema_name, table_name)::regclass = $1
$$ LANGUAGE SQL;

CREATE TABLE conditions (time bigint NOT NULL, device int, temp float);
SELECT create_hypertable('conditions', 'time', chunk_time_interval => 10);

CREATE TABLE measurements (time int NOT NULL, device int, temp float);
SELECT create_hypertable('measurements', 'time', chunk_time_interval => 10);

CREATE OR REPLACE FUNCTION bigint_now()
RETURNS bigint LANGUAGE SQL STABLE AS
$$
    SELECT coalesce(max(time), 0)
    FROM conditions
$$;

CREATE OR REPLACE FUNCTION int_now()
RETURNS int LANGUAGE SQL STABLE AS
$$
    SELECT coalesce(max(time), 0)
    FROM measurements
$$;

SELECT set_integer_now_func('conditions', 'bigint_now');
SELECT set_integer_now_func('measurements', 'int_now');

INSERT INTO conditions
SELECT t, ceil(abs(timestamp_hash(to_timestamp(t)::timestamp))%4)::int,
       abs(timestamp_hash(to_timestamp(t)::timestamp))%40
FROM generate_series(1, 100, 1) t;

INSERT INTO measurements
SELECT t, ceil(abs(timestamp_hash(to_timestamp(t)::timestamp))%4)::int,
       abs(timestamp_hash(to_timestamp(t)::timestamp))%40
FROM generate_series(1, 100, 1) t;

CREATE MATERIALIZED VIEW conditions_10
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
SELECT time_bucket(BIGINT '10', time) AS bucket, device, avg(temp) AS avg_temp
FROM conditions
GROUP BY 1,2;

-- Trying to add to a hypertable that does not have any continuous
-- aggregates should fail.
\set ON_ERROR_STOP 0
CALL add_process_hypertable_invalidations_policy('measurements',
     schedule_interval => '1 minute'::interval);
\set ON_ERROR_STOP 1

CREATE MATERIALIZED VIEW measure_10
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
SELECT time_bucket(10, time) AS bucket, device, avg(temp) AS avg_temp
FROM measurements
GROUP BY 1,2;

CALL add_process_hypertable_invalidations_policy('measurements',
     schedule_interval => '1 minute'::interval);

-- Check thresholds to make sure that what we're writing below is not
-- after the threshold.
SELECT * FROM hypertable_invalidation_thresholds;

INSERT INTO conditions VALUES (10, 4, 23.7);
INSERT INTO conditions VALUES (10, 5, 23.8), (19, 3, 23.6);
INSERT INTO conditions VALUES (60, 3, 23.7), (70, 4, 23.7);
INSERT INTO measurements VALUES (20, 4, 23.7);
INSERT INTO measurements VALUES (30, 5, 23.8), (80, 3, 23.6);

SELECT hypertable, lowest_modified_value, greatest_modified_value
  FROM hypertable_invalidations;

\set ON_ERROR_STOP 0
CALL add_process_hypertable_invalidations_policy('conditions_10',
     schedule_interval => '1 minute'::interval);
CALL add_process_hypertable_invalidations_policy('conditions',
     schedule_interval => '1 minute'::interval,
     timezone => 'QQQ');
\set ON_ERROR_STOP 1

CALL add_process_hypertable_invalidations_policy('conditions',
    schedule_interval => '1 minute'::interval,
    initial_start => 'epoch'::timestamptz + '9223371331200000000'::bigint * '1 microsecond'::interval
);

-- This should error out and print notice, respectively
\set ON_ERROR_STOP 0
CALL add_process_hypertable_invalidations_policy('conditions',
     schedule_interval => '1 minute'::interval,
     if_not_exists => false);
CALL add_process_hypertable_invalidations_policy('conditions',
     schedule_interval => '1 minute'::interval,
     if_not_exists => true);
\set ON_ERROR_STOP 1

\x on
SELECT application_name, owner,
       format('%I.%I', proc_schema, proc_name),
       schedule_interval, fixed_schedule, initial_start,
       next_start,		-- to check initial start
       hypertable_id, config
  FROM _timescaledb_config.bgw_job
  LEFT JOIN _timescaledb_internal.bgw_job_stat
    ON id = job_id
 WHERE application_name LIKE '%Move Hypertables Invalidation Policy%';
\x off

-- Get a job id and a valid configuration for testing below.
SELECT id AS job_id,
       config AS config
  FROM _timescaledb_config.bgw_job
 WHERE application_name LIKE '%Move Hypertables Invalidation Policy%'
   AND hypertable_id = 1 \gset

-- Test that the check function capture a bad configurations.
\set ON_ERROR_STOP 0
SELECT alter_job(:job_id, config => '{"hyper_id": 1}');
-- Picking something really out of bounds to check that it triggers if
-- hypertable do not exist.
SELECT alter_job(:job_id, config => '{"hypertable_id": 4711}');
SELECT alter_job(:job_id, config => '{"hypertable_id": "garbage"}');
\set ON_ERROR_STOP 1

-- This is just resetting the config to the same, but is used to get
-- coverage for the "good path" in the check function.
\x on
SELECT * FROM alter_job(:job_id, config => :'config');
\x off

-- Test to run the job and check that hypertable invalidations are
-- processed.
SELECT hypertable, lowest_modified_value, greatest_modified_value
  FROM hypertable_invalidations;

CALL run_job(get_job_id_for('conditions'));

SELECT hypertable, lowest_modified_value, greatest_modified_value
  FROM hypertable_invalidations;

-- Running it twice should work and not change anything.
CALL run_job(get_job_id_for('conditions'));

SELECT hypertable, lowest_modified_value, greatest_modified_value
  FROM hypertable_invalidations;

CALL run_job(get_job_id_for('measurements'));

SELECT hypertable, lowest_modified_value, greatest_modified_value
  FROM hypertable_invalidations;


\set ON_ERROR_STOP 0
CALL _timescaledb_functions.policy_process_hypertable_invalidations(NULL, NULL);
CALL _timescaledb_functions.policy_process_hypertable_invalidations(1, NULL);
CALL _timescaledb_functions.policy_process_hypertable_invalidations(NULL, :'config');
\set ON_ERROR_STOP 1

-- Check that a refresh with hypertable invalidation processing
-- disabled does not move the invalidations.
INSERT INTO measurements VALUES (40, 12, 12.3);
INSERT INTO measurements VALUES (50, 13, 34.5);
SELECT hypertable, lowest_modified_value, greatest_modified_value
  FROM hypertable_invalidations;
CALL refresh_continuous_aggregate('measure_10', NULL, NULL,
     options => '{"process_hypertable_invalidations": false}');
SELECT hypertable, lowest_modified_value, greatest_modified_value
  FROM hypertable_invalidations;

-- Check that a refresh with hypertable invalidation processing
-- enabled move the invalidations.
CALL refresh_continuous_aggregate('measure_10', NULL, NULL,
     options => '{"process_hypertable_invalidations": true}');
SELECT hypertable, lowest_modified_value, greatest_modified_value
  FROM hypertable_invalidations;

-- Check that a refresh by default moves the invalidations.
INSERT INTO measurements VALUES (60, 16, 12.3);
INSERT INTO measurements VALUES (61, 17, 34.5);
SELECT hypertable, lowest_modified_value, greatest_modified_value
  FROM hypertable_invalidations;
CALL refresh_continuous_aggregate('measure_10', NULL, NULL);
SELECT hypertable, lowest_modified_value, greatest_modified_value
  FROM hypertable_invalidations;

-- Check permissions. Only owner should be able to remove policy.
\set ON_ERROR_STOP 0
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
CALL remove_process_hypertable_invalidations_policy('conditions');
\set ON_ERROR_STOP 1

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
CALL remove_process_hypertable_invalidations_policy('conditions');

\set ON_ERROR_STOP 0
CALL remove_process_hypertable_invalidations_policy('conditions_10');
CALL remove_process_hypertable_invalidations_policy('conditions', if_exists => false);
CALL remove_process_hypertable_invalidations_policy('conditions', if_exists => true);
\set ON_ERROR_STOP 1

-- Add a policy that has hypertable invalidations processing disabled
-- and check that it does not move invalidations.
SELECT add_continuous_aggregate_policy('measure_10', 100::int, 10::int, '1h'::interval) as job_id \gset
SELECT jsonb_set(config, '{process_hypertable_invalidations}', 'false') AS config
  FROM _timescaledb_config.bgw_job WHERE id = :job_id \gset
SELECT jsonb_pretty(config)
  FROM alter_job(:job_id, config := :'config');
INSERT INTO measurements VALUES (70, 19, 12.3), (71, 20, 34.5);
SELECT hypertable, lowest_modified_value, greatest_modified_value FROM hypertable_invalidations;
CALL run_job(:job_id);
SELECT hypertable, lowest_modified_value, greatest_modified_value FROM hypertable_invalidations;

-- Enable invalidations and check that it now moves invalidations
SELECT jsonb_pretty(config)
  FROM alter_job(:job_id, config := jsonb_set(:'config', '{process_hypertable_invalidations}', 'true'));
CALL run_job(:job_id);
SELECT hypertable, lowest_modified_value, greatest_modified_value FROM hypertable_invalidations;
