-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Configure tests to use mock time for reproducible results
SET timescaledb.current_timestamp_mock = '2015-01-22 00:00 UTC';
SHOW timescaledb.current_timestamp_mock; 
-- initialize the bgw mock state to prevent the materialization workers from running and ignoring our mock timestamp
\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE OR REPLACE FUNCTION ts_bgw_params_create() RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

\set WAIT_ON_JOB 0
\set IMMEDIATELY_SET_UNTIL 1
\set WAIT_FOR_OTHER_TO_ADVANCE 2

-- stop the background workers from locking up the tables,
-- and remove any default jobs, e.g., telemetry so bgw_job isn't polluted
SELECT _timescaledb_internal.stop_background_workers();
DELETE FROM _timescaledb_config.bgw_job WHERE TRUE;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
SELECT * FROM _timescaledb_config.bgw_job;
-- Configuration for mock timestamp use completed

\set TEST_COLUMN 'value'
\set TEST_TABLE 'test_table'
\ir include/rand_generator.sql

--- Returns 1 if percent error (as a decimal) between the given APPROX_FUNC and EXACT_FUNC called with TEST_PARAM is below ACCURACY_REQUIREMENT, 0 otherwise. Values selected from column TEST_COLUMN in table TEST_TABLE
\set CALCULATE_PERCENT_ERROR 'SELECT SUM ( CASE WHEN ABS((SELECT :APPROX_FUNC(tdigest_create(:TEST_COLUMN), :TEST_PARAM) FROM :TEST_TABLE) - (SELECT :EXACT_FUNC(:TEST_PARAM) within group (order by :TEST_COLUMN) FROM :TEST_TABLE))/(SELECT :EXACT_FUNC(:TEST_PARAM) within group (order by :TEST_COLUMN) FROM :TEST_TABLE) < :ACCURACY_REQUIREMENT THEN 1 ELSE 0 END);'

--- Set up table to pull data from
CREATE TABLE test_table(time TIMESTAMPTZ, value FLOAT8);
SELECT create_hypertable('test_table', 'time');

-- Insert 5000 values in [0, 100) 
INSERT INTO test_table SELECT generate_series('2000-01-01'::TIMESTAMPTZ, '2013-09-08'::TIMESTAMPTZ, '1d'::interval), (gen_rand_minstd() / 2147483647.0 * 100);

--- Test t-digest creation and accurate counting
--- Use a view to avoid echoing entire serialized t-digest
CREATE OR REPLACE VIEW tdigest_temp AS SELECT tdigest_create(value) FROM test_table;
SELECT count(*) FROM tdigest_temp;
SELECT (SELECT count(*) FROM test_table) = (SELECT tdigest_count((SELECT * FROM tdigest_temp))) AS count_works;

CREATE OR REPLACE VIEW tdigest_temp AS SELECT tdigest_create(value, 400) FROM test_table;
SELECT count(*) FROM tdigest_temp;
SELECT (SELECT count(*) FROM test_table) = (SELECT tdigest_count((SELECT * FROM tdigest_temp))) AS count_works;

--- Ensure creating t-digest with no initial data works
SELECT tdigest_print(tdigest_create(NULL)) FROM test_table;

-- Make sure we can't pass empty parameter list
\set ON_ERROR_STOP 0
SELECT tdigest_create() FROM test_table;
\set ON_ERROR_STOP 1

-- Make sure we can't pass out-of-bounds compression values
\set ON_ERROR_STOP 0
SELECT tdigest_create(value, -100) FROM test_table;
SELECT tdigest_create(value, 10000) FROM test_table;
\set ON_ERROR_STOP 1

--- Test t-digest I/O

-- Test t-digest in/out
SELECT (SELECT percentile_approx(_timescaledb_internal.tdigest_in(_timescaledb_internal.tdigest_out(tdigest_create(value))), .5) FROM test_table) 
= (SELECT percentile_approx(tdigest_create(value), .5) FROM test_table) AS inout_works;

-- Test t-digest send/recv
\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE FUNCTION tdigest_test_sendrecv(tdigest)
RETURNS boolean
AS :MODULE_PATHNAME, 'ts_tdigest_test_sendrecv'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

SELECT (SELECT tdigest_test_sendrecv((SELECT tdigest_create(value) FROM test_table))) = true AS sendrecv_works;

--- Test Percentile 
--- tdigest is not deterministically generated due to parallelization changing merge orders, so we only check accuracy

-- Ensure error thrown with NaN parameter
\set ON_ERROR_STOP 0
SELECT percentile_approx(tdigest_create(value), 'NaN') FROM test_table;
\set ON_ERROR_STOP 1

-- Test Percentile Accuracy -- currently, tests for percent error between exact and estimated of no more than 1%
\set APPROX_FUNC 'percentile_approx'
\set EXACT_FUNC 'percentile_cont'
\set ACCURACY_REQUIREMENT '.01'

\set TEST_PARAM '0'
:CALCULATE_PERCENT_ERROR

\set TEST_PARAM '0.1'
:CALCULATE_PERCENT_ERROR

\set TEST_PARAM '0.2'
:CALCULATE_PERCENT_ERROR

\set TEST_PARAM '0.3'
:CALCULATE_PERCENT_ERROR

\set TEST_PARAM '0.4'
:CALCULATE_PERCENT_ERROR

\set TEST_PARAM '0.5'
:CALCULATE_PERCENT_ERROR

\set TEST_PARAM '0.6'
:CALCULATE_PERCENT_ERROR

\set TEST_PARAM '0.7'
:CALCULATE_PERCENT_ERROR

\set TEST_PARAM '0.8'
:CALCULATE_PERCENT_ERROR

\set TEST_PARAM '0.9'
:CALCULATE_PERCENT_ERROR

\set TEST_PARAM '1'
:CALCULATE_PERCENT_ERROR

-- Make sure we can't call with invalid percentile values
\set ON_ERROR_STOP 0
SELECT percentile_approx(tdigest_create(value), 2) FROM test_table;
SELECT percentile_approx(tdigest_create(value), -1) FROM test_table;
\set ON_ERROR_STOP 1

--- Test CDF 

-- Ensure error thrown with NaN parameter
\set ON_ERROR_STOP 0
SELECT cdf_approx(tdigest_create(value), 'NaN') FROM test_table;
\set ON_ERROR_STOP 1

-- Test CDF Accuracy 
\set APPROX_FUNC 'cdf_approx'
\set EXACT_FUNC 'cume_dist'
\set ACCURACY_REQUIREMENT '.01'

\set TEST_PARAM '63'
:CALCULATE_PERCENT_ERROR

\set TEST_PARAM '42'
:CALCULATE_PERCENT_ERROR

\set TEST_PARAM '100'
:CALCULATE_PERCENT_ERROR

--- Test continuous agg creation
SET timescaledb.current_timestamp_mock = '2015-01-22 00:00 UTC';
CREATE OR REPLACE VIEW cagg_view WITH (timescaledb.continuous, timescaledb.refresh_lag = '-1 hour', timescaledb.max_interval_per_job = '5500 days') AS
SELECT time_bucket('1w', time) AS week, tdigest_create(value) AS tdigest
FROM test_table
GROUP BY week;

REFRESH MATERIALIZED VIEW cagg_view;

--- Test querying from cont agg
SELECT count(percentile_approx(tdigest, .5)) FROM cagg_view;
SELECT count(cdf_approx(tdigest, 42)) FROM cagg_view;
SELECT percentile_approx(tdigest, .5) FROM cagg_view ORDER BY week LIMIT 5;
SELECT cdf_approx(tdigest, 42) FROM cagg_view ORDER BY week LIMIT 5;

--- Test updating cont agg
--- TEST INSERT
-- Establish baseline values (all values <= 100)
SELECT avg(percentile_approx(tdigest, .5)) FROM cagg_view;
SELECT avg(cdf_approx(tdigest, 100)) = 1 AS "all values <= 100" FROM cagg_view;

-- Add year's worth of data with values in [101, 201]
INSERT INTO test_table SELECT generate_series('2014-01-01'::TIMESTAMPTZ, '2014-12-31'::TIMESTAMPTZ, '1d'::interval), (gen_rand_minstd() / 2147483647.0 * 100 + 101);
REFRESH MATERIALIZED VIEW cagg_view;

-- Avg median should have increased, and not all values should be <= 100
SELECT avg(percentile_approx(tdigest, .5)) FROM cagg_view;
SELECT avg(cdf_approx(tdigest, 100)) = 1 AS "all values <= 100" FROM cagg_view;

--- TEST UPDATE 
-- Establish baseline values (all values <= 300)
SELECT avg(percentile_approx(tdigest, .5)) FROM cagg_view;
SELECT avg(cdf_approx(tdigest, 300)) = 1 AS "all values <= 300" FROM cagg_view;

-- Increase all values in [50, 100] to be in [350, 400]
UPDATE test_table SET value = value + 300 WHERE value >= 50 and value <= 100;
REFRESH MATERIALIZED VIEW cagg_view;

-- Avg median should have increased, and not all values should be <= 300
SELECT avg(percentile_approx(tdigest, .5)) FROM cagg_view;
SELECT avg(cdf_approx(tdigest, 300)) = 1 AS "all values <= 300" FROM cagg_view;

--- TEST DELETE
-- Restore updated values to original values
UPDATE test_table SET value = value - 300 WHERE value >= 300;
-- Delete newly inserted year's data
DELETE FROM test_table WHERE value >= 101;

REFRESH MATERIALIZED VIEW cagg_view;

-- Values should now be the same as before the insert/update/delete sequence
SELECT percentile_approx(tdigest, .5) FROM cagg_view ORDER BY week LIMIT 5;
SELECT cdf_approx(tdigest, 42) FROM cagg_view ORDER BY week LIMIT 5;

--- Test print
SET timescaledb.current_timestamp_mock = '2015-01-22 00:00 UTC';
CREATE OR REPLACE VIEW cagg_year_view WITH (timescaledb.continuous, timescaledb.refresh_lag = '-1 hour', timescaledb.max_interval_per_job = '5500 days') AS
SELECT time_bucket('365 days', time) AS year, tdigest_create(value) AS tdigest
FROM test_table
GROUP BY year;

REFRESH MATERIALIZED VIEW cagg_year_view;
SELECT tdigest_print(tdigest) FROM cagg_year_view ORDER BY year;

--- Test operators --- 
--- Set up table to pull data from
CREATE TABLE small_table(time TIMESTAMPTZ, value FLOAT8);
SELECT create_hypertable('small_table', 'time');
CREATE TABLE big_table(time TIMESTAMPTZ, value FLOAT8);
SELECT create_hypertable('big_table', 'time');

-- Insert 1 year's values in [0, 100) 
INSERT INTO small_table SELECT generate_series('2000-01-01'::TIMESTAMPTZ, '2001-01-01'::TIMESTAMPTZ, '1d'::interval), (gen_rand_minstd() / 2147483647.0 * 100);
-- Insert 2 year's values in [0, 100) 
INSERT INTO big_table SELECT generate_series('2000-01-01'::TIMESTAMPTZ, '2002-01-01'::TIMESTAMPTZ, '1d'::interval), (gen_rand_minstd() / 2147483647.0 * 100);

SELECT (SELECT tdigest_create(value) FROM small_table) < (SELECT tdigest_create(value) FROM big_table) AS "lt works";
SELECT (SELECT tdigest_create(value) FROM big_table) > (SELECT tdigest_create(value) FROM small_table) AS "gt works";
SELECT (SELECT tdigest_create(value) FROM small_table) = (SELECT tdigest_create(value) FROM small_table) AS "eq works";
SELECT (SELECT tdigest_create(value) FROM small_table) >= (SELECT tdigest_create(value) FROM small_table) AS "ge works";
SELECT (SELECT tdigest_create(value) FROM small_table) <= (SELECT tdigest_create(value) FROM small_table) AS "le works";
SELECT (SELECT tdigest_create(value) FROM big_table) >= (SELECT tdigest_create(value) FROM small_table) AS "ge works 2";
SELECT (SELECT tdigest_create(value) FROM small_table) <= (SELECT tdigest_create(value) FROM big_table) AS "le works 2";
