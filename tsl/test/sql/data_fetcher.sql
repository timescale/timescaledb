-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

\set DATA_NODE_1 :TEST_DBNAME _1
\set DATA_NODE_2 :TEST_DBNAME _2
\set DATA_NODE_3 :TEST_DBNAME _3

\set TEST_BASE_NAME data_fetcher
SELECT format('include/%s_run.sql', :'TEST_BASE_NAME') as "TEST_QUERY_NAME",
       format('%s/results/%s_results_cursor.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_CURSOR",
       format('%s/results/%s_results_copy.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_COPY",
       format('%s/results/%s_results_prepared.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_PREPARED"
\gset

SET ROLE :ROLE_CLUSTER_SUPERUSER;
SELECT node_name, database, node_created, database_created, extension_created
FROM (
  SELECT (add_data_node(name, host => 'localhost', DATABASE => name)).*
  FROM (VALUES (:'DATA_NODE_1'), (:'DATA_NODE_2'), (:'DATA_NODE_3')) v(name)
) a;

CREATE TABLE disttable(time timestamptz NOT NULL, device int, temp float);
SELECT * FROM create_distributed_hypertable('disttable', 'time', 'device', 3);

SELECT setseed(1);
INSERT INTO disttable
SELECT t, (abs(timestamp_hash(t::timestamp)) % 10) + 1, random() * 10
FROM generate_series('2019-01-01'::timestamptz, '2019-01-02'::timestamptz, '1 second') as t;

-- This table contains the content for precisely one batch of the copy fetcher. The fetch_size
-- will be set to 100 below and this table contains 99 tuples and the last element on the first
-- copy batch is the file trailer (#5323).
CREATE table one_batch(ts timestamptz NOT NULL, sensor_id int NOT NULL, value float NOT NULL);
SELECT create_distributed_hypertable('one_batch', 'ts');
INSERT INTO one_batch SELECT '2023-01-01'::timestamptz AS time, sensor_id, random() AS value FROM generate_series(1, 99, 1) AS g1(sensor_id) ORDER BY time;

-- Same but for the DEFAULT_FDW_FETCH_SIZE (10000)
CREATE table one_batch_default(ts timestamptz NOT NULL, sensor_id int NOT NULL, value float NOT NULL);
SELECT create_distributed_hypertable('one_batch_default', 'ts');
INSERT INTO one_batch_default SELECT '2023-01-01'::timestamptz AS time, sensor_id, random() AS value FROM generate_series(1, 9999, 1) AS g1(sensor_id) ORDER BY time;

SET client_min_messages TO error;

-- Set a smaller fetch size to ensure that the result is split into
-- mutliple batches.
ALTER FOREIGN DATA WRAPPER timescaledb_fdw OPTIONS (ADD fetch_size '100');

-- run the queries using COPY fetcher
SET timescaledb.remote_data_fetcher = 'copy';
\set ON_ERROR_STOP 0
\o :TEST_RESULTS_COPY
\ir :TEST_QUERY_NAME
\o
\set ON_ERROR_STOP 1

-- run queries using cursor fetcher
SET timescaledb.remote_data_fetcher = 'cursor';
\o :TEST_RESULTS_CURSOR
\ir :TEST_QUERY_NAME
\o
-- compare results
SELECT format('\! diff %s %s', :'TEST_RESULTS_CURSOR', :'TEST_RESULTS_COPY') as "DIFF_CMD"
\gset
:DIFF_CMD

-- run queries using prepares statement fetcher
SET timescaledb.remote_data_fetcher = 'prepared';
\o :TEST_RESULTS_PREPARED
\ir :TEST_QUERY_NAME
\o
-- compare results
SELECT format('\! diff %s %s', :'TEST_RESULTS_CURSOR', :'TEST_RESULTS_PREPARED') as "DIFF_CMD"
\gset
:DIFF_CMD

-- Test custom FDW settings. Instead of the tests above, we are not interersted
-- in comparing the results of the fetchers. In the following tests we are
-- interested in the actual outputs (e.g., costs).
ANALYZE one_batch;

SET timescaledb.remote_data_fetcher = 'copy';
\ir include/data_fetcher_fdw_settings.sql

SET timescaledb.remote_data_fetcher = 'cursor';
\ir include/data_fetcher_fdw_settings.sql


RESET ROLE;
DROP DATABASE :DATA_NODE_1;
DROP DATABASE :DATA_NODE_2;
DROP DATABASE :DATA_NODE_3;

